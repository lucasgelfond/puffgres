use std::fs;
use std::path::Path;

use anyhow::Result;
use colored::Colorize;
use dialoguer::Input;

pub async fn cmd_new(name: Option<String>) -> Result<()> {
    // Check that puffgres is initialized
    if !Path::new("puffgres/migrations").exists() {
        anyhow::bail!("puffgres is not initialized in this directory. Run 'puffgres init' first.");
    }

    // Get the migration name
    let migration_name = if let Some(name) = name {
        name
    } else {
        Input::new()
            .with_prompt("What would you like to name this migration?")
            .interact_text()?
    };

    // Sanitize the migration name for filename
    let safe_name = migration_name
        .to_lowercase()
        .replace(char::is_whitespace, "_")
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == '_')
        .collect::<String>();

    // Find the next version number
    let mut max_version = 0;
    for entry in fs::read_dir("puffgres/migrations")? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().map_or(false, |ext| ext == "toml") {
            if let Some(filename) = path.file_stem() {
                if let Some(num_str) = filename.to_str().and_then(|s| s.split('_').next()) {
                    if let Ok(num) = num_str.parse::<u32>() {
                        max_version = max_version.max(num);
                    }
                }
            }
        }
    }
    let next_version = max_version + 1;

    // Create the migration file
    let migration = format!(
        r#"# Migration for {name} table
version = {version}
mapping_name = "{name}_public"
namespace = "{name}"
columns = ["id", "name", "created_at"]

[source]
schema = "public"
table = "{name}"

[id]
column = "id"
type = "uint"

# Optional: filter which rows to sync
# [membership]
# mode = "dsl"
# predicate = "status = 'active'"

[versioning]
mode = "source_lsn"

# Optional: custom transform
# [transform]
# path = "./transforms/{name}.ts"
"#,
        name = safe_name,
        version = next_version
    );

    let migration_path = format!("puffgres/migrations/{:04}_{}.toml", next_version, safe_name);
    fs::write(&migration_path, &migration)?;
    println!("{}", format!("Created {}", migration_path).green());

    // Create the transform file
    let transform = format!(
        r#"// Transform for {name} table
// Uncomment [transform] section in migration to use this

import type {{ RowEvent, Action, TransformContext }} from 'puffgres';

export default async function transform(
  event: RowEvent,
  id: string,
  ctx: TransformContext
): Promise<Action> {{
  if (event.op === 'delete') {{
    return {{ type: 'delete', id }};
  }}

  const row = event.new!;

  return {{
    type: 'upsert',
    id,
    doc: {{
      name: row.name,
      created_at: row.created_at,
    }},
  }};
}}
"#,
        name = safe_name
    );

    let transform_path = format!("puffgres/transforms/{}.ts", safe_name);
    if !Path::new(&transform_path).exists() {
        fs::write(&transform_path, &transform)?;
        println!("{}", format!("Created {}", transform_path).green());
    }

    println!("\nNext steps:");
    println!("  1. Edit {} to match your table schema", migration_path);
    println!("  2. Run: puffgres migrate");
    println!("  3. Run: puffgres backfill {}_public\n", safe_name);

    Ok(())
}
