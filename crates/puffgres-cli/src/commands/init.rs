use std::fs;
use std::io::Write;
use std::path::Path;

use anyhow::Result;
use colored::Colorize;
use dialoguer::Input;
use tracing::info;

use crate::env::has_all_env_vars;

pub async fn cmd_init() -> Result<()> {
    println!("Initializing puffgres in current directory...\n");

    // Ask for the first migration name
    let migration_name: String = Input::new()
        .with_prompt("What would you like to name your first migration?")
        .with_initial_text("users")
        .interact_text()?;

    // Sanitize the migration name for filename
    let safe_name = migration_name
        .to_lowercase()
        .replace(char::is_whitespace, "_")
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == '_')
        .collect::<String>();

    // Create directories
    fs::create_dir_all("puffgres/migrations")?;
    fs::create_dir_all("puffgres/transforms")?;
    info!("Created puffgres/migrations/ and puffgres/transforms/");

    // Only create .env files if not all variables are already available
    if has_all_env_vars() {
        println!(
            "{}",
            "Found DATABASE_URL, TURBOPUFFER_API_KEY, and PUFFGRES_BASE_NAMESPACE in environment (from parent .env or shell)"
                .green()
        );
    } else {
        let env_content = r#"# Puffgres environment variables
# This file contains secrets and should not be committed to version control
#
# These variables can also be defined in any parent directory's .env file.
# Puffgres searches from the current directory up to the filesystem root.

# Postgres connection string (Supabase, Neon, etc.)
DATABASE_URL=postgresql://postgres:password@localhost:5432/postgres

# Turbopuffer API key
TURBOPUFFER_API_KEY=your-api-key-here

# Optional: Together AI for embeddings
# TOGETHER_API_KEY=your-together-key-here

# Namespace prefix for environment separation (e.g., PRODUCTION, DEVELOPMENT)
# PUFFGRES_BASE_NAMESPACE=
"#;

        let env_path = Path::new(".env");
        if !env_path.exists() {
            fs::write(env_path, env_content)?;
            println!("Created .env (fill in your credentials)");
        } else {
            println!(".env already exists, skipping");
        }

        let env_example_path = Path::new(".env.example");
        if !env_example_path.exists() {
            fs::write(env_example_path, env_content)?;
            println!("Created .env.example");
        }
    }

    // Create puffgres.toml config
    let config = r#"# Puffgres configuration
# Secrets are loaded from .env file
# State is stored in __puffgres_* tables in your Postgres database

[postgres]
connection_string = "${DATABASE_URL}"

[turbopuffer]
api_key = "${TURBOPUFFER_API_KEY}"

# Optional: Namespace prefix for environment separation
# base_namespace = "${PUFFGRES_BASE_NAMESPACE}"

# Optional: Configure embedding providers for transforms
# [providers.embeddings]
# type = "together"
# model = "BAAI/bge-base-en-v1.5"
# api_key = "${TOGETHER_API_KEY}"
"#;

    let config_path = Path::new("puffgres.toml");
    if !config_path.exists() {
        fs::write(config_path, config)?;
        println!("Created puffgres.toml");
    } else {
        println!("puffgres.toml already exists, skipping");
    }

    // Create example migration with the user's chosen name
    let migration = format!(
        r#"# Migration for {name} table
version = 1
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
        name = safe_name
    );

    let migration_path = format!("puffgres/migrations/0001_{}.toml", safe_name);
    if !Path::new(&migration_path).exists() {
        fs::write(&migration_path, migration)?;
        println!("Created {}", migration_path);
    }

    // Create example transform
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
        fs::write(&transform_path, transform)?;
        println!("Created {}", transform_path);
    }

    // Create .gitignore in puffgres directory
    let gitignore = "# Local transform builds\nnode_modules/\n";
    let gitignore_path = Path::new("puffgres/.gitignore");
    if !gitignore_path.exists() {
        fs::write(gitignore_path, gitignore)?;
    }

    // Also add .env to root .gitignore if it exists
    let root_gitignore = Path::new(".gitignore");
    if root_gitignore.exists() {
        let content = fs::read_to_string(root_gitignore)?;
        if !content.contains(".env") {
            let mut file = fs::OpenOptions::new()
                .append(true)
                .open(root_gitignore)?;
            writeln!(file, "\n# Puffgres secrets\n.env")?;
            println!("Added .env to .gitignore");
        }
    } else {
        fs::write(root_gitignore, "# Puffgres secrets\n.env\n")?;
        println!("Created .gitignore with .env");
    }

    println!("\n{}", "Puffgres initialized!".green().bold());
    println!("\nNext steps:");
    println!("  1. Fill in your credentials in .env");
    println!(
        "  2. Edit puffgres/migrations/0001_{}.toml for your table",
        safe_name
    );
    println!("  3. Run: puffgres setup");
    println!("  4. Run: puffgres migrate");
    println!("  5. Run: puffgres backfill {}_public", safe_name);
    println!("  6. Run: puffgres run\n");

    Ok(())
}
