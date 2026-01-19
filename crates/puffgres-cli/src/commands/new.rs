use std::fs;
use std::path::Path;

use anyhow::Result;
use colored::Colorize;
use dialoguer::{Confirm, Input};

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

    // Ask if they want a custom transform
    let use_custom_transform = Confirm::new()
        .with_prompt("Will you do a custom transformation before going to turbopuffer? (e.g., embeddings, computed fields)")
        .default(true)
        .interact()?;

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

    // Create the migration file based on transform choice
    let migration = if use_custom_transform {
        format!(
            r#"# Migration for {name} table
version = {version}
mapping_name = "{name}_public"
namespace = "{name}"

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

[transform]
type = "js"
path = "./puffgres/transforms/{name}.ts"
"#,
            name = safe_name,
            version = next_version
        )
    } else {
        format!(
            r#"# Migration for {name} table
version = {version}
mapping_name = "{name}_public"
namespace = "{name}"

# Columns to sync to turbopuffer
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
"#,
            name = safe_name,
            version = next_version
        )
    };

    let migration_path = format!("puffgres/migrations/{:04}_{}.toml", next_version, safe_name);
    fs::write(&migration_path, &migration)?;
    println!("{}", format!("Created {}", migration_path).green());

    // Only create transform file if using custom transform
    if use_custom_transform {
        let transform = format!(
            r#"import type {{ TransformInput, Action, TransformContext, DocumentId }} from 'puffgres';
import {{ getEncoding, type Tiktoken }} from 'js-tiktoken';
import Together from 'together-ai';

// Cache the tokenizer instance
let tokenizer: Tiktoken | null = null;
let togetherClient: Together | null = null;

function getTokenizer(): Tiktoken {{
  if (!tokenizer) {{
    tokenizer = getEncoding('cl100k_base');
  }}
  return tokenizer;
}}

function getTogetherClient(apiKey: string): Together {{
  if (!togetherClient) {{
    togetherClient = new Together({{ apiKey }});
  }}
  return togetherClient;
}}

function truncateToTokens(text: string, maxTokens: number): string {{
  const tok = getTokenizer();
  const tokenIds = tok.encode(text);
  if (tokenIds.length <= maxTokens) {{
    return text;
  }}
  const truncatedIds = tokenIds.slice(0, maxTokens);
  return tok.decode(truncatedIds);
}}

async function embedBatchWithTogether(texts: string[], apiKey: string): Promise<number[][]> {{
  if (texts.length === 0) return [];

  const client = getTogetherClient(apiKey);
  const response = await client.embeddings.create({{
    model: 'BAAI/bge-base-en-v1.5',
    input: texts,
  }});

  // Sort by index to ensure correct ordering
  return response.data
    .sort((a, b) => a.index - b.index)
    .map(d => d.embedding);
}}

export default async function transform(
  rows: TransformInput[],
  ctx: TransformContext
): Promise<Action[]> {{
  // Separate deletes from upserts
  const deleteActions: {{ index: number; action: Action }}[] = [];
  const upsertRows: {{ index: number; id: DocumentId; row: Record<string, unknown>; text: string }}[] = [];

  for (let i = 0; i < rows.length; i++) {{
    const {{ event, id }} = rows[i];

    if (event.op === 'delete') {{
      deleteActions.push({{ index: i, action: {{ type: 'delete', id }} }});
      continue;
    }}

    const row = event.new!;
    const combinedText = [row.content].filter(Boolean).join(' ');
    const truncatedText = truncateToTokens(combinedText, 500);

    upsertRows.push({{ index: i, id, row, text: truncatedText }});
  }}

  // Batch embed all texts at once
  const textsToEmbed = upsertRows.map(r => r.text);
  console.error(`Embedding ${{textsToEmbed.length}} texts in single batch`);

  const embeddings = await embedBatchWithTogether(textsToEmbed, ctx.env.TOGETHER_API_KEY);

  // Build upsert actions with embeddings
  const upsertActions = upsertRows.map((r, i) => ({{
    index: r.index,
    action: {{
      type: 'upsert' as const,
      id: r.id,
      doc: {{
        id: r.row.id,
        // Add your fields here
        vector: embeddings[i],
      }},
      distance_metric: 'cosine_distance' as const,
    }},
  }}));

  // Merge and sort by original index to preserve order
  const allActions = [...deleteActions, ...upsertActions]
    .sort((a, b) => a.index - b.index)
    .map(a => a.action);

  return allActions;
}}
"#
        );

        let transform_path = format!("puffgres/transforms/{}.ts", safe_name);
        if !Path::new(&transform_path).exists() {
            fs::write(&transform_path, &transform)?;
            println!("{}", format!("Created {}", transform_path).green());
        }

        println!("\nNext steps:");
        println!("  1. Edit {} to match your table schema", migration_path);
        println!(
            "  2. Edit puffgres/transforms/{}.ts with your transform logic",
            safe_name
        );
        println!("  3. Run: puffgres migrate");
        println!("  4. Run: puffgres backfill {}_public\n", safe_name);
    } else {
        println!("\nNext steps:");
        println!("  1. Edit {} to match your table schema", migration_path);
        println!("  2. Run: puffgres migrate");
        println!("  3. Run: puffgres backfill {}_public\n", safe_name);
    }

    Ok(())
}
