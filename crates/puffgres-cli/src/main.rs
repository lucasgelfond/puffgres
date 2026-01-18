use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use colored::Colorize;
use dialoguer::{Confirm, Input};
use tracing::info;

mod backfill;
mod config;
mod dlq;
mod runner;

use config::ProjectConfig;
use puffgres_pg::{MigrationTracker, PostgresStateStore};

#[derive(Parser)]
#[command(name = "puffgres")]
#[command(about = "Mirror Postgres data to turbopuffer")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Path to puffgres.toml config file
    #[arg(short, long, default_value = "puffgres.toml")]
    config: PathBuf,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize puffgres in the current directory
    Init,

    /// Create a new migration
    New {
        /// Optional name for the migration (will prompt if not provided)
        name: Option<String>,
    },

    /// Apply pending migrations
    Migrate {
        /// Show what would be applied without actually applying
        #[arg(long)]
        dry_run: bool,
    },

    /// Start the CDC replication loop
    Run {
        /// Replication slot name
        #[arg(long, default_value = "puffgres")]
        slot: String,

        /// Create the replication slot if it doesn't exist
        #[arg(long, default_value = "true")]
        create_slot: bool,

        /// Poll interval in milliseconds
        #[arg(long, default_value = "1000")]
        poll_interval_ms: u64,

        /// Skip auto-applying pending migrations
        #[arg(long)]
        skip_migrate: bool,
    },

    /// Show current sync status
    Status,

    /// Backfill existing table data to turbopuffer
    Backfill {
        /// Mapping name to backfill
        mapping: String,

        /// Batch size for processing
        #[arg(long, default_value = "1000")]
        batch_size: u32,

        /// Resume from previous checkpoint
        #[arg(long)]
        resume: bool,
    },

    /// Manage the dead letter queue
    Dlq {
        #[command(subcommand)]
        command: DlqCommands,
    },

    /// Reset local config from database state
    Reset,

    /// Remove all puffgres artifacts (dangerous)
    DangerouslyDeleteConfig,

    /// Delete all referenced turbopuffer namespaces (dangerous)
    DangerouslyResetTurbopuffer,
}

#[derive(Subcommand)]
enum DlqCommands {
    /// List DLQ entries
    List {
        /// Filter by mapping name
        #[arg(long)]
        mapping: Option<String>,

        /// Maximum number of entries to show
        #[arg(long, default_value = "50")]
        limit: i64,
    },

    /// Show details of a DLQ entry
    Show {
        /// Entry ID
        id: i32,
    },

    /// Retry DLQ entries
    Retry {
        /// Retry a specific entry by ID
        #[arg(long)]
        id: Option<i32>,

        /// Retry all entries for a mapping
        #[arg(long)]
        mapping: Option<String>,
    },

    /// Clear DLQ entries
    Clear {
        /// Clear entries for a specific mapping
        #[arg(long)]
        mapping: Option<String>,

        /// Clear all entries
        #[arg(long)]
        all: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    // Load .env file if present
    dotenvy::dotenv().ok();

    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("puffgres=info".parse().unwrap()),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Init => cmd_init().await,
        Commands::New { name } => cmd_new(name).await,
        Commands::Migrate { dry_run } => {
            let config = load_config(&cli.config)?;
            cmd_migrate(config, dry_run).await
        }
        Commands::Run {
            slot,
            create_slot,
            poll_interval_ms,
            skip_migrate,
        } => {
            let config = load_config(&cli.config)?;
            cmd_run(config, &slot, create_slot, poll_interval_ms, skip_migrate).await
        }
        Commands::Status => {
            let config = load_config(&cli.config)?;
            cmd_status(config).await
        }
        Commands::Backfill {
            mapping,
            batch_size,
            resume,
        } => {
            let config = load_config(&cli.config)?;
            cmd_backfill(config, &mapping, batch_size, resume).await
        }
        Commands::Dlq { command } => {
            let config = load_config(&cli.config)?;
            cmd_dlq(config, command).await
        }
        Commands::Reset => {
            let config = load_config(&cli.config)?;
            cmd_reset(config).await
        }
        Commands::DangerouslyDeleteConfig => {
            let config = load_config(&cli.config)?;
            cmd_dangerously_delete_config(config).await
        }
        Commands::DangerouslyResetTurbopuffer => {
            let config = load_config(&cli.config)?;
            cmd_dangerously_reset_turbopuffer(config).await
        }
    }
}

fn load_config(path: &PathBuf) -> Result<ProjectConfig> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("Failed to read config file: {}", path.display()))?;

    let config: ProjectConfig =
        toml::from_str(&content).with_context(|| "Failed to parse puffgres.toml")?;

    Ok(config)
}

async fn cmd_init() -> Result<()> {
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

    // Create .env file (not .env.example)
    let env_content = r#"# Puffgres environment variables
# This file contains secrets and should not be committed to version control

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

    println!();

    // Now prompt about creating the database tables
    println!("Puffgres will create the following tables in your Postgres database:");
    println!("  • __puffgres_migrations  - tracks applied migrations");
    println!("  • __puffgres_checkpoints - stores CDC replication state");
    println!("  • __puffgres_dlq         - dead letter queue for failed events");
    println!("  • __puffgres_backfill    - tracks backfill progress");
    println!("  • __puffgres_transforms  - stores versioned transform code");
    println!();

    let create_tables = Confirm::new()
        .with_prompt("Do you want to create these tables now?")
        .default(true)
        .interact()?;

    if create_tables {
        // Load the config and connect to database
        let config_content = fs::read_to_string("puffgres.toml")?;
        let config: ProjectConfig = toml::from_str(&config_content)
            .context("Failed to parse puffgres.toml - make sure you've filled in your .env file")?;

        // Connect and create tables
        match PostgresStateStore::connect(&config.postgres_connection_string()).await {
            Ok(_store) => {
                println!("{}", "✓ Database tables created successfully!".green());
            }
            Err(e) => {
                println!(
                    "{}",
                    format!("✗ Failed to connect to database: {}", e).red()
                );
                println!("  Make sure your DATABASE_URL in .env is correct.");
                println!("  You can run 'puffgres migrate' later to create the tables.");
            }
        }
    } else {
        println!("Tables will be created automatically when you run 'puffgres migrate'.");
    }

    println!("\n{}", "Puffgres initialized!".green().bold());
    println!("\nNext steps:");
    println!("  1. Fill in your credentials in .env");
    println!("  2. Edit puffgres/migrations/0001_{}.toml for your table", safe_name);
    println!("  3. Run: puffgres migrate");
    println!("  4. Run: puffgres backfill {}_public", safe_name);
    println!("  5. Run: puffgres run\n");

    Ok(())
}

async fn cmd_new(name: Option<String>) -> Result<()> {
    // Check that puffgres is initialized
    if !Path::new("puffgres/migrations").exists() {
        anyhow::bail!(
            "puffgres is not initialized in this directory. Run 'puffgres init' first."
        );
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

async fn cmd_migrate(config: ProjectConfig, dry_run: bool) -> Result<()> {
    info!("Checking migrations");

    // Connect to Postgres state store
    let store = PostgresStateStore::connect(&config.postgres_connection_string())
        .await
        .context("Failed to connect to Postgres")?;

    // Load local migrations
    let local = config.load_local_migrations()?;
    if local.is_empty() {
        println!("No migrations found in puffgres/migrations/");
        return Ok(());
    }

    // First validate transforms are not modified
    if let Err(e) = validate_transforms(&config, &store).await {
        eprintln!("{}", format!("Error: {}", e).red());
        eprintln!(
            "{}",
            "Cannot proceed: applied migrations have been modified locally.".red()
        );
        eprintln!("Run `puffgres reset` to reset your config to match the database state.");
        std::process::exit(1);
    }

    let tracker = MigrationTracker::new(&store);
    let status = tracker.validate(&local).await?;

    // Check for errors
    if !status.mismatched.is_empty() {
        eprintln!("\n{}", "Migration Hash Mismatches (ERROR):".red().bold());
        for m in &status.mismatched {
            eprintln!(
                "  v{} {}: local hash differs from applied",
                m.version, m.mapping_name
            );
            eprintln!("    Applied: {}", m.expected_hash);
            eprintln!("    Local:   {}", m.actual_hash);
        }
        eprintln!(
            "\n{}",
            "Error: Cannot proceed: applied migrations have been modified locally.".red()
        );
        eprintln!("Run `puffgres reset` to reset your config to match the database state.");
        std::process::exit(1);
    }

    // Show status
    if !status.applied.is_empty() {
        println!("\nAlready Applied:");
        for name in &status.applied {
            println!("  ✓ {}", name.green());
        }
    }

    if status.pending.is_empty() {
        println!("\nAll migrations are up to date.");
        return Ok(());
    }

    println!("\nPending Migrations:");
    for name in &status.pending {
        println!("  → {}", name.yellow());
    }

    if dry_run {
        println!("\n(dry run - no changes made)");
        return Ok(());
    }

    // Apply pending migrations
    let applied = tracker.apply(&local, false).await?;

    // Store migration content and transforms for reset functionality and immutability tracking
    for migration in &local {
        // Store the migration content
        store
            .store_migration_content(migration.version, &migration.mapping_name, &migration.content)
            .await?;

        // Check if this migration has a transform with a path
        let migration_config = puffgres_config::MigrationConfig::parse(&migration.content)?;
        if let Some(path) = &migration_config.transform.path {
            let transform_path = Path::new("puffgres").join(path.trim_start_matches("./"));
            if transform_path.exists() {
                let transform_content = fs::read_to_string(&transform_path)?;
                store_transform(
                    &store,
                    &migration.mapping_name,
                    migration.version,
                    &transform_content,
                )
                .await?;
            }
        }
    }

    println!("\n{}", format!("Applied {} migration(s).", applied.len()).green());
    Ok(())
}

async fn cmd_run(
    config: ProjectConfig,
    slot: &str,
    create_slot: bool,
    poll_interval_ms: u64,
    skip_migrate: bool,
) -> Result<()> {
    info!("Starting puffgres CDC replication");

    // Connect to Postgres state store (this auto-creates __puffgres_* tables if they don't exist)
    let store = PostgresStateStore::connect(&config.postgres_connection_string())
        .await
        .context("Failed to connect to Postgres")?;

    // Load and validate migrations
    let local = config.load_local_migrations()?;
    if local.is_empty() {
        anyhow::bail!("No migrations found in puffgres/migrations/");
    }

    // Validate transforms haven't been modified
    if let Err(e) = validate_transforms(&config, &store).await {
        eprintln!("{}", format!("Error: {}", e).red());
        eprintln!(
            "{}",
            "Cannot proceed: applied migrations have been modified locally.".red()
        );
        eprintln!("Run `puffgres reset` to reset your config to match the database state.");
        std::process::exit(1);
    }

    let tracker = MigrationTracker::new(&store);

    // Auto-apply pending migrations unless --skip-migrate is set
    if !skip_migrate {
        let status = tracker.validate(&local).await?;

        // Check for mismatches
        if !status.mismatched.is_empty() {
            eprintln!("\n{}", "Migration Hash Mismatches (ERROR):".red().bold());
            for m in &status.mismatched {
                eprintln!(
                    "  v{} {}: local hash differs from applied",
                    m.version, m.mapping_name
                );
            }
            eprintln!(
                "\n{}",
                "Error: Cannot proceed: applied migrations have been modified locally.".red()
            );
            eprintln!("Run `puffgres reset` to reset your config to match the database state.");
            std::process::exit(1);
        }

        // Apply pending migrations
        if !status.pending.is_empty() {
            println!("Applying {} pending migration(s)...", status.pending.len());
            let applied = tracker.apply(&local, false).await?;

            // Store migration content and transforms
            for migration in &local {
                store
                    .store_migration_content(migration.version, &migration.mapping_name, &migration.content)
                    .await?;

                let migration_config = puffgres_config::MigrationConfig::parse(&migration.content)?;
                if let Some(path) = &migration_config.transform.path {
                    let transform_path = Path::new("puffgres").join(path.trim_start_matches("./"));
                    if transform_path.exists() {
                        let transform_content = fs::read_to_string(&transform_path)?;
                        store_transform(
                            &store,
                            &migration.mapping_name,
                            migration.version,
                            &transform_content,
                        )
                        .await?;
                    }
                }
            }

            println!("{}", format!("Applied {} migration(s).", applied.len()).green());
        }
    } else {
        // Just validate, don't apply
        tracker.validate_or_fail(&local, true).await?;
    }

    // Load migrations as Mappings
    let migrations = config.load_migrations()?;
    info!(count = migrations.len(), "Loaded migrations");

    // Run the CDC loop
    runner::run_cdc_loop(
        &config,
        migrations,
        slot,
        create_slot,
        Duration::from_millis(poll_interval_ms),
    )
    .await
}

async fn cmd_status(config: ProjectConfig) -> Result<()> {
    // Connect to Postgres state store
    let store = PostgresStateStore::connect(&config.postgres_connection_string())
        .await
        .context("Failed to connect to Postgres")?;

    let checkpoints = store.get_all_checkpoints().await?;

    if checkpoints.is_empty() {
        println!("No sync state found. Run 'puffgres run' to start syncing.");
        return Ok(());
    }

    println!("\nSync Status:");
    println!("{:<30} {:>15} {:>15}", "Mapping", "LSN", "Events");
    println!("{:-<60}", "");

    for (name, checkpoint) in checkpoints {
        println!(
            "{:<30} {:>15} {:>15}",
            name, checkpoint.lsn, checkpoint.events_processed
        );
    }

    println!();
    Ok(())
}

async fn cmd_backfill(
    config: ProjectConfig,
    mapping_name: &str,
    batch_size: u32,
    resume: bool,
) -> Result<()> {
    // Connect to Postgres state store
    let store = PostgresStateStore::connect(&config.postgres_connection_string())
        .await
        .context("Failed to connect to Postgres")?;

    // Validate transforms haven't been modified
    if let Err(e) = validate_transforms(&config, &store).await {
        eprintln!("{}", format!("Error: {}", e).red());
        eprintln!(
            "{}",
            "Cannot proceed: applied migrations have been modified locally.".red()
        );
        eprintln!("Run `puffgres reset` to reset your config to match the database state.");
        std::process::exit(1);
    }

    // Load migrations to find the mapping
    let mappings = config.load_migrations()?;

    let mapping = mappings
        .iter()
        .find(|m| m.name == mapping_name)
        .context(format!("Mapping '{}' not found", mapping_name))?;

    backfill::run_backfill(&config, mapping, batch_size, resume).await
}

async fn cmd_dlq(config: ProjectConfig, command: DlqCommands) -> Result<()> {
    // Connect to Postgres state store
    let store = PostgresStateStore::connect(&config.postgres_connection_string())
        .await
        .context("Failed to connect to Postgres")?;

    match command {
        DlqCommands::List { mapping, limit } => {
            dlq::cmd_dlq_list(&store, mapping.as_deref(), limit).await
        }
        DlqCommands::Show { id } => dlq::cmd_dlq_show(&store, id).await,
        DlqCommands::Retry { id, mapping } => {
            dlq::cmd_dlq_retry(&store, id, mapping.as_deref()).await
        }
        DlqCommands::Clear { mapping, all } => {
            dlq::cmd_dlq_clear(&store, mapping.as_deref(), all).await
        }
    }
}

async fn cmd_reset(config: ProjectConfig) -> Result<()> {
    println!("Resetting local config from database state...\n");

    // Connect to Postgres state store
    let store = PostgresStateStore::connect(&config.postgres_connection_string())
        .await
        .context("Failed to connect to Postgres")?;

    // Get all applied migrations from database
    let applied = store.get_applied_migrations().await?;

    if applied.is_empty() {
        println!("No migrations found in database. Nothing to reset.");
        return Ok(());
    }

    // Get all migration content and transforms from database
    let migration_content = store.get_all_migration_content().await?;
    let transforms = store.get_all_transforms().await?;

    // Create migrations directory if it doesn't exist
    fs::create_dir_all("puffgres/migrations")?;
    fs::create_dir_all("puffgres/transforms")?;

    // Clear existing local migrations
    if Path::new("puffgres/migrations").exists() {
        for entry in fs::read_dir("puffgres/migrations")? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().map_or(false, |ext| ext == "toml") {
                fs::remove_file(&path)?;
                println!("Removed {}", path.display());
            }
        }
    }

    // Clear existing local transforms
    if Path::new("puffgres/transforms").exists() {
        for entry in fs::read_dir("puffgres/transforms")? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().map_or(false, |ext| ext == "ts" || ext == "js") {
                fs::remove_file(&path)?;
                println!("Removed {}", path.display());
            }
        }
    }

    // Write migrations from database
    if migration_content.is_empty() {
        println!("{}", "Note: Migration content not found in database.".yellow());
        println!("Applied migrations (content not stored - run 'puffgres migrate' to store):");
        for m in &applied {
            println!(
                "  v{} {} (hash: {}...)",
                m.version,
                m.mapping_name,
                &m.content_hash[..16]
            );
        }
    } else {
        println!("Restoring migrations from database:");
        for (version, mapping_name, content) in migration_content {
            let filename = format!("{:04}_{}.toml", version, mapping_name.replace("_public", ""));
            let path = format!("puffgres/migrations/{}", filename);
            fs::write(&path, &content)?;
            println!("  Restored {}", path);
        }
    }

    // Write transforms from database
    if !transforms.is_empty() {
        println!("\nRestoring transforms from database:");
        for transform in transforms {
            let path = format!("puffgres/transforms/{}.ts", transform.mapping_name);
            fs::write(&path, &transform.content)?;
            println!("  Restored {}", path);
        }
    }

    println!("\n{}", "Reset complete!".green());
    Ok(())
}

async fn cmd_dangerously_delete_config(config: ProjectConfig) -> Result<()> {
    println!("{}", "WARNING: Dangerous Operation".red().bold());
    println!();
    println!("This will remove all puffgres artifacts:");
    println!("  • Delete Postgres tables:");
    println!("    - __puffgres_migrations");
    println!("    - __puffgres_checkpoints");
    println!("    - __puffgres_dlq");
    println!("    - __puffgres_backfill");
    println!("    - __puffgres_transforms");
    println!("  • Remove local puffgres/ directory");
    println!("  • Remove puffgres.toml");
    println!();
    println!("{}", "This action may be difficult to recover from!".red());
    println!();

    let confirmation = "Yes, I want to dangerously delete my puffgres config, in particular the configurations in Postgres, my transforms, and my migrations directory, and I know that it may be difficult to recover.";

    print!("Type the following to confirm:\n\"{}\"\n\n> ", confirmation);
    io::stdout().flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    let input = input.trim();

    if input != confirmation {
        println!("\nConfirmation did not match. Aborting.");
        return Ok(());
    }

    println!("\nDeleting puffgres configuration...");

    // Connect to Postgres and drop tables
    let store = PostgresStateStore::connect(&config.postgres_connection_string())
        .await
        .context("Failed to connect to Postgres")?;

    drop_all_puffgres_tables(&store).await?;
    println!("  ✓ Deleted Postgres tables");

    // Remove local files
    if Path::new("puffgres").exists() {
        fs::remove_dir_all("puffgres")?;
        println!("  ✓ Removed puffgres/ directory");
    }

    if Path::new("puffgres.toml").exists() {
        fs::remove_file("puffgres.toml")?;
        println!("  ✓ Removed puffgres.toml");
    }

    println!("\n{}", "Puffgres configuration deleted.".green());
    Ok(())
}

async fn cmd_dangerously_reset_turbopuffer(config: ProjectConfig) -> Result<()> {
    println!("{}", "WARNING: Dangerous Operation".red().bold());
    println!();

    // Load migrations to find all namespaces
    let mappings = config.load_migrations()?;
    let namespaces: Vec<&str> = mappings.iter().map(|m| m.namespace.as_str()).collect();
    let unique_namespaces: std::collections::HashSet<&str> = namespaces.into_iter().collect();

    println!("This will delete the following turbopuffer namespaces:");
    for ns in &unique_namespaces {
        println!("  • {}", ns);
    }
    println!();
    println!("{}", "All data in these namespaces will be permanently deleted!".red());
    println!("You may need to recreate these, redo backfills, or lose data in the process.");
    println!();

    let confirmation = "Yes, I want to dangerously delete all of my referenced turbopuffer namespaces. I may need to recreate these, redo backfills, or lose data in the process, and it may be hard to recover.";

    print!("Type the following to confirm:\n\"{}\"\n\n> ", confirmation);
    io::stdout().flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    let input = input.trim();

    if input != confirmation {
        println!("\nConfirmation did not match. Aborting.");
        return Ok(());
    }

    println!("\nDeleting turbopuffer namespaces...");

    // Create turbopuffer client
    let client = rs_puff::Client::new(config.turbopuffer_api_key());

    for ns in unique_namespaces {
        match client.namespace(ns).delete_all().await {
            Ok(_) => println!("  ✓ Deleted namespace: {}", ns),
            Err(e) => println!("  ✗ Failed to delete {}: {}", ns, e),
        }
    }

    // Also clear backfill progress
    let store = PostgresStateStore::connect(&config.postgres_connection_string())
        .await
        .context("Failed to connect to Postgres")?;

    for mapping in &mappings {
        store.clear_backfill_progress(&mapping.name).await?;
    }
    println!("  ✓ Cleared backfill progress");

    // Clear checkpoints
    clear_all_checkpoints(&store).await?;
    println!("  ✓ Cleared CDC checkpoints");

    println!("\n{}", "Turbopuffer namespaces deleted.".green());
    println!("Run 'puffgres backfill <mapping>' to re-sync your data.");
    Ok(())
}

// -------------------------------------------------------------------------
// Helper functions for transforms and database operations
// -------------------------------------------------------------------------

/// Store a transform in the database for immutability tracking
async fn store_transform(
    store: &PostgresStateStore,
    mapping_name: &str,
    version: i32,
    content: &str,
) -> Result<()> {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    hasher.update(content);
    let hash = hex::encode(hasher.finalize());

    store
        .store_transform(mapping_name, version, content, &hash)
        .await
        .context("Failed to store transform")?;

    Ok(())
}

/// Validate that local transforms match what's stored in the database
async fn validate_transforms(_config: &ProjectConfig, store: &PostgresStateStore) -> Result<()> {
    use sha2::{Digest, Sha256};

    // Get stored transforms from database
    let stored = store.get_all_transforms().await?;

    if stored.is_empty() {
        return Ok(());
    }

    // Check each stored transform against local files
    for transform in stored {
        let path = format!(
            "puffgres/transforms/{}_{}.ts",
            transform.mapping_name, transform.version
        );

        // Also check the simpler path format
        let alt_path = format!("puffgres/transforms/{}.ts", transform.mapping_name);

        let local_content = if Path::new(&path).exists() {
            Some(fs::read_to_string(&path)?)
        } else if Path::new(&alt_path).exists() {
            Some(fs::read_to_string(&alt_path)?)
        } else {
            None
        };

        if let Some(content) = local_content {
            let mut hasher = Sha256::new();
            hasher.update(&content);
            let local_hash = hex::encode(hasher.finalize());

            if local_hash != transform.content_hash {
                anyhow::bail!(
                    "Transform for '{}' v{} has been modified.\n  Expected hash: {}\n  Got hash: {}",
                    transform.mapping_name,
                    transform.version,
                    transform.content_hash,
                    local_hash
                );
            }
        }
    }

    Ok(())
}

/// Drop all puffgres tables from the database
async fn drop_all_puffgres_tables(store: &PostgresStateStore) -> Result<()> {
    store
        .drop_all_tables()
        .await
        .context("Failed to drop tables")?;
    Ok(())
}

/// Clear all CDC checkpoints
async fn clear_all_checkpoints(store: &PostgresStateStore) -> Result<()> {
    store
        .clear_all_checkpoints()
        .await
        .context("Failed to clear checkpoints")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn verify_cli() {
        Cli::command().debug_assert();
    }
}
