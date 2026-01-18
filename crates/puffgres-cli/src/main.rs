use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use tracing::{info, warn};

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
    /// Initialize a new puffgres project
    Init {
        /// Directory to initialize (defaults to current directory)
        #[arg(default_value = ".")]
        path: PathBuf,
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
        Commands::Init { path } => cmd_init(&path),
        Commands::Migrate { dry_run } => {
            let config = load_config(&cli.config)?;
            cmd_migrate(config, dry_run).await
        }
        Commands::Run {
            slot,
            create_slot,
            poll_interval_ms,
        } => {
            let config = load_config(&cli.config)?;
            cmd_run(config, &slot, create_slot, poll_interval_ms).await
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
    }
}

fn load_config(path: &PathBuf) -> Result<ProjectConfig> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("Failed to read config file: {}", path.display()))?;

    let config: ProjectConfig =
        toml::from_str(&content).with_context(|| "Failed to parse puffgres.toml")?;

    Ok(config)
}

fn cmd_init(path: &PathBuf) -> Result<()> {
    info!(path = %path.display(), "Initializing puffgres project");

    // Create directories
    fs::create_dir_all(path.join("puffgres/migrations"))?;
    fs::create_dir_all(path.join("puffgres/transforms"))?;

    // Create .env.example
    let env_example = r#"# Puffgres environment variables
# Copy this to .env and fill in your values

# Postgres connection string (Supabase, Neon, etc.)
DATABASE_URL=postgresql://postgres:password@localhost:5432/postgres

# Turbopuffer API key
TURBOPUFFER_API_KEY=your-api-key-here

# Optional: Together AI for embeddings
# TOGETHER_API_KEY=your-together-key-here
"#;

    let env_example_path = path.join(".env.example");
    if !env_example_path.exists() {
        fs::write(&env_example_path, env_example)?;
        info!(path = %env_example_path.display(), "Created .env.example");
    }

    // Create default config that references env vars
    let config = r#"# Puffgres configuration
# Secrets are loaded from .env file
# State is stored in __puffgres_* tables in your Postgres database

[postgres]
connection_string = "${DATABASE_URL}"

[turbopuffer]
api_key = "${TURBOPUFFER_API_KEY}"

# Optional: Configure embedding providers for transforms
# [providers.embeddings]
# type = "together"
# model = "BAAI/bge-base-en-v1.5"
# api_key = "${TOGETHER_API_KEY}"
"#;

    let config_path = path.join("puffgres.toml");
    if !config_path.exists() {
        fs::write(&config_path, config)?;
        info!(path = %config_path.display(), "Created puffgres.toml");
    } else {
        warn!(path = %config_path.display(), "Config file already exists, skipping");
    }

    // Create example migration
    let migration = r#"# Example migration - sync users table to turbopuffer
version = 1
mapping_name = "users_public"
namespace = "users"
columns = ["id", "name", "email", "created_at"]

[source]
schema = "public"
table = "users"

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
# path = "./transforms/users.ts"
"#;

    let migration_path = path.join("puffgres/migrations/0001_users.toml");
    if !migration_path.exists() {
        fs::write(&migration_path, migration)?;
        info!(path = %migration_path.display(), "Created example migration");
    }

    // Create example transform
    let transform = r#"// Example transform for users table
// Uncomment [transform] section in migration to use this

import type { RowEvent, Action, TransformContext } from 'puffgres';

export default async function transform(
  event: RowEvent,
  id: string,
  ctx: TransformContext
): Promise<Action> {
  if (event.op === 'delete') {
    return { type: 'delete', id };
  }

  const row = event.new!;

  return {
    type: 'upsert',
    id,
    doc: {
      name: row.name,
      email: row.email,
      created_at: row.created_at,
    },
  };
}
"#;

    let transform_path = path.join("puffgres/transforms/users.ts");
    if !transform_path.exists() {
        fs::write(&transform_path, transform)?;
        info!(path = %transform_path.display(), "Created example transform");
    }

    // Create .gitignore for secrets
    let gitignore = r#"# Puffgres
.env
node_modules/
"#;

    let gitignore_path = path.join("puffgres/.gitignore");
    if !gitignore_path.exists() {
        fs::write(&gitignore_path, gitignore)?;
    }

    println!("\nPuffgres project initialized!\n");
    println!("Next steps:");
    println!("  1. Copy .env.example to .env and fill in your credentials");
    println!("  2. Edit puffgres/migrations/0001_users.toml for your table");
    println!("  3. Run: puffgres migrate");
    println!("  4. Run: puffgres run\n");

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

    let tracker = MigrationTracker::new(&store);
    let status = tracker.validate(&local).await?;

    // Check for errors
    if !status.mismatched.is_empty() {
        println!("\nMigration Hash Mismatches (ERROR):");
        for m in &status.mismatched {
            println!(
                "  v{} {}: local hash differs from applied",
                m.version, m.mapping_name
            );
            println!("    Applied: {}", m.expected_hash);
            println!("    Local:   {}", m.actual_hash);
        }
        anyhow::bail!("Cannot proceed: applied migrations have been modified locally");
    }

    // Show status
    if !status.applied.is_empty() {
        println!("\nAlready Applied:");
        for name in &status.applied {
            println!("  {}", name);
        }
    }

    if status.pending.is_empty() {
        println!("\nAll migrations are up to date.");
        return Ok(());
    }

    println!("\nPending Migrations:");
    for name in &status.pending {
        println!("  {}", name);
    }

    if dry_run {
        println!("\n(dry run - no changes made)");
        return Ok(());
    }

    // Apply pending migrations
    let applied = tracker.apply(&local, false).await?;

    println!("\nApplied {} migration(s).", applied.len());
    Ok(())
}

async fn cmd_run(
    config: ProjectConfig,
    slot: &str,
    create_slot: bool,
    poll_interval_ms: u64,
) -> Result<()> {
    info!("Starting puffgres CDC replication");

    // Connect to Postgres state store
    let store = PostgresStateStore::connect(&config.postgres_connection_string())
        .await
        .context("Failed to connect to Postgres")?;

    // Load and validate migrations
    let local = config.load_local_migrations()?;
    if local.is_empty() {
        anyhow::bail!("No migrations found in puffgres/migrations/");
    }

    // Validate migrations match what's applied (pending migrations are allowed)
    let tracker = MigrationTracker::new(&store);
    tracker.validate_or_fail(&local, true).await?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn verify_cli() {
        Cli::command().debug_assert();
    }
}
