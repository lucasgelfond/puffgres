use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use tracing::{info, warn};

mod config;
mod runner;

use config::ProjectConfig;

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
            cmd_status(config)
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
    fs::create_dir_all(path.join("puffgres/state"))?;

    // Create .env.example
    let env_example = r#"# Puffgres environment variables
# Copy this to .env and fill in your values

# Postgres connection string (Supabase, Neon, etc.)
DATABASE_URL=postgresql://postgres:password@localhost:5432/postgres

# Turbopuffer API key
TURBOPUFFER_API_KEY=your-api-key-here
"#;

    let env_example_path = path.join(".env.example");
    if !env_example_path.exists() {
        fs::write(&env_example_path, env_example)?;
        info!(path = %env_example_path.display(), "Created .env.example");
    }

    // Create default config that references env vars
    let config = r#"# Puffgres configuration
# Secrets are loaded from .env file

[postgres]
connection_string = "${DATABASE_URL}"

[turbopuffer]
api_key = "${TURBOPUFFER_API_KEY}"

[state]
path = "puffgres/state/puffgres.db"
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
"#;

    let migration_path = path.join("puffgres/migrations/0001_users.toml");
    if !migration_path.exists() {
        fs::write(&migration_path, migration)?;
        info!(path = %migration_path.display(), "Created example migration");
    }

    // Create .gitignore for secrets
    let gitignore = r#"# Puffgres
.env
puffgres/state/
"#;

    let gitignore_path = path.join("puffgres/.gitignore");
    if !gitignore_path.exists() {
        fs::write(&gitignore_path, gitignore)?;
    }

    println!("\n✓ Puffgres project initialized!\n");
    println!("Next steps:");
    println!("  1. Copy .env.example to .env and fill in your credentials");
    println!("  2. Edit puffgres/migrations/0001_users.toml for your table");
    println!("  3. Run: puffgres run\n");

    Ok(())
}

async fn cmd_run(
    config: ProjectConfig,
    slot: &str,
    create_slot: bool,
    poll_interval_ms: u64,
) -> Result<()> {
    info!("Starting puffgres CDC replication");

    // Load migrations
    let migrations = config.load_migrations()?;
    if migrations.is_empty() {
        anyhow::bail!("No migrations found in puffgres/migrations/");
    }

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

fn cmd_status(config: ProjectConfig) -> Result<()> {
    use puffgres_state::{SqliteStateStore, StateStore};

    let state_path = config.resolve_env(&config.state.path);
    let store = SqliteStateStore::open(&state_path)
        .with_context(|| format!("Failed to open state store: {}", state_path))?;

    let checkpoints = store.get_all_checkpoints()?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn verify_cli() {
        Cli::command().debug_assert();
    }
}
