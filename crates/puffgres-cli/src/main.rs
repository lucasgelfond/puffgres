use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;

mod backfill;
mod cli;
mod commands;
mod config;
mod dlq;
mod env;
mod runner;
mod validation;

use cli::{Cli, Commands, DlqCommands};
use config::ProjectConfig;
use puffgres_pg::PostgresStateStore;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing first so we can log .env loading
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("puffgres=info".parse().unwrap()),
        )
        .init();

    let cli = Cli::parse();

    // Load .env file from current directory or any parent directory
    // For `init` and `new`, try to load but don't require it
    let env_required = !matches!(cli.command, Commands::Init | Commands::New { .. });
    if let Err(e) = env::load_dotenv_from_ancestors() {
        if env_required {
            return Err(e);
        }
    }

    match cli.command {
        Commands::Init => commands::cmd_init().await,
        Commands::Setup => {
            let config = load_config(&cli.config)?;
            commands::cmd_setup(config).await
        }
        Commands::New { name } => commands::cmd_new(name).await,
        Commands::Migrate { dry_run } => {
            let config = load_config(&cli.config)?;
            commands::cmd_migrate(config, dry_run).await
        }
        Commands::Run {
            slot,
            create_slot,
            poll_interval_ms,
            skip_migrate,
        } => {
            let config = load_config(&cli.config)?;
            commands::cmd_run(config, &slot, create_slot, poll_interval_ms, skip_migrate).await
        }
        Commands::Status => {
            let config = load_config(&cli.config)?;
            commands::cmd_status(config).await
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
            commands::cmd_reset(config).await
        }
        Commands::DangerouslyDeleteConfig => {
            let config = load_config(&cli.config)?;
            commands::cmd_dangerously_delete_config(config).await
        }
        Commands::DangerouslyResetTurbopuffer => {
            let config = load_config(&cli.config)?;
            commands::cmd_dangerously_reset_turbopuffer(config).await
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

async fn cmd_backfill(
    config: ProjectConfig,
    mapping_name: &str,
    batch_size: u32,
    resume: bool,
) -> Result<()> {
    use colored::Colorize;

    // Connect to Postgres state store
    let store = PostgresStateStore::connect(&config.postgres_connection_string()?)
        .await
        .context("Failed to connect to Postgres")?;

    // Validate transforms haven't been modified
    if let Err(e) = validation::validate_transforms(&config, &store).await {
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

    // Validate that the table exists before backfilling
    let schema = &mapping.source.schema;
    let table = &mapping.source.table;

    if !store.table_exists(schema, table).await? {
        eprintln!(
            "{}",
            format!(
                "Error: Table '{}.{}' referenced in mapping '{}' does not exist.",
                schema, table, mapping_name
            )
            .red()
        );
        eprintln!(
            "{}",
            "Create the table in your database before running backfill.".yellow()
        );
        std::process::exit(1);
    }

    backfill::run_backfill(&config, mapping, batch_size, resume).await
}

async fn cmd_dlq(config: ProjectConfig, command: DlqCommands) -> Result<()> {
    // Connect to Postgres state store
    let store = PostgresStateStore::connect(&config.postgres_connection_string()?)
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
