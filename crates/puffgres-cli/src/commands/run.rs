use std::fs;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use colored::Colorize;
use puffgres_pg::{MigrationTracker, PostgresStateStore};
use tracing::info;

use crate::config::ProjectConfig;
use crate::runner;
use crate::validation::{store_transform, validate_transforms};

pub async fn cmd_run(
    config: ProjectConfig,
    slot: &str,
    create_slot: bool,
    poll_interval_ms: u64,
    skip_migrate: bool,
) -> Result<()> {
    info!("Starting puffgres CDC replication");

    // Connect to Postgres state store (this auto-creates __puffgres_* tables if they don't exist)
    let store = PostgresStateStore::connect(&config.postgres_connection_string()?)
        .await
        .context("Failed to connect to Postgres")?;

    // Load and validate migrations
    let local = config.load_local_migrations()?;
    if local.is_empty() {
        anyhow::bail!("No migrations found in puffgres/migrations/");
    }

    // Validate that all referenced tables exist before proceeding
    for migration in &local {
        let migration_config = puffgres_config::MigrationConfig::parse(&migration.content)
            .with_context(|| format!("Failed to parse migration v{} '{}'", migration.version, migration.mapping_name))?;

        let schema = &migration_config.source.schema;
        let table = &migration_config.source.table;

        if !store.table_exists(schema, table).await? {
            eprintln!(
                "{}",
                format!(
                    "Error: Table '{}.{}' referenced in migration v{} '{}' does not exist.",
                    schema, table, migration.version, migration.mapping_name
                )
                .red()
            );
            eprintln!(
                "{}",
                "Create the table in your database before running CDC replication.".yellow()
            );
            std::process::exit(1);
        }
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
