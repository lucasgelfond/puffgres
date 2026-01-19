use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use colored::Colorize;
use puffgres_pg::{MigrationTracker, PostgresStateStore};
use tracing::info;

use crate::config::ProjectConfig;
use crate::validation::{
    store_transform, validate_no_unreferenced_transforms, validate_transforms,
};

pub async fn cmd_migrate(config: ProjectConfig, dry_run: bool) -> Result<()> {
    info!("Checking migrations");

    // Connect to Postgres state store
    let store = PostgresStateStore::connect(&config.postgres_connection_string()?)
        .await
        .context("Failed to connect to Postgres")?;

    // Load local migrations
    let local = config.load_local_migrations()?;
    if local.is_empty() {
        println!("No migrations found in puffgres/migrations/");
        return Ok(());
    }

    // Check for unreferenced transforms in the transforms directory
    if let Err(e) = validate_no_unreferenced_transforms(&local) {
        eprintln!("{}", format!("Error: {}", e).red());
        std::process::exit(1);
    }

    // Validate that all referenced tables exist before proceeding
    for migration in &local {
        let migration_config = puffgres_config::MigrationConfig::parse(&migration.content)
            .with_context(|| {
                format!(
                    "Failed to parse migration v{} '{}'",
                    migration.version, migration.mapping_name
                )
            })?;

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
                "Create the table in your database before applying this migration.".yellow()
            );
            std::process::exit(1);
        }
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
            .store_migration_content(
                migration.version,
                &migration.mapping_name,
                &migration.content,
            )
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

    println!(
        "\n{}",
        format!("Applied {} migration(s).", applied.len()).green()
    );
    Ok(())
}
