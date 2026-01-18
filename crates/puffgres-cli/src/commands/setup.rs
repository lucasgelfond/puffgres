use anyhow::{Context, Result};
use colored::Colorize;
use puffgres_pg::PostgresStateStore;

use crate::config::ProjectConfig;

pub async fn cmd_setup(config: ProjectConfig) -> Result<()> {
    println!("Setting up puffgres database tables...\n");

    println!("The following tables will be created:");
    println!("  - __puffgres_migrations  - tracks applied migrations");
    println!("  - __puffgres_checkpoints - stores CDC replication state");
    println!("  - __puffgres_dlq         - dead letter queue for failed events");
    println!("  - __puffgres_backfill    - tracks backfill progress");
    println!("  - __puffgres_transforms  - stores versioned transform code");
    println!("  - __puffgres_migration_content - stores migration content");
    println!();

    // Connect to Postgres - this auto-creates the tables
    let _store = PostgresStateStore::connect(&config.postgres_connection_string()?)
        .await
        .context("Failed to connect to Postgres")?;

    println!("{}", "Database tables created successfully!".green());
    println!("\nNext steps:");
    println!("  1. Run: puffgres migrate");
    println!("  2. Run: puffgres backfill <mapping_name>");
    println!("  3. Run: puffgres run\n");

    Ok(())
}
