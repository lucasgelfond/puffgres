use anyhow::{Context, Result};
use puffgres_pg::PostgresStateStore;

use crate::config::ProjectConfig;

pub async fn cmd_status(config: ProjectConfig) -> Result<()> {
    // Connect to Postgres state store
    let store = PostgresStateStore::connect(&config.postgres_connection_string()?)
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
