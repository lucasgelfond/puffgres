use std::fs;
use std::io::{self, Write};
use std::path::Path;

use anyhow::{Context, Result};
use colored::Colorize;
use puffgres_pg::PostgresStateStore;

use crate::config::ProjectConfig;

pub async fn cmd_dangerously_delete_config(config: ProjectConfig) -> Result<()> {
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
    let store = PostgresStateStore::connect(&config.postgres_connection_string()?)
        .await
        .context("Failed to connect to Postgres")?;

    store
        .drop_all_tables()
        .await
        .context("Failed to drop tables")?;
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

pub async fn cmd_dangerously_reset_turbopuffer(config: ProjectConfig) -> Result<()> {
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
    println!(
        "{}",
        "All data in these namespaces will be permanently deleted!".red()
    );
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
    let client = rs_puff::Client::new(config.turbopuffer_api_key()?);

    for ns in unique_namespaces {
        match client.namespace(ns).delete_all().await {
            Ok(_) => println!("  ✓ Deleted namespace: {}", ns),
            Err(e) => println!("  ✗ Failed to delete {}: {}", ns, e),
        }
    }

    // Also clear backfill progress
    let store = PostgresStateStore::connect(&config.postgres_connection_string()?)
        .await
        .context("Failed to connect to Postgres")?;

    for mapping in &mappings {
        store.clear_backfill_progress(&mapping.name).await?;
    }
    println!("  ✓ Cleared backfill progress");

    // Clear checkpoints
    store
        .clear_all_checkpoints()
        .await
        .context("Failed to clear checkpoints")?;
    println!("  ✓ Cleared CDC checkpoints");

    println!("\n{}", "Turbopuffer namespaces deleted.".green());
    println!("Run 'puffgres backfill <mapping>' to re-sync your data.");
    Ok(())
}
