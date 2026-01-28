use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use colored::Colorize;
use puffgres_pg::PostgresStateStore;

use crate::config::ProjectConfig;

pub async fn cmd_reset(config: ProjectConfig) -> Result<()> {
    println!("Resetting local config from database state...\n");

    // Connect to Postgres state store
    let store = PostgresStateStore::connect(&config.postgres_connection_string()?)
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
    fs::create_dir_all("migrations")?;
    fs::create_dir_all("transforms")?;

    // Clear existing local migrations
    if Path::new("migrations").exists() {
        for entry in fs::read_dir("migrations")? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().map_or(false, |ext| ext == "toml") {
                fs::remove_file(&path)?;
                println!("Removed {}", path.display());
            }
        }
    }

    // Clear existing local transforms
    if Path::new("transforms").exists() {
        for entry in fs::read_dir("transforms")? {
            let entry = entry?;
            let path = entry.path();
            if path
                .extension()
                .map_or(false, |ext| ext == "ts" || ext == "js")
            {
                fs::remove_file(&path)?;
                println!("Removed {}", path.display());
            }
        }
    }

    // Write migrations from database
    if migration_content.is_empty() {
        println!(
            "{}",
            "Note: Migration content not found in database.".yellow()
        );
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
            let filename = format!(
                "{:04}_{}.toml",
                version,
                mapping_name.replace("_public", "")
            );
            let path = format!("migrations/{}", filename);
            fs::write(&path, &content)?;
            println!("  Restored {}", path);
        }
    }

    // Write transforms from database
    if !transforms.is_empty() {
        println!("\nRestoring transforms from database:");
        for transform in transforms {
            let path = format!("transforms/{}.ts", transform.mapping_name);
            fs::write(&path, &transform.content)?;
            println!("  Restored {}", path);
        }
    }

    println!("\n{}", "Reset complete!".green());
    Ok(())
}
