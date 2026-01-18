//! Dead Letter Queue command handlers.

use anyhow::{Context, Result};
use tracing::info;

use puffgres_core::ErrorKind;
use puffgres_pg::PostgresStateStore;

/// List DLQ entries.
pub async fn cmd_dlq_list(
    store: &PostgresStateStore,
    mapping: Option<&str>,
    limit: i64,
) -> Result<()> {
    let entries = store.get_dlq_entries(mapping, limit).await?;

    if entries.is_empty() {
        if let Some(name) = mapping {
            println!("No DLQ entries for mapping '{}'", name);
        } else {
            println!("No DLQ entries found.");
        }
        return Ok(());
    }

    println!("\nDead Letter Queue:");
    println!(
        "{:<6} {:<20} {:<15} {:<20} {:<8}",
        "ID", "Mapping", "Error Kind", "Created", "Retries"
    );
    println!("{:-<70}", "");

    for entry in &entries {
        let error_kind = ErrorKind::from_str(&entry.error_kind);
        let created = entry.created_at.format("%Y-%m-%d %H:%M");
        let retryable = if error_kind.is_retryable() {
            "(retryable)"
        } else {
            ""
        };

        println!(
            "{:<6} {:<20} {:<15} {:<20} {:<8} {}",
            entry.id,
            truncate(&entry.mapping_name, 20),
            error_kind.description(),
            created,
            entry.retry_count,
            retryable
        );
    }

    println!("\nTotal: {} entries", entries.len());
    if entries.len() as i64 == limit {
        println!("(showing first {} - use --limit to see more)", limit);
    }

    Ok(())
}

/// Show a single DLQ entry.
pub async fn cmd_dlq_show(store: &PostgresStateStore, id: i32) -> Result<()> {
    let entry = store
        .get_dlq_entry(id)
        .await?
        .context(format!("DLQ entry {} not found", id))?;

    let error_kind = ErrorKind::from_str(&entry.error_kind);

    println!("\nDLQ Entry #{}", entry.id);
    println!("{:-<60}", "");
    println!("Mapping:      {}", entry.mapping_name);
    println!("LSN:          {}", entry.lsn);
    println!("Error Kind:   {} ({})", error_kind.description(), entry.error_kind);
    println!("Retryable:    {}", if error_kind.is_retryable() { "yes" } else { "no" });
    println!("Retry Count:  {}", entry.retry_count);
    println!("Created:      {}", entry.created_at.format("%Y-%m-%d %H:%M:%S %Z"));
    println!("\nError Message:");
    println!("  {}", entry.error_message);
    println!("\nEvent JSON:");
    println!(
        "{}",
        serde_json::to_string_pretty(&entry.event_json).unwrap_or_else(|_| entry.event_json.to_string())
    );

    Ok(())
}

/// Retry DLQ entries.
pub async fn cmd_dlq_retry(
    store: &PostgresStateStore,
    id: Option<i32>,
    mapping: Option<&str>,
) -> Result<()> {
    if id.is_none() && mapping.is_none() {
        anyhow::bail!("Either --id or --mapping must be specified");
    }

    if let Some(entry_id) = id {
        // Retry a specific entry
        let entry = store
            .get_dlq_entry(entry_id)
            .await?
            .context(format!("DLQ entry {} not found", entry_id))?;

        let error_kind = ErrorKind::from_str(&entry.error_kind);

        if !error_kind.is_retryable() {
            println!(
                "Warning: Entry {} has error kind '{}' which is not typically retryable.",
                entry_id,
                error_kind.description()
            );
            println!("The entry will still be queued for retry, but it may fail again.");
        }

        store.increment_dlq_retry(entry_id).await?;
        info!(id = entry_id, "Marked DLQ entry for retry");
        println!("Marked entry {} for retry (retry count: {})", entry_id, entry.retry_count + 1);

        // TODO: Actually reprocess the event through the pipeline
        // For now, we just increment the retry count
        println!("\nNote: Actual retry processing is not yet implemented.");
        println!("The entry has been marked for retry but will need manual reprocessing.");
    } else if let Some(name) = mapping {
        // Retry all entries for a mapping
        let entries = store.get_dlq_entries(Some(name), 1000).await?;

        if entries.is_empty() {
            println!("No DLQ entries for mapping '{}'", name);
            return Ok(());
        }

        let retryable_count = entries
            .iter()
            .filter(|e| ErrorKind::from_str(&e.error_kind).is_retryable())
            .count();

        println!(
            "Found {} DLQ entries for '{}' ({} retryable)",
            entries.len(),
            name,
            retryable_count
        );

        for entry in &entries {
            store.increment_dlq_retry(entry.id).await?;
        }

        println!("Marked {} entries for retry", entries.len());
        println!("\nNote: Actual retry processing is not yet implemented.");
    }

    Ok(())
}

/// Clear DLQ entries.
pub async fn cmd_dlq_clear(
    store: &PostgresStateStore,
    mapping: Option<&str>,
    all: bool,
) -> Result<()> {
    if mapping.is_none() && !all {
        anyhow::bail!("Either --mapping or --all must be specified");
    }

    if all && mapping.is_some() {
        anyhow::bail!("Cannot use both --mapping and --all");
    }

    let count = store.clear_dlq(mapping).await?;

    if let Some(name) = mapping {
        println!("Cleared {} DLQ entries for mapping '{}'", count, name);
    } else {
        println!("Cleared {} DLQ entries", count);
    }

    Ok(())
}

/// Truncate a string to a maximum length.
fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}...", &s[..max - 3])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_truncate() {
        assert_eq!(truncate("hello", 10), "hello");
        assert_eq!(truncate("hello world", 8), "hello...");
        assert_eq!(truncate("hi", 2), "hi");
    }
}
