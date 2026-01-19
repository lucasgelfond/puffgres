//! Backfill command implementation.
//!
//! Scans existing table data and syncs to turbopuffer.

use std::collections::HashMap;
use std::io::{self, Write};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use tracing::{debug, info, warn};

use puffgres_core::{
    extract_id, Batcher, DocumentId, IdentityTransformer, Mapping, Transformer, Value, WriteRequest,
};
use puffgres_pg::{BackfillConfig, BackfillScanner, PostgresStateStore};

use crate::config::ProjectConfig;

/// Run the backfill for a specific mapping.
pub async fn run_backfill(
    config: &ProjectConfig,
    mapping: &Mapping,
    batch_size: u32,
    resume: bool,
) -> Result<()> {
    info!(
        mapping = %mapping.name,
        namespace = %mapping.namespace,
        table = format!("{}.{}", mapping.source.schema, mapping.source.table),
        batch_size,
        resume,
        "Starting backfill"
    );

    // Connect to state store
    let state_store = PostgresStateStore::connect(&config.postgres_connection_string()?)
        .await
        .context("Failed to connect to state store")?;

    // Check for existing progress if resuming
    let existing_progress = if resume {
        state_store.get_backfill_progress(&mapping.name).await?
    } else {
        // Clear any existing progress
        state_store.clear_backfill_progress(&mapping.name).await?;
        None
    };

    // Configure backfill scanner
    let backfill_config = BackfillConfig {
        connection_string: config.postgres_connection_string()?,
        schema: mapping.source.schema.clone(),
        table: mapping.source.table.clone(),
        id_column: mapping.id.column.clone(),
        columns: mapping.columns.clone(),
        batch_size,
    };

    let mut scanner = BackfillScanner::new(backfill_config)
        .await
        .context("Failed to create backfill scanner")?;

    // Resume from checkpoint if available
    if let Some(progress) = existing_progress {
        if let Some(last_id) = progress.last_id {
            info!(
                last_id = %last_id,
                processed = progress.processed_rows,
                "Resuming from checkpoint"
            );
            scanner.resume_from(last_id, progress.processed_rows);
        }
    }

    // Initialize turbopuffer client
    let tp_client = rs_puff::Client::new(config.turbopuffer_api_key()?);

    // Create transformer
    let transformer = IdentityTransformer::new(mapping.columns.clone());

    // Create batcher
    let mut batcher = Batcher::new(mapping.batching.clone());

    // Progress tracking
    let mut last_progress_update = Instant::now();
    let progress_interval = Duration::from_secs(1);

    // Main backfill loop
    loop {
        let events = scanner.next_batch().await?;

        if events.is_empty() {
            // Done!
            break;
        }

        // Process events through transform pipeline
        for event in &events {
            let id = match extract_id(event, &mapping.id.column, mapping.id.id_type) {
                Ok(id) => id,
                Err(e) => {
                    warn!(
                        mapping = %mapping.name,
                        error = %e,
                        "Failed to extract ID during backfill"
                    );
                    continue;
                }
            };

            let action = match transformer.transform(event, id) {
                Ok(action) => action,
                Err(e) => {
                    warn!(
                        mapping = %mapping.name,
                        error = %e,
                        "Transform failed during backfill"
                    );
                    continue;
                }
            };

            if !action.requires_write() {
                continue;
            }

            // Add to batcher
            if let Some(batch) = batcher.add(&mapping.namespace, action, 0) {
                let request = WriteRequest::from_batch(batch);
                flush_batch(&tp_client, &request).await?;
            }
        }

        // Flush any remaining items in the batcher
        for batch in batcher.flush_all() {
            let request = WriteRequest::from_batch(batch);
            flush_batch(&tp_client, &request).await?;
        }

        // Update progress in database
        let progress = scanner.progress();
        state_store
            .update_backfill_progress(
                &mapping.name,
                progress.last_id.as_deref(),
                progress.total_rows,
                progress.processed_rows,
                "in_progress",
            )
            .await?;

        // Print progress periodically
        if last_progress_update.elapsed() >= progress_interval {
            print!("\r{}", progress.format());
            io::stdout().flush().ok();
            last_progress_update = Instant::now();
        }
    }

    // Final flush
    for batch in batcher.flush_all() {
        let request = WriteRequest::from_batch(batch);
        flush_batch(&tp_client, &request).await?;
    }

    // Mark as complete
    let final_progress = scanner.progress();
    state_store
        .update_backfill_progress(
            &mapping.name,
            final_progress.last_id.as_deref(),
            final_progress.total_rows,
            final_progress.processed_rows,
            "completed",
        )
        .await?;

    // Print final status
    println!("\r{}", final_progress.format());
    println!("\nBackfill complete!");

    Ok(())
}

/// Flush a batch to turbopuffer.
async fn flush_batch(client: &rs_puff::Client, request: &WriteRequest) -> Result<()> {
    if request.is_empty() {
        return Ok(());
    }

    debug!(
        namespace = %request.namespace,
        upserts = request.upserts.len(),
        "Flushing backfill batch"
    );

    // Build upsert rows
    let upsert_rows: Option<Vec<HashMap<String, serde_json::Value>>> = if request.upserts.is_empty()
    {
        None
    } else {
        Some(
            request
                .upserts
                .iter()
                .map(|doc| {
                    let mut row: HashMap<String, serde_json::Value> = doc
                        .attributes
                        .iter()
                        .map(|(k, v)| (k.clone(), convert_value_to_json(v)))
                        .collect();
                    row.insert("id".to_string(), convert_doc_id_to_json(&doc.id));
                    row.insert("__backfill".to_string(), serde_json::Value::Bool(true));
                    row
                })
                .collect(),
        )
    };

    let params = rs_puff::WriteParams {
        upsert_rows,
        deletes: None,
        ..Default::default()
    };

    client
        .namespace(&request.namespace)
        .write(params)
        .await
        .context("Failed to write to turbopuffer")?;

    Ok(())
}

fn convert_doc_id_to_json(id: &DocumentId) -> serde_json::Value {
    match id {
        DocumentId::Uint(u) => serde_json::Value::Number((*u).into()),
        DocumentId::Int(i) => serde_json::Value::Number((*i).into()),
        DocumentId::Uuid(s) | DocumentId::String(s) => serde_json::Value::String(s.clone()),
    }
}

fn convert_value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(i) => serde_json::Value::Number((*i).into()),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::String(s) => serde_json::Value::String(s.clone()),
        Value::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(convert_value_to_json).collect())
        }
        Value::Object(obj) => serde_json::Value::Object(
            obj.iter()
                .map(|(k, v)| (k.clone(), convert_value_to_json(v)))
                .collect(),
        ),
    }
}
