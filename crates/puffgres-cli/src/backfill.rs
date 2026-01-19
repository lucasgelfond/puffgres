//! Backfill command implementation.
//!
//! Scans existing table data and syncs to turbopuffer.

use std::collections::HashMap;
use std::io::{self, Write};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use tracing::{debug, info, warn};

use puffgres_core::{
    extract_id, Action, BatchConfig, Batcher, DocumentId, IdentityTransformer, JsTransformer,
    Mapping, TransformType, Transformer, Value, WriteRequest,
};
use puffgres_pg::{BackfillConfig, BackfillScanner, PostgresStateStore};

use crate::config::ProjectConfig;
use crate::env::{get_max_retries, get_transform_batch_size, get_upload_batch_size};

/// Wrapper for different transformer types.
enum MappingTransformer {
    Identity(IdentityTransformer),
    Js(JsTransformer),
}

impl MappingTransformer {
    fn transform_batch(
        &self,
        rows: &[(&puffgres_core::RowEvent, DocumentId)],
    ) -> puffgres_core::Result<Vec<Action>> {
        match self {
            MappingTransformer::Identity(t) => t.transform_batch(rows),
            MappingTransformer::Js(t) => t.transform_batch(rows),
        }
    }
}

/// Create the appropriate transformer for a mapping.
fn create_transformer(mapping: &Mapping) -> MappingTransformer {
    match &mapping.transform {
        Some(config) if config.transform_type == TransformType::Js => {
            if let Some(path) = &config.path {
                MappingTransformer::Js(JsTransformer::new(path))
            } else {
                // No path specified, use identity
                MappingTransformer::Identity(IdentityTransformer::new(mapping.columns.clone()))
            }
        }
        _ => MappingTransformer::Identity(IdentityTransformer::new(mapping.columns.clone())),
    }
}

/// Run the backfill for a specific mapping.
pub async fn run_backfill(
    config: &ProjectConfig,
    mapping: &Mapping,
    batch_size: u32,
    resume: bool,
) -> Result<()> {
    // Load batch and retry configuration from environment
    let transform_batch_size = get_transform_batch_size();
    let upload_batch_size = get_upload_batch_size();
    let max_retries = get_max_retries();

    info!(
        mapping = %mapping.name,
        namespace = %mapping.namespace,
        table = format!("{}.{}", mapping.source.schema, mapping.source.table),
        batch_size,
        transform_batch_size,
        upload_batch_size,
        max_retries,
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
    // When a transform is configured, fetch all columns so the transform has access to everything
    let backfill_config = BackfillConfig {
        connection_string: config.postgres_connection_string()?,
        schema: mapping.source.schema.clone(),
        table: mapping.source.table.clone(),
        id_column: mapping.id.column.clone(),
        columns: get_backfill_columns(mapping),
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

    // Create transformer - uses JS transform if configured, otherwise identity
    let transformer = create_transformer(mapping);

    // Create batcher with transform batch size from environment
    let batch_config = BatchConfig::with_max_rows(transform_batch_size);
    let mut batcher = Batcher::new(batch_config);

    // Progress tracking
    let mut last_progress_update = Instant::now();
    let progress_interval = Duration::from_secs(1);

    // Batch size for sending to JS transform (500 rows at a time)
    const JS_TRANSFORM_BATCH_SIZE: usize = 500;

    // Main backfill loop
    loop {
        let events = scanner.next_batch().await?;

        if events.is_empty() {
            // Done!
            break;
        }

        // Collect events with their IDs for batch processing
        let mut transform_input: Vec<(&puffgres_core::RowEvent, DocumentId)> = Vec::new();

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

            transform_input.push((event, id));

            // When we have enough rows, process a batch
            if transform_input.len() >= JS_TRANSFORM_BATCH_SIZE {
                process_transform_batch(
                    &transformer,
                    &transform_input,
                    &mapping,
                    &mut batcher,
                    &tp_client,
                    upload_batch_size,
                    max_retries,
                )
                .await?;
                transform_input.clear();
            }
        }

        // Process any remaining rows
        if !transform_input.is_empty() {
            process_transform_batch(
                &transformer,
                &transform_input,
                &mapping,
                &mut batcher,
                &tp_client,
                upload_batch_size,
                max_retries,
            )
            .await?;
        }

        // Flush any remaining items in the batcher
        for batch in batcher.flush_all() {
            let request = WriteRequest::from_batch(batch);
            flush_batch(&tp_client, &request, upload_batch_size, max_retries).await?;
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
        flush_batch(&tp_client, &request, upload_batch_size, max_retries).await?;
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

/// Process a batch of rows through the transform.
async fn process_transform_batch(
    transformer: &MappingTransformer,
    rows: &[(&puffgres_core::RowEvent, DocumentId)],
    mapping: &Mapping,
    batcher: &mut Batcher,
    tp_client: &rs_puff::Client,
    upload_batch_size: usize,
    max_retries: u32,
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    debug!(
        mapping = %mapping.name,
        batch_size = rows.len(),
        "Processing transform batch"
    );

    let actions = match transformer.transform_batch(rows) {
        Ok(actions) => actions,
        Err(e) => {
            warn!(
                mapping = %mapping.name,
                error = %e,
                batch_size = rows.len(),
                "Transform batch failed during backfill"
            );
            return Ok(());
        }
    };

    for action in actions {
        if !action.requires_write() {
            continue;
        }

        // Add to batcher
        if let Some(batch) = batcher.add(&mapping.namespace, action, 0) {
            let request = WriteRequest::from_batch(batch);
            flush_batch(tp_client, &request, upload_batch_size, max_retries).await?;
        }
    }

    Ok(())
}

/// Flush a batch to turbopuffer with chunking and retry logic.
async fn flush_batch(
    client: &rs_puff::Client,
    request: &WriteRequest,
    upload_batch_size: usize,
    max_retries: u32,
) -> Result<()> {
    if request.is_empty() {
        return Ok(());
    }

    debug!(
        namespace = %request.namespace,
        upserts = request.upserts.len(),
        "Flushing backfill batch"
    );

    // Build all upsert rows
    let all_upsert_rows: Vec<HashMap<String, serde_json::Value>> = request
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
        .collect();

    // Upload in chunks (backfill is upserts-only, no deletes)
    for chunk in all_upsert_rows.chunks(upload_batch_size) {
        let params = rs_puff::WriteParams {
            upsert_rows: Some(chunk.to_vec()),
            deletes: None,
            distance_metric: request.distance_metric,
            ..Default::default()
        };
        write_with_retry(client, &request.namespace, params, max_retries).await?;
    }

    Ok(())
}

/// Write to turbopuffer with exponential backoff retry.
async fn write_with_retry(
    client: &rs_puff::Client,
    namespace: &str,
    params: rs_puff::WriteParams,
    max_retries: u32,
) -> Result<()> {
    let base_delay_ms = 100u64;

    for attempt in 0..=max_retries {
        match client.namespace(namespace).write(params.clone()).await {
            Ok(_) => return Ok(()),
            Err(e) => {
                if attempt == max_retries {
                    return Err(e).context("Failed to write to turbopuffer after all retries");
                }

                let delay_ms = base_delay_ms * (1 << attempt);
                warn!(
                    namespace = namespace,
                    attempt = attempt + 1,
                    max_retries,
                    delay_ms,
                    error = %e,
                    "Turbopuffer write failed, retrying"
                );

                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
        }
    }

    unreachable!()
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

/// Check if a mapping has a custom JS transform configured.
/// When true, we should fetch all columns from Postgres so the transform has access to everything.
pub fn has_custom_transform(mapping: &Mapping) -> bool {
    mapping
        .transform
        .as_ref()
        .map(|t| t.transform_type == TransformType::Js && t.path.is_some())
        .unwrap_or(false)
}

/// Get the columns to fetch from Postgres for a mapping.
/// Returns empty vec (meaning all columns) when a custom transform is configured.
pub fn get_backfill_columns(mapping: &Mapping) -> Vec<String> {
    if has_custom_transform(mapping) {
        vec![] // Empty = fetch all columns
    } else {
        mapping.columns.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use puffgres_core::{IdType, TransformConfig};

    fn make_mapping_without_transform() -> Mapping {
        Mapping::builder("test")
            .namespace("test")
            .source("public", "users")
            .id("id", IdType::Uint)
            .columns(vec!["id".into(), "name".into(), "email".into()])
            .build()
            .unwrap()
    }

    fn make_mapping_with_transform() -> Mapping {
        Mapping::builder("test")
            .namespace("test")
            .source("public", "users")
            .id("id", IdType::Uint)
            .columns(vec!["id".into(), "name".into(), "email".into()])
            .transform(TransformConfig {
                transform_type: TransformType::Js,
                path: Some("./transforms/test.ts".into()),
                entry: None,
            })
            .build()
            .unwrap()
    }

    fn make_mapping_with_transform_no_path() -> Mapping {
        Mapping::builder("test")
            .namespace("test")
            .source("public", "users")
            .id("id", IdType::Uint)
            .columns(vec!["id".into(), "name".into(), "email".into()])
            .transform(TransformConfig {
                transform_type: TransformType::Js,
                path: None,
                entry: None,
            })
            .build()
            .unwrap()
    }

    #[test]
    fn test_has_custom_transform_false_when_no_transform() {
        let mapping = make_mapping_without_transform();
        assert!(!has_custom_transform(&mapping));
    }

    #[test]
    fn test_has_custom_transform_true_when_js_transform_with_path() {
        let mapping = make_mapping_with_transform();
        assert!(has_custom_transform(&mapping));
    }

    #[test]
    fn test_has_custom_transform_false_when_js_transform_without_path() {
        let mapping = make_mapping_with_transform_no_path();
        assert!(!has_custom_transform(&mapping));
    }

    #[test]
    fn test_get_backfill_columns_returns_columns_without_transform() {
        let mapping = make_mapping_without_transform();
        let columns = get_backfill_columns(&mapping);
        assert_eq!(columns, vec!["id", "name", "email"]);
    }

    #[test]
    fn test_get_backfill_columns_returns_empty_with_transform() {
        let mapping = make_mapping_with_transform();
        let columns = get_backfill_columns(&mapping);
        assert!(columns.is_empty(), "Should return empty vec to fetch all columns when transform is configured");
    }

    #[test]
    fn test_get_backfill_columns_returns_columns_without_path() {
        let mapping = make_mapping_with_transform_no_path();
        let columns = get_backfill_columns(&mapping);
        assert_eq!(columns, vec!["id", "name", "email"], "Should use columns when transform has no path");
    }
}
