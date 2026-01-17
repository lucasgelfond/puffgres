use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};
use tracing::{debug, error, info, warn};

use puffgres_core::{
    extract_id, Batcher, DocumentId, IdentityTransformer, Mapping, Router, Transformer, Value,
    WriteRequest,
};
use puffgres_pg::{PollerConfig, Wal2JsonPoller};
use puffgres_state::{SqliteStateStore, StateStore};

use crate::config::ProjectConfig;

/// Run the CDC replication loop.
pub async fn run_cdc_loop(
    config: &ProjectConfig,
    mappings: Vec<Mapping>,
    slot: &str,
    create_slot: bool,
    poll_interval: Duration,
) -> Result<()> {
    // Initialize components
    let pg_config = PollerConfig {
        connection_string: config.postgres_connection_string(),
        slot_name: slot.to_string(),
        create_slot,
        max_changes: 1000,
    };

    let poller = Wal2JsonPoller::connect(pg_config)
        .await
        .context("Failed to connect to Postgres")?;

    let state_path = config.resolve_env(&config.state.path);
    let state_store =
        SqliteStateStore::open(&state_path).context("Failed to open state store")?;

    let tp_client = rs_puff::Client::new(config.turbopuffer_api_key());

    let router = Router::new(mappings.clone());

    // Create transformers for each mapping
    let transformers: Vec<_> = mappings
        .iter()
        .map(|m| (m.name.clone(), IdentityTransformer::new(m.columns.clone())))
        .collect();

    info!(
        slot = slot,
        mappings = mappings.len(),
        "Starting CDC loop"
    );

    // Main loop
    let mut total_events: u64 = 0;
    let mut batchers: std::collections::HashMap<String, Batcher> = std::collections::HashMap::new();

    loop {
        // Poll for changes
        let events = match poller.poll().await {
            Ok(events) => events,
            Err(e) => {
                error!(error = %e, "Failed to poll for changes");
                tokio::time::sleep(poll_interval).await;
                continue;
            }
        };

        if events.is_empty() {
            tokio::time::sleep(poll_interval).await;
            continue;
        }

        debug!(count = events.len(), "Processing events");

        // Process each event
        for event in &events {
            // Route to matching mappings
            let matched = router.route(event);

            for mapping in matched {
                // Get or create batcher for this namespace
                let batcher = batchers
                    .entry(mapping.namespace.clone())
                    .or_insert_with(|| Batcher::new(mapping.batching.clone()));

                // Find transformer for this mapping
                let transformer = transformers
                    .iter()
                    .find(|(name, _)| name == &mapping.name)
                    .map(|(_, t)| t)
                    .unwrap();

                // Extract ID and transform
                let id = match extract_id(event, &mapping.id.column, mapping.id.id_type) {
                    Ok(id) => id,
                    Err(e) => {
                        warn!(
                            mapping = %mapping.name,
                            error = %e,
                            "Failed to extract ID"
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
                            "Transform failed"
                        );
                        continue;
                    }
                };

                if !action.requires_write() {
                    continue;
                }

                // Add to batcher
                if let Some(batch) = batcher.add(&mapping.namespace, action, event.lsn) {
                    // Batch is full, flush it
                    let request = WriteRequest::from_batch(batch);
                    if let Err(e) = flush_batch(&tp_client, &state_store, &mapping.name, request).await
                    {
                        error!(
                            mapping = %mapping.name,
                            error = %e,
                            "Failed to flush batch"
                        );
                    }
                }
            }
        }

        total_events += events.len() as u64;

        // Flush all pending batches
        for (namespace, batcher) in &mut batchers {
            for batch in batcher.flush_all() {
                let request = WriteRequest::from_batch(batch);

                // Find mapping name for this namespace
                let mapping_name = mappings
                    .iter()
                    .find(|m| &m.namespace == namespace)
                    .map(|m| m.name.as_str())
                    .unwrap_or(namespace);

                if let Err(e) = flush_batch(&tp_client, &state_store, mapping_name, request).await {
                    error!(
                        namespace = %namespace,
                        error = %e,
                        "Failed to flush batch"
                    );
                }
            }
        }

        // Log progress periodically
        if total_events % 100 == 0 && total_events > 0 {
            info!(total_events = total_events, "Progress");
        }

        tokio::time::sleep(poll_interval).await;
    }
}

async fn flush_batch(
    client: &rs_puff::Client,
    state_store: &SqliteStateStore,
    mapping_name: &str,
    request: WriteRequest,
) -> Result<()> {
    let lsn = request.lsn;
    let count = request.upserts.len() + request.deletes.len();

    if request.is_empty() {
        return Ok(());
    }

    info!(
        mapping = mapping_name,
        namespace = %request.namespace,
        upserts = request.upserts.len(),
        deletes = request.deletes.len(),
        lsn = lsn,
        "Flushing batch"
    );

    // Build upsert rows
    let upsert_rows: Option<Vec<HashMap<String, serde_json::Value>>> =
        if request.upserts.is_empty() {
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
                        row.insert(
                            "__source_lsn".to_string(),
                            serde_json::Value::Number(request.lsn.into()),
                        );
                        row
                    })
                    .collect(),
            )
        };

    // Build delete IDs
    let deletes: Option<Vec<serde_json::Value>> = if request.deletes.is_empty() {
        None
    } else {
        Some(
            request
                .deletes
                .iter()
                .map(convert_doc_id_to_json)
                .collect(),
        )
    };

    let params = rs_puff::WriteParams {
        upsert_rows,
        deletes,
        ..Default::default()
    };

    // Write to turbopuffer
    client
        .namespace(&request.namespace)
        .write(params)
        .await
        .context("Failed to write to turbopuffer")?;

    // Update checkpoint
    let mut checkpoint = state_store
        .get_checkpoint(mapping_name)?
        .unwrap_or_default();

    checkpoint.lsn = lsn;
    checkpoint.events_processed += count as u64;

    state_store
        .save_checkpoint(mapping_name, &checkpoint)
        .context("Failed to save checkpoint")?;

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
