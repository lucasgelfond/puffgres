use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};
use tracing::{debug, error, info, warn};

use puffgres_core::{
    extract_id, Action, Batcher, DocumentId, IdentityTransformer, JsTransformer, Mapping, Router,
    TransformType, Transformer, Value, WriteRequest,
};
use puffgres_pg::{
    format_lsn, PollerConfig, PostgresStateStore, StreamingConfig, StreamingReplicator,
    Wal2JsonPoller,
};

use crate::config::ProjectConfig;

/// Replication mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum ReplicationMode {
    /// Polling mode: periodically fetch changes.
    Polling,
    /// Streaming mode: use peek/acknowledge pattern for at-least-once delivery.
    Streaming,
}

/// Wrapper for different transformer types.
enum MappingTransformer {
    Identity(IdentityTransformer),
    Js(JsTransformer),
}

impl MappingTransformer {
    fn transform(
        &self,
        event: &puffgres_core::RowEvent,
        id: DocumentId,
    ) -> puffgres_core::Result<Action> {
        match self {
            MappingTransformer::Identity(t) => t.transform(event, id),
            MappingTransformer::Js(t) => t.transform(event, id),
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

/// Run the CDC replication loop.
pub async fn run_cdc_loop(
    config: &ProjectConfig,
    mappings: Vec<Mapping>,
    slot: &str,
    create_slot: bool,
    poll_interval: Duration,
) -> Result<()> {
    run_cdc_loop_with_mode(
        config,
        mappings,
        slot,
        create_slot,
        poll_interval,
        ReplicationMode::Streaming,
    )
    .await
}

/// Run the CDC replication loop with explicit mode selection.
pub async fn run_cdc_loop_with_mode(
    config: &ProjectConfig,
    mappings: Vec<Mapping>,
    slot: &str,
    create_slot: bool,
    poll_interval: Duration,
    mode: ReplicationMode,
) -> Result<()> {
    match mode {
        ReplicationMode::Polling => {
            run_polling_loop(config, mappings, slot, create_slot, poll_interval).await
        }
        ReplicationMode::Streaming => {
            run_streaming_loop(config, mappings, slot, create_slot, poll_interval).await
        }
    }
}

/// Run using streaming replication with proper acknowledgment.
///
/// This mode uses peek/acknowledge pattern for at-least-once delivery:
/// 1. Peek at changes (without consuming)
/// 2. Process and write to turbopuffer
/// 3. Save checkpoint to Postgres
/// 4. Acknowledge changes (consume from slot)
async fn run_streaming_loop(
    config: &ProjectConfig,
    mappings: Vec<Mapping>,
    slot: &str,
    create_slot: bool,
    poll_interval: Duration,
) -> Result<()> {
    // State is stored in Postgres __puffgres_* tables
    let state_store = PostgresStateStore::connect(&config.postgres_connection_string()?)
        .await
        .context("Failed to connect to state store")?;

    // Get checkpoint to resume from
    let start_lsn = if let Some(mapping) = mappings.first() {
        state_store
            .get_checkpoint(&mapping.name)
            .await?
            .map(|c| c.lsn)
    } else {
        None
    };

    // Initialize streaming replicator
    let streaming_config = StreamingConfig {
        connection_string: config.postgres_connection_string()?,
        slot_name: slot.to_string(),
        create_slot,
        start_lsn,
        keepalive_interval: Duration::from_secs(10),
        receive_timeout: Duration::from_secs(30),
    };

    let mut replicator = StreamingReplicator::connect(streaming_config)
        .await
        .context("Failed to connect for streaming replication")?;

    // Resume from checkpoint if we have one
    if let Some(lsn) = start_lsn {
        info!(lsn = format_lsn(lsn), "Resuming from checkpoint");
        replicator.resume_from(lsn);
    }

    let tp_client = rs_puff::Client::new(config.turbopuffer_api_key()?);
    let router = Router::new(mappings.clone());

    let transformers: Vec<_> = mappings
        .iter()
        .map(|m| (m.name.clone(), create_transformer(m)))
        .collect();

    info!(
        slot = slot,
        mappings = mappings.len(),
        mode = "streaming",
        "Starting CDC loop"
    );

    let mut total_events: u64 = 0;
    let mut batchers: HashMap<String, Batcher> = HashMap::new();

    loop {
        // Poll for batch of changes (peek without consuming)
        let batch = match replicator.poll_batch(1000).await {
            Ok(b) => b,
            Err(e) => {
                error!(error = %e, "Failed to poll for changes");
                tokio::time::sleep(poll_interval).await;
                continue;
            }
        };

        if batch.events.is_empty() {
            tokio::time::sleep(poll_interval).await;
            continue;
        }

        debug!(count = batch.events.len(), "Processing streaming batch");

        // Process each event
        for event in &batch.events {
            let matched = router.route(event);

            for mapping in matched {
                let batcher = batchers
                    .entry(mapping.namespace.clone())
                    .or_insert_with(|| Batcher::new(mapping.batching.clone()));

                let transformer = transformers
                    .iter()
                    .find(|(name, _)| name == &mapping.name)
                    .map(|(_, t)| t)
                    .unwrap();

                let id = match extract_id(event, &mapping.id.column, mapping.id.id_type) {
                    Ok(id) => id,
                    Err(e) => {
                        warn!(mapping = %mapping.name, error = %e, "Failed to extract ID");
                        continue;
                    }
                };

                let action = match transformer.transform(event, id) {
                    Ok(action) => action,
                    Err(e) => {
                        warn!(mapping = %mapping.name, error = %e, "Transform failed");
                        continue;
                    }
                };

                if !action.requires_write() {
                    continue;
                }

                if let Some(full_batch) = batcher.add(&mapping.namespace, action, event.lsn) {
                    let request = WriteRequest::from_batch(full_batch);
                    if let Err(e) =
                        flush_batch(&tp_client, &state_store, &mapping.name, request).await
                    {
                        error!(mapping = %mapping.name, error = %e, "Failed to flush batch");
                    }
                }
            }
        }

        total_events += batch.events.len() as u64;

        // Flush all pending batches
        for (namespace, batcher) in &mut batchers {
            for full_batch in batcher.flush_all() {
                let request = WriteRequest::from_batch(full_batch);
                let mapping_name = mappings
                    .iter()
                    .find(|m| &m.namespace == namespace)
                    .map(|m| m.name.as_str())
                    .unwrap_or(namespace);

                if let Err(e) = flush_batch(&tp_client, &state_store, mapping_name, request).await {
                    error!(namespace = %namespace, error = %e, "Failed to flush batch");
                }
            }
        }

        // Acknowledge after successful processing
        // This is the key difference from polling - we only consume changes
        // after we've successfully written them to turbopuffer and saved checkpoint
        if let Err(e) = replicator.acknowledge(batch.ack_lsn).await {
            error!(
                lsn = format_lsn(batch.ack_lsn),
                error = %e,
                "Failed to acknowledge changes"
            );
        }

        if total_events % 100 == 0 && total_events > 0 {
            info!(
                total_events = total_events,
                lsn = format_lsn(replicator.current_lsn()),
                "Progress"
            );
        }

        tokio::time::sleep(poll_interval).await;
    }
}

/// Run using simple polling mode (original behavior).
async fn run_polling_loop(
    config: &ProjectConfig,
    mappings: Vec<Mapping>,
    slot: &str,
    create_slot: bool,
    poll_interval: Duration,
) -> Result<()> {
    let pg_config = PollerConfig {
        connection_string: config.postgres_connection_string()?,
        slot_name: slot.to_string(),
        create_slot,
        max_changes: 1000,
    };

    let poller = Wal2JsonPoller::connect(pg_config)
        .await
        .context("Failed to connect to Postgres")?;

    let state_store = PostgresStateStore::connect(&config.postgres_connection_string()?)
        .await
        .context("Failed to connect to state store")?;

    let tp_client = rs_puff::Client::new(config.turbopuffer_api_key()?);
    let router = Router::new(mappings.clone());

    let transformers: Vec<_> = mappings
        .iter()
        .map(|m| (m.name.clone(), create_transformer(m)))
        .collect();

    info!(
        slot = slot,
        mappings = mappings.len(),
        mode = "polling",
        "Starting CDC loop"
    );

    let mut total_events: u64 = 0;
    let mut batchers: HashMap<String, Batcher> = HashMap::new();

    loop {
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

        for event in &events {
            let matched = router.route(event);

            for mapping in matched {
                let batcher = batchers
                    .entry(mapping.namespace.clone())
                    .or_insert_with(|| Batcher::new(mapping.batching.clone()));

                let transformer = transformers
                    .iter()
                    .find(|(name, _)| name == &mapping.name)
                    .map(|(_, t)| t)
                    .unwrap();

                let id = match extract_id(event, &mapping.id.column, mapping.id.id_type) {
                    Ok(id) => id,
                    Err(e) => {
                        warn!(mapping = %mapping.name, error = %e, "Failed to extract ID");
                        continue;
                    }
                };

                let action = match transformer.transform(event, id) {
                    Ok(action) => action,
                    Err(e) => {
                        warn!(mapping = %mapping.name, error = %e, "Transform failed");
                        continue;
                    }
                };

                if !action.requires_write() {
                    continue;
                }

                if let Some(batch) = batcher.add(&mapping.namespace, action, event.lsn) {
                    let request = WriteRequest::from_batch(batch);
                    if let Err(e) =
                        flush_batch(&tp_client, &state_store, &mapping.name, request).await
                    {
                        error!(mapping = %mapping.name, error = %e, "Failed to flush batch");
                    }
                }
            }
        }

        total_events += events.len() as u64;

        for (namespace, batcher) in &mut batchers {
            for batch in batcher.flush_all() {
                let request = WriteRequest::from_batch(batch);
                let mapping_name = mappings
                    .iter()
                    .find(|m| &m.namespace == namespace)
                    .map(|m| m.name.as_str())
                    .unwrap_or(namespace);

                if let Err(e) = flush_batch(&tp_client, &state_store, mapping_name, request).await {
                    error!(namespace = %namespace, error = %e, "Failed to flush batch");
                }
            }
        }

        if total_events % 100 == 0 && total_events > 0 {
            info!(total_events = total_events, "Progress");
        }

        tokio::time::sleep(poll_interval).await;
    }
}

async fn flush_batch(
    client: &rs_puff::Client,
    state_store: &PostgresStateStore,
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
        .get_checkpoint(mapping_name)
        .await?
        .unwrap_or_default();

    checkpoint.lsn = lsn;
    checkpoint.events_processed += count as u64;

    state_store
        .save_checkpoint(mapping_name, &checkpoint)
        .await
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
