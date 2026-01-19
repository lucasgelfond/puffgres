use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};
use tracing::{debug, error, info, warn};

use puffgres_core::{
    extract_id, Action, BatchConfig, Batcher, DocumentId, IdentityTransformer, JsTransformer,
    Mapping, Router, TransformType, Transformer, Value, WriteRequest,
};
use puffgres_pg::{format_lsn, PostgresStateStore, ReplicationStream, ReplicationStreamConfig};

use crate::config::ProjectConfig;
use crate::env::{get_max_retries, get_transform_batch_size, get_upload_batch_size};

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

/// Run the CDC replication loop using true push-based streaming.
///
/// This uses pgwire-replication to receive changes in real-time via the
/// PostgreSQL streaming replication protocol. Changes arrive immediately
/// as they're committed - no polling required.
pub async fn run_cdc_loop(
    config: &ProjectConfig,
    mappings: Vec<Mapping>,
    slot: &str,
    publication: &str,
    create_slot: bool,
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

    // Build list of tables for publication
    let publication_tables: Vec<String> = mappings
        .iter()
        .map(|m| format!("{}.{}", m.source.schema, m.source.table))
        .collect();

    // Initialize streaming replication
    let repl_config = ReplicationStreamConfig {
        connection_string: config.postgres_connection_string()?,
        slot_name: slot.to_string(),
        publication_name: publication.to_string(),
        create_slot,
        create_publication: true,
        publication_tables,
        start_lsn,
        ..Default::default()
    };

    // Use state_store's connection for control plane operations (slot/publication setup)
    // pgwire-replication handles only the replication plane
    let mut stream = ReplicationStream::connect(repl_config, state_store.client())
        .await
        .context("Failed to connect for streaming replication")?;

    let tp_client = rs_puff::Client::new(config.turbopuffer_api_key()?);
    let router = Router::new(mappings.clone());

    let transformers: Vec<_> = mappings
        .iter()
        .map(|m| (m.name.clone(), create_transformer(m)))
        .collect();

    // Load batch configuration from environment
    let transform_batch_size = get_transform_batch_size();
    let upload_batch_size = get_upload_batch_size();
    let max_retries = get_max_retries();

    info!(
        slot = slot,
        publication = publication,
        mappings = mappings.len(),
        transform_batch_size,
        upload_batch_size,
        max_retries,
        "Starting push-based streaming CDC"
    );

    let mut total_events: u64 = 0;
    let mut batchers: HashMap<String, Batcher> = HashMap::new();
    let batch_config = BatchConfig::with_max_rows(transform_batch_size);

    // Main streaming loop - events arrive as they happen (no polling)
    while let Some(batch) = stream.recv_batch().await? {
        if batch.events.is_empty() {
            // Empty transaction (e.g., only system tables changed)
            stream.acknowledge(batch.ack_lsn);
            continue;
        }

        debug!(count = batch.events.len(), "Processing transaction batch");

        // Process each event
        for event in &batch.events {
            let matched = router.route(event);

            for mapping in matched {
                let batcher = batchers
                    .entry(mapping.namespace.clone())
                    .or_insert_with(|| Batcher::new(batch_config.clone()));

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
                    if let Err(e) = flush_batch(
                        &tp_client,
                        &state_store,
                        &mapping.name,
                        request,
                        upload_batch_size,
                        max_retries,
                    )
                    .await
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

                if let Err(e) = flush_batch(
                    &tp_client,
                    &state_store,
                    mapping_name,
                    request,
                    upload_batch_size,
                    max_retries,
                )
                .await
                {
                    error!(namespace = %namespace, error = %e, "Failed to flush batch");
                }
            }
        }

        // Acknowledge after successful processing
        stream.acknowledge(batch.ack_lsn);

        if total_events % 100 == 0 && total_events > 0 {
            info!(
                total_events = total_events,
                lsn = format_lsn(stream.ack_lsn()),
                "Progress"
            );
        }
    }

    info!("Replication stream ended");
    Ok(())
}

async fn flush_batch(
    client: &rs_puff::Client,
    state_store: &PostgresStateStore,
    mapping_name: &str,
    request: WriteRequest,
    upload_batch_size: usize,
    max_retries: u32,
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
            row.insert(
                "__source_lsn".to_string(),
                serde_json::Value::Number(request.lsn.into()),
            );
            row
        })
        .collect();

    // Build all delete IDs
    let all_deletes: Vec<serde_json::Value> =
        request.deletes.iter().map(convert_doc_id_to_json).collect();

    // Upload upserts in batches
    for chunk in all_upsert_rows.chunks(upload_batch_size) {
        let params = rs_puff::WriteParams {
            upsert_rows: Some(chunk.to_vec()),
            deletes: None,
            distance_metric: request.distance_metric,
            ..Default::default()
        };

        write_with_retry(client, &request.namespace, params, max_retries).await?;
    }

    // Upload deletes in batches
    for chunk in all_deletes.chunks(upload_batch_size) {
        let params = rs_puff::WriteParams {
            upsert_rows: None,
            deletes: Some(chunk.to_vec()),
            distance_metric: request.distance_metric,
            ..Default::default()
        };

        write_with_retry(client, &request.namespace, params, max_retries).await?;
    }

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
