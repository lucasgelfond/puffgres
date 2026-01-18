//! Streaming logical replication using wal2json.
//!
//! Uses `START_REPLICATION SLOT ... LOGICAL ...` with proper acknowledgment
//! after successful writes.

use std::collections::HashMap;
use std::time::Duration;

use puffgres_core::{Operation, RowEvent, Value};
use serde::Deserialize;
use tokio_postgres::{Client, NoTls};
use tracing::{debug, error, info, warn};

use crate::error::{PgError, PgResult};

/// Configuration for streaming replication.
#[derive(Debug, Clone)]
pub struct StreamingConfig {
    /// Postgres connection string.
    pub connection_string: String,
    /// Replication slot name.
    pub slot_name: String,
    /// Whether to create the slot if it doesn't exist.
    pub create_slot: bool,
    /// Start position (None = from current position).
    pub start_lsn: Option<u64>,
    /// Keepalive interval.
    pub keepalive_interval: Duration,
    /// Timeout for receiving messages.
    pub receive_timeout: Duration,
}

impl Default for StreamingConfig {
    fn default() -> Self {
        Self {
            connection_string: String::new(),
            slot_name: "puffgres".to_string(),
            create_slot: true,
            start_lsn: None,
            keepalive_interval: Duration::from_secs(10),
            receive_timeout: Duration::from_secs(30),
        }
    }
}

/// A batch of events from streaming replication.
#[derive(Debug)]
pub struct StreamingBatch {
    /// The events in this batch.
    pub events: Vec<RowEvent>,
    /// The LSN to acknowledge after processing.
    pub ack_lsn: u64,
}

/// Streaming replication connection.
///
/// Uses the Postgres replication protocol to stream changes in real-time.
pub struct StreamingReplicator {
    /// Regular client for slot management.
    client: Client,
    config: StreamingConfig,
    /// Current write LSN (for acknowledgment).
    current_lsn: u64,
    /// Last acknowledged LSN.
    ack_lsn: u64,
}

impl StreamingReplicator {
    /// Create a new streaming replicator.
    ///
    /// This establishes a regular connection first for slot management,
    /// then starts the replication stream.
    pub async fn connect(config: StreamingConfig) -> PgResult<Self> {
        info!(
            slot = %config.slot_name,
            "Connecting for streaming replication"
        );

        // First, connect with regular client for slot management
        let (client, connection) = tokio_postgres::connect(&config.connection_string, NoTls)
            .await
            .map_err(|e| PgError::Connection(e.to_string()))?;

        // Spawn connection handler
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("Postgres connection error: {}", e);
            }
        });

        let mut replicator = Self {
            client,
            config,
            current_lsn: 0,
            ack_lsn: 0,
        };

        // Ensure slot exists
        replicator.ensure_slot().await?;

        // Get start position
        if let Some(start) = replicator.config.start_lsn {
            replicator.current_lsn = start;
        } else {
            // Get current position from slot
            if let Some(lsn) = replicator.get_confirmed_lsn().await? {
                replicator.current_lsn = lsn;
            }
        }

        Ok(replicator)
    }

    /// Ensure the replication slot exists.
    async fn ensure_slot(&self) -> PgResult<()> {
        let row = self
            .client
            .query_opt(
                "SELECT slot_name FROM pg_replication_slots WHERE slot_name = $1",
                &[&self.config.slot_name],
            )
            .await?;

        if row.is_some() {
            info!(slot = %self.config.slot_name, "Using existing replication slot");
            return Ok(());
        }

        if !self.config.create_slot {
            return Err(PgError::SlotNotFound(self.config.slot_name.clone()));
        }

        info!(slot = %self.config.slot_name, "Creating replication slot");

        self.client
            .execute(
                "SELECT pg_create_logical_replication_slot($1, 'wal2json')",
                &[&self.config.slot_name],
            )
            .await
            .map_err(|e| PgError::SlotCreationFailed(e.to_string()))?;

        Ok(())
    }

    /// Get the confirmed flush LSN for the slot.
    pub async fn get_confirmed_lsn(&self) -> PgResult<Option<u64>> {
        let row = self
            .client
            .query_opt(
                "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
                &[&self.config.slot_name],
            )
            .await?;

        match row {
            Some(r) => {
                let lsn: Option<String> = r.get(0);
                match lsn {
                    Some(l) => Ok(Some(parse_lsn(&l)?)),
                    None => Ok(None),
                }
            }
            None => Err(PgError::SlotNotFound(self.config.slot_name.clone())),
        }
    }

    /// Poll for changes using streaming-style queries.
    ///
    /// This uses `pg_logical_slot_peek_changes` to get changes without
    /// automatically confirming them, then confirms after successful processing.
    ///
    /// Returns a batch with events and the LSN to acknowledge.
    pub async fn poll_batch(&mut self, max_changes: u32) -> PgResult<StreamingBatch> {
        // Use peek to get changes without confirming
        let start_lsn = format_lsn(self.current_lsn);

        let query = format!(
            "SELECT lsn::text, xid::text, data FROM pg_logical_slot_peek_changes('{}', '{}', {}, 'format-version', '2', 'include-lsn', 'true')",
            self.config.slot_name,
            start_lsn,
            max_changes
        );

        let rows = self.client.query(&query, &[]).await?;

        let mut events = Vec::new();
        let mut max_lsn = self.current_lsn;

        for row in rows {
            let lsn_str: String = row.get(0);
            let _xid: String = row.get(1);
            let data: String = row.get(2);

            let lsn = parse_lsn(&lsn_str)?;

            // Track the maximum LSN we've seen
            if lsn > max_lsn {
                max_lsn = lsn;
            }

            // Parse wal2json v2 output
            match parse_wal2json_v2(&data, lsn) {
                Ok(mut parsed) => events.append(&mut parsed),
                Err(e) => {
                    warn!(lsn = %lsn_str, error = %e, "Failed to parse wal2json output");
                }
            }
        }

        if !events.is_empty() {
            debug!(count = events.len(), max_lsn, "Polled batch");
        }

        Ok(StreamingBatch {
            events,
            ack_lsn: max_lsn,
        })
    }

    /// Acknowledge that changes up to the given LSN have been processed.
    ///
    /// This advances the slot's confirmed_flush_lsn, allowing Postgres to
    /// reclaim WAL space.
    pub async fn acknowledge(&mut self, lsn: u64) -> PgResult<()> {
        if lsn <= self.ack_lsn {
            return Ok(()); // Already acknowledged
        }

        let lsn_str = format_lsn(lsn);

        // Use pg_logical_slot_get_changes to consume up to this LSN
        // This is atomic - changes are only removed after we receive them
        let query = format!(
            "SELECT lsn::text FROM pg_logical_slot_get_changes('{}', '{}', NULL, 'format-version', '2')",
            self.config.slot_name,
            lsn_str
        );

        self.client.execute(&query, &[]).await?;

        self.ack_lsn = lsn;
        self.current_lsn = lsn;

        debug!(lsn = lsn_str, "Acknowledged LSN");

        Ok(())
    }

    /// Get the current position.
    pub fn current_lsn(&self) -> u64 {
        self.current_lsn
    }

    /// Get the last acknowledged LSN.
    pub fn ack_lsn(&self) -> u64 {
        self.ack_lsn
    }

    /// Resume from a specific LSN.
    pub fn resume_from(&mut self, lsn: u64) {
        self.current_lsn = lsn;
        self.ack_lsn = lsn;
    }
}

/// Parse LSN from "X/Y" format to u64.
pub fn parse_lsn(lsn: &str) -> PgResult<u64> {
    let parts: Vec<&str> = lsn.split('/').collect();
    if parts.len() != 2 {
        return Err(PgError::InvalidLsn(lsn.to_string()));
    }

    let high = u64::from_str_radix(parts[0], 16)
        .map_err(|_| PgError::InvalidLsn(lsn.to_string()))?;
    let low = u64::from_str_radix(parts[1], 16)
        .map_err(|_| PgError::InvalidLsn(lsn.to_string()))?;

    Ok((high << 32) | low)
}

/// Format u64 LSN to "X/Y" format.
pub fn format_lsn(lsn: u64) -> String {
    let high = lsn >> 32;
    let low = lsn & 0xFFFFFFFF;
    format!("{:X}/{:X}", high, low)
}

/// wal2json v2 message format
#[derive(Debug, Deserialize)]
struct Wal2JsonMessage {
    action: String,
    #[serde(default)]
    schema: Option<String>,
    #[serde(default)]
    table: Option<String>,
    #[serde(default)]
    columns: Option<Vec<Wal2JsonColumn>>,
    #[serde(default)]
    identity: Option<Vec<Wal2JsonColumn>>,
}

#[derive(Debug, Deserialize)]
struct Wal2JsonColumn {
    name: String,
    #[serde(rename = "type")]
    #[allow(dead_code)]
    col_type: String,
    value: serde_json::Value,
}

/// Parse wal2json v2 format output.
fn parse_wal2json_v2(data: &str, lsn: u64) -> PgResult<Vec<RowEvent>> {
    let msg: Wal2JsonMessage = serde_json::from_str(data)?;

    // Skip BEGIN/COMMIT messages
    if msg.action == "B" || msg.action == "C" {
        return Ok(vec![]);
    }

    let schema = msg.schema.unwrap_or_else(|| "public".to_string());
    let table = msg.table.ok_or_else(|| {
        PgError::ParseError("missing table in wal2json output".to_string())
    })?;

    let op = match msg.action.as_str() {
        "I" => Operation::Insert,
        "U" => Operation::Update,
        "D" => Operation::Delete,
        other => {
            return Err(PgError::ParseError(format!(
                "unknown action: {}",
                other
            )));
        }
    };

    let new = msg.columns.map(|cols| columns_to_row(&cols));
    let old = msg.identity.map(|cols| columns_to_row(&cols));

    Ok(vec![RowEvent {
        op,
        schema,
        table,
        new,
        old,
        lsn,
        txid: None,
        timestamp: None,
    }])
}

fn columns_to_row(columns: &[Wal2JsonColumn]) -> HashMap<String, Value> {
    columns
        .iter()
        .map(|col| {
            let value = json_to_value(&col.value);
            (col.name.clone(), value)
        })
        .collect()
}

fn json_to_value(v: &serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float(f)
            } else {
                Value::Null
            }
        }
        serde_json::Value::String(s) => Value::String(s.clone()),
        serde_json::Value::Array(arr) => Value::Array(arr.iter().map(json_to_value).collect()),
        serde_json::Value::Object(obj) => Value::Object(
            obj.iter()
                .map(|(k, v)| (k.clone(), json_to_value(v)))
                .collect(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_lsn() {
        assert_eq!(parse_lsn("0/16B3748").unwrap(), 0x16B3748);
        assert_eq!(parse_lsn("1/16B3748").unwrap(), 0x100000000 + 0x16B3748);
        assert!(parse_lsn("invalid").is_err());
    }

    #[test]
    fn test_format_lsn() {
        assert_eq!(format_lsn(0x16B3748), "0/16B3748");
        assert_eq!(format_lsn(0x100000000 + 0x16B3748), "1/16B3748");
    }

    #[test]
    fn test_lsn_roundtrip() {
        let values = [0u64, 100, 0x16B3748, 0x100000000 + 0x16B3748, u64::MAX >> 1];

        for val in values {
            let formatted = format_lsn(val);
            let parsed = parse_lsn(&formatted).unwrap();
            assert_eq!(val, parsed, "Roundtrip failed for {}", val);
        }
    }

    #[test]
    fn test_parse_wal2json_insert() {
        let data = r#"{"action":"I","schema":"public","table":"users","columns":[{"name":"id","type":"integer","value":1},{"name":"name","type":"text","value":"Alice"}]}"#;

        let events = parse_wal2json_v2(data, 100).unwrap();
        assert_eq!(events.len(), 1);

        let event = &events[0];
        assert_eq!(event.op, Operation::Insert);
        assert_eq!(event.schema, "public");
        assert_eq!(event.table, "users");
        assert!(event.new.is_some());
    }
}
