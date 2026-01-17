use std::collections::HashMap;

use puffgres_core::{Operation, RowEvent, Value};
use serde::Deserialize;
use tokio_postgres::{Client, NoTls};
use tracing::{debug, info, warn};

use crate::error::{PgError, PgResult};

/// Configuration for the wal2json poller.
#[derive(Debug, Clone)]
pub struct PollerConfig {
    /// Postgres connection string.
    pub connection_string: String,
    /// Replication slot name.
    pub slot_name: String,
    /// Whether to create the slot if it doesn't exist.
    pub create_slot: bool,
    /// Maximum number of changes to fetch per poll.
    pub max_changes: u32,
}

impl Default for PollerConfig {
    fn default() -> Self {
        Self {
            connection_string: String::new(),
            slot_name: "puffgres".to_string(),
            create_slot: true,
            max_changes: 1000,
        }
    }
}

/// Polls Postgres for changes using wal2json.
pub struct Wal2JsonPoller {
    client: Client,
    config: PollerConfig,
}

impl Wal2JsonPoller {
    /// Connect to Postgres and create a poller.
    pub async fn connect(config: PollerConfig) -> PgResult<Self> {
        info!(
            slot = %config.slot_name,
            "Connecting to Postgres"
        );

        let (client, connection) = tokio_postgres::connect(&config.connection_string, NoTls)
            .await
            .map_err(|e| PgError::Connection(e.to_string()))?;

        // Spawn connection handler
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("Postgres connection error: {}", e);
            }
        });

        let poller = Self { client, config };

        // Ensure slot exists
        poller.ensure_slot().await?;

        Ok(poller)
    }

    /// Ensure the replication slot exists.
    async fn ensure_slot(&self) -> PgResult<()> {
        // Check if slot exists
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

        // Create the slot
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

    /// Poll for changes and return RowEvents.
    pub async fn poll(&self) -> PgResult<Vec<RowEvent>> {
        let query = format!(
            "SELECT lsn::text, xid::text, data FROM pg_logical_slot_get_changes('{}', NULL, {}, 'format-version', '2', 'include-lsn', 'true')",
            self.config.slot_name,
            self.config.max_changes
        );

        let rows = self.client.query(&query, &[]).await?;

        let mut events = Vec::new();

        for row in rows {
            let lsn: String = row.get(0);
            let _xid: String = row.get(1);
            let data: String = row.get(2);

            let lsn_num = parse_lsn(&lsn)?;

            // Parse wal2json v2 output
            match parse_wal2json_v2(&data, lsn_num) {
                Ok(mut parsed_events) => {
                    events.append(&mut parsed_events);
                }
                Err(e) => {
                    warn!(lsn = %lsn, error = %e, "Failed to parse wal2json output");
                }
            }
        }

        if !events.is_empty() {
            debug!(count = events.len(), "Polled events");
        }

        Ok(events)
    }

    /// Get the current confirmed LSN.
    pub async fn get_confirmed_lsn(&self) -> PgResult<Option<u64>> {
        let row = self
            .client
            .query_opt(
                "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1",
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

    /// Drop the replication slot (for cleanup).
    pub async fn drop_slot(&self) -> PgResult<()> {
        self.client
            .execute(
                "SELECT pg_drop_replication_slot($1)",
                &[&self.config.slot_name],
            )
            .await?;
        Ok(())
    }
}

/// Parse LSN from "X/Y" format to u64.
fn parse_lsn(lsn: &str) -> PgResult<u64> {
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
    #[allow(dead_code)] // Required for serde deserialization
    col_type: String,
    value: serde_json::Value,
}

/// Parse wal2json v2 format output.
fn parse_wal2json_v2(data: &str, lsn: u64) -> PgResult<Vec<RowEvent>> {
    // wal2json v2 outputs one JSON object per line for each change
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

    // Convert columns to row map
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
    fn test_parse_wal2json_insert() {
        let data = r#"{"action":"I","schema":"public","table":"users","columns":[{"name":"id","type":"integer","value":1},{"name":"name","type":"text","value":"Alice"}]}"#;

        let events = parse_wal2json_v2(data, 100).unwrap();
        assert_eq!(events.len(), 1);

        let event = &events[0];
        assert_eq!(event.op, Operation::Insert);
        assert_eq!(event.schema, "public");
        assert_eq!(event.table, "users");
        assert!(event.new.is_some());

        let new = event.new.as_ref().unwrap();
        assert_eq!(new.get("id"), Some(&Value::Int(1)));
        assert_eq!(new.get("name"), Some(&Value::String("Alice".into())));
    }

    #[test]
    fn test_parse_wal2json_update() {
        let data = r#"{"action":"U","schema":"public","table":"users","columns":[{"name":"id","type":"integer","value":1},{"name":"name","type":"text","value":"Bob"}],"identity":[{"name":"id","type":"integer","value":1}]}"#;

        let events = parse_wal2json_v2(data, 100).unwrap();
        assert_eq!(events.len(), 1);

        let event = &events[0];
        assert_eq!(event.op, Operation::Update);
        assert!(event.new.is_some());
        assert!(event.old.is_some());
    }

    #[test]
    fn test_parse_wal2json_delete() {
        let data = r#"{"action":"D","schema":"public","table":"users","identity":[{"name":"id","type":"integer","value":1}]}"#;

        let events = parse_wal2json_v2(data, 100).unwrap();
        assert_eq!(events.len(), 1);

        let event = &events[0];
        assert_eq!(event.op, Operation::Delete);
        assert!(event.new.is_none());
        assert!(event.old.is_some());
    }

    #[test]
    fn test_parse_wal2json_begin_commit() {
        let begin = r#"{"action":"B"}"#;
        let commit = r#"{"action":"C"}"#;

        assert!(parse_wal2json_v2(begin, 100).unwrap().is_empty());
        assert!(parse_wal2json_v2(commit, 100).unwrap().is_empty());
    }
}
