//! True push-based streaming replication client using pgwire-replication.

use std::collections::HashMap;
use std::time::Duration;

use pgwire_replication::{ReplicationClient, ReplicationConfig as PgwireConfig, ReplicationEvent};
use puffgres_core::{Operation, RowEvent, Value};
use tracing::{debug, info, warn};

use tokio_postgres::Client;

use super::lsn::{format_lsn, parse_lsn};
use super::pgoutput::{ColumnInfo, ColumnValue, PgOutputDecoder, PgOutputMessage};
use super::publication::ensure_publication;
use super::relation_cache::RelationCache;
use super::slot::{ensure_slot, get_confirmed_flush_lsn};
use super::validation::validate_all_tables_readable;
use crate::error::{PgError, PgResult};

/// Configuration for streaming replication.
#[derive(Debug, Clone)]
pub struct ReplicationStreamConfig {
    /// Postgres connection string.
    pub connection_string: String,
    /// Replication slot name.
    pub slot_name: String,
    /// Publication name (required for pgoutput).
    pub publication_name: String,
    /// Whether to create the slot if it doesn't exist.
    pub create_slot: bool,
    /// Whether to create the publication if it doesn't exist.
    pub create_publication: bool,
    /// Tables to include in the publication (if creating).
    /// Format: "schema.table"
    pub publication_tables: Vec<String>,
    /// Start position (None = from current confirmed_flush_lsn).
    pub start_lsn: Option<u64>,
    /// Status update interval for keepalives.
    pub status_interval: Duration,
}

impl Default for ReplicationStreamConfig {
    fn default() -> Self {
        Self {
            connection_string: String::new(),
            slot_name: "puffgres".to_string(),
            publication_name: "puffgres_pub".to_string(),
            create_slot: true,
            create_publication: true,
            publication_tables: vec![],
            start_lsn: None,
            status_interval: Duration::from_secs(10),
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

/// State for the current transaction being assembled.
struct TransactionState {
    xid: u32,
    timestamp: i64,
    events: Vec<RowEvent>,
}

/// True push-based streaming replication client.
///
/// Uses pgwire-replication to receive changes in real-time via the
/// PostgreSQL streaming replication protocol.
pub struct ReplicationStream {
    /// The underlying pgwire-replication client.
    client: ReplicationClient,
    /// Relation cache for OID -> table name mapping.
    relation_cache: RelationCache,
    /// pgoutput decoder.
    decoder: PgOutputDecoder,
    /// Current transaction being assembled.
    current_txn: Option<TransactionState>,
    /// Last acknowledged LSN.
    ack_lsn: u64,
}

impl ReplicationStream {
    /// Connect and start streaming.
    ///
    /// This will:
    /// 1. Ensure the replication slot exists (create if needed)
    /// 2. Ensure the publication exists (create if needed)
    /// 3. Start the streaming replication connection
    ///
    /// The `control_client` is a tokio-postgres Client used for control plane operations
    /// (creating slots, publications, querying LSN). This should be separate from the
    /// replication connection. pgwire-replication handles only the replication plane.
    pub async fn connect(config: ReplicationStreamConfig, control_client: &Client) -> PgResult<Self> {
        info!(
            slot = %config.slot_name,
            publication = %config.publication_name,
            "Connecting for streaming replication"
        );

        // First ensure prerequisites using the control plane connection
        Self::ensure_prerequisites(&config, control_client).await?;

        // Parse connection string to extract host, port, user, password, database
        let conn_params = Self::parse_connection_string(&config.connection_string)?;

        debug!(
            host = %conn_params.host,
            port = conn_params.port,
            user = %conn_params.user,
            database = %conn_params.database,
            sslmode = ?conn_params.sslmode,
            password_len = conn_params.password.len(),
            "Parsed connection parameters for pgwire-replication"
        );

        // Get start LSN
        let start_lsn = if let Some(lsn) = config.start_lsn {
            pgwire_replication::Lsn::from(lsn)
        } else {
            // Get current confirmed_flush_lsn from slot
            let lsn = Self::get_confirmed_lsn(&config, control_client).await?;
            pgwire_replication::Lsn::from(lsn.unwrap_or(0))
        };

        info!(start_lsn = %format_lsn(start_lsn.into()), "Starting replication stream");

        // Build TLS config from sslmode
        let tls_mode_str = conn_params.sslmode.as_deref().unwrap_or("disabled");
        debug!(sslmode = %tls_mode_str, "Configuring TLS for pgwire-replication");

        let tls = match conn_params.sslmode.as_deref() {
            Some("require") => pgwire_replication::TlsConfig::require(),
            Some("verify-ca") => pgwire_replication::TlsConfig::verify_ca(None),
            Some("verify-full") => pgwire_replication::TlsConfig::verify_full(None),
            _ => pgwire_replication::TlsConfig::disabled(),
        };

        // Build pgwire-replication config
        let pgwire_config = PgwireConfig {
            host: conn_params.host,
            port: conn_params.port,
            user: conn_params.user,
            password: conn_params.password,
            database: conn_params.database,
            slot: config.slot_name.clone(),
            publication: config.publication_name.clone(),
            start_lsn,
            stop_at_lsn: None,
            status_interval: config.status_interval,
            idle_wakeup_interval: Duration::from_secs(10),
            buffer_events: 8192,
            tls,
        };

        debug!(
            host = %pgwire_config.host,
            port = pgwire_config.port,
            user = %pgwire_config.user,
            database = %pgwire_config.database,
            slot = %pgwire_config.slot,
            publication = %pgwire_config.publication,
            "Attempting pgwire-replication connection"
        );

        let client = ReplicationClient::connect(pgwire_config).await.map_err(|e| {
            warn!(
                error = %e,
                "pgwire-replication connection failed"
            );
            PgError::Replication(e.to_string())
        })?;

        info!("pgwire-replication connection established successfully");

        Ok(Self {
            client,
            relation_cache: RelationCache::new(),
            decoder: PgOutputDecoder::new(),
            current_txn: None,
            ack_lsn: start_lsn.into(),
        })
    }

    /// Receive the next batch of row events.
    ///
    /// This blocks until a complete transaction is received or the stream ends.
    /// Returns None if the stream has ended.
    pub async fn recv_batch(&mut self) -> PgResult<Option<StreamingBatch>> {
        info!("Waiting for replication events...");

        loop {
            let event = self
                .client
                .recv()
                .await
                .map_err(|e| PgError::Replication(e.to_string()))?;

            debug!("Received event: {:?}", event);

            let Some(event) = event else {
                info!("Replication stream ended (recv returned None)");
                return Ok(None);
            };

            match event {
                ReplicationEvent::XLogData { wal_end, data, .. } => {
                    let wal_end_u64: u64 = wal_end.into();

                    // Decode pgoutput message
                    let msg = match self.decoder.decode(&data) {
                        Ok(m) => m,
                        Err(e) => {
                            warn!(error = %e, "Failed to decode pgoutput message");
                            continue;
                        }
                    };

                    debug!(wal_end = %format_lsn(wal_end_u64), msg = ?msg, "Decoded XLogData");

                    // Handle Begin/Commit from XLogData (pgoutput encodes them here)
                    match &msg {
                        PgOutputMessage::Begin(begin) => {
                            info!(xid = begin.xid, "Transaction begin");
                            self.current_txn = Some(TransactionState {
                                xid: begin.xid,
                                timestamp: begin.timestamp,
                                events: Vec::new(),
                            });
                        }
                        PgOutputMessage::Commit(commit) => {
                            info!(lsn = %format_lsn(commit.end_lsn), "Transaction commit");
                            if let Some(txn) = self.current_txn.take() {
                                return Ok(Some(StreamingBatch {
                                    events: txn.events,
                                    ack_lsn: commit.end_lsn,
                                }));
                            }
                        }
                        PgOutputMessage::Relation(rel) => {
                            debug!(table = %rel.name, "Relation metadata");
                            self.relation_cache.update(rel);
                        }
                        PgOutputMessage::Insert(insert) => {
                            if self.current_txn.is_some() {
                                if let Ok(event) = self.to_row_event_insert(insert, wal_end_u64) {
                                    info!(op = "insert", table = %event.table, "Row change");
                                    self.current_txn.as_mut().unwrap().events.push(event);
                                }
                            }
                        }
                        PgOutputMessage::Update(update) => {
                            if self.current_txn.is_some() {
                                if let Ok(event) = self.to_row_event_update(update, wal_end_u64) {
                                    info!(op = "update", table = %event.table, "Row change");
                                    self.current_txn.as_mut().unwrap().events.push(event);
                                }
                            }
                        }
                        PgOutputMessage::Delete(delete) => {
                            if self.current_txn.is_some() {
                                if let Ok(event) = self.to_row_event_delete(delete, wal_end_u64) {
                                    info!(op = "delete", table = %event.table, "Row change");
                                    self.current_txn.as_mut().unwrap().events.push(event);
                                }
                            }
                        }
                        _ => {}
                    }
                }
                ReplicationEvent::KeepAlive { wal_end, reply_requested, .. } => {
                    debug!(wal_end = %format_lsn(wal_end.into()), reply_requested, "Keepalive");
                }
                ReplicationEvent::StoppedAt { reached } => {
                    info!(lsn = %format_lsn(reached.into()), "Stream stopped");
                    return Ok(None);
                }
                // These are alternative event types - pgwire-replication may send Begin/Commit
                // either as separate events or encoded in XLogData depending on version
                ReplicationEvent::Begin { xid, commit_time_micros, .. } => {
                    info!(xid = xid, "Transaction begin (protocol event)");
                    self.current_txn = Some(TransactionState {
                        xid,
                        timestamp: commit_time_micros,
                        events: Vec::new(),
                    });
                }
                ReplicationEvent::Commit { end_lsn, .. } => {
                    let end_lsn_u64: u64 = end_lsn.into();
                    info!(lsn = %format_lsn(end_lsn_u64), "Transaction commit (protocol event)");
                    if let Some(txn) = self.current_txn.take() {
                        return Ok(Some(StreamingBatch {
                            events: txn.events,
                            ack_lsn: end_lsn_u64,
                        }));
                    }
                }
            }
        }
    }

    fn to_row_event_insert(
        &self,
        insert: &super::pgoutput::InsertMessage,
        lsn: u64,
    ) -> PgResult<RowEvent> {
        let relation = self
            .relation_cache
            .get(insert.relation_id)
            .ok_or(PgError::RelationNotFound(insert.relation_id))?;

        let new = self.tuple_to_row_map(&insert.tuple, &relation.columns)?;
        let (txid, timestamp) = self.current_txn_info();

        Ok(RowEvent {
            op: Operation::Insert,
            schema: relation.namespace.clone(),
            table: relation.name.clone(),
            new: Some(new),
            old: None,
            lsn,
            txid,
            timestamp,
        })
    }

    fn to_row_event_update(
        &self,
        update: &super::pgoutput::UpdateMessage,
        lsn: u64,
    ) -> PgResult<RowEvent> {
        let relation = self
            .relation_cache
            .get(update.relation_id)
            .ok_or(PgError::RelationNotFound(update.relation_id))?;

        let new = self.tuple_to_row_map(&update.new_tuple, &relation.columns)?;
        let old = update
            .old_tuple
            .as_ref()
            .map(|t| self.tuple_to_row_map(t, &relation.columns))
            .transpose()?;
        let (txid, timestamp) = self.current_txn_info();

        Ok(RowEvent {
            op: Operation::Update,
            schema: relation.namespace.clone(),
            table: relation.name.clone(),
            new: Some(new),
            old,
            lsn,
            txid,
            timestamp,
        })
    }

    fn to_row_event_delete(
        &self,
        delete: &super::pgoutput::DeleteMessage,
        lsn: u64,
    ) -> PgResult<RowEvent> {
        let relation = self
            .relation_cache
            .get(delete.relation_id)
            .ok_or(PgError::RelationNotFound(delete.relation_id))?;

        let old = self.tuple_to_row_map(&delete.old_tuple, &relation.columns)?;
        let (txid, timestamp) = self.current_txn_info();

        Ok(RowEvent {
            op: Operation::Delete,
            schema: relation.namespace.clone(),
            table: relation.name.clone(),
            new: None,
            old: Some(old),
            lsn,
            txid,
            timestamp,
        })
    }

    fn current_txn_info(&self) -> (Option<u64>, Option<String>) {
        self.current_txn.as_ref().map_or((None, None), |txn| {
            (
                Some(txn.xid as u64),
                Some(format_pg_timestamp(txn.timestamp)),
            )
        })
    }

    fn tuple_to_row_map(
        &self,
        tuple: &super::pgoutput::TupleData,
        columns: &[ColumnInfo],
    ) -> PgResult<HashMap<String, Value>> {
        let mut row = HashMap::new();

        for (col_value, col_info) in tuple.columns.iter().zip(columns.iter()) {
            let value = match col_value {
                ColumnValue::Null => Value::Null,
                ColumnValue::Unchanged => continue, // Skip unchanged TOAST values
                ColumnValue::Text(s) => parse_text_value(s, col_info.type_oid),
                ColumnValue::Binary(_) => {
                    // Binary format not commonly used in pgoutput, treat as string
                    Value::String("<binary>".to_string())
                }
            };
            row.insert(col_info.name.clone(), value);
        }

        Ok(row)
    }

    /// Acknowledge that events up to the given LSN have been processed.
    pub fn acknowledge(&mut self, lsn: u64) {
        if lsn > self.ack_lsn {
            debug!(
                lsn = %format_lsn(lsn),
                prev_ack = %format_lsn(self.ack_lsn),
                "Acknowledging LSN"
            );
            self.client
                .update_applied_lsn(pgwire_replication::Lsn::from(lsn));
            self.ack_lsn = lsn;
        }
    }

    /// Get the last acknowledged LSN.
    pub fn ack_lsn(&self) -> u64 {
        self.ack_lsn
    }

    /// Ensure replication slot and publication exist.
    async fn ensure_prerequisites(config: &ReplicationStreamConfig, client: &Client) -> PgResult<()> {
        // First, validate that we can actually read from the tables
        // This catches issues where tables don't exist or permissions are wrong
        if !config.publication_tables.is_empty() {
            validate_all_tables_readable(client, &config.publication_tables).await?;
        }

        // Ensure slot exists with correct plugin
        ensure_slot(client, &config.slot_name, config.create_slot).await?;

        // Ensure publication exists with correct tables
        ensure_publication(
            client,
            &config.publication_name,
            &config.publication_tables,
            config.create_publication,
        )
        .await?;

        Ok(())
    }

    /// Get confirmed_flush_lsn for the slot.
    async fn get_confirmed_lsn(config: &ReplicationStreamConfig, client: &Client) -> PgResult<Option<u64>> {
        let lsn_str = get_confirmed_flush_lsn(client, &config.slot_name).await?;
        match lsn_str {
            Some(l) => Ok(Some(parse_lsn(&l)?)),
            None => Ok(None),
        }
    }

    /// Parse connection string into components.
    fn parse_connection_string(conn_str: &str) -> PgResult<ConnectionParams> {
        // Handle both URL format (postgres://...) and key-value format
        if conn_str.starts_with("postgres://") || conn_str.starts_with("postgresql://") {
            Self::parse_url_connection_string(conn_str)
        } else {
            Self::parse_keyvalue_connection_string(conn_str)
        }
    }

    fn parse_url_connection_string(conn_str: &str) -> PgResult<ConnectionParams> {
        // postgres://user:password@host:port/database?sslmode=require
        let url = url::Url::parse(conn_str)
            .map_err(|e| PgError::Connection(format!("Invalid connection URL: {}", e)))?;

        let host = url.host_str().unwrap_or("localhost").to_string();
        let port = url.port().unwrap_or(5432);
        // URL-decode username and password since they may contain percent-encoded special characters
        let user = percent_encoding::percent_decode_str(url.username())
            .decode_utf8()
            .map(|s| s.to_string())
            .unwrap_or_else(|_| url.username().to_string());
        let password = url
            .password()
            .map(|p| {
                percent_encoding::percent_decode_str(p)
                    .decode_utf8()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|_| p.to_string())
            })
            .unwrap_or_default();
        let database = url.path().trim_start_matches('/').to_string();
        let sslmode = url.query_pairs().find(|(k, _)| k == "sslmode").map(|(_, v)| v.to_string());

        Ok(ConnectionParams {
            host,
            port,
            user,
            password,
            database,
            sslmode,
        })
    }

    fn parse_keyvalue_connection_string(conn_str: &str) -> PgResult<ConnectionParams> {
        // host=localhost port=5432 user=postgres password=... dbname=... sslmode=require
        let mut host = "localhost".to_string();
        let mut port = 5432u16;
        let mut user = "postgres".to_string();
        let mut password = String::new();
        let mut database = "postgres".to_string();
        let mut sslmode = None;

        for part in conn_str.split_whitespace() {
            if let Some((key, value)) = part.split_once('=') {
                match key {
                    "host" => host = value.to_string(),
                    "port" => {
                        port = value
                            .parse()
                            .map_err(|_| PgError::Connection("Invalid port".into()))?
                    }
                    "user" => user = value.to_string(),
                    "password" => password = value.to_string(),
                    "dbname" | "database" => database = value.to_string(),
                    "sslmode" => sslmode = Some(value.to_string()),
                    _ => {}
                }
            }
        }

        Ok(ConnectionParams {
            host,
            port,
            user,
            password,
            database,
            sslmode,
        })
    }
}

struct ConnectionParams {
    host: String,
    port: u16,
    user: String,
    password: String,
    database: String,
    sslmode: Option<String>,
}

/// Parse a text-format value based on its PostgreSQL type OID.
fn parse_text_value(s: &str, type_oid: u32) -> Value {
    // Common PostgreSQL type OIDs
    match type_oid {
        16 => Value::Bool(s == "t" || s == "true"), // bool
        20 | 21 | 23 => s
            .parse::<i64>()
            .map(Value::Int)
            .unwrap_or(Value::String(s.to_string())), // int8, int2, int4
        700 | 701 => s
            .parse::<f64>()
            .map(Value::Float)
            .unwrap_or(Value::String(s.to_string())), // float4, float8
        1700 => s
            .parse::<f64>()
            .map(Value::Float)
            .unwrap_or(Value::String(s.to_string())), // numeric
        25 | 1043 => Value::String(s.to_string()),  // text, varchar
        114 | 3802 => {
            // json, jsonb
            serde_json::from_str::<serde_json::Value>(s)
                .map(Value::from)
                .unwrap_or(Value::String(s.to_string()))
        }
        2950 => Value::String(s.to_string()), // uuid
        1082 | 1114 | 1184 => Value::String(s.to_string()), // date, timestamp, timestamptz
        1009 | 1015 | 1016 => {
            // text[], varchar[], int8[]
            // PostgreSQL array format: {elem1,elem2,...}
            Value::String(s.to_string()) // Keep as string for now
        }
        _ => Value::String(s.to_string()), // Default to string
    }
}

/// Format PostgreSQL timestamp (microseconds since 2000-01-01) to ISO string.
fn format_pg_timestamp(micros: i64) -> String {
    // PostgreSQL epoch is 2000-01-01 00:00:00 UTC
    // Unix epoch is 1970-01-01 00:00:00 UTC
    // Difference: 946684800 seconds
    const PG_EPOCH_OFFSET: i64 = 946_684_800;

    let unix_secs = (micros / 1_000_000) + PG_EPOCH_OFFSET;
    let nanos = ((micros % 1_000_000) * 1000) as u32;

    chrono::DateTime::from_timestamp(unix_secs, nanos)
        .map(|dt| dt.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string())
        .unwrap_or_else(|| format!("{}us", micros))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_url_sslmode() {
        let params = ReplicationStream::parse_url_connection_string(
            "postgres://user:pass@host:5432/db?sslmode=require"
        ).unwrap();
        assert_eq!(params.sslmode, Some("require".to_string()));

        let params = ReplicationStream::parse_url_connection_string(
            "postgres://user:pass@host:5432/db?sslmode=verify-full"
        ).unwrap();
        assert_eq!(params.sslmode, Some("verify-full".to_string()));

        let params = ReplicationStream::parse_url_connection_string(
            "postgres://user:pass@host:5432/db"
        ).unwrap();
        assert_eq!(params.sslmode, None);
    }

    #[test]
    fn test_parse_keyvalue_sslmode() {
        let params = ReplicationStream::parse_keyvalue_connection_string(
            "host=localhost port=5432 user=postgres dbname=test sslmode=require"
        ).unwrap();
        assert_eq!(params.sslmode, Some("require".to_string()));

        let params = ReplicationStream::parse_keyvalue_connection_string(
            "host=localhost port=5432 user=postgres dbname=test"
        ).unwrap();
        assert_eq!(params.sslmode, None);
    }
}
