//! True push-based streaming replication client using pgwire-replication.

use std::collections::HashMap;
use std::time::Duration;

use pgwire_replication::{ReplicationClient, ReplicationConfig as PgwireConfig, ReplicationEvent};
use puffgres_core::{Operation, RowEvent, Value};
use tokio_postgres::NoTls;
use tracing::{debug, info, warn};

use super::lsn::{format_lsn, parse_lsn};
use super::pgoutput::{ColumnInfo, ColumnValue, PgOutputDecoder, PgOutputMessage};
use super::relation_cache::RelationCache;
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
    pub async fn connect(config: ReplicationStreamConfig) -> PgResult<Self> {
        info!(
            slot = %config.slot_name,
            publication = %config.publication_name,
            "Connecting for streaming replication"
        );

        // First ensure prerequisites using regular postgres connection
        Self::ensure_prerequisites(&config).await?;

        // Parse connection string to extract host, port, user, password, database
        let conn_params = Self::parse_connection_string(&config.connection_string)?;

        // Get start LSN
        let start_lsn = if let Some(lsn) = config.start_lsn {
            pgwire_replication::Lsn::from(lsn)
        } else {
            // Get current confirmed_flush_lsn from slot
            let lsn = Self::get_confirmed_lsn(&config).await?;
            pgwire_replication::Lsn::from(lsn.unwrap_or(0))
        };

        info!(start_lsn = %format_lsn(start_lsn.into()), "Starting replication stream");

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
            tls: pgwire_replication::TlsConfig::disabled(),
        };

        let client = ReplicationClient::connect(pgwire_config)
            .await
            .map_err(|e| PgError::Replication(e.to_string()))?;

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
        loop {
            let event = self
                .client
                .recv()
                .await
                .map_err(|e| PgError::Replication(e.to_string()))?;

            match event {
                Some(ReplicationEvent::XLogData { wal_end, data, .. }) => {
                    let wal_end_u64: u64 = wal_end.into();

                    // Decode the pgoutput message
                    let msg = match self.decoder.decode(&data) {
                        Ok(m) => m,
                        Err(e) => {
                            warn!(error = %e, "Failed to decode pgoutput message, skipping");
                            continue;
                        }
                    };

                    // Process the message
                    if let Some(batch) = self.process_message(msg, wal_end_u64)? {
                        return Ok(Some(batch));
                    }
                }
                Some(ReplicationEvent::KeepAlive {
                    wal_end,
                    reply_requested,
                    ..
                }) => {
                    debug!(wal_end = %format_lsn(wal_end.into()), reply_requested, "Received keepalive");
                    // Keepalives are handled automatically by pgwire-replication
                }
                Some(ReplicationEvent::StoppedAt { reached }) => {
                    info!(lsn = %format_lsn(reached.into()), "Replication stream stopped");
                    return Ok(None);
                }
                Some(_) => {
                    // Handle any other event types (e.g., Begin, Commit at protocol level)
                    // These are rare and we can safely skip them
                    continue;
                }
                None => {
                    info!("Replication stream ended");
                    return Ok(None);
                }
            }
        }
    }

    /// Process a decoded pgoutput message.
    ///
    /// Returns Some(batch) when a transaction is complete.
    fn process_message(
        &mut self,
        msg: PgOutputMessage,
        lsn: u64,
    ) -> PgResult<Option<StreamingBatch>> {
        match msg {
            PgOutputMessage::Begin(begin) => {
                debug!(xid = begin.xid, final_lsn = %format_lsn(begin.final_lsn), "Transaction begin");
                self.current_txn = Some(TransactionState {
                    xid: begin.xid,
                    timestamp: begin.timestamp,
                    events: Vec::new(),
                });
                Ok(None)
            }
            PgOutputMessage::Commit(commit) => {
                debug!(lsn = %format_lsn(commit.end_lsn), "Transaction commit");
                if let Some(txn) = self.current_txn.take() {
                    if !txn.events.is_empty() {
                        info!(
                            xid = txn.xid,
                            count = txn.events.len(),
                            lsn = %format_lsn(commit.end_lsn),
                            "Completed transaction batch"
                        );
                    }
                    Ok(Some(StreamingBatch {
                        events: txn.events,
                        ack_lsn: commit.end_lsn,
                    }))
                } else {
                    warn!("Received commit without begin");
                    Ok(None)
                }
            }
            PgOutputMessage::Relation(rel) => {
                debug!(
                    relation_id = rel.relation_id,
                    schema = %rel.namespace,
                    table = %rel.name,
                    "Caching relation metadata"
                );
                self.relation_cache.update(&rel);
                Ok(None)
            }
            PgOutputMessage::Insert(insert) => {
                let event = self.to_row_event_insert(&insert, lsn)?;
                if let Some(ref mut txn) = self.current_txn {
                    info!(
                        op = "insert",
                        schema = %event.schema,
                        table = %event.table,
                        lsn = %format_lsn(lsn),
                        "Received change"
                    );
                    txn.events.push(event);
                }
                Ok(None)
            }
            PgOutputMessage::Update(update) => {
                let event = self.to_row_event_update(&update, lsn)?;
                if let Some(ref mut txn) = self.current_txn {
                    info!(
                        op = "update",
                        schema = %event.schema,
                        table = %event.table,
                        lsn = %format_lsn(lsn),
                        "Received change"
                    );
                    txn.events.push(event);
                }
                Ok(None)
            }
            PgOutputMessage::Delete(delete) => {
                let event = self.to_row_event_delete(&delete, lsn)?;
                if let Some(ref mut txn) = self.current_txn {
                    info!(
                        op = "delete",
                        schema = %event.schema,
                        table = %event.table,
                        lsn = %format_lsn(lsn),
                        "Received change"
                    );
                    txn.events.push(event);
                }
                Ok(None)
            }
            PgOutputMessage::Truncate(_) => {
                warn!("Truncate operations are not supported, skipping");
                Ok(None)
            }
            PgOutputMessage::Type(_) | PgOutputMessage::Origin(_) | PgOutputMessage::Message(_) => {
                // These don't produce row events
                Ok(None)
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
    async fn ensure_prerequisites(config: &ReplicationStreamConfig) -> PgResult<()> {
        let (client, connection) = tokio_postgres::connect(&config.connection_string, NoTls)
            .await
            .map_err(|e| PgError::Connection(e.to_string()))?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("Postgres connection error: {}", e);
            }
        });

        // Check/create replication slot with pgoutput
        let slot_exists: bool = client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
                &[&config.slot_name],
            )
            .await?
            .get(0);

        if !slot_exists {
            if config.create_slot {
                info!(slot = %config.slot_name, "Creating replication slot with pgoutput");
                client
                    .execute(
                        "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
                        &[&config.slot_name],
                    )
                    .await
                    .map_err(|e| PgError::SlotCreationFailed(e.to_string()))?;
            } else {
                return Err(PgError::SlotNotFound(config.slot_name.clone()));
            }
        } else {
            info!(slot = %config.slot_name, "Using existing replication slot");
        }

        // Check/create publication
        let pub_exists: bool = client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)",
                &[&config.publication_name],
            )
            .await?
            .get(0);

        if !pub_exists {
            if config.create_publication {
                if config.publication_tables.is_empty() {
                    info!(publication = %config.publication_name, "Creating publication for all tables");
                    client
                        .execute(
                            &format!(
                                "CREATE PUBLICATION {} FOR ALL TABLES",
                                quote_ident(&config.publication_name)
                            ),
                            &[],
                        )
                        .await
                        .map_err(|e| PgError::Replication(e.to_string()))?;
                } else {
                    let tables = config
                        .publication_tables
                        .iter()
                        .map(|t| quote_ident(t))
                        .collect::<Vec<_>>()
                        .join(", ");
                    info!(publication = %config.publication_name, tables = %tables, "Creating publication");
                    client
                        .execute(
                            &format!(
                                "CREATE PUBLICATION {} FOR TABLE {}",
                                quote_ident(&config.publication_name),
                                tables
                            ),
                            &[],
                        )
                        .await
                        .map_err(|e| PgError::Replication(e.to_string()))?;
                }
            } else {
                return Err(PgError::PublicationNotFound(
                    config.publication_name.clone(),
                ));
            }
        } else {
            info!(publication = %config.publication_name, "Using existing publication");
        }

        Ok(())
    }

    /// Get confirmed_flush_lsn for the slot.
    async fn get_confirmed_lsn(config: &ReplicationStreamConfig) -> PgResult<Option<u64>> {
        let (client, connection) = tokio_postgres::connect(&config.connection_string, NoTls)
            .await
            .map_err(|e| PgError::Connection(e.to_string()))?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("Postgres connection error: {}", e);
            }
        });

        let row = client
            .query_opt(
                "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
                &[&config.slot_name],
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
        // postgres://user:password@host:port/database
        let url = url::Url::parse(conn_str)
            .map_err(|e| PgError::Connection(format!("Invalid connection URL: {}", e)))?;

        let host = url.host_str().unwrap_or("localhost").to_string();
        let port = url.port().unwrap_or(5432);
        let user = url.username().to_string();
        let password = url.password().unwrap_or("").to_string();
        let database = url.path().trim_start_matches('/').to_string();

        Ok(ConnectionParams {
            host,
            port,
            user,
            password,
            database,
        })
    }

    fn parse_keyvalue_connection_string(conn_str: &str) -> PgResult<ConnectionParams> {
        // host=localhost port=5432 user=postgres password=... dbname=...
        let mut host = "localhost".to_string();
        let mut port = 5432u16;
        let mut user = "postgres".to_string();
        let mut password = String::new();
        let mut database = "postgres".to_string();

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
        })
    }
}

struct ConnectionParams {
    host: String,
    port: u16,
    user: String,
    password: String,
    database: String,
}

/// Quote an identifier for use in SQL.
fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
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
