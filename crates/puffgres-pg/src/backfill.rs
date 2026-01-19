//! Backfill scanner for syncing existing table data.
//!
//! Scans a Postgres table and produces RowEvents for processing
//! through the existing transform pipeline.

use std::collections::HashMap;
use std::time::Instant;

use puffgres_core::{Operation, RowEvent, Value};
use tokio_postgres::{Client, Row};
use tracing::{debug, info};

use crate::connect::connect_postgres;
use crate::error::PgResult;

/// Configuration for backfill scanning.
#[derive(Debug, Clone)]
pub struct BackfillConfig {
    /// Postgres connection string.
    pub connection_string: String,
    /// Schema name.
    pub schema: String,
    /// Table name.
    pub table: String,
    /// ID column name.
    pub id_column: String,
    /// Columns to select.
    pub columns: Vec<String>,
    /// Batch size for cursor pagination.
    pub batch_size: u32,
}

/// Progress information for backfill.
#[derive(Debug, Clone)]
pub struct BackfillProgress {
    /// Last processed ID (for resumption).
    pub last_id: Option<String>,
    /// Total rows in the table (estimated).
    pub total_rows: Option<i64>,
    /// Rows processed so far (read from postgres).
    pub processed_rows: i64,
    /// Rows upserted to turbopuffer.
    pub upserted_rows: i64,
    /// Rows processed per second.
    pub rows_per_second: f64,
    /// Rows upserted per second.
    pub upserts_per_second: f64,
    /// Percentage complete.
    pub percent_complete: f64,
    /// Elapsed time in seconds.
    pub elapsed_secs: f64,
    /// Estimated time remaining in seconds.
    pub eta_secs: Option<f64>,
}

/// Spinner frames for animation.
const SPINNER_FRAMES: &[char] = &['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];

impl BackfillProgress {
    /// Format elapsed time as human-readable string.
    fn format_duration(secs: f64) -> String {
        let total_secs = secs as u64;
        let hours = total_secs / 3600;
        let mins = (total_secs % 3600) / 60;
        let secs = total_secs % 60;
        if hours > 0 {
            format!("{}h{}m{}s", hours, mins, secs)
        } else if mins > 0 {
            format!("{}m{}s", mins, secs)
        } else {
            format!("{}s", secs)
        }
    }

    /// Format as a progress line with spinner.
    pub fn format(&self, spinner_frame: usize) -> String {
        let spinner = SPINNER_FRAMES[spinner_frame % SPINNER_FRAMES.len()];
        let elapsed = Self::format_duration(self.elapsed_secs);
        let eta = self
            .eta_secs
            .map(|s| format!(" ETA {}", Self::format_duration(s)))
            .unwrap_or_default();

        if let Some(total) = self.total_rows {
            format!(
                "{} [{:>5.1}%] {}/{} read | {} upserted ({:.0} read/s, {:.0} upsert/s) [{}{}]",
                spinner,
                self.percent_complete,
                self.processed_rows,
                total,
                self.upserted_rows,
                self.rows_per_second,
                self.upserts_per_second,
                elapsed,
                eta
            )
        } else {
            format!(
                "{} {} read | {} upserted ({:.0} read/s, {:.0} upsert/s) [{}{}]",
                spinner,
                self.processed_rows,
                self.upserted_rows,
                self.rows_per_second,
                self.upserts_per_second,
                elapsed,
                eta
            )
        }
    }
}

/// Backfill scanner that iterates through a table.
pub struct BackfillScanner {
    client: Client,
    config: BackfillConfig,
    /// Last processed ID for cursor pagination.
    last_id: Option<String>,
    /// Total rows (estimated from statistics).
    total_rows: Option<i64>,
    /// Rows processed.
    processed_rows: i64,
    /// Start time for rate calculation.
    start_time: Instant,
}

impl BackfillScanner {
    /// Create a new backfill scanner.
    pub async fn new(config: BackfillConfig) -> PgResult<Self> {
        let client = connect_postgres(&config.connection_string).await?;

        let mut scanner = Self {
            client,
            config,
            last_id: None,
            total_rows: None,
            processed_rows: 0,
            start_time: Instant::now(),
        };

        // Estimate total rows
        scanner.estimate_total_rows().await?;

        Ok(scanner)
    }

    /// Resume from a specific ID.
    pub fn resume_from(&mut self, last_id: String, processed_rows: i64) {
        self.last_id = Some(last_id);
        self.processed_rows = processed_rows;
    }

    /// Estimate total rows using table statistics.
    async fn estimate_total_rows(&mut self) -> PgResult<()> {
        let query = format!(
            "SELECT reltuples::bigint FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = $1 AND c.relname = $2"
        );

        let row = self
            .client
            .query_opt(&query, &[&self.config.schema, &self.config.table])
            .await?;

        if let Some(r) = row {
            let estimate: i64 = r.get(0);
            if estimate > 0 {
                self.total_rows = Some(estimate);
                info!(estimate, "Estimated total rows from statistics");
            }
        }

        Ok(())
    }

    /// Get current progress.
    pub fn progress(&self, upserted_rows: i64) -> BackfillProgress {
        let elapsed_secs = self.start_time.elapsed().as_secs_f64();
        let rows_per_second = if elapsed_secs > 0.0 {
            self.processed_rows as f64 / elapsed_secs
        } else {
            0.0
        };
        let upserts_per_second = if elapsed_secs > 0.0 {
            upserted_rows as f64 / elapsed_secs
        } else {
            0.0
        };

        let percent_complete = if let Some(total) = self.total_rows {
            if total > 0 {
                (self.processed_rows as f64 / total as f64) * 100.0
            } else {
                0.0
            }
        } else {
            0.0
        };

        // Calculate ETA based on rows_per_second
        let eta_secs = if let Some(total) = self.total_rows {
            if rows_per_second > 0.0 {
                let remaining = total - self.processed_rows;
                if remaining > 0 {
                    Some(remaining as f64 / rows_per_second)
                } else {
                    Some(0.0)
                }
            } else {
                None
            }
        } else {
            None
        };

        BackfillProgress {
            last_id: self.last_id.clone(),
            total_rows: self.total_rows,
            processed_rows: self.processed_rows,
            upserted_rows,
            rows_per_second,
            upserts_per_second,
            percent_complete,
            elapsed_secs,
            eta_secs,
        }
    }

    /// Get the last processed ID (for saving checkpoint).
    pub fn last_id(&self) -> Option<&str> {
        self.last_id.as_deref()
    }

    /// Check if the scan is complete.
    pub fn is_complete(&self) -> bool {
        if let Some(total) = self.total_rows {
            self.processed_rows >= total
        } else {
            false // Can't know without total
        }
    }

    /// Fetch the next batch of rows as RowEvents.
    pub async fn next_batch(&mut self) -> PgResult<Vec<RowEvent>> {
        // Build the SELECT query with cursor pagination
        let columns_list = if self.config.columns.is_empty() {
            "*".to_string()
        } else {
            // Always include the ID column
            let mut cols = self.config.columns.clone();
            if !cols.contains(&self.config.id_column) {
                cols.insert(0, self.config.id_column.clone());
            }
            cols.join(", ")
        };

        let query = if self.last_id.is_some() {
            format!(
                "SELECT {} FROM {}.{} WHERE {}::text > $1 ORDER BY {} LIMIT {}",
                columns_list,
                self.config.schema,
                self.config.table,
                self.config.id_column,
                self.config.id_column,
                self.config.batch_size
            )
        } else {
            format!(
                "SELECT {} FROM {}.{} ORDER BY {} LIMIT {}",
                columns_list,
                self.config.schema,
                self.config.table,
                self.config.id_column,
                self.config.batch_size
            )
        };

        let rows: Vec<Row> = if let Some(ref last_id) = self.last_id {
            self.client.query(&query, &[&last_id]).await?
        } else {
            self.client.query(&query, &[]).await?
        };

        if rows.is_empty() {
            debug!("Backfill scan complete - no more rows");
            return Ok(vec![]);
        }

        let mut events = Vec::with_capacity(rows.len());

        for row in &rows {
            // Extract the row as a HashMap
            let mut row_map = HashMap::new();
            let mut current_id = String::new();

            for (i, column) in row.columns().iter().enumerate() {
                let name = column.name();
                let value = row_to_value(row, i)?;

                if name == self.config.id_column {
                    current_id = value_to_string(&value);
                }

                row_map.insert(name.to_string(), value);
            }

            // Update last_id for cursor pagination
            if !current_id.is_empty() {
                self.last_id = Some(current_id);
            }

            // Create a synthetic INSERT event for backfill
            events.push(RowEvent {
                op: Operation::Insert,
                schema: self.config.schema.clone(),
                table: self.config.table.clone(),
                new: Some(row_map),
                old: None,
                lsn: 0, // Backfill doesn't have a real LSN
                txid: None,
                timestamp: None,
            });
        }

        self.processed_rows += events.len() as i64;

        debug!(
            batch_size = events.len(),
            total = self.processed_rows,
            "Fetched backfill batch"
        );

        Ok(events)
    }
}

/// Convert a row column to a Value.
fn row_to_value(row: &Row, index: usize) -> PgResult<Value> {
    let column = &row.columns()[index];
    let type_info = column.type_();

    // Handle different Postgres types
    match type_info.name() {
        "bool" => {
            let v: Option<bool> = row.get(index);
            Ok(v.map(Value::Bool).unwrap_or(Value::Null))
        }
        "int2" | "int4" => {
            let v: Option<i32> = row.get(index);
            Ok(v.map(|i| Value::Int(i as i64)).unwrap_or(Value::Null))
        }
        "int8" => {
            let v: Option<i64> = row.get(index);
            Ok(v.map(Value::Int).unwrap_or(Value::Null))
        }
        "float4" | "float8" | "numeric" => {
            let v: Option<f64> = row.try_get(index).ok().flatten();
            Ok(v.map(Value::Float).unwrap_or(Value::Null))
        }
        "text" | "varchar" | "char" | "bpchar" | "name" => {
            let v: Option<String> = row.get(index);
            Ok(v.map(Value::String).unwrap_or(Value::Null))
        }
        "uuid" => match row.try_get::<_, Option<uuid::Uuid>>(index) {
            Ok(Some(u)) => Ok(Value::String(u.to_string())),
            Ok(None) => Ok(Value::Null),
            Err(_) => Ok(Value::Null),
        },
        "timestamp" | "timestamptz" | "date" | "time" | "timetz" => {
            // Convert timestamps to string representation
            match row.try_get::<_, Option<chrono::DateTime<chrono::Utc>>>(index) {
                Ok(Some(dt)) => Ok(Value::String(dt.to_rfc3339())),
                Ok(None) => Ok(Value::Null),
                Err(_) => Ok(Value::Null),
            }
        }
        "json" | "jsonb" => {
            let v: Option<serde_json::Value> = row.get(index);
            Ok(v.map(json_to_value).unwrap_or(Value::Null))
        }
        _ => {
            // Fallback: try to get as string
            let v: Option<String> = row.try_get(index).ok().flatten();
            Ok(v.map(Value::String).unwrap_or(Value::Null))
        }
    }
}

/// Convert a serde_json::Value to a puffgres Value.
fn json_to_value(v: serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float(f)
            } else {
                Value::Null
            }
        }
        serde_json::Value::String(s) => Value::String(s),
        serde_json::Value::Array(arr) => Value::Array(arr.into_iter().map(json_to_value).collect()),
        serde_json::Value::Object(obj) => Value::Object(
            obj.into_iter()
                .map(|(k, v)| (k, json_to_value(v)))
                .collect(),
        ),
    }
}

/// Convert a Value to a string for cursor pagination.
fn value_to_string(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::String(s) => s.clone(),
        Value::Array(_) | Value::Object(_) => serde_json::to_string(value).unwrap_or_default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_progress_format() {
        let progress = BackfillProgress {
            last_id: Some("100".to_string()),
            total_rows: Some(10000),
            processed_rows: 4520,
            upserted_rows: 4000,
            rows_per_second: 2340.5,
            upserts_per_second: 2100.0,
            percent_complete: 45.2,
            elapsed_secs: 65.0,
            eta_secs: Some(120.0),
        };

        let formatted = progress.format(0);
        assert!(formatted.contains("45.2%"));
        assert!(formatted.contains("4520"));
        assert!(formatted.contains("10000"));
        assert!(formatted.contains("4000"));
        assert!(formatted.contains("upserted"));
        assert!(formatted.contains("1m5s")); // elapsed
        assert!(formatted.contains("ETA 2m0s")); // eta
    }

    #[test]
    fn test_value_to_string() {
        assert_eq!(value_to_string(&Value::Int(42)), "42");
        assert_eq!(value_to_string(&Value::String("hello".into())), "hello");
        assert_eq!(value_to_string(&Value::Bool(true)), "true");
        assert_eq!(value_to_string(&Value::Null), "");
    }
}
