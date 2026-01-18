//! PostgreSQL-backed state storage for puffgres.
//!
//! All puffgres state is stored in the user's Postgres database in __puffgres_* tables.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio_postgres::Client;
use tracing::{debug, info};

use crate::error::{PgError, PgResult};

/// Checkpoint state for a mapping.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Checkpoint {
    /// The last successfully processed LSN.
    pub lsn: u64,
    /// Number of events processed.
    pub events_processed: u64,
    /// Last update timestamp.
    pub updated_at: Option<DateTime<Utc>>,
}

/// Applied migration record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppliedMigration {
    pub id: i32,
    pub version: i32,
    pub mapping_name: String,
    pub content_hash: String,
    pub applied_at: DateTime<Utc>,
}

/// Dead letter queue entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqEntry {
    pub id: i32,
    pub mapping_name: String,
    pub lsn: u64,
    pub event_json: serde_json::Value,
    pub error_message: String,
    pub error_kind: String,
    pub retry_count: i32,
    pub created_at: DateTime<Utc>,
}

/// Backfill progress.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackfillProgress {
    pub mapping_name: String,
    pub last_id: Option<String>,
    pub total_rows: Option<i64>,
    pub processed_rows: i64,
    pub status: String,
    pub updated_at: DateTime<Utc>,
}

/// Stored transform for immutability tracking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredTransform {
    pub id: i32,
    pub mapping_name: String,
    pub version: i32,
    pub content: String,
    pub content_hash: String,
    pub created_at: DateTime<Utc>,
}

/// PostgreSQL-backed state store.
///
/// Stores all puffgres state in __puffgres_* tables in the user's database.
pub struct PostgresStateStore {
    client: Client,
}

impl PostgresStateStore {
    /// Create a new state store and connect to Postgres.
    pub async fn connect(connection_string: &str) -> PgResult<Self> {
        let (client, connection) = tokio_postgres::connect(connection_string, tokio_postgres::NoTls)
            .await
            .map_err(|e| PgError::Connection(e.to_string()))?;

        // Spawn connection handler
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!(error = %e, "Postgres connection error");
            }
        });

        let store = Self { client };
        store.ensure_schema().await?;

        Ok(store)
    }

    /// Create a state store from an existing client (for testing or connection pooling).
    pub async fn from_client(client: Client) -> PgResult<Self> {
        let store = Self { client };
        store.ensure_schema().await?;
        Ok(store)
    }

    /// Ensure all required tables exist.
    async fn ensure_schema(&self) -> PgResult<()> {
        debug!("Ensuring puffgres state schema exists");

        // Migration tracking
        self.client
            .execute(
                r#"
                CREATE TABLE IF NOT EXISTS __puffgres_migrations (
                    id SERIAL PRIMARY KEY,
                    version INTEGER NOT NULL,
                    mapping_name TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    applied_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(version, mapping_name)
                )
                "#,
                &[],
            )
            .await
            .map_err(|e| PgError::Postgres(e.to_string()))?;

        // CDC checkpoints
        self.client
            .execute(
                r#"
                CREATE TABLE IF NOT EXISTS __puffgres_checkpoints (
                    mapping_name TEXT PRIMARY KEY,
                    lsn BIGINT NOT NULL,
                    events_processed BIGINT DEFAULT 0,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
                "#,
                &[],
            )
            .await
            .map_err(|e| PgError::Postgres(e.to_string()))?;

        // Dead letter queue
        self.client
            .execute(
                r#"
                CREATE TABLE IF NOT EXISTS __puffgres_dlq (
                    id SERIAL PRIMARY KEY,
                    mapping_name TEXT NOT NULL,
                    lsn BIGINT NOT NULL,
                    event_json JSONB NOT NULL,
                    error_message TEXT NOT NULL,
                    error_kind TEXT NOT NULL,
                    retry_count INT DEFAULT 0,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
                "#,
                &[],
            )
            .await
            .map_err(|e| PgError::Postgres(e.to_string()))?;

        // Backfill progress
        self.client
            .execute(
                r#"
                CREATE TABLE IF NOT EXISTS __puffgres_backfill (
                    mapping_name TEXT PRIMARY KEY,
                    last_id TEXT,
                    total_rows BIGINT,
                    processed_rows BIGINT DEFAULT 0,
                    status TEXT DEFAULT 'pending',
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
                "#,
                &[],
            )
            .await
            .map_err(|e| PgError::Postgres(e.to_string()))?;

        // Transform storage for immutability tracking
        self.client
            .execute(
                r#"
                CREATE TABLE IF NOT EXISTS __puffgres_transforms (
                    id SERIAL PRIMARY KEY,
                    mapping_name TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    content TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(mapping_name, version)
                )
                "#,
                &[],
            )
            .await
            .map_err(|e| PgError::Postgres(e.to_string()))?;

        // Migration content storage for reset functionality
        self.client
            .execute(
                r#"
                CREATE TABLE IF NOT EXISTS __puffgres_migration_content (
                    id SERIAL PRIMARY KEY,
                    version INTEGER NOT NULL,
                    mapping_name TEXT NOT NULL,
                    content TEXT NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(version, mapping_name)
                )
                "#,
                &[],
            )
            .await
            .map_err(|e| PgError::Postgres(e.to_string()))?;

        info!("Puffgres state schema initialized");
        Ok(())
    }

    // -------------------------------------------------------------------------
    // Checkpoint methods
    // -------------------------------------------------------------------------

    /// Get the checkpoint for a mapping.
    pub async fn get_checkpoint(&self, mapping_name: &str) -> PgResult<Option<Checkpoint>> {
        let row = self
            .client
            .query_opt(
                r#"
                SELECT lsn, events_processed, updated_at
                FROM __puffgres_checkpoints
                WHERE mapping_name = $1
                "#,
                &[&mapping_name],
            )
            .await
            .map_err(|e| PgError::Postgres(e.to_string()))?;

        Ok(row.map(|r| Checkpoint {
            lsn: r.get::<_, i64>(0) as u64,
            events_processed: r.get::<_, i64>(1) as u64,
            updated_at: r.get(2),
        }))
    }

    /// Save a checkpoint for a mapping.
    pub async fn save_checkpoint(&self, mapping_name: &str, checkpoint: &Checkpoint) -> PgResult<()> {
        self.client
            .execute(
                r#"
                INSERT INTO __puffgres_checkpoints (mapping_name, lsn, events_processed, updated_at)
                VALUES ($1, $2, $3, NOW())
                ON CONFLICT (mapping_name)
                DO UPDATE SET lsn = $2, events_processed = $3, updated_at = NOW()
                "#,
                &[
                    &mapping_name,
                    &(checkpoint.lsn as i64),
                    &(checkpoint.events_processed as i64),
                ],
            )
            .await
            .map_err(|e| PgError::Postgres(e.to_string()))?;

        Ok(())
    }

    /// Get all checkpoints.
    pub async fn get_all_checkpoints(&self) -> PgResult<Vec<(String, Checkpoint)>> {
        let rows = self
            .client
            .query(
                r#"
                SELECT mapping_name, lsn, events_processed, updated_at
                FROM __puffgres_checkpoints
                ORDER BY mapping_name
                "#,
                &[],
            )
            .await
            .map_err(|e| PgError::Postgres(e.to_string()))?;

        Ok(rows
            .into_iter()
            .map(|r| {
                (
                    r.get(0),
                    Checkpoint {
                        lsn: r.get::<_, i64>(1) as u64,
                        events_processed: r.get::<_, i64>(2) as u64,
                        updated_at: r.get(3),
                    },
                )
            })
            .collect())
    }

    /// Get the minimum LSN across all mappings (safe restart point).
    pub async fn get_min_lsn(&self) -> PgResult<Option<u64>> {
        let row = self
            .client
            .query_opt(
                "SELECT MIN(lsn) FROM __puffgres_checkpoints",
                &[],
            )
            .await
            .map_err(|e| PgError::Postgres(e.to_string()))?;

        Ok(row.and_then(|r| r.get::<_, Option<i64>>(0).map(|lsn| lsn as u64)))
    }

    // -------------------------------------------------------------------------
    // Migration tracking methods
    // -------------------------------------------------------------------------

    /// Get all applied migrations.
    pub async fn get_applied_migrations(&self) -> PgResult<Vec<AppliedMigration>> {
        let rows = self
            .client
            .query(
                r#"
                SELECT id, version, mapping_name, content_hash, applied_at
                FROM __puffgres_migrations
                ORDER BY version, mapping_name
                "#,
                &[],
            )
            .await
            .map_err(|e| PgError::Postgres(e.to_string()))?;

        Ok(rows
            .into_iter()
            .map(|r| AppliedMigration {
                id: r.get(0),
                version: r.get(1),
                mapping_name: r.get(2),
                content_hash: r.get(3),
                applied_at: r.get(4),
            })
            .collect())
    }

    /// Get an applied migration by version and mapping name.
    pub async fn get_applied_migration(
        &self,
        version: i32,
        mapping_name: &str,
    ) -> PgResult<Option<AppliedMigration>> {
        let row = self
            .client
            .query_opt(
                r#"
                SELECT id, version, mapping_name, content_hash, applied_at
                FROM __puffgres_migrations
                WHERE version = $1 AND mapping_name = $2
                "#,
                &[&version, &mapping_name],
            )
            .await
            .map_err(|e| PgError::Postgres(e.to_string()))?;

        Ok(row.map(|r| AppliedMigration {
            id: r.get(0),
            version: r.get(1),
            mapping_name: r.get(2),
            content_hash: r.get(3),
            applied_at: r.get(4),
        }))
    }

    /// Record a migration as applied.
    pub async fn record_migration(
        &self,
        version: i32,
        mapping_name: &str,
        content_hash: &str,
    ) -> PgResult<()> {
        self.client
            .execute(
                r#"
                INSERT INTO __puffgres_migrations (version, mapping_name, content_hash)
                VALUES ($1, $2, $3)
                "#,
                &[&version, &mapping_name, &content_hash],
            )
            .await
            .map_err(|e| PgError::Postgres(e.to_string()))?;

        info!(version, mapping_name, "Recorded migration");
        Ok(())
    }

    // -------------------------------------------------------------------------
    // DLQ methods
    // -------------------------------------------------------------------------

    /// Add an entry to the dead letter queue.
    pub async fn add_to_dlq(
        &self,
        mapping_name: &str,
        lsn: u64,
        event_json: &serde_json::Value,
        error_message: &str,
        error_kind: &str,
    ) -> PgResult<i32> {
        let row = self
            .client
            .query_one(
                r#"
                INSERT INTO __puffgres_dlq (mapping_name, lsn, event_json, error_message, error_kind)
                VALUES ($1, $2, $3, $4, $5)
                RETURNING id
                "#,
                &[
                    &mapping_name,
                    &(lsn as i64),
                    &event_json,
                    &error_message,
                    &error_kind,
                ],
            )
            .await
            .map_err(|e| PgError::Postgres(e.to_string()))?;

        Ok(row.get(0))
    }

    /// Get DLQ entries for a mapping.
    pub async fn get_dlq_entries(
        &self,
        mapping_name: Option<&str>,
        limit: i64,
    ) -> PgResult<Vec<DlqEntry>> {
        let rows = if let Some(name) = mapping_name {
            self.client
                .query(
                    r#"
                    SELECT id, mapping_name, lsn, event_json, error_message, error_kind, retry_count, created_at
                    FROM __puffgres_dlq
                    WHERE mapping_name = $1
                    ORDER BY created_at DESC
                    LIMIT $2
                    "#,
                    &[&name, &limit],
                )
                .await
        } else {
            self.client
                .query(
                    r#"
                    SELECT id, mapping_name, lsn, event_json, error_message, error_kind, retry_count, created_at
                    FROM __puffgres_dlq
                    ORDER BY created_at DESC
                    LIMIT $1
                    "#,
                    &[&limit],
                )
                .await
        }
        .map_err(|e| PgError::Postgres(e.to_string()))?;

        Ok(rows
            .into_iter()
            .map(|r| DlqEntry {
                id: r.get(0),
                mapping_name: r.get(1),
                lsn: r.get::<_, i64>(2) as u64,
                event_json: r.get(3),
                error_message: r.get(4),
                error_kind: r.get(5),
                retry_count: r.get(6),
                created_at: r.get(7),
            })
            .collect())
    }

    /// Get a single DLQ entry by ID.
    pub async fn get_dlq_entry(&self, id: i32) -> PgResult<Option<DlqEntry>> {
        let row = self
            .client
            .query_opt(
                r#"
                SELECT id, mapping_name, lsn, event_json, error_message, error_kind, retry_count, created_at
                FROM __puffgres_dlq
                WHERE id = $1
                "#,
                &[&id],
            )
            .await
            .map_err(|e| PgError::Postgres(e.to_string()))?;

        Ok(row.map(|r| DlqEntry {
            id: r.get(0),
            mapping_name: r.get(1),
            lsn: r.get::<_, i64>(2) as u64,
            event_json: r.get(3),
            error_message: r.get(4),
            error_kind: r.get(5),
            retry_count: r.get(6),
            created_at: r.get(7),
        }))
    }

    /// Increment retry count for a DLQ entry.
    pub async fn increment_dlq_retry(&self, id: i32) -> PgResult<()> {
        self.client
            .execute(
                "UPDATE __puffgres_dlq SET retry_count = retry_count + 1 WHERE id = $1",
                &[&id],
            )
            .await
            .map_err(|e| PgError::Postgres(e.to_string()))?;

        Ok(())
    }

    /// Delete a DLQ entry.
    pub async fn delete_dlq_entry(&self, id: i32) -> PgResult<()> {
        self.client
            .execute("DELETE FROM __puffgres_dlq WHERE id = $1", &[&id])
            .await
            .map_err(|e| PgError::Postgres(e.to_string()))?;

        Ok(())
    }

    /// Clear DLQ entries for a mapping (or all if None).
    pub async fn clear_dlq(&self, mapping_name: Option<&str>) -> PgResult<u64> {
        let count = if let Some(name) = mapping_name {
            self.client
                .execute(
                    "DELETE FROM __puffgres_dlq WHERE mapping_name = $1",
                    &[&name],
                )
                .await
        } else {
            self.client
                .execute("DELETE FROM __puffgres_dlq", &[])
                .await
        }
        .map_err(|e| PgError::Postgres(e.to_string()))?;

        Ok(count)
    }

    // -------------------------------------------------------------------------
    // Backfill progress methods
    // -------------------------------------------------------------------------

    /// Get backfill progress for a mapping.
    pub async fn get_backfill_progress(&self, mapping_name: &str) -> PgResult<Option<BackfillProgress>> {
        let row = self
            .client
            .query_opt(
                r#"
                SELECT mapping_name, last_id, total_rows, processed_rows, status, updated_at
                FROM __puffgres_backfill
                WHERE mapping_name = $1
                "#,
                &[&mapping_name],
            )
            .await
            .map_err(|e| PgError::Postgres(e.to_string()))?;

        Ok(row.map(|r| BackfillProgress {
            mapping_name: r.get(0),
            last_id: r.get(1),
            total_rows: r.get(2),
            processed_rows: r.get::<_, i64>(3),
            status: r.get(4),
            updated_at: r.get(5),
        }))
    }

    /// Update backfill progress.
    pub async fn update_backfill_progress(
        &self,
        mapping_name: &str,
        last_id: Option<&str>,
        total_rows: Option<i64>,
        processed_rows: i64,
        status: &str,
    ) -> PgResult<()> {
        self.client
            .execute(
                r#"
                INSERT INTO __puffgres_backfill (mapping_name, last_id, total_rows, processed_rows, status, updated_at)
                VALUES ($1, $2, $3, $4, $5, NOW())
                ON CONFLICT (mapping_name)
                DO UPDATE SET last_id = $2, total_rows = $3, processed_rows = $4, status = $5, updated_at = NOW()
                "#,
                &[&mapping_name, &last_id, &total_rows, &processed_rows, &status],
            )
            .await
            .map_err(|e| PgError::Postgres(e.to_string()))?;

        Ok(())
    }

    /// Clear backfill progress for a mapping.
    pub async fn clear_backfill_progress(&self, mapping_name: &str) -> PgResult<()> {
        self.client
            .execute(
                "DELETE FROM __puffgres_backfill WHERE mapping_name = $1",
                &[&mapping_name],
            )
            .await
            .map_err(|e| PgError::Postgres(e.to_string()))?;

        Ok(())
    }

    // -------------------------------------------------------------------------
    // Transform storage methods
    // -------------------------------------------------------------------------

    /// Store a transform for immutability tracking.
    pub async fn store_transform(
        &self,
        mapping_name: &str,
        version: i32,
        content: &str,
        content_hash: &str,
    ) -> PgResult<()> {
        self.client
            .execute(
                r#"
                INSERT INTO __puffgres_transforms (mapping_name, version, content, content_hash)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (mapping_name, version) DO NOTHING
                "#,
                &[&mapping_name, &version, &content, &content_hash],
            )
            .await
            .map_err(|e| PgError::Postgres(e.to_string()))?;

        info!(mapping_name, version, "Stored transform");
        Ok(())
    }

    /// Get a stored transform by mapping name and version.
    pub async fn get_transform(
        &self,
        mapping_name: &str,
        version: i32,
    ) -> PgResult<Option<StoredTransform>> {
        let row = self
            .client
            .query_opt(
                r#"
                SELECT id, mapping_name, version, content, content_hash, created_at
                FROM __puffgres_transforms
                WHERE mapping_name = $1 AND version = $2
                "#,
                &[&mapping_name, &version],
            )
            .await
            .map_err(|e| PgError::Postgres(e.to_string()))?;

        Ok(row.map(|r| StoredTransform {
            id: r.get(0),
            mapping_name: r.get(1),
            version: r.get(2),
            content: r.get(3),
            content_hash: r.get(4),
            created_at: r.get(5),
        }))
    }

    /// Get all stored transforms.
    pub async fn get_all_transforms(&self) -> PgResult<Vec<StoredTransform>> {
        let rows = self
            .client
            .query(
                r#"
                SELECT id, mapping_name, version, content, content_hash, created_at
                FROM __puffgres_transforms
                ORDER BY mapping_name, version
                "#,
                &[],
            )
            .await
            .map_err(|e| PgError::Postgres(e.to_string()))?;

        Ok(rows
            .into_iter()
            .map(|r| StoredTransform {
                id: r.get(0),
                mapping_name: r.get(1),
                version: r.get(2),
                content: r.get(3),
                content_hash: r.get(4),
                created_at: r.get(5),
            })
            .collect())
    }

    // -------------------------------------------------------------------------
    // Migration content storage methods
    // -------------------------------------------------------------------------

    /// Store migration content for reset functionality.
    pub async fn store_migration_content(
        &self,
        version: i32,
        mapping_name: &str,
        content: &str,
    ) -> PgResult<()> {
        self.client
            .execute(
                r#"
                INSERT INTO __puffgres_migration_content (version, mapping_name, content)
                VALUES ($1, $2, $3)
                ON CONFLICT (version, mapping_name) DO NOTHING
                "#,
                &[&version, &mapping_name, &content],
            )
            .await
            .map_err(|e| PgError::Postgres(e.to_string()))?;

        Ok(())
    }

    /// Get migration content by version and mapping name.
    pub async fn get_migration_content(
        &self,
        version: i32,
        mapping_name: &str,
    ) -> PgResult<Option<String>> {
        let row = self
            .client
            .query_opt(
                r#"
                SELECT content
                FROM __puffgres_migration_content
                WHERE version = $1 AND mapping_name = $2
                "#,
                &[&version, &mapping_name],
            )
            .await
            .map_err(|e| PgError::Postgres(e.to_string()))?;

        Ok(row.map(|r| r.get(0)))
    }

    /// Get all migration content.
    pub async fn get_all_migration_content(&self) -> PgResult<Vec<(i32, String, String)>> {
        let rows = self
            .client
            .query(
                r#"
                SELECT version, mapping_name, content
                FROM __puffgres_migration_content
                ORDER BY version, mapping_name
                "#,
                &[],
            )
            .await
            .map_err(|e| PgError::Postgres(e.to_string()))?;

        Ok(rows
            .into_iter()
            .map(|r| (r.get(0), r.get(1), r.get(2)))
            .collect())
    }

    // -------------------------------------------------------------------------
    // Table validation methods
    // -------------------------------------------------------------------------

    /// Check if a table exists in the database.
    pub async fn table_exists(&self, schema: &str, table: &str) -> PgResult<bool> {
        let row = self
            .client
            .query_opt(
                r#"
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = $1 AND table_name = $2
                "#,
                &[&schema, &table],
            )
            .await
            .map_err(|e| PgError::Postgres(e.to_string()))?;

        Ok(row.is_some())
    }

    /// Validate that a table exists, returning an error with a helpful message if not.
    pub async fn validate_table_exists(&self, schema: &str, table: &str) -> PgResult<()> {
        if !self.table_exists(schema, table).await? {
            return Err(PgError::TableNotFound {
                schema: schema.to_string(),
                table: table.to_string(),
            });
        }
        Ok(())
    }

    // -------------------------------------------------------------------------
    // Cleanup methods
    // -------------------------------------------------------------------------

    /// Clear all checkpoints.
    pub async fn clear_all_checkpoints(&self) -> PgResult<u64> {
        let count = self
            .client
            .execute("DELETE FROM __puffgres_checkpoints", &[])
            .await
            .map_err(|e| PgError::Postgres(e.to_string()))?;

        info!(count, "Cleared all checkpoints");
        Ok(count)
    }

    /// Drop all puffgres tables.
    pub async fn drop_all_tables(&self) -> PgResult<()> {
        let tables = [
            "__puffgres_migrations",
            "__puffgres_checkpoints",
            "__puffgres_dlq",
            "__puffgres_backfill",
            "__puffgres_transforms",
            "__puffgres_migration_content",
        ];

        for table in &tables {
            self.client
                .execute(&format!("DROP TABLE IF EXISTS {} CASCADE", table), &[])
                .await
                .map_err(|e| PgError::Postgres(e.to_string()))?;
            info!(table, "Dropped table");
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checkpoint_default() {
        let cp = Checkpoint::default();
        assert_eq!(cp.lsn, 0);
        assert_eq!(cp.events_processed, 0);
        assert!(cp.updated_at.is_none());
    }
}
