//! Table validation for replication.
//!
//! Validates that tables exist and are readable before setting up replication.

use tokio_postgres::Client;
use tracing::{debug, warn};

use crate::error::{PgError, PgResult};
use super::publication::{parse_table_ref, quote_ident};

/// Check if a table exists.
pub async fn table_exists(client: &Client, schema: &str, table: &str) -> PgResult<bool> {
    let exists: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)",
            &[&schema, &table],
        )
        .await?
        .get(0);

    Ok(exists)
}

/// Check if we can read from a table.
pub async fn table_readable(client: &Client, schema: &str, table: &str) -> PgResult<bool> {
    let query = format!(
        "SELECT 1 FROM {}.{} LIMIT 0",
        quote_ident(schema),
        quote_ident(table)
    );

    match client.execute(&query, &[]).await {
        Ok(_) => Ok(true),
        Err(e) => {
            debug!(
                schema = schema,
                table = table,
                error = %e,
                "Table is not readable"
            );
            Ok(false)
        }
    }
}

/// Validate that a table exists.
pub async fn validate_table_exists(client: &Client, schema: &str, table: &str) -> PgResult<()> {
    if !table_exists(client, schema, table).await? {
        return Err(PgError::TableNotFound {
            schema: schema.to_string(),
            table: table.to_string(),
        });
    }
    Ok(())
}

/// Validate that a table exists and is readable.
pub async fn validate_table_readable(client: &Client, schema: &str, table: &str) -> PgResult<()> {
    validate_table_exists(client, schema, table).await?;

    if !table_readable(client, schema, table).await? {
        return Err(PgError::Replication(format!(
            "Cannot read from table {}.{} - check permissions",
            schema, table
        )));
    }

    Ok(())
}

/// Validate that all specified tables exist and are readable.
///
/// Tables can be specified as "schema.table" or just "table" (defaults to "public" schema).
pub async fn validate_all_tables_readable(client: &Client, tables: &[String]) -> PgResult<()> {
    for table_ref in tables {
        let (schema, table) = parse_table_ref(table_ref);
        validate_table_readable(client, schema, table).await?;
    }

    debug!(tables = ?tables, "All tables are readable");
    Ok(())
}

/// Check if logical replication is properly configured by verifying the slot and publication.
pub async fn check_replication_setup(
    client: &Client,
    slot_name: &str,
    publication_name: &str,
) -> PgResult<ReplicationStatus> {
    // Check slot
    let slot_row = client
        .query_opt(
            r#"
            SELECT plugin, confirmed_flush_lsn::text
            FROM pg_replication_slots
            WHERE slot_name = $1
            "#,
            &[&slot_name],
        )
        .await?;

    let slot_status = match slot_row {
        Some(row) => {
            let plugin: Option<String> = row.get(0);
            let lsn: Option<String> = row.get(1);
            if plugin.as_deref() == Some("pgoutput") {
                SlotStatus::Ready { lsn }
            } else {
                SlotStatus::WrongPlugin { plugin }
            }
        }
        None => SlotStatus::Missing,
    };

    // Check publication
    let pub_row = client
        .query_opt(
            "SELECT 1 FROM pg_publication WHERE pubname = $1",
            &[&publication_name],
        )
        .await?;

    let publication_status = if pub_row.is_some() {
        PublicationStatus::Exists
    } else {
        PublicationStatus::Missing
    };

    Ok(ReplicationStatus {
        slot: slot_status,
        publication: publication_status,
    })
}

/// Status of a replication slot.
#[derive(Debug, Clone, PartialEq)]
pub enum SlotStatus {
    /// Slot is ready with pgoutput plugin.
    Ready { lsn: Option<String> },
    /// Slot exists but uses wrong plugin.
    WrongPlugin { plugin: Option<String> },
    /// Slot doesn't exist.
    Missing,
}

impl SlotStatus {
    pub fn is_ready(&self) -> bool {
        matches!(self, SlotStatus::Ready { .. })
    }

    pub fn needs_reset(&self) -> bool {
        matches!(self, SlotStatus::Missing | SlotStatus::WrongPlugin { .. })
    }
}

/// Status of a publication.
#[derive(Debug, Clone, PartialEq)]
pub enum PublicationStatus {
    /// Publication exists.
    Exists,
    /// Publication doesn't exist.
    Missing,
}

impl PublicationStatus {
    pub fn is_ready(&self) -> bool {
        matches!(self, PublicationStatus::Exists)
    }
}

/// Overall replication setup status.
#[derive(Debug, Clone)]
pub struct ReplicationStatus {
    pub slot: SlotStatus,
    pub publication: PublicationStatus,
}

impl ReplicationStatus {
    /// Check if replication is fully set up.
    pub fn is_ready(&self) -> bool {
        self.slot.is_ready() && self.publication.is_ready()
    }

    /// Check if a reset is needed.
    pub fn needs_reset(&self) -> bool {
        self.slot.needs_reset() || !self.publication.is_ready()
    }
}

/// Reset replication by dropping and recreating slot and publication.
pub async fn reset_replication(
    client: &Client,
    slot_name: &str,
    publication_name: &str,
    tables: &[String],
) -> PgResult<()> {
    use super::slot::{drop_slot, create_slot, slot_exists};
    use super::publication::{drop_publication, create_publication_for_tables, create_publication_all_tables};

    warn!(
        slot = slot_name,
        publication = publication_name,
        "Resetting replication setup"
    );

    // Drop slot if exists
    if slot_exists(client, slot_name).await? {
        drop_slot(client, slot_name).await?;
    }

    // Drop publication
    drop_publication(client, publication_name).await?;

    // Recreate publication
    if tables.is_empty() {
        create_publication_all_tables(client, publication_name).await?;
    } else {
        create_publication_for_tables(client, publication_name, tables).await?;
    }

    // Recreate slot
    create_slot(client, slot_name).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slot_status_is_ready() {
        assert!(SlotStatus::Ready { lsn: Some("0/0".to_string()) }.is_ready());
        assert!(SlotStatus::Ready { lsn: None }.is_ready());
        assert!(!SlotStatus::Missing.is_ready());
        assert!(!SlotStatus::WrongPlugin { plugin: Some("test_decoding".to_string()) }.is_ready());
    }

    #[test]
    fn test_slot_status_needs_reset() {
        assert!(!SlotStatus::Ready { lsn: Some("0/0".to_string()) }.needs_reset());
        assert!(SlotStatus::Missing.needs_reset());
        assert!(SlotStatus::WrongPlugin { plugin: None }.needs_reset());
    }

    #[test]
    fn test_publication_status_is_ready() {
        assert!(PublicationStatus::Exists.is_ready());
        assert!(!PublicationStatus::Missing.is_ready());
    }

    #[test]
    fn test_replication_status_is_ready() {
        let ready = ReplicationStatus {
            slot: SlotStatus::Ready { lsn: None },
            publication: PublicationStatus::Exists,
        };
        assert!(ready.is_ready());
        assert!(!ready.needs_reset());

        let missing_slot = ReplicationStatus {
            slot: SlotStatus::Missing,
            publication: PublicationStatus::Exists,
        };
        assert!(!missing_slot.is_ready());
        assert!(missing_slot.needs_reset());

        let missing_pub = ReplicationStatus {
            slot: SlotStatus::Ready { lsn: None },
            publication: PublicationStatus::Missing,
        };
        assert!(!missing_pub.is_ready());
        assert!(missing_pub.needs_reset());
    }

    // Integration tests

    #[tokio::test]
    #[ignore] // Requires live database
    async fn test_table_exists() {
        let conn_str = std::env::var("TEST_DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/test".to_string());

        let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
            .await
            .expect("Failed to connect");

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        // Create a test table
        client
            .execute("CREATE TABLE IF NOT EXISTS test_exists_table (id SERIAL PRIMARY KEY)", &[])
            .await
            .unwrap();

        // Should exist
        assert!(table_exists(&client, "public", "test_exists_table").await.unwrap());

        // Should not exist
        assert!(!table_exists(&client, "public", "nonexistent_table_xyz").await.unwrap());

        // Clean up
        client.execute("DROP TABLE IF EXISTS test_exists_table", &[]).await.unwrap();
    }

    #[tokio::test]
    #[ignore] // Requires live database
    async fn test_table_readable() {
        let conn_str = std::env::var("TEST_DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/test".to_string());

        let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
            .await
            .expect("Failed to connect");

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        // Create a test table
        client
            .execute("CREATE TABLE IF NOT EXISTS test_readable_table (id SERIAL PRIMARY KEY)", &[])
            .await
            .unwrap();

        // Should be readable
        assert!(table_readable(&client, "public", "test_readable_table").await.unwrap());

        // Non-existent table should not be readable
        assert!(!table_readable(&client, "public", "nonexistent_table_xyz").await.unwrap());

        // Clean up
        client.execute("DROP TABLE IF EXISTS test_readable_table", &[]).await.unwrap();
    }

    #[tokio::test]
    #[ignore] // Requires live database
    async fn test_validate_all_tables_readable() {
        let conn_str = std::env::var("TEST_DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/test".to_string());

        let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
            .await
            .expect("Failed to connect");

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        // Create test tables
        client
            .execute("CREATE TABLE IF NOT EXISTS test_validate_t1 (id SERIAL PRIMARY KEY)", &[])
            .await
            .unwrap();
        client
            .execute("CREATE TABLE IF NOT EXISTS test_validate_t2 (id SERIAL PRIMARY KEY)", &[])
            .await
            .unwrap();

        // All should be readable
        let tables = vec![
            "public.test_validate_t1".to_string(),
            "test_validate_t2".to_string(), // Without schema
        ];
        validate_all_tables_readable(&client, &tables).await.unwrap();

        // With non-existent table should fail
        let bad_tables = vec![
            "public.test_validate_t1".to_string(),
            "public.nonexistent_xyz".to_string(),
        ];
        let result = validate_all_tables_readable(&client, &bad_tables).await;
        assert!(result.is_err());

        // Clean up
        client.execute("DROP TABLE IF EXISTS test_validate_t1", &[]).await.unwrap();
        client.execute("DROP TABLE IF EXISTS test_validate_t2", &[]).await.unwrap();
    }

    #[tokio::test]
    #[ignore] // Requires live database
    async fn test_check_replication_setup() {
        let conn_str = std::env::var("TEST_DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/test".to_string());

        let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
            .await
            .expect("Failed to connect");

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        // Check with non-existent slot/publication
        let status = check_replication_setup(&client, "nonexistent_slot", "nonexistent_pub").await.unwrap();
        assert_eq!(status.slot, SlotStatus::Missing);
        assert_eq!(status.publication, PublicationStatus::Missing);
        assert!(!status.is_ready());
        assert!(status.needs_reset());
    }
}
