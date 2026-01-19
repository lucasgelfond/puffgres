//! Replication slot management.
//!
//! Handles creating, verifying, and recreating PostgreSQL logical replication slots.

use tokio_postgres::Client;
use tracing::{info, warn};

use crate::error::{PgError, PgResult};

/// Check if a replication slot exists.
pub async fn slot_exists(client: &Client, slot_name: &str) -> PgResult<bool> {
    let exists: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
            &[&slot_name],
        )
        .await?
        .get(0);

    Ok(exists)
}

/// Get the plugin used by a replication slot.
pub async fn get_slot_plugin(client: &Client, slot_name: &str) -> PgResult<Option<String>> {
    let row = client
        .query_opt(
            "SELECT plugin FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await?;

    Ok(row.and_then(|r| r.get(0)))
}

/// Create a logical replication slot with pgoutput.
pub async fn create_slot(client: &Client, slot_name: &str) -> PgResult<()> {
    info!(slot = %slot_name, "Creating replication slot with pgoutput");
    client
        .execute(
            "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&slot_name],
        )
        .await
        .map_err(|e| PgError::SlotCreationFailed(e.to_string()))?;

    Ok(())
}

/// Drop a replication slot.
pub async fn drop_slot(client: &Client, slot_name: &str) -> PgResult<()> {
    info!(slot = %slot_name, "Dropping replication slot");
    client
        .execute("SELECT pg_drop_replication_slot($1)", &[&slot_name])
        .await
        .map_err(|e| PgError::Replication(format!("Failed to drop slot: {}", e)))?;

    Ok(())
}

/// Ensure a replication slot exists with the correct plugin.
///
/// If the slot doesn't exist, creates it.
/// If the slot exists but uses the wrong plugin, drops and recreates it.
pub async fn ensure_slot(client: &Client, slot_name: &str, create_if_missing: bool) -> PgResult<()> {
    if slot_exists(client, slot_name).await? {
        // Slot exists - verify it's using pgoutput plugin
        let plugin = get_slot_plugin(client, slot_name).await?;

        if plugin.as_deref() != Some("pgoutput") {
            warn!(
                slot = %slot_name,
                plugin = ?plugin,
                "Existing slot uses wrong plugin, dropping and recreating"
            );
            drop_slot(client, slot_name).await?;
            create_slot(client, slot_name).await?;
            info!(slot = %slot_name, "Recreated replication slot with pgoutput");
        } else {
            info!(slot = %slot_name, "Using existing replication slot");
        }
    } else if create_if_missing {
        create_slot(client, slot_name).await?;
    } else {
        return Err(PgError::SlotNotFound(slot_name.to_string()));
    }

    Ok(())
}

/// Get the confirmed_flush_lsn for a slot.
pub async fn get_confirmed_flush_lsn(client: &Client, slot_name: &str) -> PgResult<Option<String>> {
    let row = client
        .query_opt(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await?;

    Ok(row.and_then(|r| r.get(0)))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: These tests require a running Postgres instance with logical replication enabled.
    // They are designed to be run with a test database.

    #[tokio::test]
    #[ignore] // Requires live database
    async fn test_slot_lifecycle() {
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

        let slot_name = "test_slot_lifecycle";

        // Clean up any existing slot
        let _ = drop_slot(&client, slot_name).await;

        // Verify slot doesn't exist
        assert!(!slot_exists(&client, slot_name).await.unwrap());

        // Create slot
        create_slot(&client, slot_name).await.unwrap();
        assert!(slot_exists(&client, slot_name).await.unwrap());

        // Verify plugin
        let plugin = get_slot_plugin(&client, slot_name).await.unwrap();
        assert_eq!(plugin, Some("pgoutput".to_string()));

        // Get LSN (should be some value)
        let lsn = get_confirmed_flush_lsn(&client, slot_name).await.unwrap();
        assert!(lsn.is_some());

        // Drop slot
        drop_slot(&client, slot_name).await.unwrap();
        assert!(!slot_exists(&client, slot_name).await.unwrap());
    }

    #[tokio::test]
    #[ignore] // Requires live database
    async fn test_ensure_slot_creates_when_missing() {
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

        let slot_name = "test_ensure_creates";

        // Clean up
        let _ = drop_slot(&client, slot_name).await;

        // Ensure creates the slot
        ensure_slot(&client, slot_name, true).await.unwrap();
        assert!(slot_exists(&client, slot_name).await.unwrap());

        // Clean up
        drop_slot(&client, slot_name).await.unwrap();
    }

    #[tokio::test]
    #[ignore] // Requires live database
    async fn test_ensure_slot_errors_when_not_creating() {
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

        let slot_name = "test_ensure_no_create";

        // Clean up
        let _ = drop_slot(&client, slot_name).await;

        // Ensure should error when create_if_missing is false
        let result = ensure_slot(&client, slot_name, false).await;
        assert!(matches!(result, Err(PgError::SlotNotFound(_))));
    }
}
