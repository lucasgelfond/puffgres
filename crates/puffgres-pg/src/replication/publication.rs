//! Publication management.
//!
//! Handles creating, verifying, and updating PostgreSQL publications for logical replication.

use std::collections::HashSet;

use tokio_postgres::Client;
use tracing::{debug, info};

use crate::error::{PgError, PgResult};

/// Quote an identifier for use in SQL (double quotes).
pub fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// Quote a possibly schema-qualified table name (e.g., "public.users" -> "public"."users").
pub fn quote_table_name(s: &str) -> String {
    if let Some((schema, table)) = s.split_once('.') {
        format!("{}.{}", quote_ident(schema), quote_ident(table))
    } else {
        quote_ident(s)
    }
}

/// Parse a table reference into (schema, table).
/// If no schema is specified, defaults to "public".
pub fn parse_table_ref(table_ref: &str) -> (&str, &str) {
    if let Some((schema, table)) = table_ref.split_once('.') {
        (schema, table)
    } else {
        ("public", table_ref)
    }
}

/// Check if a publication exists.
pub async fn publication_exists(client: &Client, publication_name: &str) -> PgResult<bool> {
    let exists: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)",
            &[&publication_name],
        )
        .await?
        .get(0);

    Ok(exists)
}

/// Get the tables in a publication.
pub async fn get_publication_tables(client: &Client, publication_name: &str) -> PgResult<HashSet<String>> {
    let rows = client
        .query(
            r#"
            SELECT schemaname, tablename
            FROM pg_publication_tables
            WHERE pubname = $1
            "#,
            &[&publication_name],
        )
        .await?;

    let tables: HashSet<String> = rows
        .iter()
        .map(|r| {
            let schema: String = r.get(0);
            let table: String = r.get(1);
            format!("{}.{}", schema, table)
        })
        .collect();

    Ok(tables)
}

/// Create a publication for all tables.
pub async fn create_publication_all_tables(client: &Client, publication_name: &str) -> PgResult<()> {
    info!(publication = %publication_name, "Creating publication for all tables");
    client
        .execute(
            &format!(
                "CREATE PUBLICATION {} FOR ALL TABLES",
                quote_ident(publication_name)
            ),
            &[],
        )
        .await?;

    Ok(())
}

/// Create a publication for specific tables.
pub async fn create_publication_for_tables(
    client: &Client,
    publication_name: &str,
    tables: &[String],
) -> PgResult<()> {
    let quoted_tables = tables
        .iter()
        .map(|t| quote_table_name(t))
        .collect::<Vec<_>>()
        .join(", ");

    info!(publication = %publication_name, tables = %quoted_tables, "Creating publication");
    client
        .execute(
            &format!(
                "CREATE PUBLICATION {} FOR TABLE {}",
                quote_ident(publication_name),
                quoted_tables
            ),
            &[],
        )
        .await?;

    Ok(())
}

/// Add tables to an existing publication.
pub async fn add_tables_to_publication(
    client: &Client,
    publication_name: &str,
    tables: &[String],
) -> PgResult<()> {
    if tables.is_empty() {
        return Ok(());
    }

    let quoted_tables = tables
        .iter()
        .map(|t| quote_table_name(t))
        .collect::<Vec<_>>()
        .join(", ");

    info!(
        publication = %publication_name,
        tables = %quoted_tables,
        "Adding tables to publication"
    );

    client
        .execute(
            &format!(
                "ALTER PUBLICATION {} ADD TABLE {}",
                quote_ident(publication_name),
                quoted_tables
            ),
            &[],
        )
        .await
        .map_err(|e| PgError::Replication(format!("Failed to add tables to publication: {}", e)))?;

    Ok(())
}

/// Drop a publication.
pub async fn drop_publication(client: &Client, publication_name: &str) -> PgResult<()> {
    info!(publication = %publication_name, "Dropping publication");
    client
        .execute(
            &format!("DROP PUBLICATION IF EXISTS {}", quote_ident(publication_name)),
            &[],
        )
        .await?;

    Ok(())
}

/// Ensure a publication exists with the required tables.
///
/// If the publication doesn't exist, creates it.
/// If the publication exists but is missing tables, adds them.
pub async fn ensure_publication(
    client: &Client,
    publication_name: &str,
    tables: &[String],
    create_if_missing: bool,
) -> PgResult<()> {
    if publication_exists(client, publication_name).await? {
        if !tables.is_empty() {
            ensure_publication_has_tables(client, publication_name, tables).await?;
        } else {
            info!(publication = %publication_name, "Using existing publication");
        }
    } else if create_if_missing {
        if tables.is_empty() {
            create_publication_all_tables(client, publication_name).await?;
        } else {
            create_publication_for_tables(client, publication_name, tables).await?;
        }
    } else {
        return Err(PgError::PublicationNotFound(publication_name.to_string()));
    }

    Ok(())
}

/// Ensure a publication has all the required tables.
pub async fn ensure_publication_has_tables(
    client: &Client,
    publication_name: &str,
    required_tables: &[String],
) -> PgResult<()> {
    let current_tables = get_publication_tables(client, publication_name).await?;

    debug!(
        publication = %publication_name,
        current = ?current_tables,
        required = ?required_tables,
        "Checking publication tables"
    );

    // Find missing tables
    let missing: Vec<String> = required_tables
        .iter()
        .filter(|t| {
            // Normalize the table reference (add public schema if missing)
            let (schema, table) = parse_table_ref(t);
            let normalized = format!("{}.{}", schema, table);
            !current_tables.contains(&normalized)
        })
        .cloned()
        .collect();

    if missing.is_empty() {
        info!(
            publication = %publication_name,
            tables = ?current_tables,
            "Publication has all required tables"
        );
        return Ok(());
    }

    add_tables_to_publication(client, publication_name, &missing).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quote_ident_simple() {
        assert_eq!(quote_ident("users"), "\"users\"");
        assert_eq!(quote_ident("public"), "\"public\"");
    }

    #[test]
    fn test_quote_ident_with_quotes() {
        assert_eq!(quote_ident("my\"table"), "\"my\"\"table\"");
    }

    #[test]
    fn test_quote_table_name_simple() {
        assert_eq!(quote_table_name("users"), "\"users\"");
    }

    #[test]
    fn test_quote_table_name_with_schema() {
        assert_eq!(quote_table_name("public.users"), "\"public\".\"users\"");
        assert_eq!(quote_table_name("my_schema.my_table"), "\"my_schema\".\"my_table\"");
    }

    #[test]
    fn test_quote_table_name_with_special_chars() {
        assert_eq!(quote_table_name("public.My\"Table"), "\"public\".\"My\"\"Table\"");
    }

    #[test]
    fn test_parse_table_ref_with_schema() {
        assert_eq!(parse_table_ref("public.users"), ("public", "users"));
        assert_eq!(parse_table_ref("myschema.mytable"), ("myschema", "mytable"));
    }

    #[test]
    fn test_parse_table_ref_without_schema() {
        assert_eq!(parse_table_ref("users"), ("public", "users"));
    }

    // Integration tests that require a live database

    #[tokio::test]
    #[ignore] // Requires live database
    async fn test_publication_lifecycle() {
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

        let pub_name = "test_pub_lifecycle";

        // Clean up any existing publication
        let _ = drop_publication(&client, pub_name).await;

        // Verify publication doesn't exist
        assert!(!publication_exists(&client, pub_name).await.unwrap());

        // Create publication for all tables
        create_publication_all_tables(&client, pub_name).await.unwrap();
        assert!(publication_exists(&client, pub_name).await.unwrap());

        // Drop publication
        drop_publication(&client, pub_name).await.unwrap();
        assert!(!publication_exists(&client, pub_name).await.unwrap());
    }

    #[tokio::test]
    #[ignore] // Requires live database with test tables
    async fn test_publication_with_specific_tables() {
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

        // Create a test table first
        client
            .execute("CREATE TABLE IF NOT EXISTS test_pub_table (id SERIAL PRIMARY KEY)", &[])
            .await
            .unwrap();

        let pub_name = "test_pub_specific";

        // Clean up
        let _ = drop_publication(&client, pub_name).await;

        // Create publication for specific table
        let tables = vec!["public.test_pub_table".to_string()];
        create_publication_for_tables(&client, pub_name, &tables).await.unwrap();

        // Verify tables
        let pub_tables = get_publication_tables(&client, pub_name).await.unwrap();
        assert!(pub_tables.contains("public.test_pub_table"));

        // Clean up
        drop_publication(&client, pub_name).await.unwrap();
        client.execute("DROP TABLE IF EXISTS test_pub_table", &[]).await.unwrap();
    }

    #[tokio::test]
    #[ignore] // Requires live database with test tables
    async fn test_ensure_publication_adds_missing_tables() {
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
            .execute("CREATE TABLE IF NOT EXISTS test_ensure_t1 (id SERIAL PRIMARY KEY)", &[])
            .await
            .unwrap();
        client
            .execute("CREATE TABLE IF NOT EXISTS test_ensure_t2 (id SERIAL PRIMARY KEY)", &[])
            .await
            .unwrap();

        let pub_name = "test_ensure_pub";

        // Clean up
        let _ = drop_publication(&client, pub_name).await;

        // Create publication with first table only
        let tables = vec!["public.test_ensure_t1".to_string()];
        create_publication_for_tables(&client, pub_name, &tables).await.unwrap();

        // Now ensure it has both tables
        let all_tables = vec![
            "public.test_ensure_t1".to_string(),
            "public.test_ensure_t2".to_string(),
        ];
        ensure_publication_has_tables(&client, pub_name, &all_tables).await.unwrap();

        // Verify both tables are there
        let pub_tables = get_publication_tables(&client, pub_name).await.unwrap();
        assert!(pub_tables.contains("public.test_ensure_t1"));
        assert!(pub_tables.contains("public.test_ensure_t2"));

        // Clean up
        drop_publication(&client, pub_name).await.unwrap();
        client.execute("DROP TABLE IF EXISTS test_ensure_t1", &[]).await.unwrap();
        client.execute("DROP TABLE IF EXISTS test_ensure_t2", &[]).await.unwrap();
    }
}
