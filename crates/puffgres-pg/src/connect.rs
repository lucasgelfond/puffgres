//! Shared Postgres connection utilities with TLS support.

use std::sync::Arc;

use rustls::ClientConfig;
use tokio_postgres::Client;
use tokio_postgres_rustls_improved::MakeRustlsConnect;

use crate::error::{PgError, PgResult};

/// Connect to Postgres with appropriate TLS settings based on sslmode in connection string.
/// Spawns the connection task and returns only the client.
pub async fn connect_postgres(connection_string: &str) -> PgResult<Client> {
    let requires_tls = requires_tls(connection_string);

    if requires_tls {
        let config = ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .map_err(|e| PgError::Connection(format!("TLS config error: {}", e)))?
            .with_root_certificates(root_certs())
            .with_no_client_auth();

        let connector = MakeRustlsConnect::new(config);

        let (client, connection) = tokio_postgres::connect(connection_string, connector)
            .await
            .map_err(|e| PgError::Connection(e.to_string()))?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!(error = %e, "Postgres connection error");
            }
        });

        Ok(client)
    } else {
        let (client, connection) = tokio_postgres::connect(connection_string, tokio_postgres::NoTls)
            .await
            .map_err(|e| PgError::Connection(e.to_string()))?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!(error = %e, "Postgres connection error");
            }
        });

        Ok(client)
    }
}

/// Get root certificates from webpki-roots.
fn root_certs() -> rustls::RootCertStore {
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    roots
}

/// Check if the connection string requires TLS.
fn requires_tls(connection_string: &str) -> bool {
    connection_string.contains("sslmode=require")
        || connection_string.contains("sslmode=verify-ca")
        || connection_string.contains("sslmode=verify-full")
}
