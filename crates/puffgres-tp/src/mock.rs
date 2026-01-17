use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

use puffgres_core::WriteRequest;

use crate::client::{TurbopufferClient, WriteResponse};
use crate::error::TpResult;

/// A mock turbopuffer client for testing.
#[derive(Clone, Default)]
pub struct MockClient {
    state: Arc<Mutex<MockState>>,
}

#[derive(Default)]
struct MockState {
    /// Recorded write requests by namespace.
    writes: HashMap<String, Vec<WriteRequest>>,
    /// Namespaces that exist.
    namespaces: std::collections::HashSet<String>,
    /// If set, all operations will fail with this error.
    fail_with: Option<String>,
}

impl MockClient {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a mock client that simulates failures.
    pub fn failing(error_message: impl Into<String>) -> Self {
        let client = Self::new();
        client.state.lock().unwrap().fail_with = Some(error_message.into());
        client
    }

    /// Get all write requests for a namespace.
    pub fn get_writes(&self, namespace: &str) -> Vec<WriteRequest> {
        let state = self.state.lock().unwrap();
        state.writes.get(namespace).cloned().unwrap_or_default()
    }

    /// Get total number of write requests across all namespaces.
    pub fn total_writes(&self) -> usize {
        let state = self.state.lock().unwrap();
        state.writes.values().map(|v| v.len()).sum()
    }

    /// Get total number of upserts across all writes.
    pub fn total_upserts(&self) -> usize {
        let state = self.state.lock().unwrap();
        state
            .writes
            .values()
            .flat_map(|v| v.iter())
            .map(|w| w.upserts.len())
            .sum()
    }

    /// Get total number of deletes across all writes.
    pub fn total_deletes(&self) -> usize {
        let state = self.state.lock().unwrap();
        state
            .writes
            .values()
            .flat_map(|v| v.iter())
            .map(|w| w.deletes.len())
            .sum()
    }

    /// Clear all recorded writes.
    pub fn clear(&self) {
        let mut state = self.state.lock().unwrap();
        state.writes.clear();
    }

    /// Pre-create a namespace.
    pub fn create_namespace(&self, namespace: impl Into<String>) {
        let mut state = self.state.lock().unwrap();
        state.namespaces.insert(namespace.into());
    }
}

impl TurbopufferClient for MockClient {
    fn write(&self, request: WriteRequest) -> impl Future<Output = TpResult<WriteResponse>> + Send {
        let state = self.state.clone();
        async move {
            let mut state = state.lock().unwrap();

            if let Some(ref error) = state.fail_with {
                return Err(crate::error::TpError::Network(error.clone()));
            }

            let namespace = request.namespace.clone();
            let affected_count = request.upserts.len() + request.deletes.len();

            state
                .writes
                .entry(namespace.clone())
                .or_default()
                .push(request);

            // Auto-create namespace on write
            state.namespaces.insert(namespace);

            Ok(WriteResponse {
                affected_count,
                affected_ids: vec![],
            })
        }
    }

    fn namespace_exists(&self, namespace: &str) -> impl Future<Output = TpResult<bool>> + Send {
        let state = self.state.clone();
        let namespace = namespace.to_string();
        async move {
            let state = state.lock().unwrap();

            if let Some(ref error) = state.fail_with {
                return Err(crate::error::TpError::Network(error.clone()));
            }

            Ok(state.namespaces.contains(&namespace))
        }
    }

    fn delete_namespace(&self, namespace: &str) -> impl Future<Output = TpResult<()>> + Send {
        let state = self.state.clone();
        let namespace = namespace.to_string();
        async move {
            let mut state = state.lock().unwrap();

            if let Some(ref error) = state.fail_with {
                return Err(crate::error::TpError::Network(error.clone()));
            }

            state.namespaces.remove(&namespace);
            state.writes.remove(&namespace);
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use puffgres_core::{DocumentId, UpsertDoc, Value};

    fn make_write_request(namespace: &str, num_upserts: usize) -> WriteRequest {
        let upserts: Vec<UpsertDoc> = (0..num_upserts)
            .map(|i| UpsertDoc {
                id: DocumentId::Uint(i as u64),
                attributes: [("name".into(), Value::String(format!("doc_{}", i)))]
                    .into_iter()
                    .collect(),
            })
            .collect();

        WriteRequest {
            namespace: namespace.into(),
            upserts,
            deletes: vec![],
            lsn: 100,
        }
    }

    #[tokio::test]
    async fn test_mock_client_records_writes() {
        let client = MockClient::new();

        let request = make_write_request("test_ns", 3);
        client.write(request).await.unwrap();

        assert_eq!(client.total_writes(), 1);
        assert_eq!(client.total_upserts(), 3);

        let writes = client.get_writes("test_ns");
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].upserts.len(), 3);
    }

    #[tokio::test]
    async fn test_mock_client_multiple_namespaces() {
        let client = MockClient::new();

        client.write(make_write_request("ns1", 2)).await.unwrap();
        client.write(make_write_request("ns2", 3)).await.unwrap();
        client.write(make_write_request("ns1", 1)).await.unwrap();

        assert_eq!(client.total_writes(), 3);
        assert_eq!(client.get_writes("ns1").len(), 2);
        assert_eq!(client.get_writes("ns2").len(), 1);
    }

    #[tokio::test]
    async fn test_mock_client_namespace_exists() {
        let client = MockClient::new();

        assert!(!client.namespace_exists("test").await.unwrap());

        client.create_namespace("test");
        assert!(client.namespace_exists("test").await.unwrap());

        // Write auto-creates namespace
        client
            .write(make_write_request("auto_ns", 1))
            .await
            .unwrap();
        assert!(client.namespace_exists("auto_ns").await.unwrap());
    }

    #[tokio::test]
    async fn test_mock_client_failing() {
        let client = MockClient::failing("simulated failure");

        let result = client.write(make_write_request("test", 1)).await;
        assert!(result.is_err());

        let result = client.namespace_exists("test").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_mock_client_clear() {
        let client = MockClient::new();

        client.write(make_write_request("test", 5)).await.unwrap();
        assert_eq!(client.total_writes(), 1);

        client.clear();
        assert_eq!(client.total_writes(), 0);
    }
}
