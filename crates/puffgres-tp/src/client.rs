use std::collections::HashMap;
use std::future::Future;

use puffgres_core::{DocumentId, WriteRequest};

use crate::error::{TpError, TpResult};

/// Trait for turbopuffer client operations.
pub trait TurbopufferClient: Send + Sync {
    /// Execute a write request (upserts and deletes).
    fn write(&self, request: WriteRequest) -> impl Future<Output = TpResult<WriteResponse>> + Send;

    /// Check if a namespace exists.
    fn namespace_exists(&self, namespace: &str) -> impl Future<Output = TpResult<bool>> + Send;

    /// Delete all documents in a namespace.
    fn delete_namespace(&self, namespace: &str) -> impl Future<Output = TpResult<()>> + Send;
}

/// Response from a write operation.
#[derive(Debug, Clone, Default)]
pub struct WriteResponse {
    /// Number of documents affected.
    pub affected_count: usize,
    /// IDs of affected documents (if requested).
    pub affected_ids: Vec<DocumentId>,
}

/// Adapter that wraps rs-puff client.
pub struct RsPuffAdapter {
    client: rs_puff::Client,
}

impl RsPuffAdapter {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            client: rs_puff::Client::new(api_key),
        }
    }

    fn convert_doc_id_to_json(id: &DocumentId) -> serde_json::Value {
        match id {
            DocumentId::Uint(u) => serde_json::Value::Number((*u).into()),
            DocumentId::Int(i) => serde_json::Value::Number((*i).into()),
            DocumentId::Uuid(s) | DocumentId::String(s) => serde_json::Value::String(s.clone()),
        }
    }

    fn convert_value_to_json(value: &puffgres_core::Value) -> serde_json::Value {
        match value {
            puffgres_core::Value::Null => serde_json::Value::Null,
            puffgres_core::Value::Bool(b) => serde_json::Value::Bool(*b),
            puffgres_core::Value::Int(i) => serde_json::Value::Number((*i).into()),
            puffgres_core::Value::Float(f) => serde_json::Number::from_f64(*f)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            puffgres_core::Value::String(s) => serde_json::Value::String(s.clone()),
            puffgres_core::Value::Array(arr) => {
                serde_json::Value::Array(arr.iter().map(Self::convert_value_to_json).collect())
            }
            puffgres_core::Value::Object(obj) => serde_json::Value::Object(
                obj.iter()
                    .map(|(k, v)| (k.clone(), Self::convert_value_to_json(v)))
                    .collect(),
            ),
        }
    }
}

impl TurbopufferClient for RsPuffAdapter {
    fn write(&self, request: WriteRequest) -> impl Future<Output = TpResult<WriteResponse>> + Send {
        let namespace = request.namespace.clone();
        let upserts_len = request.upserts.len();
        let deletes_len = request.deletes.len();

        // Build upsert rows
        let upsert_rows: Option<Vec<HashMap<String, serde_json::Value>>> =
            if request.upserts.is_empty() {
                None
            } else {
                Some(
                    request
                        .upserts
                        .iter()
                        .map(|doc| {
                            let mut row: HashMap<String, serde_json::Value> = doc
                                .attributes
                                .iter()
                                .map(|(k, v)| (k.clone(), Self::convert_value_to_json(v)))
                                .collect();
                            row.insert("id".to_string(), Self::convert_doc_id_to_json(&doc.id));
                            row.insert(
                                "__source_lsn".to_string(),
                                serde_json::Value::Number(request.lsn.into()),
                            );
                            row
                        })
                        .collect(),
                )
            };

        // Build delete IDs
        let deletes: Option<Vec<serde_json::Value>> = if request.deletes.is_empty() {
            None
        } else {
            Some(
                request
                    .deletes
                    .iter()
                    .map(Self::convert_doc_id_to_json)
                    .collect(),
            )
        };

        let params = rs_puff::WriteParams {
            upsert_rows,
            deletes,
            ..Default::default()
        };

        let ns = self.client.namespace(&namespace);

        async move {
            ns.write(params)
                .await
                .map_err(|e| TpError::RsPuff(e.to_string()))?;

            Ok(WriteResponse {
                affected_count: upserts_len + deletes_len,
                affected_ids: vec![],
            })
        }
    }

    fn namespace_exists(&self, namespace: &str) -> impl Future<Output = TpResult<bool>> + Send {
        let ns = self.client.namespace(namespace);
        async move {
            ns.exists()
                .await
                .map_err(|e| TpError::RsPuff(e.to_string()))
        }
    }

    fn delete_namespace(&self, namespace: &str) -> impl Future<Output = TpResult<()>> + Send {
        let ns = self.client.namespace(namespace);
        async move {
            ns.delete_all()
                .await
                .map_err(|e| TpError::RsPuff(e.to_string()))?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use puffgres_core::Value;

    #[test]
    fn test_convert_doc_id() {
        assert_eq!(
            RsPuffAdapter::convert_doc_id_to_json(&DocumentId::Uint(42)),
            serde_json::json!(42)
        );
        assert_eq!(
            RsPuffAdapter::convert_doc_id_to_json(&DocumentId::String("abc".into())),
            serde_json::json!("abc")
        );
    }

    #[test]
    fn test_convert_value() {
        assert_eq!(
            RsPuffAdapter::convert_value_to_json(&Value::String("hello".into())),
            serde_json::json!("hello")
        );
        assert_eq!(
            RsPuffAdapter::convert_value_to_json(&Value::Int(42)),
            serde_json::json!(42)
        );
        assert_eq!(
            RsPuffAdapter::convert_value_to_json(&Value::Bool(true)),
            serde_json::json!(true)
        );
    }
}
