//! JavaScript/TypeScript transform support.
//!
//! Executes transforms by calling out to Node.js.

use std::collections::HashMap;
use std::io::Write;
use std::process::{Command, Stdio};

use crate::action::{Action, DocumentId};
use crate::error::{Error, Result};
use crate::types::{Operation, RowEvent, Value};

/// A transformer that executes JavaScript/TypeScript transforms via Node.js.
pub struct JsTransformer {
    /// Path to the transform file.
    transform_path: String,
    /// Path to the transform runner script.
    runner_path: Option<String>,
}

impl JsTransformer {
    /// Create a new JS transformer.
    pub fn new(transform_path: impl Into<String>) -> Self {
        Self {
            transform_path: transform_path.into(),
            runner_path: None,
        }
    }

    /// Set the path to the transform runner script.
    pub fn with_runner_path(mut self, path: impl Into<String>) -> Self {
        self.runner_path = Some(path.into());
        self
    }

    /// Transform a batch of row events by calling the JS transform.
    /// Takes a slice of (event, id) pairs and returns a Vec of Actions.
    pub fn transform_batch(&self, rows: &[(&RowEvent, DocumentId)]) -> Result<Vec<Action>> {
        if rows.is_empty() {
            return Ok(vec![]);
        }

        // Serialize the rows array to JSON
        let rows_json: Vec<serde_json::Value> = rows
            .iter()
            .map(|(event, id)| {
                let event_json = serde_json::json!({
                    "op": match event.op {
                        Operation::Insert => "insert",
                        Operation::Update => "update",
                        Operation::Delete => "delete",
                    },
                    "schema": event.schema,
                    "table": event.table,
                    "new": event.new.as_ref().map(|m| value_map_to_json(m)),
                    "old": event.old.as_ref().map(|m| value_map_to_json(m)),
                    "lsn": event.lsn,
                });

                let id_json = match id {
                    DocumentId::Uint(u) => serde_json::json!(u),
                    DocumentId::Int(i) => serde_json::json!(i),
                    DocumentId::Uuid(s) | DocumentId::String(s) => serde_json::json!(s),
                };

                serde_json::json!({
                    "event": event_json,
                    "id": id_json,
                })
            })
            .collect();

        // Build the runner command
        let runner_script = self.runner_path.as_deref().unwrap_or("puffgres-transform");
        let rows_json_str = serde_json::to_string(&rows_json).unwrap();

        // Spawn the process with stdin piped to avoid "Argument list too long" errors
        let mut child = Command::new("npx")
            .arg(runner_script)
            .arg(&self.transform_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .envs(std::env::vars())
            .spawn()
            .map_err(|e| Error::TransformError(format!("Failed to spawn transform: {}", e)))?;

        // Write JSON to stdin
        if let Some(mut stdin) = child.stdin.take() {
            stdin
                .write_all(rows_json_str.as_bytes())
                .map_err(|e| Error::TransformError(format!("Failed to write to transform stdin: {}", e)))?;
        }

        // Wait for the process to complete
        let output = child
            .wait_with_output()
            .map_err(|e| Error::TransformError(format!("Failed to run transform: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::TransformError(format!(
                "Transform failed: {}",
                stderr
            )));
        }

        // Parse the result array
        let stdout = String::from_utf8_lossy(&output.stdout);
        let results: Vec<serde_json::Value> = serde_json::from_str(&stdout).map_err(|e| {
            Error::TransformError(format!("Failed to parse transform result: {}", e))
        })?;

        if results.len() != rows.len() {
            return Err(Error::TransformError(format!(
                "Transform returned {} results, expected {}",
                results.len(),
                rows.len()
            )));
        }

        // Convert each result to an Action
        results
            .iter()
            .zip(rows.iter())
            .map(|(result, (_, id))| parse_action(result, id.clone()))
            .collect()
    }

    /// Transform a single row event (convenience wrapper).
    pub fn transform(&self, event: &RowEvent, id: DocumentId) -> Result<Action> {
        let results = self.transform_batch(&[(event, id.clone())])?;
        results.into_iter().next().ok_or_else(|| {
            Error::TransformError("Transform returned empty result".into())
        })
    }
}

fn value_map_to_json(map: &HashMap<String, Value>) -> serde_json::Value {
    serde_json::Value::Object(
        map.iter()
            .map(|(k, v)| (k.clone(), value_to_json(v)))
            .collect(),
    )
}

fn value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(i) => serde_json::Value::Number((*i).into()),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::String(s) => serde_json::Value::String(s.clone()),
        Value::Array(arr) => serde_json::Value::Array(arr.iter().map(value_to_json).collect()),
        Value::Object(obj) => serde_json::Value::Object(
            obj.iter()
                .map(|(k, v)| (k.clone(), value_to_json(v)))
                .collect(),
        ),
    }
}

fn parse_action(result: &serde_json::Value, default_id: DocumentId) -> Result<Action> {
    let obj = result
        .as_object()
        .ok_or_else(|| Error::TransformError("Transform result must be an object".into()))?;

    let action_type = obj
        .get("type")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::TransformError("Transform result must have a 'type' field".into()))?;

    match action_type {
        "upsert" => {
            let id = parse_id(obj.get("id"), default_id)?;
            let doc = obj.get("doc").and_then(|v| v.as_object()).ok_or_else(|| {
                Error::TransformError("Upsert action must have a 'doc' field".into())
            })?;

            let attributes: HashMap<String, Value> = doc
                .iter()
                .map(|(k, v)| (k.clone(), json_to_value(v)))
                .collect();

            // Parse distance_metric if present
            let distance_metric = obj
                .get("distance_metric")
                .and_then(|v| v.as_str())
                .and_then(|s| match s {
                    "cosine_distance" => Some(rs_puff::DistanceMetric::CosineDistance),
                    "euclidean_squared" => Some(rs_puff::DistanceMetric::EuclideanSquared),
                    _ => None,
                });

            if let Some(metric) = distance_metric {
                Ok(Action::upsert_with_metric(id, attributes, metric))
            } else {
                Ok(Action::upsert(id, attributes))
            }
        }
        "delete" => {
            let id = parse_id(obj.get("id"), default_id)?;
            Ok(Action::delete(id))
        }
        "skip" => Ok(Action::skip()),
        _ => Err(Error::TransformError(format!(
            "Unknown action type: {}",
            action_type
        ))),
    }
}

fn parse_id(id_value: Option<&serde_json::Value>, default: DocumentId) -> Result<DocumentId> {
    match id_value {
        Some(serde_json::Value::Number(n)) => {
            if let Some(u) = n.as_u64() {
                Ok(DocumentId::Uint(u))
            } else if let Some(i) = n.as_i64() {
                Ok(DocumentId::Int(i))
            } else {
                Ok(default)
            }
        }
        Some(serde_json::Value::String(s)) => {
            // Try to detect if it's a UUID
            if s.len() == 36 && s.contains('-') {
                Ok(DocumentId::Uuid(s.clone()))
            } else {
                Ok(DocumentId::String(s.clone()))
            }
        }
        _ => Ok(default),
    }
}

fn json_to_value(json: &serde_json::Value) -> Value {
    match json {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float(f)
            } else {
                Value::Null
            }
        }
        serde_json::Value::String(s) => Value::String(s.clone()),
        serde_json::Value::Array(arr) => Value::Array(arr.iter().map(json_to_value).collect()),
        serde_json::Value::Object(obj) => Value::Object(
            obj.iter()
                .map(|(k, v)| (k.clone(), json_to_value(v)))
                .collect(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_to_json() {
        let value = Value::Object(
            [
                ("name".to_string(), Value::String("Alice".to_string())),
                ("age".to_string(), Value::Int(30)),
            ]
            .into_iter()
            .collect(),
        );

        let json = value_to_json(&value);
        assert!(json.is_object());
        assert_eq!(json["name"], "Alice");
        assert_eq!(json["age"], 30);
    }

    #[test]
    fn test_json_to_value() {
        let json = serde_json::json!({
            "name": "Bob",
            "active": true,
            "scores": [1, 2, 3]
        });

        let value = json_to_value(&json);
        match value {
            Value::Object(obj) => {
                assert!(matches!(obj.get("name"), Some(Value::String(_))));
                assert!(matches!(obj.get("active"), Some(Value::Bool(true))));
                assert!(matches!(obj.get("scores"), Some(Value::Array(_))));
            }
            _ => panic!("Expected object"),
        }
    }

    #[test]
    fn test_parse_action_skip() {
        let json = serde_json::json!({ "type": "skip" });
        let action = parse_action(&json, DocumentId::Uint(1)).unwrap();
        assert!(matches!(action, Action::Skip));
    }

    #[test]
    fn test_parse_action_delete() {
        let json = serde_json::json!({ "type": "delete", "id": 42 });
        let action = parse_action(&json, DocumentId::Uint(1)).unwrap();
        match action {
            Action::Delete { id } => assert_eq!(id, DocumentId::Uint(42)),
            _ => panic!("Expected delete"),
        }
    }

    #[test]
    fn test_parse_action_upsert() {
        let json = serde_json::json!({
            "type": "upsert",
            "id": "abc-123",
            "doc": {
                "name": "Test",
                "value": 100
            }
        });
        let action = parse_action(&json, DocumentId::Uint(1)).unwrap();
        match action {
            Action::Upsert { id, doc, .. } => {
                assert_eq!(id, DocumentId::String("abc-123".to_string()));
                assert!(doc.contains_key("name"));
                assert!(doc.contains_key("value"));
            }
            _ => panic!("Expected upsert"),
        }
    }
}
