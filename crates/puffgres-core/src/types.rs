use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A value from a Postgres row, supporting common types.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Array(Vec<Value>),
    Object(HashMap<String, Value>),
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float(f) => Some(*f),
            Value::Int(i) => Some(*i as f64),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }
}

impl From<serde_json::Value> for Value {
    fn from(v: serde_json::Value) -> Self {
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
            serde_json::Value::Array(arr) => {
                Value::Array(arr.into_iter().map(Value::from).collect())
            }
            serde_json::Value::Object(obj) => {
                Value::Object(obj.into_iter().map(|(k, v)| (k, Value::from(v))).collect())
            }
        }
    }
}

impl From<Value> for serde_json::Value {
    fn from(v: Value) -> Self {
        match v {
            Value::Null => serde_json::Value::Null,
            Value::Bool(b) => serde_json::Value::Bool(b),
            Value::Int(i) => serde_json::Value::Number(i.into()),
            Value::Float(f) => serde_json::Number::from_f64(f)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            Value::String(s) => serde_json::Value::String(s),
            Value::Array(arr) => {
                serde_json::Value::Array(arr.into_iter().map(serde_json::Value::from).collect())
            }
            Value::Object(obj) => serde_json::Value::Object(
                obj.into_iter()
                    .map(|(k, v)| (k, serde_json::Value::from(v)))
                    .collect(),
            ),
        }
    }
}

/// The type of database operation that produced this event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Operation {
    Insert,
    Update,
    Delete,
}

/// A row map containing column name to value mappings.
pub type RowMap = HashMap<String, Value>;

/// A change event derived from the Postgres WAL.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RowEvent {
    /// The type of operation (insert, update, delete).
    pub op: Operation,
    /// The schema name (e.g., "public").
    pub schema: String,
    /// The table name.
    pub table: String,
    /// The new row values (present for insert/update).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new: Option<RowMap>,
    /// The old row values (present for update/delete with replica identity).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub old: Option<RowMap>,
    /// The log sequence number (monotonic source position).
    pub lsn: u64,
    /// Optional transaction ID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub txid: Option<u64>,
    /// Optional timestamp.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,
}

impl RowEvent {
    /// Get the relevant row data for this event.
    /// For inserts/updates, returns new; for deletes, returns old.
    pub fn row(&self) -> Option<&RowMap> {
        match self.op {
            Operation::Insert | Operation::Update => self.new.as_ref(),
            Operation::Delete => self.old.as_ref(),
        }
    }

    /// Get a value from the new row.
    pub fn get_new(&self, column: &str) -> Option<&Value> {
        self.new.as_ref().and_then(|row| row.get(column))
    }

    /// Get a value from the old row.
    pub fn get_old(&self, column: &str) -> Option<&Value> {
        self.old.as_ref().and_then(|row| row.get(column))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_is_null() {
        assert!(Value::Null.is_null());
        assert!(!Value::Bool(true).is_null());
        assert!(!Value::Int(42).is_null());
        assert!(!Value::String("test".into()).is_null());
    }

    #[test]
    fn test_value_accessors() {
        assert_eq!(Value::Bool(true).as_bool(), Some(true));
        assert_eq!(Value::Int(42).as_i64(), Some(42));
        assert_eq!(Value::Float(3.14).as_f64(), Some(3.14));
        assert_eq!(Value::Int(42).as_f64(), Some(42.0));
        assert_eq!(Value::String("hello".into()).as_str(), Some("hello"));
    }

    #[test]
    fn test_value_json_roundtrip() {
        let original = Value::Object(
            [
                ("name".to_string(), Value::String("test".into())),
                ("count".to_string(), Value::Int(42)),
                ("active".to_string(), Value::Bool(true)),
            ]
            .into_iter()
            .collect(),
        );

        let json: serde_json::Value = original.clone().into();
        let back: Value = json.into();
        assert_eq!(original, back);
    }

    #[test]
    fn test_row_event_row() {
        let insert = RowEvent {
            op: Operation::Insert,
            schema: "public".into(),
            table: "users".into(),
            new: Some([("id".into(), Value::Int(1))].into_iter().collect()),
            old: None,
            lsn: 100,
            txid: None,
            timestamp: None,
        };
        assert!(insert.row().is_some());
        assert_eq!(insert.get_new("id"), Some(&Value::Int(1)));

        let delete = RowEvent {
            op: Operation::Delete,
            schema: "public".into(),
            table: "users".into(),
            new: None,
            old: Some([("id".into(), Value::Int(1))].into_iter().collect()),
            lsn: 101,
            txid: None,
            timestamp: None,
        };
        assert!(delete.row().is_some());
        assert_eq!(delete.get_old("id"), Some(&Value::Int(1)));
    }
}
