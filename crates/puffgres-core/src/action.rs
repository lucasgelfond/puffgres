use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::types::Value;

/// A document to be written to turbopuffer.
pub type Document = HashMap<String, Value>;

/// The result of transforming a RowEvent.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Action {
    /// Upsert a document into the namespace.
    Upsert {
        /// The document ID (from the id column mapping).
        id: DocumentId,
        /// The document to upsert.
        doc: Document,
    },
    /// Delete a document from the namespace.
    Delete {
        /// The document ID to delete.
        id: DocumentId,
    },
    /// Skip this event (no action needed).
    Skip,
    /// An error occurred during transformation.
    Error {
        /// The error kind for classification.
        kind: ErrorKind,
        /// Human-readable error message.
        message: String,
    },
}

/// A document ID in turbopuffer.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DocumentId {
    Uint(u64),
    Int(i64),
    Uuid(String),
    String(String),
}

impl DocumentId {
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            DocumentId::Uint(u) => Some(*u),
            DocumentId::Int(i) if *i >= 0 => Some(*i as u64),
            _ => None,
        }
    }
}

impl From<u64> for DocumentId {
    fn from(v: u64) -> Self {
        DocumentId::Uint(v)
    }
}

impl From<i64> for DocumentId {
    fn from(v: i64) -> Self {
        DocumentId::Int(v)
    }
}

impl From<String> for DocumentId {
    fn from(v: String) -> Self {
        DocumentId::String(v)
    }
}

impl From<&str> for DocumentId {
    fn from(v: &str) -> Self {
        DocumentId::String(v.to_string())
    }
}

/// Classification of transform errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorKind {
    /// Missing required column in the row.
    MissingColumn,
    /// Invalid data type for a column.
    InvalidType,
    /// Transform function failed.
    TransformFailed,
    /// Membership predicate evaluation failed.
    PredicateFailed,
    /// Generic/unknown error.
    Unknown,
}

impl Action {
    /// Create an upsert action.
    pub fn upsert(id: impl Into<DocumentId>, doc: Document) -> Self {
        Action::Upsert { id: id.into(), doc }
    }

    /// Create a delete action.
    pub fn delete(id: impl Into<DocumentId>) -> Self {
        Action::Delete { id: id.into() }
    }

    /// Create a skip action.
    pub fn skip() -> Self {
        Action::Skip
    }

    /// Create an error action.
    pub fn error(kind: ErrorKind, message: impl Into<String>) -> Self {
        Action::Error {
            kind,
            message: message.into(),
        }
    }

    /// Check if this action requires a write to turbopuffer.
    pub fn requires_write(&self) -> bool {
        matches!(self, Action::Upsert { .. } | Action::Delete { .. })
    }

    /// Check if this is an error action.
    pub fn is_error(&self) -> bool {
        matches!(self, Action::Error { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_action_upsert() {
        let doc: Document = [("name".into(), Value::String("test".into()))]
            .into_iter()
            .collect();
        let action = Action::upsert(42u64, doc.clone());

        match action {
            Action::Upsert { id, doc: d } => {
                assert_eq!(id, DocumentId::Uint(42));
                assert_eq!(d, doc);
            }
            _ => panic!("Expected Upsert"),
        }
    }

    #[test]
    fn test_action_requires_write() {
        assert!(Action::upsert(1u64, HashMap::new()).requires_write());
        assert!(Action::delete(1u64).requires_write());
        assert!(!Action::skip().requires_write());
        assert!(!Action::error(ErrorKind::Unknown, "test").requires_write());
    }

    #[test]
    fn test_document_id_conversions() {
        let id: DocumentId = 42u64.into();
        assert_eq!(id.as_u64(), Some(42));

        let id: DocumentId = (-5i64).into();
        assert_eq!(id.as_u64(), None);

        let id: DocumentId = "abc".into();
        assert!(matches!(id, DocumentId::String(_)));
    }
}
