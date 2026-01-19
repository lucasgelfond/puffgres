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
        /// Distance metric for vector fields.
        #[serde(skip_serializing_if = "Option::is_none")]
        distance_metric: Option<rs_puff::DistanceMetric>,
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
    // Permanent errors - will not succeed on retry
    /// Missing required column in the row.
    MissingColumn,
    /// Invalid data type for a column.
    InvalidType,
    /// Transform function failed.
    TransformFailed,
    /// Membership predicate evaluation failed.
    PredicateFailed,
    /// Schema error (e.g., column doesn't exist).
    SchemaError,
    /// Invalid data that cannot be serialized.
    InvalidData,

    // Retryable errors - may succeed on retry
    /// Network error (connection failed, timeout).
    NetworkError,
    /// Rate limit exceeded.
    RateLimited,
    /// Service temporarily unavailable.
    ServiceUnavailable,
    /// Timeout waiting for response.
    Timeout,

    /// Generic/unknown error.
    Unknown,
}

impl ErrorKind {
    /// Check if this error kind is retryable.
    ///
    /// Retryable errors are transient issues that may succeed on retry:
    /// - Network errors
    /// - Rate limits
    /// - Timeouts
    /// - Service unavailability
    ///
    /// Permanent errors should not be retried:
    /// - Schema errors
    /// - Transform errors
    /// - Invalid data
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            ErrorKind::NetworkError
                | ErrorKind::RateLimited
                | ErrorKind::ServiceUnavailable
                | ErrorKind::Timeout
        )
    }

    /// Get a human-readable description of this error kind.
    pub fn description(&self) -> &'static str {
        match self {
            ErrorKind::MissingColumn => "Missing column",
            ErrorKind::InvalidType => "Invalid type",
            ErrorKind::TransformFailed => "Transform failed",
            ErrorKind::PredicateFailed => "Predicate failed",
            ErrorKind::SchemaError => "Schema error",
            ErrorKind::InvalidData => "Invalid data",
            ErrorKind::NetworkError => "Network error",
            ErrorKind::RateLimited => "Rate limited",
            ErrorKind::ServiceUnavailable => "Service unavailable",
            ErrorKind::Timeout => "Timeout",
            ErrorKind::Unknown => "Unknown error",
        }
    }

    /// Convert from string (for deserialization from DLQ).
    pub fn from_str(s: &str) -> Self {
        match s {
            "missing_column" => ErrorKind::MissingColumn,
            "invalid_type" => ErrorKind::InvalidType,
            "transform_failed" => ErrorKind::TransformFailed,
            "predicate_failed" => ErrorKind::PredicateFailed,
            "schema_error" => ErrorKind::SchemaError,
            "invalid_data" => ErrorKind::InvalidData,
            "network_error" => ErrorKind::NetworkError,
            "rate_limited" => ErrorKind::RateLimited,
            "service_unavailable" => ErrorKind::ServiceUnavailable,
            "timeout" => ErrorKind::Timeout,
            _ => ErrorKind::Unknown,
        }
    }

    /// Convert to string (for serialization to DLQ).
    pub fn as_str(&self) -> &'static str {
        match self {
            ErrorKind::MissingColumn => "missing_column",
            ErrorKind::InvalidType => "invalid_type",
            ErrorKind::TransformFailed => "transform_failed",
            ErrorKind::PredicateFailed => "predicate_failed",
            ErrorKind::SchemaError => "schema_error",
            ErrorKind::InvalidData => "invalid_data",
            ErrorKind::NetworkError => "network_error",
            ErrorKind::RateLimited => "rate_limited",
            ErrorKind::ServiceUnavailable => "service_unavailable",
            ErrorKind::Timeout => "timeout",
            ErrorKind::Unknown => "unknown",
        }
    }
}

impl Action {
    /// Create an upsert action.
    pub fn upsert(id: impl Into<DocumentId>, doc: Document) -> Self {
        Action::Upsert {
            id: id.into(),
            doc,
            distance_metric: None,
        }
    }

    /// Create an upsert action with a distance metric for vector fields.
    pub fn upsert_with_metric(
        id: impl Into<DocumentId>,
        doc: Document,
        distance_metric: rs_puff::DistanceMetric,
    ) -> Self {
        Action::Upsert {
            id: id.into(),
            doc,
            distance_metric: Some(distance_metric),
        }
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

    #[test]
    fn test_error_kind_retryable() {
        // Permanent errors
        assert!(!ErrorKind::MissingColumn.is_retryable());
        assert!(!ErrorKind::InvalidType.is_retryable());
        assert!(!ErrorKind::TransformFailed.is_retryable());
        assert!(!ErrorKind::PredicateFailed.is_retryable());
        assert!(!ErrorKind::SchemaError.is_retryable());
        assert!(!ErrorKind::InvalidData.is_retryable());
        assert!(!ErrorKind::Unknown.is_retryable());

        // Retryable errors
        assert!(ErrorKind::NetworkError.is_retryable());
        assert!(ErrorKind::RateLimited.is_retryable());
        assert!(ErrorKind::ServiceUnavailable.is_retryable());
        assert!(ErrorKind::Timeout.is_retryable());
    }

    #[test]
    fn test_error_kind_roundtrip() {
        let kinds = [
            ErrorKind::MissingColumn,
            ErrorKind::InvalidType,
            ErrorKind::TransformFailed,
            ErrorKind::NetworkError,
            ErrorKind::RateLimited,
        ];

        for kind in kinds {
            let s = kind.as_str();
            let parsed = ErrorKind::from_str(s);
            assert_eq!(kind, parsed);
        }
    }
}
