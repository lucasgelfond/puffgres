use crate::action::{Action, Document, DocumentId};
use crate::error::{Error, Result};
use crate::types::{Operation, RowEvent, Value};

/// Trait for transforming row events into turbopuffer actions.
pub trait Transformer: Send + Sync {
    /// Transform a row event into an action.
    fn transform(&self, event: &RowEvent, id: DocumentId) -> Result<Action>;
}

/// Identity transformer that maps selected columns directly to the document.
pub struct IdentityTransformer {
    /// Columns to include in the document.
    columns: Vec<String>,
}

impl IdentityTransformer {
    pub fn new(columns: Vec<String>) -> Self {
        Self { columns }
    }

    /// Create an identity transformer that includes all columns from the row.
    pub fn all() -> Self {
        Self { columns: vec![] }
    }
}

impl Transformer for IdentityTransformer {
    fn transform(&self, event: &RowEvent, id: DocumentId) -> Result<Action> {
        match event.op {
            Operation::Delete => Ok(Action::delete(id)),
            Operation::Insert | Operation::Update => {
                let row = event.new.as_ref().ok_or_else(|| {
                    Error::TransformError("missing new row for insert/update".into())
                })?;

                let doc: Document = if self.columns.is_empty() {
                    // Include all columns
                    row.clone()
                } else {
                    // Include only selected columns
                    self.columns
                        .iter()
                        .filter_map(|col| row.get(col).map(|v| (col.clone(), v.clone())))
                        .collect()
                };

                Ok(Action::upsert(id, doc))
            }
        }
    }
}

/// A transformer that wraps a function.
pub struct FnTransformer<F>
where
    F: Fn(&RowEvent, DocumentId) -> Result<Action> + Send + Sync,
{
    func: F,
}

impl<F> FnTransformer<F>
where
    F: Fn(&RowEvent, DocumentId) -> Result<Action> + Send + Sync,
{
    pub fn new(func: F) -> Self {
        Self { func }
    }
}

impl<F> Transformer for FnTransformer<F>
where
    F: Fn(&RowEvent, DocumentId) -> Result<Action> + Send + Sync,
{
    fn transform(&self, event: &RowEvent, id: DocumentId) -> Result<Action> {
        (self.func)(event, id)
    }
}

/// Extract the document ID from a row event based on the configured id column.
pub fn extract_id(event: &RowEvent, id_column: &str, id_type: IdType) -> Result<DocumentId> {
    let row = event.row().ok_or(Error::MissingId)?;
    let value = row
        .get(id_column)
        .ok_or_else(|| Error::MissingColumn(id_column.to_string()))?;

    match (id_type, value) {
        (IdType::Uint, Value::Int(i)) if *i >= 0 => Ok(DocumentId::Uint(*i as u64)),
        (IdType::Int, Value::Int(i)) => Ok(DocumentId::Int(*i)),
        (IdType::Uuid, Value::String(s)) => Ok(DocumentId::Uuid(s.clone())),
        (IdType::String, Value::String(s)) => Ok(DocumentId::String(s.clone())),
        (IdType::String, Value::Int(i)) => Ok(DocumentId::String(i.to_string())),
        (expected, actual) => Err(Error::InvalidIdType(format!(
            "expected {:?}, got {:?}",
            expected, actual
        ))),
    }
}

/// The type of the ID column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdType {
    Uint,
    Int,
    Uuid,
    String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_event(op: Operation, new: Option<HashMap<String, Value>>) -> RowEvent {
        RowEvent {
            op,
            schema: "public".into(),
            table: "users".into(),
            new,
            old: None,
            lsn: 100,
            txid: None,
            timestamp: None,
        }
    }

    #[test]
    fn test_identity_transformer_insert() {
        let transformer = IdentityTransformer::new(vec!["name".into(), "email".into()]);

        let event = make_event(
            Operation::Insert,
            Some(
                [
                    ("id".into(), Value::Int(1)),
                    ("name".into(), Value::String("Alice".into())),
                    ("email".into(), Value::String("alice@example.com".into())),
                    ("extra".into(), Value::String("ignored".into())),
                ]
                .into_iter()
                .collect(),
            ),
        );

        let action = transformer.transform(&event, 1u64.into()).unwrap();

        match action {
            Action::Upsert { id, doc } => {
                assert_eq!(id, DocumentId::Uint(1));
                assert_eq!(doc.len(), 2);
                assert!(doc.contains_key("name"));
                assert!(doc.contains_key("email"));
                assert!(!doc.contains_key("extra"));
            }
            _ => panic!("Expected Upsert"),
        }
    }

    #[test]
    fn test_identity_transformer_all_columns() {
        let transformer = IdentityTransformer::all();

        let event = make_event(
            Operation::Insert,
            Some(
                [
                    ("id".into(), Value::Int(1)),
                    ("name".into(), Value::String("Alice".into())),
                ]
                .into_iter()
                .collect(),
            ),
        );

        let action = transformer.transform(&event, 1u64.into()).unwrap();

        match action {
            Action::Upsert { doc, .. } => {
                assert_eq!(doc.len(), 2);
            }
            _ => panic!("Expected Upsert"),
        }
    }

    #[test]
    fn test_identity_transformer_delete() {
        let transformer = IdentityTransformer::new(vec!["name".into()]);

        let event = RowEvent {
            op: Operation::Delete,
            schema: "public".into(),
            table: "users".into(),
            new: None,
            old: Some([("id".into(), Value::Int(1))].into_iter().collect()),
            lsn: 100,
            txid: None,
            timestamp: None,
        };

        let action = transformer.transform(&event, 1u64.into()).unwrap();
        assert!(matches!(action, Action::Delete { .. }));
    }

    #[test]
    fn test_extract_id() {
        let event = make_event(
            Operation::Insert,
            Some([("id".into(), Value::Int(42))].into_iter().collect()),
        );

        let id = extract_id(&event, "id", IdType::Uint).unwrap();
        assert_eq!(id, DocumentId::Uint(42));

        let id = extract_id(&event, "id", IdType::Int).unwrap();
        assert_eq!(id, DocumentId::Int(42));
    }

    #[test]
    fn test_extract_id_uuid() {
        let uuid = "550e8400-e29b-41d4-a716-446655440000";
        let event = make_event(
            Operation::Insert,
            Some(
                [("id".into(), Value::String(uuid.into()))]
                    .into_iter()
                    .collect(),
            ),
        );

        let id = extract_id(&event, "id", IdType::Uuid).unwrap();
        assert_eq!(id, DocumentId::Uuid(uuid.into()));
    }
}
