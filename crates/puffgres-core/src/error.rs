use thiserror::Error;

/// Errors that can occur in puffgres-core.
#[derive(Debug, Error)]
pub enum Error {
    #[error("missing required column: {0}")]
    MissingColumn(String),

    #[error("invalid column type for '{column}': expected {expected}, got {actual}")]
    InvalidColumnType {
        column: String,
        expected: String,
        actual: String,
    },

    #[error("predicate evaluation failed: {0}")]
    PredicateError(String),

    #[error("transform error: {0}")]
    TransformError(String),

    #[error("serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("batch size exceeded: {size} > {max}")]
    BatchSizeExceeded { size: usize, max: usize },

    #[error("no id column found in row")]
    MissingId,

    #[error("invalid id type: {0}")]
    InvalidIdType(String),
}

pub type Result<T> = std::result::Result<T, Error>;
