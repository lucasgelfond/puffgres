use thiserror::Error;

/// Errors that can occur when parsing or validating configuration.
#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to parse TOML: {0}")]
    ParseError(#[from] toml::de::Error),

    #[error("missing required field: {field}")]
    MissingField { field: String },

    #[error("invalid id type '{value}': expected one of uint, int, uuid, string")]
    InvalidIdType { value: String },

    #[error("invalid membership mode '{value}': expected one of dsl, view, lookup")]
    InvalidMembershipMode { value: String },

    #[error("invalid predicate syntax: {message}")]
    InvalidPredicate { message: String },

    #[error("invalid versioning mode '{value}': expected one of source_lsn, column, none")]
    InvalidVersioningMode { value: String },

    #[error("missing id column '{column}' in columns list")]
    IdColumnNotInColumns { column: String },

    #[error("DSL membership requires 'predicate' field")]
    MissingPredicate,

    #[error("column versioning requires 'column' field")]
    MissingVersioningColumn,

    #[error("version must be a positive integer, got {0}")]
    InvalidVersion(i64),

    #[error("transform configuration error: {0}")]
    TransformError(String),
}

pub type ConfigResult<T> = Result<T, ConfigError>;
