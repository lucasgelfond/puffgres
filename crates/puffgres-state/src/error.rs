use thiserror::Error;

#[derive(Debug, Error)]
pub enum StateError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("state file not found: {0}")]
    NotFound(String),

    #[error("serialization error: {0}")]
    Serialization(String),
}

pub type StateResult<T> = Result<T, StateError>;
