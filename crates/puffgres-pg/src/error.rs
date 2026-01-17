use thiserror::Error;

#[derive(Debug, Error)]
pub enum PgError {
    #[error("postgres error: {0}")]
    Postgres(String),

    #[error("connection failed: {0}")]
    Connection(String),

    #[error("replication slot '{0}' does not exist")]
    SlotNotFound(String),

    #[error("failed to create replication slot: {0}")]
    SlotCreationFailed(String),

    #[error("wal2json parse error: {0}")]
    ParseError(String),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("invalid LSN format: {0}")]
    InvalidLsn(String),
}

impl From<tokio_postgres::Error> for PgError {
    fn from(e: tokio_postgres::Error) -> Self {
        // Extract database error details if available
        if let Some(db_err) = e.as_db_error() {
            let msg = format!(
                "{}: {} (code: {})",
                db_err.severity(),
                db_err.message(),
                db_err.code().code()
            );
            PgError::Postgres(msg)
        } else {
            PgError::Postgres(e.to_string())
        }
    }
}

pub type PgResult<T> = Result<T, PgError>;
