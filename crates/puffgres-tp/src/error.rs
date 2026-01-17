use thiserror::Error;

/// Errors from turbopuffer operations.
#[derive(Debug, Error)]
pub enum TpError {
    #[error("network error: {0}")]
    Network(String),

    #[error("rate limited (429)")]
    RateLimited,

    #[error("server error ({status}): {message}")]
    ServerError { status: u16, message: String },

    #[error("validation error: {0}")]
    Validation(String),

    #[error("namespace not found: {0}")]
    NamespaceNotFound(String),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("rs-puff error: {0}")]
    RsPuff(String),
}

impl TpError {
    /// Check if this error is retryable.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            TpError::Network(_)
                | TpError::RateLimited
                | TpError::ServerError {
                    status: 500..=599,
                    ..
                }
        )
    }

    /// Check if this error is permanent.
    pub fn is_permanent(&self) -> bool {
        matches!(self, TpError::Validation(_))
    }
}

pub type TpResult<T> = Result<T, TpError>;
