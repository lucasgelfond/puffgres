//! Tokio runtime for async operations.

use std::sync::OnceLock;
use tokio::runtime::Runtime;

/// Global Tokio runtime for async operations.
pub static RUNTIME: OnceLock<Runtime> = OnceLock::new();

/// Get or create the global runtime.
pub fn get_runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        Runtime::new().expect("Failed to create Tokio runtime")
    })
}
