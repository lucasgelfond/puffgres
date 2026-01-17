mod error;
mod sqlite;

pub use error::{StateError, StateResult};
pub use sqlite::SqliteStateStore;

/// Checkpoint state for a mapping.
#[derive(Debug, Clone, Default)]
pub struct Checkpoint {
    /// The last successfully processed LSN.
    pub lsn: u64,
    /// Number of events processed.
    pub events_processed: u64,
}

/// Trait for state storage backends.
pub trait StateStore: Send + Sync {
    /// Get the checkpoint for a mapping.
    fn get_checkpoint(&self, mapping_name: &str) -> StateResult<Option<Checkpoint>>;

    /// Save a checkpoint for a mapping.
    fn save_checkpoint(&self, mapping_name: &str, checkpoint: &Checkpoint) -> StateResult<()>;

    /// Get all checkpoints.
    fn get_all_checkpoints(&self) -> StateResult<Vec<(String, Checkpoint)>>;

    /// Get the minimum LSN across all mappings (safe restart point).
    fn get_min_lsn(&self) -> StateResult<Option<u64>> {
        let checkpoints = self.get_all_checkpoints()?;
        Ok(checkpoints.iter().map(|(_, c)| c.lsn).min())
    }
}
