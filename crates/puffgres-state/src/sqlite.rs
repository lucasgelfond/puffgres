use std::path::Path;
use std::sync::Mutex;

use rusqlite::Connection;
use tracing::info;

use crate::error::StateResult;
use crate::{Checkpoint, StateStore};

/// SQLite-backed state store.
pub struct SqliteStateStore {
    conn: Mutex<Connection>,
}

impl SqliteStateStore {
    /// Open or create a state store at the given path.
    pub fn open(path: impl AsRef<Path>) -> StateResult<Self> {
        let path = path.as_ref();
        info!(path = %path.display(), "Opening state store");

        let conn = Connection::open(path)?;

        // Create tables if they don't exist
        conn.execute(
            "CREATE TABLE IF NOT EXISTS checkpoints (
                mapping_name TEXT PRIMARY KEY,
                lsn INTEGER NOT NULL,
                events_processed INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )",
            [],
        )?;

        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Create an in-memory state store (for testing).
    pub fn in_memory() -> StateResult<Self> {
        let conn = Connection::open_in_memory()?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS checkpoints (
                mapping_name TEXT PRIMARY KEY,
                lsn INTEGER NOT NULL,
                events_processed INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )",
            [],
        )?;

        Ok(Self {
            conn: Mutex::new(conn),
        })
    }
}

impl StateStore for SqliteStateStore {
    fn get_checkpoint(&self, mapping_name: &str) -> StateResult<Option<Checkpoint>> {
        let conn = self.conn.lock().unwrap();

        let mut stmt = conn.prepare(
            "SELECT lsn, events_processed FROM checkpoints WHERE mapping_name = ?1",
        )?;

        let result = stmt.query_row([mapping_name], |row| {
            Ok(Checkpoint {
                lsn: row.get::<_, i64>(0)? as u64,
                events_processed: row.get::<_, i64>(1)? as u64,
            })
        });

        match result {
            Ok(checkpoint) => Ok(Some(checkpoint)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn save_checkpoint(&self, mapping_name: &str, checkpoint: &Checkpoint) -> StateResult<()> {
        let conn = self.conn.lock().unwrap();

        conn.execute(
            "INSERT INTO checkpoints (mapping_name, lsn, events_processed, updated_at)
             VALUES (?1, ?2, ?3, CURRENT_TIMESTAMP)
             ON CONFLICT(mapping_name) DO UPDATE SET
                lsn = ?2,
                events_processed = ?3,
                updated_at = CURRENT_TIMESTAMP",
            rusqlite::params![
                mapping_name,
                checkpoint.lsn as i64,
                checkpoint.events_processed as i64
            ],
        )?;

        Ok(())
    }

    fn get_all_checkpoints(&self) -> StateResult<Vec<(String, Checkpoint)>> {
        let conn = self.conn.lock().unwrap();

        let mut stmt = conn.prepare("SELECT mapping_name, lsn, events_processed FROM checkpoints")?;

        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                Checkpoint {
                    lsn: row.get::<_, i64>(1)? as u64,
                    events_processed: row.get::<_, i64>(2)? as u64,
                },
            ))
        })?;

        let mut result = Vec::new();
        for row in rows {
            result.push(row?);
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checkpoint_roundtrip() {
        let store = SqliteStateStore::in_memory().unwrap();

        // Initially no checkpoint
        assert!(store.get_checkpoint("test_mapping").unwrap().is_none());

        // Save checkpoint
        let checkpoint = Checkpoint {
            lsn: 12345,
            events_processed: 100,
        };
        store.save_checkpoint("test_mapping", &checkpoint).unwrap();

        // Read it back
        let loaded = store.get_checkpoint("test_mapping").unwrap().unwrap();
        assert_eq!(loaded.lsn, 12345);
        assert_eq!(loaded.events_processed, 100);
    }

    #[test]
    fn test_checkpoint_update() {
        let store = SqliteStateStore::in_memory().unwrap();

        store
            .save_checkpoint("test", &Checkpoint { lsn: 100, events_processed: 10 })
            .unwrap();

        store
            .save_checkpoint("test", &Checkpoint { lsn: 200, events_processed: 20 })
            .unwrap();

        let loaded = store.get_checkpoint("test").unwrap().unwrap();
        assert_eq!(loaded.lsn, 200);
        assert_eq!(loaded.events_processed, 20);
    }

    #[test]
    fn test_get_all_checkpoints() {
        let store = SqliteStateStore::in_memory().unwrap();

        store
            .save_checkpoint("mapping1", &Checkpoint { lsn: 100, events_processed: 10 })
            .unwrap();
        store
            .save_checkpoint("mapping2", &Checkpoint { lsn: 200, events_processed: 20 })
            .unwrap();

        let all = store.get_all_checkpoints().unwrap();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn test_get_min_lsn() {
        let store = SqliteStateStore::in_memory().unwrap();

        assert!(store.get_min_lsn().unwrap().is_none());

        store
            .save_checkpoint("mapping1", &Checkpoint { lsn: 300, events_processed: 0 })
            .unwrap();
        store
            .save_checkpoint("mapping2", &Checkpoint { lsn: 100, events_processed: 0 })
            .unwrap();
        store
            .save_checkpoint("mapping3", &Checkpoint { lsn: 200, events_processed: 0 })
            .unwrap();

        assert_eq!(store.get_min_lsn().unwrap(), Some(100));
    }
}
