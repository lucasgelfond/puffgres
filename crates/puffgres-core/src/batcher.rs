use std::collections::HashMap;

use crate::action::Action;
use crate::mapping::BatchConfig;

/// A batch of actions to be sent to a single namespace.
#[derive(Debug, Clone)]
pub struct Batch {
    pub namespace: String,
    pub actions: Vec<Action>,
    pub lsn: u64,
    estimated_size: usize,
}

impl Batch {
    fn new(namespace: String, lsn: u64) -> Self {
        Self {
            namespace,
            actions: Vec::new(),
            lsn,
            estimated_size: 0,
        }
    }

    fn add(&mut self, action: Action, size: usize) {
        self.actions.push(action);
        self.estimated_size += size;
    }

    fn is_empty(&self) -> bool {
        self.actions.is_empty()
    }

    fn len(&self) -> usize {
        self.actions.len()
    }

    fn size(&self) -> usize {
        self.estimated_size
    }
}

/// Groups actions into batches by namespace, respecting size limits.
pub struct Batcher {
    config: BatchConfig,
    batches: HashMap<String, Batch>,
    current_lsn: u64,
}

impl Batcher {
    pub fn new(config: BatchConfig) -> Self {
        Self {
            config,
            batches: HashMap::new(),
            current_lsn: 0,
        }
    }

    /// Add an action for a namespace. Returns a batch if one is ready to flush.
    pub fn add(&mut self, namespace: &str, action: Action, lsn: u64) -> Option<Batch> {
        self.current_lsn = lsn;

        let size = estimate_action_size(&action);

        let batch = self
            .batches
            .entry(namespace.to_string())
            .or_insert_with(|| Batch::new(namespace.to_string(), lsn));

        // Check if adding this action would exceed limits
        let would_exceed =
            batch.len() >= self.config.max_rows || (batch.size() + size) > self.config.max_bytes;

        if would_exceed && !batch.is_empty() {
            // Flush the current batch and start a new one
            let ready = std::mem::replace(batch, Batch::new(namespace.to_string(), lsn));
            batch.add(action, size);
            Some(ready)
        } else {
            batch.add(action, size);
            None
        }
    }

    /// Flush all pending batches.
    pub fn flush_all(&mut self) -> Vec<Batch> {
        let mut result = Vec::new();
        for (_, batch) in self.batches.drain() {
            if !batch.is_empty() {
                result.push(batch);
            }
        }
        result
    }

    /// Flush a specific namespace's batch.
    pub fn flush(&mut self, namespace: &str) -> Option<Batch> {
        self.batches.remove(namespace).filter(|b| !b.is_empty())
    }

    /// Get the number of pending actions across all namespaces.
    pub fn pending_count(&self) -> usize {
        self.batches.values().map(|b| b.len()).sum()
    }

    /// Get the current LSN.
    pub fn current_lsn(&self) -> u64 {
        self.current_lsn
    }
}

/// Estimate the size of an action in bytes.
fn estimate_action_size(action: &Action) -> usize {
    match action {
        Action::Upsert { doc, .. } => {
            // Rough estimate: serialize to JSON and measure
            serde_json::to_string(doc).map(|s| s.len()).unwrap_or(100)
        }
        Action::Delete { .. } => 50, // ID only
        Action::Skip => 0,
        Action::Error { message, .. } => message.len() + 50,
    }
}

/// A write request ready to be sent to turbopuffer.
#[derive(Debug, Clone)]
pub struct WriteRequest {
    pub namespace: String,
    pub upserts: Vec<UpsertDoc>,
    pub deletes: Vec<crate::action::DocumentId>,
    pub lsn: u64,
}

/// A document to upsert.
#[derive(Debug, Clone)]
pub struct UpsertDoc {
    pub id: crate::action::DocumentId,
    pub attributes: crate::action::Document,
}

impl WriteRequest {
    /// Build a write request from a batch.
    pub fn from_batch(batch: Batch) -> Self {
        let mut upserts = Vec::new();
        let mut deletes = Vec::new();

        for action in batch.actions {
            match action {
                Action::Upsert { id, doc } => {
                    upserts.push(UpsertDoc {
                        id,
                        attributes: doc,
                    });
                }
                Action::Delete { id } => {
                    deletes.push(id);
                }
                Action::Skip | Action::Error { .. } => {}
            }
        }

        WriteRequest {
            namespace: batch.namespace,
            upserts,
            deletes,
            lsn: batch.lsn,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.upserts.is_empty() && self.deletes.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value;

    fn make_upsert(id: u64) -> Action {
        let doc = [("name".into(), Value::String("test".into()))]
            .into_iter()
            .collect();
        Action::upsert(id, doc)
    }

    #[test]
    fn test_batcher_basic() {
        let config = BatchConfig {
            max_rows: 10,
            max_bytes: 1024 * 1024,
            flush_interval_ms: 100,
        };
        let mut batcher = Batcher::new(config);

        // Add a few actions
        assert!(batcher.add("ns1", make_upsert(1), 100).is_none());
        assert!(batcher.add("ns1", make_upsert(2), 101).is_none());
        assert!(batcher.add("ns2", make_upsert(1), 102).is_none());

        assert_eq!(batcher.pending_count(), 3);

        let batches = batcher.flush_all();
        assert_eq!(batches.len(), 2);
    }

    #[test]
    fn test_batcher_max_rows() {
        let config = BatchConfig {
            max_rows: 3,
            max_bytes: 1024 * 1024,
            flush_interval_ms: 100,
        };
        let mut batcher = Batcher::new(config);

        // Add actions until we hit the limit
        assert!(batcher.add("ns1", make_upsert(1), 100).is_none());
        assert!(batcher.add("ns1", make_upsert(2), 101).is_none());
        assert!(batcher.add("ns1", make_upsert(3), 102).is_none());

        // Fourth action should trigger a flush
        let batch = batcher.add("ns1", make_upsert(4), 103);
        assert!(batch.is_some());
        let batch = batch.unwrap();
        assert_eq!(batch.actions.len(), 3);

        // The fourth action should be in the new batch
        assert_eq!(batcher.pending_count(), 1);
    }

    #[test]
    fn test_write_request_from_batch() {
        let mut batch = Batch::new("test_ns".into(), 100);
        batch.add(make_upsert(1), 50);
        batch.add(make_upsert(2), 50);
        batch.add(Action::delete(3u64), 20);
        batch.add(Action::skip(), 0);

        let request = WriteRequest::from_batch(batch);
        assert_eq!(request.namespace, "test_ns");
        assert_eq!(request.upserts.len(), 2);
        assert_eq!(request.deletes.len(), 1);
        assert_eq!(request.lsn, 100);
    }

    #[test]
    fn test_batcher_flush_specific_namespace() {
        let config = BatchConfig::default();
        let mut batcher = Batcher::new(config);

        batcher.add("ns1", make_upsert(1), 100);
        batcher.add("ns2", make_upsert(1), 101);

        let batch = batcher.flush("ns1");
        assert!(batch.is_some());
        assert_eq!(batch.unwrap().namespace, "ns1");

        assert_eq!(batcher.pending_count(), 1);
    }
}
