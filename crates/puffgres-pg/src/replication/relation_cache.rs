//! Cache for PostgreSQL relation metadata.
//!
//! PostgreSQL sends Relation messages before the first DML on each table
//! in a replication session. We cache these to resolve relation_id in
//! subsequent Insert/Update/Delete messages.

use std::collections::HashMap;

use super::pgoutput::{ColumnInfo, RelationMessage, ReplicaIdentity};

/// Cached information about a PostgreSQL relation (table).
#[derive(Debug, Clone)]
pub struct RelationInfo {
    pub namespace: String,
    pub name: String,
    pub columns: Vec<ColumnInfo>,
    pub replica_identity: ReplicaIdentity,
}

impl From<&RelationMessage> for RelationInfo {
    fn from(msg: &RelationMessage) -> Self {
        Self {
            namespace: msg.namespace.clone(),
            name: msg.name.clone(),
            columns: msg.columns.clone(),
            replica_identity: msg.replica_identity,
        }
    }
}

/// Cache of relation OID to table metadata mappings.
#[derive(Debug, Default)]
pub struct RelationCache {
    relations: HashMap<u32, RelationInfo>,
}

impl RelationCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Update the cache with a Relation message.
    pub fn update(&mut self, msg: &RelationMessage) {
        self.relations.insert(msg.relation_id, msg.into());
    }

    /// Look up relation info by OID.
    pub fn get(&self, relation_id: u32) -> Option<&RelationInfo> {
        self.relations.get(&relation_id)
    }

    /// Clear the cache (e.g., on reconnect).
    pub fn clear(&mut self) {
        self.relations.clear();
    }

    /// Number of cached relations.
    pub fn len(&self) -> usize {
        self.relations.len()
    }

    /// Check if cache is empty.
    pub fn is_empty(&self) -> bool {
        self.relations.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_update_and_get() {
        let mut cache = RelationCache::new();

        let msg = RelationMessage {
            relation_id: 16384,
            namespace: "public".to_string(),
            name: "users".to_string(),
            replica_identity: ReplicaIdentity::Default,
            columns: vec![ColumnInfo {
                flags: 1,
                name: "id".to_string(),
                type_oid: 23,
                type_modifier: -1,
            }],
        };

        cache.update(&msg);

        let info = cache.get(16384).unwrap();
        assert_eq!(info.namespace, "public");
        assert_eq!(info.name, "users");
        assert_eq!(info.columns.len(), 1);
        assert_eq!(info.columns[0].name, "id");
    }

    #[test]
    fn test_cache_miss() {
        let cache = RelationCache::new();
        assert!(cache.get(12345).is_none());
    }

    #[test]
    fn test_cache_clear() {
        let mut cache = RelationCache::new();

        let msg = RelationMessage {
            relation_id: 16384,
            namespace: "public".to_string(),
            name: "users".to_string(),
            replica_identity: ReplicaIdentity::Default,
            columns: vec![],
        };

        cache.update(&msg);
        assert_eq!(cache.len(), 1);

        cache.clear();
        assert!(cache.is_empty());
    }
}
