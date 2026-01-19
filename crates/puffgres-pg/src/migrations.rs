//! Migration tracking and validation.
//!
//! Handles applying new migrations and validating that local migration files
//! match the hashes of already-applied migrations.

use sha2::{Digest, Sha256};
use tracing::{info, warn};

use crate::error::{PgError, PgResult};
use crate::state::PostgresStateStore;

/// Result of migration validation.
#[derive(Debug)]
pub struct MigrationStatus {
    /// Migrations that are already applied and match.
    pub applied: Vec<String>,
    /// Migrations that need to be applied.
    pub pending: Vec<String>,
    /// Migrations that have hash mismatches (error condition).
    pub mismatched: Vec<MigrationMismatch>,
}

/// A migration hash mismatch.
#[derive(Debug)]
pub struct MigrationMismatch {
    pub version: i32,
    pub mapping_name: String,
    pub expected_hash: String,
    pub actual_hash: String,
}

impl MigrationStatus {
    /// Check if there are any errors (mismatches).
    pub fn has_errors(&self) -> bool {
        !self.mismatched.is_empty()
    }

    /// Check if all migrations are applied.
    pub fn is_up_to_date(&self) -> bool {
        self.pending.is_empty() && self.mismatched.is_empty()
    }
}

/// A local migration file.
pub struct LocalMigration {
    pub version: i32,
    pub mapping_name: String,
    pub content: String,
}

impl LocalMigration {
    /// Compute the content hash (SHA-256).
    ///
    /// Line endings are normalized to LF before hashing to ensure consistent
    /// hashes across different platforms (Windows CRLF vs Unix LF).
    pub fn content_hash(&self) -> String {
        compute_content_hash(&self.content)
    }
}

/// Migration tracker that validates and applies migrations.
pub struct MigrationTracker<'a> {
    store: &'a PostgresStateStore,
}

impl<'a> MigrationTracker<'a> {
    /// Create a new migration tracker.
    pub fn new(store: &'a PostgresStateStore) -> Self {
        Self { store }
    }

    /// Validate local migrations against applied migrations.
    ///
    /// Returns the status of all migrations:
    /// - Applied migrations that match
    /// - Pending migrations that need to be applied
    /// - Mismatched migrations (local file differs from applied)
    pub async fn validate(&self, local: &[LocalMigration]) -> PgResult<MigrationStatus> {
        let applied = self.store.get_applied_migrations().await?;

        let mut status = MigrationStatus {
            applied: Vec::new(),
            pending: Vec::new(),
            mismatched: Vec::new(),
        };

        for migration in local {
            let hash = migration.content_hash();

            // Check if this migration is already applied
            if let Some(existing) = applied.iter().find(|a| {
                a.version == migration.version && a.mapping_name == migration.mapping_name
            }) {
                if existing.content_hash == hash {
                    // Match - all good
                    status
                        .applied
                        .push(format!("v{} {}", migration.version, migration.mapping_name));
                } else {
                    // Hash mismatch - this is an error
                    status.mismatched.push(MigrationMismatch {
                        version: migration.version,
                        mapping_name: migration.mapping_name.clone(),
                        expected_hash: existing.content_hash.clone(),
                        actual_hash: hash,
                    });
                }
            } else {
                // Not applied yet - pending
                status
                    .pending
                    .push(format!("v{} {}", migration.version, migration.mapping_name));
            }
        }

        Ok(status)
    }

    /// Apply pending migrations.
    ///
    /// This records the migration in __puffgres_migrations but does NOT
    /// modify the source Postgres tables. Migrations are just config files
    /// that define how data is synced.
    pub async fn apply(&self, local: &[LocalMigration], dry_run: bool) -> PgResult<Vec<String>> {
        let status = self.validate(local).await?;

        // Check for mismatches first
        if status.has_errors() {
            let mismatches: Vec<String> = status
                .mismatched
                .iter()
                .map(|m| {
                    format!(
                        "v{} {}: expected hash {} but got {}",
                        m.version, m.mapping_name, m.expected_hash, m.actual_hash
                    )
                })
                .collect();

            return Err(PgError::Postgres(format!(
                "Migration hash mismatch(es):\n{}",
                mismatches.join("\n")
            )));
        }

        if status.pending.is_empty() {
            info!("All migrations already applied");
            return Ok(Vec::new());
        }

        let mut applied = Vec::new();

        for migration in local {
            let name = format!("v{} {}", migration.version, migration.mapping_name);

            // Skip already applied
            if status.applied.contains(&name) {
                continue;
            }

            let hash = migration.content_hash();

            if dry_run {
                info!(
                    version = migration.version,
                    mapping = %migration.mapping_name,
                    hash = %hash,
                    "Would apply migration"
                );
            } else {
                self.store
                    .record_migration(migration.version, &migration.mapping_name, &hash)
                    .await?;

                info!(
                    version = migration.version,
                    mapping = %migration.mapping_name,
                    "Applied migration"
                );
            }

            applied.push(name);
        }

        Ok(applied)
    }

    /// Validate that all local migrations match applied migrations.
    ///
    /// Returns an error if:
    /// - Any migration has a hash mismatch
    /// - Any pending migrations exist (unless allow_pending is true)
    pub async fn validate_or_fail(
        &self,
        local: &[LocalMigration],
        allow_pending: bool,
    ) -> PgResult<()> {
        let status = self.validate(local).await?;

        if !status.mismatched.is_empty() {
            let errors: Vec<String> = status
                .mismatched
                .iter()
                .map(|m| {
                    format!(
                        "Migration v{} '{}' has been modified.\n  Expected: {}\n  Got: {}\n  \
                         Applied migrations cannot be modified.",
                        m.version, m.mapping_name, m.expected_hash, m.actual_hash
                    )
                })
                .collect();

            return Err(PgError::Postgres(errors.join("\n\n")));
        }

        if !allow_pending && !status.pending.is_empty() {
            warn!(
                pending = ?status.pending,
                "Pending migrations found. Run 'puffgres migrate' first."
            );
            return Err(PgError::Postgres(format!(
                "Pending migrations: {}. Run 'puffgres migrate' to apply them.",
                status.pending.join(", ")
            )));
        }

        Ok(())
    }
}

/// Compute content hash for a migration TOML string.
///
/// Line endings are normalized to LF before hashing to ensure consistent
/// hashes across different platforms (Windows CRLF vs Unix LF).
pub fn compute_content_hash(content: &str) -> String {
    let mut hasher = Sha256::new();
    // Normalize CRLF to LF for consistent hashing across platforms
    let normalized = content.replace("\r\n", "\n");
    hasher.update(&normalized);
    let result = hasher.finalize();
    hex::encode(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_content_hash() {
        let migration = LocalMigration {
            version: 1,
            mapping_name: "users".to_string(),
            content: "version = 1\nmapping_name = \"users\"".to_string(),
        };

        let hash = migration.content_hash();
        assert!(!hash.is_empty());
        assert_eq!(hash.len(), 64); // SHA-256 hex

        // Same content = same hash
        let migration2 = LocalMigration {
            version: 1,
            mapping_name: "users".to_string(),
            content: "version = 1\nmapping_name = \"users\"".to_string(),
        };
        assert_eq!(migration.content_hash(), migration2.content_hash());
    }

    #[test]
    fn test_content_hash_differs() {
        let m1 = LocalMigration {
            version: 1,
            mapping_name: "users".to_string(),
            content: "content1".to_string(),
        };

        let m2 = LocalMigration {
            version: 1,
            mapping_name: "users".to_string(),
            content: "content2".to_string(),
        };

        assert_ne!(m1.content_hash(), m2.content_hash());
    }

    #[test]
    fn test_compute_content_hash() {
        let hash = compute_content_hash("test content");
        assert_eq!(hash.len(), 64);
    }

    #[test]
    fn test_content_hash_normalizes_line_endings() {
        // LF line endings (Unix)
        let lf_content = "version = 1\nmapping_name = \"users\"\n[source]\ntable = \"users\"\n";

        // CRLF line endings (Windows)
        let crlf_content =
            "version = 1\r\nmapping_name = \"users\"\r\n[source]\r\ntable = \"users\"\r\n";

        let lf_migration = LocalMigration {
            version: 1,
            mapping_name: "users".to_string(),
            content: lf_content.to_string(),
        };

        let crlf_migration = LocalMigration {
            version: 1,
            mapping_name: "users".to_string(),
            content: crlf_content.to_string(),
        };

        // Both should produce the same hash after normalization
        assert_eq!(
            lf_migration.content_hash(),
            crlf_migration.content_hash(),
            "CRLF and LF line endings should produce the same hash"
        );

        // Also test the standalone function
        assert_eq!(
            compute_content_hash(lf_content),
            compute_content_hash(crlf_content),
            "compute_content_hash should normalize line endings"
        );
    }

    #[test]
    fn test_content_hash_mixed_line_endings() {
        // Mixed line endings (some files may have inconsistent line endings)
        let mixed_content = "line1\r\nline2\nline3\r\nline4\n";
        let normalized_content = "line1\nline2\nline3\nline4\n";

        assert_eq!(
            compute_content_hash(mixed_content),
            compute_content_hash(normalized_content),
            "Mixed line endings should normalize to LF"
        );
    }
}
