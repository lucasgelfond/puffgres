//! Validation utilities for puffgres.

use std::collections::HashSet;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use sha2::{Digest, Sha256};

use puffgres_config::MigrationConfig;
use puffgres_pg::{LocalMigration, PostgresStateStore};

use crate::config::ProjectConfig;

/// Validate that a table exists in the database.
#[allow(dead_code)]
pub async fn validate_table_exists(
    store: &PostgresStateStore,
    schema: &str,
    table: &str,
    migration_version: i32,
    mapping_name: &str,
) -> Result<()> {
    if !store.table_exists(schema, table).await? {
        anyhow::bail!(
            "Table '{}.{}' referenced in migration v{} '{}' does not exist. \
             Create the table in your database before proceeding.",
            schema,
            table,
            migration_version,
            mapping_name
        );
    }
    Ok(())
}

/// Validate that all tables referenced by migrations exist.
#[allow(dead_code)]
pub async fn validate_all_tables_exist(
    store: &PostgresStateStore,
    migrations: &[LocalMigration],
) -> Result<()> {
    for migration in migrations {
        let config = MigrationConfig::parse(&migration.content).with_context(|| {
            format!(
                "Failed to parse migration v{} '{}'",
                migration.version, migration.mapping_name
            )
        })?;

        validate_table_exists(
            store,
            &config.source.schema,
            &config.source.table,
            migration.version,
            &migration.mapping_name,
        )
        .await?;
    }
    Ok(())
}

/// Get all transform paths referenced by migrations.
pub fn get_referenced_transforms(migrations: &[LocalMigration]) -> Result<HashSet<String>> {
    let mut referenced = HashSet::new();

    for migration in migrations {
        let config = MigrationConfig::parse(&migration.content).with_context(|| {
            format!(
                "Failed to parse migration v{} '{}'",
                migration.version, migration.mapping_name
            )
        })?;

        if let Some(path) = &config.transform.path {
            // Normalize the path
            let normalized = path.trim_start_matches("./");
            referenced.insert(normalized.to_string());
        }
    }

    Ok(referenced)
}

/// Validate that there are no unreferenced transforms in the transforms directory.
///
/// Returns an error if there are .ts or .js files in puffgres/transforms/ that
/// are not referenced by any migration's [transform].path.
pub fn validate_no_unreferenced_transforms(migrations: &[LocalMigration]) -> Result<()> {
    let transforms_dir = Path::new("puffgres/transforms");
    if !transforms_dir.exists() {
        return Ok(());
    }

    // Get all referenced transforms
    let referenced = get_referenced_transforms(migrations)?;

    // Find all transform files in the directory
    let mut unreferenced = Vec::new();

    for entry in fs::read_dir(transforms_dir)? {
        let entry = entry?;
        let path = entry.path();

        // Only check .ts and .js files
        let is_transform = path
            .extension()
            .map_or(false, |ext| ext == "ts" || ext == "js");

        if !is_transform {
            continue;
        }

        // Check if this file is referenced
        let relative_path = format!("transforms/{}", path.file_name().unwrap().to_string_lossy());

        if !referenced.contains(&relative_path) {
            unreferenced.push(path.display().to_string());
        }
    }

    if !unreferenced.is_empty() {
        anyhow::bail!(
            "Found unreferenced transform files:\n  {}\n\n\
             These transforms are not referenced by any migration's [transform].path.\n\
             Either:\n\
             - Reference them in a migration by adding [transform] section with path\n\
             - Remove them if they are no longer needed",
            unreferenced.join("\n  ")
        );
    }

    Ok(())
}

/// Validate that transforms haven't been modified since they were stored.
pub async fn validate_transforms(
    _config: &ProjectConfig,
    store: &PostgresStateStore,
) -> Result<()> {
    // Get stored transforms from database
    let stored = store.get_all_transforms().await?;

    if stored.is_empty() {
        return Ok(());
    }

    // Check each stored transform against local files
    for transform in stored {
        let path = format!(
            "puffgres/transforms/{}_{}.ts",
            transform.mapping_name, transform.version
        );

        // Also check the simpler path format
        let alt_path = format!("puffgres/transforms/{}.ts", transform.mapping_name);

        let local_content = if Path::new(&path).exists() {
            Some(fs::read_to_string(&path)?)
        } else if Path::new(&alt_path).exists() {
            Some(fs::read_to_string(&alt_path)?)
        } else {
            None
        };

        if let Some(content) = local_content {
            let mut hasher = Sha256::new();
            hasher.update(&content);
            let local_hash = hex::encode(hasher.finalize());

            if local_hash != transform.content_hash {
                anyhow::bail!(
                    "Transform for '{}' v{} has been modified.\n  Expected hash: {}\n  Got hash: {}",
                    transform.mapping_name,
                    transform.version,
                    transform.content_hash,
                    local_hash
                );
            }
        }
    }

    Ok(())
}

/// Store a transform in the database for immutability tracking.
pub async fn store_transform(
    store: &PostgresStateStore,
    mapping_name: &str,
    version: i32,
    content: &str,
) -> Result<()> {
    let mut hasher = Sha256::new();
    hasher.update(content);
    let hash = hex::encode(hasher.finalize());

    store
        .store_transform(mapping_name, version, content, &hash)
        .await
        .context("Failed to store transform")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use tempfile::TempDir;

    fn create_test_migration(
        name: &str,
        table: &str,
        transform_path: Option<&str>,
    ) -> LocalMigration {
        let transform_section = transform_path
            .map(|p| format!("\n[transform]\npath = \"{}\"", p))
            .unwrap_or_default();

        let content = format!(
            r#"version = 1
mapping_name = "{name}"
namespace = "test"
columns = ["id"]

[source]
schema = "public"
table = "{table}"

[id]
column = "id"
type = "uint"

[versioning]
mode = "source_lsn"
{transform_section}
"#,
            name = name,
            table = table,
            transform_section = transform_section
        );

        LocalMigration {
            version: 1,
            mapping_name: name.to_string(),
            content,
        }
    }

    #[test]
    fn test_get_referenced_transforms_empty() {
        let migrations = vec![create_test_migration("users", "users", None)];
        let referenced = get_referenced_transforms(&migrations).unwrap();
        assert!(referenced.is_empty());
    }

    #[test]
    fn test_get_referenced_transforms_with_path() {
        let migrations = vec![create_test_migration(
            "users",
            "users",
            Some("./transforms/users.ts"),
        )];
        let referenced = get_referenced_transforms(&migrations).unwrap();
        assert_eq!(referenced.len(), 1);
        assert!(referenced.contains("transforms/users.ts"));
    }

    #[test]
    fn test_validate_no_unreferenced_transforms_no_dir() {
        // Should succeed when transforms dir doesn't exist
        let migrations = vec![create_test_migration("users", "users", None)];
        assert!(validate_no_unreferenced_transforms(&migrations).is_ok());
    }

    // Note: Tests that change the current working directory must run serially
    // to avoid race conditions with parallel test execution.

    #[test]
    #[serial]
    fn test_validate_no_unreferenced_transforms_all_referenced() {
        let temp_dir = TempDir::new().unwrap();
        let transforms_dir = temp_dir.path().join("puffgres/transforms");
        std::fs::create_dir_all(&transforms_dir).unwrap();

        // Create a transform file
        std::fs::write(transforms_dir.join("users.ts"), "// transform").unwrap();

        // Change to temp directory for the test
        let original_dir = std::env::current_dir().unwrap();
        std::env::set_current_dir(temp_dir.path()).unwrap();

        let migrations = vec![create_test_migration(
            "users",
            "users",
            Some("./transforms/users.ts"),
        )];

        let result = validate_no_unreferenced_transforms(&migrations);

        // Restore original directory
        std::env::set_current_dir(original_dir).unwrap();

        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_validate_no_unreferenced_transforms_unreferenced() {
        let temp_dir = TempDir::new().unwrap();
        let transforms_dir = temp_dir.path().join("puffgres/transforms");
        std::fs::create_dir_all(&transforms_dir).unwrap();

        // Create a transform file that is NOT referenced
        std::fs::write(transforms_dir.join("orphan.ts"), "// orphan transform").unwrap();

        // Change to temp directory for the test
        let original_dir = std::env::current_dir().unwrap();
        std::env::set_current_dir(temp_dir.path()).unwrap();

        let migrations = vec![create_test_migration("users", "users", None)];

        let result = validate_no_unreferenced_transforms(&migrations);

        // Restore original directory
        std::env::set_current_dir(original_dir).unwrap();

        assert!(result.is_err());
        let err = result.unwrap_err();
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("unreferenced transform"),
            "Error: {}",
            err_msg
        );
        assert!(
            err_msg.contains("orphan"),
            "Error should mention 'orphan': {}",
            err_msg
        );
    }
}
