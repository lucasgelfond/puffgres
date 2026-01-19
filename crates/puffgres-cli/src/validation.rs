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
/// This includes both explicit paths from [transform].path and implicit paths
/// based on mapping_name (which is how transforms are looked up at runtime).
pub fn get_referenced_transforms(migrations: &[LocalMigration]) -> Result<HashSet<String>> {
    let mut referenced = HashSet::new();

    for migration in migrations {
        let config = MigrationConfig::parse(&migration.content).with_context(|| {
            format!(
                "Failed to parse migration v{} '{}'",
                migration.version, migration.mapping_name
            )
        })?;

        // Add explicit path if specified
        if let Some(path) = &config.transform.path {
            // Extract just the filename and construct consistent path format
            // that matches what validate_no_unreferenced_transforms() expects
            let path_obj = Path::new(path);
            if let Some(filename) = path_obj.file_name() {
                referenced.insert(format!("transforms/{}", filename.to_string_lossy()));
            }
        }

        // Also add implicit paths based on mapping_name
        // These are the patterns used by validate_transforms() and runtime lookup
        referenced.insert(format!("transforms/{}.ts", config.mapping_name));
        referenced.insert(format!("transforms/{}_{}.ts", config.mapping_name, config.version));
        referenced.insert(format!("transforms/{}.js", config.mapping_name));
        referenced.insert(format!("transforms/{}_{}.js", config.mapping_name, config.version));
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

/// Validate that transform files don't contain console.log calls.
///
/// console.log writes to stdout which breaks the transform protocol.
/// Users should use console.error for debugging instead.
pub fn validate_no_console_log_in_transforms() -> Result<()> {
    let transforms_dir = Path::new("puffgres/transforms");
    if !transforms_dir.exists() {
        return Ok(());
    }

    let mut files_with_console_log = Vec::new();

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

        let content = fs::read_to_string(&path)?;
        if content.contains("console.log") {
            files_with_console_log.push(path.display().to_string());
        }
    }

    if !files_with_console_log.is_empty() {
        anyhow::bail!(
            "Found console.log in transform files:\n  {}\n\n\
             Adding anything to stdout breaks the transform logic for now.\n\
             Use console.error for debugging. (We will find a better solution for this in the future!)",
            files_with_console_log.join("\n  ")
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
    fn test_get_referenced_transforms_includes_implicit_paths() {
        // Even without explicit path, we should have implicit paths based on mapping_name
        let migrations = vec![create_test_migration("users", "users", None)];
        let referenced = get_referenced_transforms(&migrations).unwrap();
        // Should include implicit paths: transforms/users.ts, transforms/users_1.ts, etc.
        assert!(referenced.contains("transforms/users.ts"));
        assert!(referenced.contains("transforms/users_1.ts"));
        assert!(referenced.contains("transforms/users.js"));
        assert!(referenced.contains("transforms/users_1.js"));
    }

    #[test]
    fn test_get_referenced_transforms_with_explicit_path() {
        let migrations = vec![create_test_migration(
            "users",
            "users",
            Some("./transforms/custom_users.ts"),
        )];
        let referenced = get_referenced_transforms(&migrations).unwrap();
        // Should include both explicit and implicit paths
        assert!(referenced.contains("transforms/custom_users.ts"));
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

    #[test]
    #[serial]
    fn test_validate_transforms_referenced_by_mapping_name() {
        // Test that transforms named after mapping_name are considered referenced
        let temp_dir = TempDir::new().unwrap();
        let transforms_dir = temp_dir.path().join("puffgres/transforms");
        std::fs::create_dir_all(&transforms_dir).unwrap();

        // Create a transform file named after the mapping_name (not explicitly in path)
        std::fs::write(transforms_dir.join("users_public.ts"), "// transform").unwrap();

        // Change to temp directory for the test
        let original_dir = std::env::current_dir().unwrap();
        std::env::set_current_dir(temp_dir.path()).unwrap();

        // Migration has mapping_name "users_public" but different explicit path
        let migrations = vec![create_test_migration(
            "users_public",
            "users",
            Some("./transforms/different.ts"),
        )];

        let result = validate_no_unreferenced_transforms(&migrations);

        // Restore original directory
        std::env::set_current_dir(original_dir).unwrap();

        // Should pass because users_public.ts matches the mapping_name implicit path
        assert!(result.is_ok(), "Transform matching mapping_name should be considered referenced");
    }

    #[test]
    #[serial]
    fn test_validate_no_console_log_passes_without_console_log() {
        let temp_dir = TempDir::new().unwrap();
        let transforms_dir = temp_dir.path().join("puffgres/transforms");
        std::fs::create_dir_all(&transforms_dir).unwrap();

        // Create a transform without console.log
        std::fs::write(
            transforms_dir.join("clean.ts"),
            "export function transform(row) { return row; }",
        )
        .unwrap();

        let original_dir = std::env::current_dir().unwrap();
        std::env::set_current_dir(temp_dir.path()).unwrap();

        let result = validate_no_console_log_in_transforms();

        std::env::set_current_dir(original_dir).unwrap();

        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn test_validate_no_console_log_fails_with_console_log() {
        let temp_dir = TempDir::new().unwrap();
        let transforms_dir = temp_dir.path().join("puffgres/transforms");
        std::fs::create_dir_all(&transforms_dir).unwrap();

        // Create a transform with console.log
        std::fs::write(
            transforms_dir.join("logging.ts"),
            "export function transform(row) { console.log(row); return row; }",
        )
        .unwrap();

        let original_dir = std::env::current_dir().unwrap();
        std::env::set_current_dir(temp_dir.path()).unwrap();

        let result = validate_no_console_log_in_transforms();

        std::env::set_current_dir(original_dir).unwrap();

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("console.log"),
            "Error should mention console.log: {}",
            err_msg
        );
        assert!(
            err_msg.contains("logging.ts"),
            "Error should mention the file: {}",
            err_msg
        );
        assert!(
            err_msg.contains("console.error"),
            "Error should suggest console.error: {}",
            err_msg
        );
    }

    #[test]
    #[serial]
    fn test_validate_console_error_is_allowed() {
        let temp_dir = TempDir::new().unwrap();
        let transforms_dir = temp_dir.path().join("puffgres/transforms");
        std::fs::create_dir_all(&transforms_dir).unwrap();

        // Create a transform with console.error (should be allowed)
        std::fs::write(
            transforms_dir.join("debug.ts"),
            "export function transform(row) { console.error('debug:', row); return row; }",
        )
        .unwrap();

        let original_dir = std::env::current_dir().unwrap();
        std::env::set_current_dir(temp_dir.path()).unwrap();

        let result = validate_no_console_log_in_transforms();

        std::env::set_current_dir(original_dir).unwrap();

        assert!(result.is_ok(), "console.error should be allowed");
    }
}
