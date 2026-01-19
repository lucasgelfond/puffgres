//! Validation utilities for puffgres.

use std::collections::HashSet;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use sha2::{Digest, Sha256};

use puffgres_config::{IdTypeConfig, MigrationConfig};
use puffgres_pg::{IdColumnSample, LocalMigration, PostgresStateStore};

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

// -------------------------------------------------------------------------
// ID Column Type Validation
// -------------------------------------------------------------------------

/// Infer the ID type from sampled column values and PostgreSQL type.
///
/// Type inference logic:
/// - PostgreSQL `uuid` type → Uuid
/// - PostgreSQL `int2/int4/int8/serial/bigserial/integer/smallint/bigint` → Uint (if all positive) or Int
/// - All values parse as UUID → Uuid
/// - All values parse as integers → Uint (if all non-negative) or Int
/// - Fallback → String
pub fn infer_id_type(sample: &IdColumnSample) -> IdTypeConfig {
    let pg_type = sample.pg_type.to_lowercase();

    // Check PostgreSQL native type first
    if pg_type == "uuid" {
        return IdTypeConfig::Uuid;
    }

    // Check for integer types in PostgreSQL
    let is_pg_integer = matches!(
        pg_type.as_str(),
        "integer" | "int" | "int2" | "int4" | "int8" | "smallint" | "bigint" | "serial" | "bigserial"
    );

    if is_pg_integer {
        // Check if all values are non-negative
        let all_non_negative = sample.values.iter().all(|v| {
            v.parse::<i64>()
                .map(|n| n >= 0)
                .unwrap_or(false)
        });
        return if all_non_negative {
            IdTypeConfig::Uint
        } else {
            IdTypeConfig::Int
        };
    }

    // If not a known PostgreSQL type, try to infer from values
    if sample.values.is_empty() {
        // Empty table, can't infer - default to String (safest)
        return IdTypeConfig::String;
    }

    // Try UUID parsing
    let all_uuid = sample.values.iter().all(|v| {
        uuid::Uuid::parse_str(v).is_ok()
    });
    if all_uuid {
        return IdTypeConfig::Uuid;
    }

    // Try integer parsing
    let parsed_ints: Vec<Option<i64>> = sample
        .values
        .iter()
        .map(|v| v.parse::<i64>().ok())
        .collect();

    let all_integers = parsed_ints.iter().all(|v| v.is_some());
    if all_integers {
        let all_non_negative = parsed_ints.iter().all(|v| v.unwrap() >= 0);
        return if all_non_negative {
            IdTypeConfig::Uint
        } else {
            IdTypeConfig::Int
        };
    }

    // Fallback to String
    IdTypeConfig::String
}

/// Check if sampled values are compatible with the configured ID type.
///
/// Returns true if the values match the configured type.
pub fn values_match_type(sample: &IdColumnSample, configured: IdTypeConfig) -> bool {
    // Empty samples are always valid (can't disprove compatibility)
    if sample.values.is_empty() {
        return true;
    }

    match configured {
        IdTypeConfig::Uuid => {
            // PostgreSQL uuid type is always valid
            if sample.pg_type.to_lowercase() == "uuid" {
                return true;
            }
            // Otherwise check if all values parse as UUID
            sample.values.iter().all(|v| uuid::Uuid::parse_str(v).is_ok())
        }
        IdTypeConfig::Uint => {
            // Check if all values are non-negative integers
            sample.values.iter().all(|v| {
                v.parse::<i64>()
                    .map(|n| n >= 0)
                    .unwrap_or(false)
            })
        }
        IdTypeConfig::Int => {
            // Check if all values are integers (positive or negative)
            sample.values.iter().all(|v| v.parse::<i64>().is_ok())
        }
        IdTypeConfig::String => {
            // String accepts anything
            true
        }
    }
}

/// Validate that the ID column type matches the configured type.
///
/// Samples up to 5 rows and checks if values are compatible with the configured type.
/// Returns an error with a helpful message if there's a mismatch.
pub async fn validate_id_column_type(
    store: &PostgresStateStore,
    schema: &str,
    table: &str,
    column: &str,
    configured_type: IdTypeConfig,
    version: i32,
    mapping_name: &str,
) -> Result<()> {
    let sample = store
        .sample_id_column(schema, table, column, 5)
        .await
        .context("Failed to sample ID column")?;

    if !values_match_type(&sample, configured_type) {
        let inferred_type = infer_id_type(&sample);

        let sample_display: Vec<&str> = sample.values.iter().take(5).map(|s| s.as_str()).collect();

        anyhow::bail!(
            "ID column type mismatch in migration v{} '{}':\n\
             Table '{}.{}' column '{}' is configured as '{:?}', but sampled values suggest '{:?}'.\n\
             Sampled values: {:?}\n\
             PostgreSQL column type: {}\n\n\
             To fix: Update the [id] section in your migration config:\n\
             [id]\n\
             column = \"{}\"\n\
             type = \"{}\"",
            version,
            mapping_name,
            schema,
            table,
            column,
            configured_type,
            inferred_type,
            sample_display,
            sample.pg_type,
            column,
            format!("{:?}", inferred_type).to_lowercase()
        );
    }

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
    #[serial]
    fn test_validate_no_unreferenced_transforms_no_dir() {
        // Should succeed when transforms dir doesn't exist
        let temp_dir = TempDir::new().unwrap();

        // Change to temp directory (which has no puffgres/transforms)
        let original_dir = std::env::current_dir().unwrap();
        std::env::set_current_dir(temp_dir.path()).unwrap();

        let migrations = vec![create_test_migration("users", "users", None)];
        let result = validate_no_unreferenced_transforms(&migrations);

        // Restore original directory
        std::env::set_current_dir(original_dir).unwrap();

        assert!(result.is_ok());
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

    // -------------------------------------------------------------------------
    // ID Column Type Validation Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_infer_id_type_uuid_from_pg_type() {
        let sample = IdColumnSample {
            values: vec!["52d91dc3-165c-4a7f-878e-c38450eeecec".to_string()],
            pg_type: "uuid".to_string(),
        };
        assert_eq!(infer_id_type(&sample), IdTypeConfig::Uuid);
    }

    #[test]
    fn test_infer_id_type_uuid_from_values() {
        // UUID strings in a text column should be inferred as UUID
        let sample = IdColumnSample {
            values: vec![
                "52d91dc3-165c-4a7f-878e-c38450eeecec".to_string(),
                "52d986a9-598c-48e6-8441-71f4f3a9402c".to_string(),
            ],
            pg_type: "text".to_string(),
        };
        assert_eq!(infer_id_type(&sample), IdTypeConfig::Uuid);
    }

    #[test]
    fn test_infer_id_type_uint_from_positive_integers() {
        let sample = IdColumnSample {
            values: vec!["1".to_string(), "2".to_string(), "100".to_string()],
            pg_type: "integer".to_string(),
        };
        assert_eq!(infer_id_type(&sample), IdTypeConfig::Uint);
    }

    #[test]
    fn test_infer_id_type_int_from_negative_integers() {
        let sample = IdColumnSample {
            values: vec!["-1".to_string(), "2".to_string(), "100".to_string()],
            pg_type: "integer".to_string(),
        };
        assert_eq!(infer_id_type(&sample), IdTypeConfig::Int);
    }

    #[test]
    fn test_infer_id_type_uint_from_bigint() {
        let sample = IdColumnSample {
            values: vec!["1".to_string(), "9999999999".to_string()],
            pg_type: "bigint".to_string(),
        };
        assert_eq!(infer_id_type(&sample), IdTypeConfig::Uint);
    }

    #[test]
    fn test_infer_id_type_string_from_text_values() {
        let sample = IdColumnSample {
            values: vec!["abc123".to_string(), "user_42".to_string()],
            pg_type: "text".to_string(),
        };
        assert_eq!(infer_id_type(&sample), IdTypeConfig::String);
    }

    #[test]
    fn test_infer_id_type_empty_returns_string() {
        let sample = IdColumnSample {
            values: vec![],
            pg_type: "text".to_string(),
        };
        assert_eq!(infer_id_type(&sample), IdTypeConfig::String);
    }

    #[test]
    fn test_values_match_type_uuid_valid() {
        let sample = IdColumnSample {
            values: vec![
                "52d91dc3-165c-4a7f-878e-c38450eeecec".to_string(),
                "52d986a9-598c-48e6-8441-71f4f3a9402c".to_string(),
            ],
            pg_type: "uuid".to_string(),
        };
        assert!(values_match_type(&sample, IdTypeConfig::Uuid));
    }

    #[test]
    fn test_values_match_type_uuid_invalid() {
        // Integer values don't match UUID config
        let sample = IdColumnSample {
            values: vec!["1".to_string(), "2".to_string()],
            pg_type: "integer".to_string(),
        };
        assert!(!values_match_type(&sample, IdTypeConfig::Uuid));
    }

    #[test]
    fn test_values_match_type_uint_valid() {
        let sample = IdColumnSample {
            values: vec!["1".to_string(), "100".to_string(), "0".to_string()],
            pg_type: "integer".to_string(),
        };
        assert!(values_match_type(&sample, IdTypeConfig::Uint));
    }

    #[test]
    fn test_values_match_type_uint_invalid_negative() {
        // Negative values don't match Uint config
        let sample = IdColumnSample {
            values: vec!["1".to_string(), "-5".to_string()],
            pg_type: "integer".to_string(),
        };
        assert!(!values_match_type(&sample, IdTypeConfig::Uint));
    }

    #[test]
    fn test_values_match_type_int_valid_with_negatives() {
        let sample = IdColumnSample {
            values: vec!["1".to_string(), "-5".to_string(), "100".to_string()],
            pg_type: "integer".to_string(),
        };
        assert!(values_match_type(&sample, IdTypeConfig::Int));
    }

    #[test]
    fn test_values_match_type_string_always_valid() {
        // String type accepts any values
        let sample = IdColumnSample {
            values: vec!["anything".to_string(), "123".to_string(), "uuid-like".to_string()],
            pg_type: "text".to_string(),
        };
        assert!(values_match_type(&sample, IdTypeConfig::String));
    }

    #[test]
    fn test_values_match_type_empty_always_valid() {
        // Empty table passes validation for any type
        let sample = IdColumnSample {
            values: vec![],
            pg_type: "integer".to_string(),
        };
        assert!(values_match_type(&sample, IdTypeConfig::Uuid));
        assert!(values_match_type(&sample, IdTypeConfig::Uint));
        assert!(values_match_type(&sample, IdTypeConfig::Int));
        assert!(values_match_type(&sample, IdTypeConfig::String));
    }
}
