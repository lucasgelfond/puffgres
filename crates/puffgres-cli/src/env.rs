use anyhow::{Context, Result};
use tracing::{info, warn};

/// Default batch size for processing transforms (rows per batch).
pub const DEFAULT_TRANSFORM_BATCH_SIZE: usize = 100;

/// Validate that we're running from a puffgres project directory.
///
/// A puffgres project directory has a `migrations/` directory (created by `puffgres init`).
/// This should be called for commands that require being in the project directory.
pub fn validate_project_directory() -> Result<()> {
    let cwd = std::env::current_dir().context("Failed to get current directory")?;
    let migrations_dir = cwd.join("migrations");

    if !migrations_dir.exists() {
        // Check if user is in a parent directory with puffgres/ subdirectory
        let puffgres_subdir = cwd.join("puffgres");
        if puffgres_subdir.exists() && puffgres_subdir.join("migrations").exists() {
            anyhow::bail!(
                "Not in a puffgres project directory.\n\n\
                 Found puffgres/ subdirectory. Please run commands from inside it:\n\n\
                 \x20 cd puffgres\n\
                 \x20 puffgres <command>"
            );
        }

        anyhow::bail!(
            "Not in a puffgres project directory.\n\n\
             Expected to find migrations/ directory.\n\
             Current directory: {}\n\n\
             To create a new puffgres project, run 'puffgres init' first.",
            cwd.display()
        );
    }

    Ok(())
}

/// Default batch size for uploading to turbopuffer (documents per API call).
pub const DEFAULT_UPLOAD_BATCH_SIZE: usize = 500;

/// Default maximum retries for failed turbopuffer uploads.
pub const DEFAULT_MAX_RETRIES: u32 = 5;

/// Warn if the database URL appears to be using a connection pooler.
/// Logical replication requires a direct connection to Postgres and does not work
/// through connection poolers like PgBouncer.
pub fn warn_if_pooler_url(url: &str) {
    // Check for common pooler indicators in the hostname
    // Most poolers add "-pooler" to the hostname (e.g., Neon, Supabase)
    if url.contains("-pooler.") || url.contains("-pooler:") {
        warn!(
            "DATABASE_URL appears to use a connection pooler (-pooler in hostname). \
             Logical replication requires a direct connection to Postgres and will not work through a pooler."
        );
    }
}

/// Get the transform batch size from environment or use default.
pub fn get_transform_batch_size() -> usize {
    std::env::var("PUFFGRES_TRANSFORM_BATCH_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_TRANSFORM_BATCH_SIZE)
}

/// Get the upload batch size from environment or use default.
pub fn get_upload_batch_size() -> usize {
    std::env::var("PUFFGRES_UPLOAD_BATCH_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_UPLOAD_BATCH_SIZE)
}

/// Get the max retries from environment or use default.
pub fn get_max_retries() -> u32 {
    std::env::var("PUFFGRES_MAX_RETRIES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_MAX_RETRIES)
}

/// Load .env files using Next.js-style hierarchical loading.
///
/// Files are loaded in this priority order (highest wins):
/// 1. `.env.{env}.local` - Local overrides for specific environment
/// 2. `.env.local` - Local overrides (typically gitignored)
/// 3. `.env.{env}` - Environment-specific settings
/// 4. `.env` - Base/default settings
///
/// Since dotenvy doesn't override existing variables, we load highest priority first.
/// Only `.env` is required; all other files are optional.
pub fn load_dotenv_from_ancestors(env_name: Option<&str>) -> Result<()> {
    let cwd = std::env::current_dir().context("Failed to get current directory")?;

    // Find the directory containing a .env file
    let env_dir = find_env_directory(&cwd)?;

    // Load files in priority order (highest first, since dotenvy won't override)
    let mut loaded_files = Vec::new();

    if let Some(name) = env_name {
        // .env.{name}.local - highest priority
        if let Some(path) = try_load_env_file(&env_dir, &format!(".env.{}.local", name))? {
            loaded_files.push(path);
        }
    }

    // .env.local
    if let Some(path) = try_load_env_file(&env_dir, ".env.local")? {
        loaded_files.push(path);
    }

    if let Some(name) = env_name {
        // .env.{name}
        if let Some(path) = try_load_env_file(&env_dir, &format!(".env.{}", name))? {
            loaded_files.push(path);
        }
    }

    // .env - base file (required)
    let base_env_path = env_dir.join(".env");
    dotenvy::from_path(&base_env_path)
        .with_context(|| format!("Failed to load .env from {}", base_env_path.display()))?;
    loaded_files.push(base_env_path);

    // Log all loaded files (in priority order)
    for path in &loaded_files {
        info!("Loaded {}", path.display());
    }

    Ok(())
}

/// Find the nearest ancestor directory containing a .env file.
fn find_env_directory(start: &std::path::Path) -> Result<std::path::PathBuf> {
    let mut current = start;
    loop {
        if current.join(".env").exists() {
            return Ok(current.to_path_buf());
        }

        match current.parent() {
            Some(parent) => current = parent,
            None => break,
        }
    }

    anyhow::bail!(
        "No .env file found.\n\n\
        Searched from {} to filesystem root.\n\n\
        Hint: Create a .env file with your DATABASE_URL and TURBOPUFFER_API_KEY:\n\
        \n  \
        DATABASE_URL=postgresql://user:pass@host:5432/db\n  \
        TURBOPUFFER_API_KEY=your-api-key\n\n\
        Or run 'puffgres init' to create one.",
        start.display()
    )
}

/// Try to load an env file, returning the path if it exists and was loaded.
fn try_load_env_file(dir: &std::path::Path, filename: &str) -> Result<Option<std::path::PathBuf>> {
    let path = dir.join(filename);
    if path.exists() {
        dotenvy::from_path(&path)
            .with_context(|| format!("Failed to load {} from {}", filename, path.display()))?;
        Ok(Some(path))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    #[serial]
    fn test_load_dotenv_from_current_directory() {
        let temp_dir = TempDir::new().unwrap();
        let env_path = temp_dir.path().join(".env");
        fs::write(&env_path, "TEST_VAR_CURRENT=hello").unwrap();

        let original_dir = std::env::current_dir().unwrap();
        std::env::set_current_dir(temp_dir.path()).unwrap();

        // Clear any existing value
        std::env::remove_var("TEST_VAR_CURRENT");

        let result = load_dotenv_from_ancestors(None);
        assert!(result.is_ok(), "Should find .env in current directory");
        assert_eq!(
            std::env::var("TEST_VAR_CURRENT").unwrap(),
            "hello",
            "Should load env var from .env"
        );

        // Cleanup
        std::env::set_current_dir(original_dir).unwrap();
        std::env::remove_var("TEST_VAR_CURRENT");
    }

    #[test]
    #[serial]
    fn test_load_dotenv_from_parent_directory() {
        let parent_dir = TempDir::new().unwrap();
        let child_dir = parent_dir.path().join("subdir");
        fs::create_dir(&child_dir).unwrap();

        // Create .env in parent, not in child
        let env_path = parent_dir.path().join(".env");
        fs::write(&env_path, "TEST_VAR_PARENT=world").unwrap();

        let original_dir = std::env::current_dir().unwrap();
        std::env::set_current_dir(&child_dir).unwrap();

        // Clear any existing value
        std::env::remove_var("TEST_VAR_PARENT");

        let result = load_dotenv_from_ancestors(None);
        assert!(result.is_ok(), "Should find .env in parent directory");
        assert_eq!(
            std::env::var("TEST_VAR_PARENT").unwrap(),
            "world",
            "Should load env var from parent .env"
        );

        // Cleanup
        std::env::set_current_dir(original_dir).unwrap();
        std::env::remove_var("TEST_VAR_PARENT");
    }

    #[test]
    #[serial]
    fn test_load_dotenv_from_grandparent_directory() {
        let grandparent_dir = TempDir::new().unwrap();
        let parent_dir = grandparent_dir.path().join("parent");
        let child_dir = parent_dir.join("child");
        fs::create_dir_all(&child_dir).unwrap();

        // Create .env only in grandparent
        let env_path = grandparent_dir.path().join(".env");
        fs::write(&env_path, "TEST_VAR_GRANDPARENT=nested").unwrap();

        let original_dir = std::env::current_dir().unwrap();
        std::env::set_current_dir(&child_dir).unwrap();

        // Clear any existing value
        std::env::remove_var("TEST_VAR_GRANDPARENT");

        let result = load_dotenv_from_ancestors(None);
        assert!(result.is_ok(), "Should find .env in grandparent directory");
        assert_eq!(
            std::env::var("TEST_VAR_GRANDPARENT").unwrap(),
            "nested",
            "Should load env var from grandparent .env"
        );

        // Cleanup
        std::env::set_current_dir(original_dir).unwrap();
        std::env::remove_var("TEST_VAR_GRANDPARENT");
    }

    #[test]
    #[serial]
    fn test_load_dotenv_prefers_closest_env_file() {
        let parent_dir = TempDir::new().unwrap();
        let child_dir = parent_dir.path().join("subdir");
        fs::create_dir(&child_dir).unwrap();

        // Create .env in both parent and child
        fs::write(parent_dir.path().join(".env"), "TEST_VAR_CLOSEST=parent").unwrap();
        fs::write(child_dir.join(".env"), "TEST_VAR_CLOSEST=child").unwrap();

        let original_dir = std::env::current_dir().unwrap();
        std::env::set_current_dir(&child_dir).unwrap();

        // Clear any existing value
        std::env::remove_var("TEST_VAR_CLOSEST");

        let result = load_dotenv_from_ancestors(None);
        assert!(result.is_ok());
        assert_eq!(
            std::env::var("TEST_VAR_CLOSEST").unwrap(),
            "child",
            "Should prefer .env in current directory over parent"
        );

        // Cleanup
        std::env::set_current_dir(original_dir).unwrap();
        std::env::remove_var("TEST_VAR_CLOSEST");
    }

    #[test]
    #[serial]
    fn test_load_dotenv_error_when_not_found() {
        let temp_dir = TempDir::new().unwrap();
        // Don't create any .env file

        let original_dir = std::env::current_dir().unwrap();
        std::env::set_current_dir(temp_dir.path()).unwrap();

        let result = load_dotenv_from_ancestors(None);
        assert!(result.is_err(), "Should error when no .env file found");

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("No .env file found"),
            "Error should mention no .env found"
        );

        // Cleanup
        std::env::set_current_dir(original_dir).unwrap();
    }

    #[test]
    #[serial]
    fn test_load_dotenv_with_env_name() {
        let temp_dir = TempDir::new().unwrap();
        // Base .env is required
        fs::write(temp_dir.path().join(".env"), "TEST_VAR_BASE=base").unwrap();
        // Environment-specific file
        fs::write(
            temp_dir.path().join(".env.development"),
            "TEST_VAR_DEV=devvalue",
        )
        .unwrap();

        let original_dir = std::env::current_dir().unwrap();
        std::env::set_current_dir(temp_dir.path()).unwrap();

        // Clear any existing values
        std::env::remove_var("TEST_VAR_DEV");
        std::env::remove_var("TEST_VAR_BASE");

        let result = load_dotenv_from_ancestors(Some("development"));
        assert!(
            result.is_ok(),
            "Should load .env and .env.development: {:?}",
            result
        );
        assert_eq!(
            std::env::var("TEST_VAR_DEV").unwrap(),
            "devvalue",
            "Should load env var from .env.development"
        );
        assert_eq!(
            std::env::var("TEST_VAR_BASE").unwrap(),
            "base",
            "Should also load env var from base .env"
        );

        // Cleanup
        std::env::set_current_dir(original_dir).unwrap();
        std::env::remove_var("TEST_VAR_DEV");
        std::env::remove_var("TEST_VAR_BASE");
    }

    #[test]
    #[serial]
    fn test_hierarchical_override_env_specific_wins() {
        let temp_dir = TempDir::new().unwrap();
        // Base .env sets a value
        fs::write(temp_dir.path().join(".env"), "TEST_VAR_OVERRIDE=from_base").unwrap();
        // .env.development overrides it
        fs::write(
            temp_dir.path().join(".env.development"),
            "TEST_VAR_OVERRIDE=from_dev",
        )
        .unwrap();

        let original_dir = std::env::current_dir().unwrap();
        std::env::set_current_dir(temp_dir.path()).unwrap();

        std::env::remove_var("TEST_VAR_OVERRIDE");

        let result = load_dotenv_from_ancestors(Some("development"));
        assert!(result.is_ok());
        assert_eq!(
            std::env::var("TEST_VAR_OVERRIDE").unwrap(),
            "from_dev",
            ".env.development should override .env"
        );

        // Cleanup
        std::env::set_current_dir(original_dir).unwrap();
        std::env::remove_var("TEST_VAR_OVERRIDE");
    }

    #[test]
    #[serial]
    fn test_hierarchical_override_local_wins() {
        let temp_dir = TempDir::new().unwrap();
        fs::write(temp_dir.path().join(".env"), "TEST_VAR_LOCAL=from_base").unwrap();
        fs::write(
            temp_dir.path().join(".env.local"),
            "TEST_VAR_LOCAL=from_local",
        )
        .unwrap();

        let original_dir = std::env::current_dir().unwrap();
        std::env::set_current_dir(temp_dir.path()).unwrap();

        std::env::remove_var("TEST_VAR_LOCAL");

        let result = load_dotenv_from_ancestors(None);
        assert!(result.is_ok());
        assert_eq!(
            std::env::var("TEST_VAR_LOCAL").unwrap(),
            "from_local",
            ".env.local should override .env"
        );

        // Cleanup
        std::env::set_current_dir(original_dir).unwrap();
        std::env::remove_var("TEST_VAR_LOCAL");
    }

    #[test]
    #[serial]
    fn test_hierarchical_full_chain() {
        let temp_dir = TempDir::new().unwrap();
        // All four files in the hierarchy
        fs::write(temp_dir.path().join(".env"), "VAR=base").unwrap();
        fs::write(temp_dir.path().join(".env.local"), "VAR=local").unwrap();
        fs::write(temp_dir.path().join(".env.staging"), "VAR=staging").unwrap();
        fs::write(
            temp_dir.path().join(".env.staging.local"),
            "VAR=staging_local",
        )
        .unwrap();

        let original_dir = std::env::current_dir().unwrap();
        std::env::set_current_dir(temp_dir.path()).unwrap();

        std::env::remove_var("VAR");

        let result = load_dotenv_from_ancestors(Some("staging"));
        assert!(result.is_ok());
        assert_eq!(
            std::env::var("VAR").unwrap(),
            "staging_local",
            ".env.staging.local should win over all others"
        );

        // Cleanup
        std::env::set_current_dir(original_dir).unwrap();
        std::env::remove_var("VAR");
    }

    #[test]
    #[serial]
    fn test_env_specific_optional_when_base_exists() {
        let temp_dir = TempDir::new().unwrap();
        // Only base .env exists, no .env.production
        fs::write(temp_dir.path().join(".env"), "TEST_VAR_OPT=base").unwrap();

        let original_dir = std::env::current_dir().unwrap();
        std::env::set_current_dir(temp_dir.path()).unwrap();

        std::env::remove_var("TEST_VAR_OPT");

        // Should succeed even without .env.production
        let result = load_dotenv_from_ancestors(Some("production"));
        assert!(
            result.is_ok(),
            "Should succeed with just .env when .env.production doesn't exist"
        );
        assert_eq!(
            std::env::var("TEST_VAR_OPT").unwrap(),
            "base",
            "Should load from base .env"
        );

        // Cleanup
        std::env::set_current_dir(original_dir).unwrap();
        std::env::remove_var("TEST_VAR_OPT");
    }

    #[test]
    fn test_warn_if_pooler_url_detects_pooler_with_dot() {
        // This test verifies the function runs without panic on pooler URLs
        // The actual warning is logged via tracing, which we're not capturing here
        warn_if_pooler_url(
            "postgresql://user:pass@ep-snowy-bar-a1b2c3d4-pooler.us-east-1.aws.neon.tech/db",
        );
    }

    #[test]
    fn test_warn_if_pooler_url_detects_pooler_with_port() {
        // Test pooler detection when followed by a port
        warn_if_pooler_url("postgresql://user:pass@mydb-pooler:5432/db");
    }

    #[test]
    fn test_warn_if_pooler_url_no_warning_for_direct_connection() {
        // This should not trigger a warning (no -pooler in hostname)
        warn_if_pooler_url(
            "postgresql://user:pass@ep-snowy-bar-a1b2c3d4.us-east-1.aws.neon.tech/db",
        );
    }

    #[test]
    fn test_warn_if_pooler_url_no_warning_for_localhost() {
        // Localhost should not trigger a warning
        warn_if_pooler_url("postgresql://user:pass@localhost:5432/db");
    }
}
