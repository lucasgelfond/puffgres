use anyhow::{Context, Result};
use tracing::{info, warn};

/// Default batch size for processing transforms (rows per batch).
pub const DEFAULT_TRANSFORM_BATCH_SIZE: usize = 100;

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

/// Load .env file from current directory or any parent directory
/// Searches from the current working directory up to the filesystem root,
/// loading the first .env file found.
pub fn load_dotenv_from_ancestors() -> Result<()> {
    let cwd = std::env::current_dir().context("Failed to get current directory")?;

    let mut current = cwd.as_path();
    loop {
        let env_path = current.join(".env");
        if env_path.exists() {
            dotenvy::from_path(&env_path)
                .with_context(|| format!("Failed to load .env from {}", env_path.display()))?;
            info!("Loaded .env from {}", env_path.display());
            return Ok(());
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
        cwd.display()
    )
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

        let result = load_dotenv_from_ancestors();
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

        let result = load_dotenv_from_ancestors();
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

        let result = load_dotenv_from_ancestors();
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

        let result = load_dotenv_from_ancestors();
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

        let result = load_dotenv_from_ancestors();
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
