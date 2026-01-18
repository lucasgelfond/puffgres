use anyhow::{Context, Result};
use tracing::info;

/// Check if all required puffgres environment variables are set.
/// Returns true if DATABASE_URL, TURBOPUFFER_API_KEY, and PUFFGRES_BASE_NAMESPACE are all present.
pub fn has_all_env_vars() -> bool {
    std::env::var("DATABASE_URL").is_ok()
        && std::env::var("TURBOPUFFER_API_KEY").is_ok()
        && std::env::var("PUFFGRES_BASE_NAMESPACE").is_ok()
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
    #[serial]
    fn test_has_all_env_vars_returns_true_when_all_set() {
        // Clear first
        std::env::remove_var("DATABASE_URL");
        std::env::remove_var("TURBOPUFFER_API_KEY");
        std::env::remove_var("PUFFGRES_BASE_NAMESPACE");

        // Set all three
        std::env::set_var("DATABASE_URL", "postgres://localhost/test");
        std::env::set_var("TURBOPUFFER_API_KEY", "test-key");
        std::env::set_var("PUFFGRES_BASE_NAMESPACE", "test-namespace");

        assert!(
            has_all_env_vars(),
            "Should return true when all three env vars are set"
        );

        // Cleanup
        std::env::remove_var("DATABASE_URL");
        std::env::remove_var("TURBOPUFFER_API_KEY");
        std::env::remove_var("PUFFGRES_BASE_NAMESPACE");
    }

    #[test]
    #[serial]
    fn test_has_all_env_vars_returns_false_when_missing_database_url() {
        // Clear all
        std::env::remove_var("DATABASE_URL");
        std::env::remove_var("TURBOPUFFER_API_KEY");
        std::env::remove_var("PUFFGRES_BASE_NAMESPACE");

        // Set only two
        std::env::set_var("TURBOPUFFER_API_KEY", "test-key");
        std::env::set_var("PUFFGRES_BASE_NAMESPACE", "test-namespace");

        assert!(
            !has_all_env_vars(),
            "Should return false when DATABASE_URL is missing"
        );

        // Cleanup
        std::env::remove_var("TURBOPUFFER_API_KEY");
        std::env::remove_var("PUFFGRES_BASE_NAMESPACE");
    }

    #[test]
    #[serial]
    fn test_has_all_env_vars_returns_false_when_missing_turbopuffer_key() {
        // Clear all
        std::env::remove_var("DATABASE_URL");
        std::env::remove_var("TURBOPUFFER_API_KEY");
        std::env::remove_var("PUFFGRES_BASE_NAMESPACE");

        // Set only two
        std::env::set_var("DATABASE_URL", "postgres://localhost/test");
        std::env::set_var("PUFFGRES_BASE_NAMESPACE", "test-namespace");

        assert!(
            !has_all_env_vars(),
            "Should return false when TURBOPUFFER_API_KEY is missing"
        );

        // Cleanup
        std::env::remove_var("DATABASE_URL");
        std::env::remove_var("PUFFGRES_BASE_NAMESPACE");
    }

    #[test]
    #[serial]
    fn test_has_all_env_vars_returns_false_when_missing_base_namespace() {
        // Clear all
        std::env::remove_var("DATABASE_URL");
        std::env::remove_var("TURBOPUFFER_API_KEY");
        std::env::remove_var("PUFFGRES_BASE_NAMESPACE");

        // Set only two
        std::env::set_var("DATABASE_URL", "postgres://localhost/test");
        std::env::set_var("TURBOPUFFER_API_KEY", "test-key");

        assert!(
            !has_all_env_vars(),
            "Should return false when PUFFGRES_BASE_NAMESPACE is missing"
        );

        // Cleanup
        std::env::remove_var("DATABASE_URL");
        std::env::remove_var("TURBOPUFFER_API_KEY");
    }

    #[test]
    #[serial]
    fn test_has_all_env_vars_returns_false_when_none_set() {
        // Clear all
        std::env::remove_var("DATABASE_URL");
        std::env::remove_var("TURBOPUFFER_API_KEY");
        std::env::remove_var("PUFFGRES_BASE_NAMESPACE");

        assert!(
            !has_all_env_vars(),
            "Should return false when no env vars are set"
        );
    }
}
