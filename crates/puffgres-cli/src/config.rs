use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;

use puffgres_config::MigrationConfig;
use puffgres_core::Mapping;

/// Project configuration from puffgres.toml
#[derive(Debug, Deserialize)]
pub struct ProjectConfig {
    pub postgres: PostgresConfig,
    pub turbopuffer: TurbopufferConfig,
    pub state: StateConfig,
}

#[derive(Debug, Deserialize)]
pub struct PostgresConfig {
    pub connection_string: String,
}

#[derive(Debug, Deserialize)]
pub struct TurbopufferConfig {
    pub api_key: String,
}

#[derive(Debug, Deserialize)]
pub struct StateConfig {
    pub path: String,
}

impl ProjectConfig {
    /// Resolve environment variables in a string.
    /// Supports ${VAR_NAME} syntax.
    pub fn resolve_env(&self, s: &str) -> String {
        let mut result = s.to_string();

        // Find all ${...} patterns
        while let Some(start) = result.find("${") {
            if let Some(end) = result[start..].find('}') {
                let var_name = &result[start + 2..start + end];
                let value = std::env::var(var_name).unwrap_or_default();
                result = format!("{}{}{}", &result[..start], value, &result[start + end + 1..]);
            } else {
                break;
            }
        }

        result
    }

    /// Get the resolved Postgres connection string.
    pub fn postgres_connection_string(&self) -> String {
        self.resolve_env(&self.postgres.connection_string)
    }

    /// Get the resolved Turbopuffer API key.
    pub fn turbopuffer_api_key(&self) -> String {
        self.resolve_env(&self.turbopuffer.api_key)
    }

    /// Load all migrations from the migrations directory.
    pub fn load_migrations(&self) -> Result<Vec<Mapping>> {
        let migrations_dir = Path::new("puffgres/migrations");

        if !migrations_dir.exists() {
            return Ok(vec![]);
        }

        let mut mappings = Vec::new();

        for entry in fs::read_dir(migrations_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().map_or(false, |ext| ext == "toml") {
                let content = fs::read_to_string(&path)
                    .with_context(|| format!("Failed to read migration: {}", path.display()))?;

                let config = MigrationConfig::parse(&content)
                    .with_context(|| format!("Failed to parse migration: {}", path.display()))?;

                let mapping = puffgres_config::to_mapping(&config)
                    .with_context(|| format!("Invalid migration: {}", path.display()))?;

                mappings.push(mapping);
            }
        }

        // Sort by version
        mappings.sort_by_key(|m| m.version);

        Ok(mappings)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_env() {
        std::env::set_var("TEST_VAR", "hello");

        let config = ProjectConfig {
            postgres: PostgresConfig {
                connection_string: "postgres://${TEST_VAR}".to_string(),
            },
            turbopuffer: TurbopufferConfig {
                api_key: "key".to_string(),
            },
            state: StateConfig {
                path: "state.db".to_string(),
            },
        };

        assert_eq!(config.resolve_env("${TEST_VAR}"), "hello");
        assert_eq!(config.resolve_env("prefix_${TEST_VAR}_suffix"), "prefix_hello_suffix");
        assert_eq!(config.resolve_env("no_vars"), "no_vars");
    }
}
