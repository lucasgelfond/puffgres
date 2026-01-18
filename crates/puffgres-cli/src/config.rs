use std::fs;
use std::path::Path;

use anyhow::{bail, Context, Result};
use serde::Deserialize;

use puffgres_config::MigrationConfig;
use puffgres_core::Mapping;
use puffgres_pg::LocalMigration;

/// Project configuration from puffgres.toml
#[derive(Debug, Deserialize)]
pub struct ProjectConfig {
    pub postgres: PostgresConfig,
    pub turbopuffer: TurbopufferConfig,
    /// Optional embedding providers configuration.
    #[serde(default)]
    #[allow(dead_code)]
    pub providers: ProvidersConfig,
}

#[derive(Debug, Deserialize)]
pub struct PostgresConfig {
    pub connection_string: String,
}

#[derive(Debug, Deserialize)]
pub struct TurbopufferConfig {
    pub api_key: String,
    /// Optional base namespace prefix for environment separation (e.g., "PRODUCTION", "DEVELOPMENT").
    /// If set, all turbopuffer namespaces will be prefixed with this value.
    #[serde(default)]
    pub base_namespace: Option<String>,
}

/// Configuration for external providers (embeddings, etc.)
#[derive(Debug, Default, Deserialize)]
#[allow(dead_code)]
pub struct ProvidersConfig {
    /// Embedding provider configuration.
    pub embeddings: Option<EmbeddingProviderConfig>,
}

/// Embedding provider configuration.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct EmbeddingProviderConfig {
    /// Provider type: "together", "openai", etc.
    #[serde(rename = "type")]
    pub provider_type: String,
    /// Model name.
    pub model: String,
    /// API key (supports ${ENV_VAR} syntax).
    pub api_key: String,
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
    /// Returns an error if required environment variables are not set.
    pub fn postgres_connection_string(&self) -> Result<String> {
        self.resolve_env_required(&self.postgres.connection_string, "DATABASE_URL")
    }

    /// Get the resolved Turbopuffer API key.
    /// Returns an error if required environment variables are not set.
    pub fn turbopuffer_api_key(&self) -> Result<String> {
        self.resolve_env_required(&self.turbopuffer.api_key, "TURBOPUFFER_API_KEY")
    }

    /// Resolve environment variables in a string, returning an error if any are missing.
    fn resolve_env_required(&self, s: &str, hint_var: &str) -> Result<String> {
        let mut result = s.to_string();
        let mut missing_vars = Vec::new();

        // Find all ${...} patterns
        while let Some(start) = result.find("${") {
            if let Some(end) = result[start..].find('}') {
                let var_name = &result[start + 2..start + end];
                match std::env::var(var_name) {
                    Ok(value) if !value.is_empty() => {
                        result = format!("{}{}{}", &result[..start], value, &result[start + end + 1..]);
                    }
                    _ => {
                        missing_vars.push(var_name.to_string());
                        // Replace with empty to continue parsing other vars
                        result = format!("{}{}", &result[..start], &result[start + end + 1..]);
                    }
                }
            } else {
                break;
            }
        }

        if !missing_vars.is_empty() {
            bail!(
                "Missing required environment variable: {}\n\n\
                 Make sure {} is set in your .env file or environment.\n\
                 Example: {}=postgresql://user:password@localhost:5432/database",
                missing_vars.join(", "),
                hint_var,
                hint_var
            );
        }

        if result.is_empty() {
            bail!(
                "Environment variable {} is empty.\n\n\
                 Make sure {} is set to a valid value in your .env file or environment.\n\
                 Example: {}=postgresql://user:password@localhost:5432/database",
                hint_var,
                hint_var,
                hint_var
            );
        }

        Ok(result)
    }

    /// Get the resolved base namespace prefix, if configured.
    pub fn base_namespace(&self) -> Option<String> {
        self.turbopuffer
            .base_namespace
            .as_ref()
            .map(|ns| self.resolve_env(ns))
            .filter(|ns| !ns.is_empty())
    }

    /// Apply the base namespace prefix to a namespace name.
    pub fn apply_namespace_prefix(&self, namespace: &str) -> String {
        if let Some(prefix) = self.base_namespace() {
            format!("{}_{}", prefix, namespace)
        } else {
            namespace.to_string()
        }
    }

    /// Load all migrations from the migrations directory.
    /// Applies the base namespace prefix if configured.
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

                let mut mapping = puffgres_config::to_mapping(&config)
                    .with_context(|| format!("Invalid migration: {}", path.display()))?;

                // Apply base namespace prefix if configured
                mapping.namespace = self.apply_namespace_prefix(&mapping.namespace);

                mappings.push(mapping);
            }
        }

        // Sort by version
        mappings.sort_by_key(|m| m.version);

        Ok(mappings)
    }

    /// Load all local migration files with their content for hashing.
    pub fn load_local_migrations(&self) -> Result<Vec<LocalMigration>> {
        let migrations_dir = Path::new("puffgres/migrations");

        if !migrations_dir.exists() {
            return Ok(vec![]);
        }

        let mut migrations = Vec::new();

        for entry in fs::read_dir(migrations_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().map_or(false, |ext| ext == "toml") {
                let content = fs::read_to_string(&path)
                    .with_context(|| format!("Failed to read migration: {}", path.display()))?;

                let config = MigrationConfig::parse(&content)
                    .with_context(|| format!("Failed to parse migration: {}", path.display()))?;

                migrations.push(LocalMigration {
                    version: config.version as i32,
                    mapping_name: config.mapping_name.clone(),
                    content,
                });
            }
        }

        // Sort by version
        migrations.sort_by_key(|m| m.version);

        Ok(migrations)
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
                base_namespace: None,
            },
            providers: ProvidersConfig::default(),
        };

        assert_eq!(config.resolve_env("${TEST_VAR}"), "hello");
        assert_eq!(config.resolve_env("prefix_${TEST_VAR}_suffix"), "prefix_hello_suffix");
        assert_eq!(config.resolve_env("no_vars"), "no_vars");
    }
}
