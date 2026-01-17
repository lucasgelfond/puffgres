use serde::Deserialize;

use crate::error::ConfigResult;

/// Raw migration configuration as parsed from TOML.
#[derive(Debug, Deserialize)]
pub struct MigrationConfig {
    /// Version number (monotonically increasing).
    pub version: i64,
    /// Stable identifier for this mapping.
    pub mapping_name: String,
    /// Target turbopuffer namespace.
    pub namespace: String,
    /// Source relation configuration.
    pub source: SourceConfig,
    /// ID column configuration.
    pub id: IdConfig,
    /// Columns to extract from the row.
    #[serde(default)]
    pub columns: Vec<String>,
    /// Membership configuration.
    #[serde(default)]
    pub membership: MembershipConfig,
    /// Transform configuration.
    #[serde(default)]
    pub transform: TransformConfig,
    /// Batching configuration.
    #[serde(default)]
    pub batching: BatchingConfig,
    /// Versioning configuration.
    #[serde(default)]
    pub versioning: VersioningConfig,
}

impl MigrationConfig {
    /// Parse a migration config from a TOML string.
    pub fn parse(toml_str: &str) -> ConfigResult<Self> {
        let config: MigrationConfig = toml::from_str(toml_str)?;
        Ok(config)
    }
}

/// Source relation configuration.
#[derive(Debug, Deserialize)]
pub struct SourceConfig {
    /// Schema name (e.g., "public").
    pub schema: String,
    /// Table or view name.
    #[serde(alias = "view")]
    pub table: String,
}

/// ID column configuration (raw from TOML).
#[derive(Debug, Deserialize)]
pub struct IdConfig {
    /// Column name.
    pub column: String,
    /// ID type.
    #[serde(rename = "type")]
    pub id_type: IdTypeConfig,
}

/// ID type configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IdTypeConfig {
    Uint,
    Int,
    Uuid,
    String,
}

impl IdTypeConfig {
    pub fn to_core_type(self) -> puffgres_core::IdType {
        match self {
            IdTypeConfig::Uint => puffgres_core::IdType::Uint,
            IdTypeConfig::Int => puffgres_core::IdType::Int,
            IdTypeConfig::Uuid => puffgres_core::IdType::Uuid,
            IdTypeConfig::String => puffgres_core::IdType::String,
        }
    }
}

/// Membership configuration.
#[derive(Debug, Default, Deserialize)]
pub struct MembershipConfig {
    /// Membership mode.
    #[serde(default)]
    pub mode: MembershipMode,
    /// Predicate expression (for DSL mode).
    pub predicate: Option<String>,
}

/// Membership mode.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MembershipMode {
    /// Evaluate a DSL predicate against row data.
    Dsl,
    /// Source is a view that already filters membership.
    View,
    /// Fetch current row by PK to evaluate.
    Lookup,
    /// Include all rows (default).
    #[default]
    All,
}

/// Transform configuration.
#[derive(Debug, Default, Deserialize)]
pub struct TransformConfig {
    /// Transform type.
    #[serde(rename = "type", default)]
    pub transform_type: TransformType,
    /// Path to transform file (for JS or Rust).
    pub path: Option<String>,
    /// Entry function name.
    pub entry: Option<String>,
}

/// Transform type.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TransformType {
    /// Identity transform (selected columns only).
    #[default]
    Identity,
    /// JavaScript transform.
    Js,
    /// Rust transform.
    Rust,
}

/// Batching configuration.
#[derive(Debug, Deserialize)]
pub struct BatchingConfig {
    /// Maximum rows per batch.
    #[serde(default = "default_max_rows")]
    pub batch_max_rows: usize,
    /// Maximum bytes per batch.
    #[serde(default = "default_max_bytes")]
    pub batch_max_bytes: usize,
    /// Flush interval in milliseconds.
    #[serde(default = "default_flush_interval")]
    pub flush_interval_ms: u64,
}

impl Default for BatchingConfig {
    fn default() -> Self {
        Self {
            batch_max_rows: default_max_rows(),
            batch_max_bytes: default_max_bytes(),
            flush_interval_ms: default_flush_interval(),
        }
    }
}

fn default_max_rows() -> usize {
    1000
}

fn default_max_bytes() -> usize {
    4 * 1024 * 1024
}

fn default_flush_interval() -> u64 {
    100
}

/// Versioning configuration.
#[derive(Debug, Default, Deserialize)]
pub struct VersioningConfig {
    /// Versioning mode.
    #[serde(default)]
    pub mode: VersioningMode,
    /// Column name (for column mode).
    pub column: Option<String>,
}

/// Versioning mode.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VersioningMode {
    /// Use source LSN for versioning.
    #[default]
    SourceLsn,
    /// Use a specific column.
    Column,
    /// No versioning.
    None,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_minimal_migration() {
        let toml = r#"
version = 1
mapping_name = "users_public"
namespace = "users"

[source]
schema = "public"
table = "users"

[id]
column = "id"
type = "uint"
"#;

        let config = MigrationConfig::parse(toml).unwrap();
        assert_eq!(config.version, 1);
        assert_eq!(config.mapping_name, "users_public");
        assert_eq!(config.namespace, "users");
        assert_eq!(config.source.schema, "public");
        assert_eq!(config.source.table, "users");
        assert_eq!(config.id.column, "id");
        assert_eq!(config.id.id_type, IdTypeConfig::Uint);
    }

    #[test]
    fn test_parse_full_migration() {
        let toml = r#"
version = 2
mapping_name = "active_pages"
namespace = "pages"
columns = ["page_id", "title", "content", "status", "deleted_at"]

[source]
schema = "public"
table = "pages"

[id]
column = "page_id"
type = "uuid"

[membership]
mode = "dsl"
predicate = "status = 'published' AND deleted_at IS NULL"

[transform]
type = "identity"

[batching]
batch_max_rows = 500
batch_max_bytes = 2097152
flush_interval_ms = 50

[versioning]
mode = "source_lsn"
"#;

        let config = MigrationConfig::parse(toml).unwrap();
        assert_eq!(config.version, 2);
        assert_eq!(config.mapping_name, "active_pages");
        assert_eq!(config.id.id_type, IdTypeConfig::Uuid);
        assert_eq!(config.columns.len(), 5);
        assert_eq!(config.membership.mode, MembershipMode::Dsl);
        assert!(config.membership.predicate.is_some());
        assert_eq!(config.batching.batch_max_rows, 500);
        assert_eq!(config.versioning.mode, VersioningMode::SourceLsn);
    }

    #[test]
    fn test_parse_with_view_source() {
        let toml = r#"
version = 1
mapping_name = "active_users_view"
namespace = "users"

[source]
schema = "public"
view = "active_users"

[id]
column = "id"
type = "int"

[membership]
mode = "view"
"#;

        let config = MigrationConfig::parse(toml).unwrap();
        assert_eq!(config.source.table, "active_users");
        assert_eq!(config.membership.mode, MembershipMode::View);
    }

    #[test]
    fn test_parse_column_versioning() {
        let toml = r#"
version = 1
mapping_name = "test"
namespace = "test"

[source]
schema = "public"
table = "test"

[id]
column = "id"
type = "uint"

[versioning]
mode = "column"
column = "updated_at"
"#;

        let config = MigrationConfig::parse(toml).unwrap();
        assert_eq!(config.versioning.mode, VersioningMode::Column);
        assert_eq!(config.versioning.column, Some("updated_at".into()));
    }

    #[test]
    fn test_id_type_conversions() {
        assert!(matches!(
            IdTypeConfig::Uint.to_core_type(),
            puffgres_core::IdType::Uint
        ));
        assert!(matches!(
            IdTypeConfig::Uuid.to_core_type(),
            puffgres_core::IdType::Uuid
        ));
    }
}
