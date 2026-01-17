use crate::predicate::Predicate;
use crate::transform::IdType;

/// Configuration for a mapping from Postgres to turbopuffer.
#[derive(Debug, Clone)]
pub struct Mapping {
    /// Stable identifier for this mapping.
    pub name: String,
    /// Version number (monotonically increasing).
    pub version: u32,
    /// Target turbopuffer namespace.
    pub namespace: String,
    /// Source relation.
    pub source: Source,
    /// ID column configuration.
    pub id: IdConfig,
    /// Columns to extract from the row.
    pub columns: Vec<String>,
    /// Membership predicate (determines which rows belong).
    pub membership: MembershipConfig,
    /// Batching configuration.
    pub batching: BatchConfig,
    /// Versioning mode for anti-regression.
    pub versioning: VersioningMode,
}

/// Source relation (table or view).
#[derive(Debug, Clone)]
pub struct Source {
    pub schema: String,
    pub table: String,
}

impl Source {
    pub fn new(schema: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            schema: schema.into(),
            table: table.into(),
        }
    }

    /// Check if this source matches a row event.
    pub fn matches(&self, schema: &str, table: &str) -> bool {
        self.schema == schema && self.table == table
    }
}

/// ID column configuration.
#[derive(Debug, Clone)]
pub struct IdConfig {
    pub column: String,
    pub id_type: IdType,
}

/// Membership configuration.
#[derive(Debug, Clone)]
pub enum MembershipConfig {
    /// Evaluate a DSL predicate against row data.
    Dsl(Predicate),
    /// Source is a view that already filters membership.
    View,
    /// Always include all rows.
    All,
}

impl MembershipConfig {
    /// Create a DSL membership config by parsing a predicate string.
    pub fn dsl(predicate: &str) -> crate::Result<Self> {
        let pred = Predicate::parse(predicate)?;
        Ok(MembershipConfig::Dsl(pred))
    }
}

/// Batching configuration.
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Maximum rows per batch.
    pub max_rows: usize,
    /// Maximum bytes per batch (approximate).
    pub max_bytes: usize,
    /// Flush interval in milliseconds.
    pub flush_interval_ms: u64,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_rows: 1000,
            max_bytes: 4 * 1024 * 1024, // 4MB
            flush_interval_ms: 100,
        }
    }
}

/// Versioning mode for anti-regression.
#[derive(Debug, Clone, Default)]
pub enum VersioningMode {
    /// Use the source LSN for versioning.
    #[default]
    SourceLsn,
    /// Use a specific column for versioning.
    Column(String),
    /// No versioning (not recommended).
    None,
}

impl Mapping {
    /// Create a builder for constructing a mapping.
    pub fn builder(name: impl Into<String>) -> MappingBuilder {
        MappingBuilder::new(name)
    }
}

/// Builder for constructing a Mapping.
pub struct MappingBuilder {
    name: String,
    version: u32,
    namespace: Option<String>,
    source: Option<Source>,
    id: Option<IdConfig>,
    columns: Vec<String>,
    membership: MembershipConfig,
    batching: BatchConfig,
    versioning: VersioningMode,
}

impl MappingBuilder {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version: 1,
            namespace: None,
            source: None,
            id: None,
            columns: vec![],
            membership: MembershipConfig::All,
            batching: BatchConfig::default(),
            versioning: VersioningMode::default(),
        }
    }

    pub fn version(mut self, version: u32) -> Self {
        self.version = version;
        self
    }

    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    pub fn source(mut self, schema: impl Into<String>, table: impl Into<String>) -> Self {
        self.source = Some(Source::new(schema, table));
        self
    }

    pub fn id(mut self, column: impl Into<String>, id_type: IdType) -> Self {
        self.id = Some(IdConfig {
            column: column.into(),
            id_type,
        });
        self
    }

    pub fn columns(mut self, columns: Vec<String>) -> Self {
        self.columns = columns;
        self
    }

    pub fn membership(mut self, config: MembershipConfig) -> Self {
        self.membership = config;
        self
    }

    pub fn membership_dsl(mut self, predicate: &str) -> crate::Result<Self> {
        self.membership = MembershipConfig::dsl(predicate)?;
        Ok(self)
    }

    pub fn batching(mut self, config: BatchConfig) -> Self {
        self.batching = config;
        self
    }

    pub fn versioning(mut self, mode: VersioningMode) -> Self {
        self.versioning = mode;
        self
    }

    pub fn build(self) -> crate::Result<Mapping> {
        let namespace = self
            .namespace
            .ok_or_else(|| crate::Error::MissingColumn("namespace".into()))?;
        let source = self
            .source
            .ok_or_else(|| crate::Error::MissingColumn("source".into()))?;
        let id = self
            .id
            .ok_or_else(|| crate::Error::MissingColumn("id".into()))?;

        Ok(Mapping {
            name: self.name,
            version: self.version,
            namespace,
            source,
            id,
            columns: self.columns,
            membership: self.membership,
            batching: self.batching,
            versioning: self.versioning,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_source_matches() {
        let source = Source::new("public", "users");
        assert!(source.matches("public", "users"));
        assert!(!source.matches("public", "posts"));
        assert!(!source.matches("private", "users"));
    }

    #[test]
    fn test_mapping_builder() {
        let mapping = Mapping::builder("users_public")
            .version(1)
            .namespace("users")
            .source("public", "users")
            .id("id", IdType::Uint)
            .columns(vec!["name".into(), "email".into()])
            .build()
            .unwrap();

        assert_eq!(mapping.name, "users_public");
        assert_eq!(mapping.namespace, "users");
        assert!(mapping.source.matches("public", "users"));
    }

    #[test]
    fn test_mapping_builder_with_dsl() {
        let mapping = Mapping::builder("active_users")
            .namespace("users")
            .source("public", "users")
            .id("id", IdType::Uint)
            .membership_dsl("status = 'active' AND deleted_at IS NULL")
            .unwrap()
            .build()
            .unwrap();

        assert!(matches!(mapping.membership, MembershipConfig::Dsl(_)));
    }
}
