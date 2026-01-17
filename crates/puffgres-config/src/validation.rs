use puffgres_core::Predicate;

use crate::error::{ConfigError, ConfigResult};
use crate::migration::{MembershipMode, MigrationConfig, VersioningMode};

/// Validate a migration configuration.
/// Returns a list of validation errors (empty if valid).
pub fn validate_migration(config: &MigrationConfig) -> ConfigResult<()> {
    validate_version(config)?;
    validate_id_in_columns(config)?;
    validate_membership(config)?;
    validate_versioning(config)?;
    Ok(())
}

fn validate_version(config: &MigrationConfig) -> ConfigResult<()> {
    if config.version <= 0 {
        return Err(ConfigError::InvalidVersion(config.version));
    }
    Ok(())
}

fn validate_id_in_columns(_config: &MigrationConfig) -> ConfigResult<()> {
    // If columns are specified, the id column should typically be included
    // (though this is a warning, not an error - the transform might not need it)
    // For now, we don't enforce this strictly
    Ok(())
}

fn validate_membership(config: &MigrationConfig) -> ConfigResult<()> {
    match config.membership.mode {
        MembershipMode::Dsl => {
            let predicate = config
                .membership
                .predicate
                .as_ref()
                .ok_or(ConfigError::MissingPredicate)?;

            // Validate that the predicate parses correctly
            Predicate::parse(predicate).map_err(|e| ConfigError::InvalidPredicate {
                message: e.to_string(),
            })?;
        }
        MembershipMode::View | MembershipMode::Lookup | MembershipMode::All => {
            // No additional validation needed
        }
    }
    Ok(())
}

fn validate_versioning(config: &MigrationConfig) -> ConfigResult<()> {
    if config.versioning.mode == VersioningMode::Column && config.versioning.column.is_none() {
        return Err(ConfigError::MissingVersioningColumn);
    }
    Ok(())
}

/// Convert a validated migration config to a core Mapping.
pub fn to_mapping(config: &MigrationConfig) -> ConfigResult<puffgres_core::Mapping> {
    validate_migration(config)?;

    let membership = match config.membership.mode {
        MembershipMode::All => puffgres_core::MembershipConfig::All,
        MembershipMode::View => puffgres_core::MembershipConfig::View,
        MembershipMode::Dsl => {
            let predicate = config.membership.predicate.as_ref().unwrap();
            let pred = Predicate::parse(predicate).map_err(|e| ConfigError::InvalidPredicate {
                message: e.to_string(),
            })?;
            puffgres_core::MembershipConfig::Dsl(pred)
        }
        MembershipMode::Lookup => {
            // Lookup mode is not yet fully implemented, treat as All for now
            puffgres_core::MembershipConfig::All
        }
    };

    let versioning = match config.versioning.mode {
        VersioningMode::SourceLsn => puffgres_core::VersioningMode::SourceLsn,
        VersioningMode::Column => {
            let col = config.versioning.column.clone().unwrap();
            puffgres_core::VersioningMode::Column(col)
        }
        VersioningMode::None => puffgres_core::VersioningMode::None,
    };

    let mapping = puffgres_core::Mapping::builder(&config.mapping_name)
        .version(config.version as u32)
        .namespace(&config.namespace)
        .source(&config.source.schema, &config.source.table)
        .id(&config.id.column, config.id.id_type.to_core_type())
        .columns(config.columns.clone())
        .membership(membership)
        .batching(puffgres_core::BatchConfig {
            max_rows: config.batching.batch_max_rows,
            max_bytes: config.batching.batch_max_bytes,
            flush_interval_ms: config.batching.flush_interval_ms,
        })
        .versioning(versioning)
        .build()
        .map_err(|e| ConfigError::MissingField {
            field: e.to_string(),
        })?;

    Ok(mapping)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migration::MigrationConfig;

    fn parse_and_validate(toml: &str) -> ConfigResult<()> {
        let config = MigrationConfig::parse(toml)?;
        validate_migration(&config)
    }

    #[test]
    fn test_validate_minimal_valid() {
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
"#;
        assert!(parse_and_validate(toml).is_ok());
    }

    #[test]
    fn test_validate_invalid_version() {
        let toml = r#"
version = 0
mapping_name = "test"
namespace = "test"

[source]
schema = "public"
table = "test"

[id]
column = "id"
type = "uint"
"#;
        let result = parse_and_validate(toml);
        assert!(matches!(result, Err(ConfigError::InvalidVersion(0))));
    }

    #[test]
    fn test_validate_dsl_missing_predicate() {
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

[membership]
mode = "dsl"
"#;
        let result = parse_and_validate(toml);
        assert!(matches!(result, Err(ConfigError::MissingPredicate)));
    }

    #[test]
    fn test_validate_dsl_invalid_predicate() {
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

[membership]
mode = "dsl"
predicate = "invalid syntax @@@@"
"#;
        let result = parse_and_validate(toml);
        assert!(matches!(result, Err(ConfigError::InvalidPredicate { .. })));
    }

    #[test]
    fn test_validate_column_versioning_missing_column() {
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
"#;
        let result = parse_and_validate(toml);
        assert!(matches!(result, Err(ConfigError::MissingVersioningColumn)));
    }

    #[test]
    fn test_to_mapping() {
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

columns = ["id", "name", "email"]

[membership]
mode = "dsl"
predicate = "status = 'active'"
"#;
        let config = MigrationConfig::parse(toml).unwrap();
        let mapping = to_mapping(&config).unwrap();

        assert_eq!(mapping.name, "users_public");
        assert_eq!(mapping.namespace, "users");
        assert!(mapping.source.matches("public", "users"));
    }
}
