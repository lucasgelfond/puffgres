mod error;
mod migration;
mod validation;

pub use error::{ConfigError, ConfigResult};
pub use migration::{
    IdTypeConfig, MembershipMode, MigrationConfig, SourceConfig, TransformConfig, VersioningConfig,
};
pub use validation::{to_mapping, validate_migration};
