use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "puffgres")]
#[command(about = "Mirror Postgres data to turbopuffer")]
#[command(version)]
pub struct Cli {
    /// Environment to load (loads .env.{ENV} instead of .env)
    #[arg(short, long, global = true)]
    pub env: Option<String>,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Initialize puffgres in the current directory (creates config files)
    Init,

    /// Set up database tables and replication slot
    Setup,

    /// Create a new migration
    New {
        /// Optional name for the migration (will prompt if not provided)
        name: Option<String>,
    },

    /// Apply pending migrations
    Migrate {
        /// Show what would be applied without actually applying
        #[arg(long)]
        dry_run: bool,
    },

    /// Start the CDC replication loop
    Run {
        /// Replication slot name
        #[arg(long, default_value = "puffgres")]
        slot: String,

        /// Publication name for logical replication
        #[arg(long, default_value = "puffgres_pub")]
        publication: String,

        /// Create the replication slot if it doesn't exist
        #[arg(long, default_value = "true")]
        create_slot: bool,

        /// Skip auto-applying pending migrations
        #[arg(long)]
        skip_migrate: bool,
    },

    /// Show current sync status
    Status,

    /// Backfill existing table data to turbopuffer
    Backfill {
        /// Mapping name to backfill
        mapping: String,

        /// Batch size for processing
        #[arg(long, default_value = "1000")]
        batch_size: u32,

        /// Resume from previous checkpoint
        #[arg(long)]
        resume: bool,
    },

    /// Manage the dead letter queue
    Dlq {
        #[command(subcommand)]
        command: DlqCommands,
    },

    /// Reset local config from database state
    Reset,

    /// Remove all puffgres artifacts (dangerous)
    DangerouslyDeleteConfig,

    /// Delete all referenced turbopuffer namespaces (dangerous)
    DangerouslyResetTurbopuffer,
}

#[derive(Subcommand)]
pub enum DlqCommands {
    /// List DLQ entries
    List {
        /// Filter by mapping name
        #[arg(long)]
        mapping: Option<String>,

        /// Maximum number of entries to show
        #[arg(long, default_value = "50")]
        limit: i64,
    },

    /// Show details of a DLQ entry
    Show {
        /// Entry ID
        id: i32,
    },

    /// Retry DLQ entries
    Retry {
        /// Retry a specific entry by ID
        #[arg(long)]
        id: Option<i32>,

        /// Retry all entries for a mapping
        #[arg(long)]
        mapping: Option<String>,
    },

    /// Clear DLQ entries
    Clear {
        /// Clear entries for a specific mapping
        #[arg(long)]
        mapping: Option<String>,

        /// Clear all entries
        #[arg(long)]
        all: bool,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn verify_cli() {
        Cli::command().debug_assert();
    }
}
