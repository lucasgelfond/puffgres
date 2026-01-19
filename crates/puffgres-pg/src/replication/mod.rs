//! True push-based streaming replication using PostgreSQL's native protocol.
//!
//! This module provides push-based CDC (Change Data Capture) using the
//! PostgreSQL streaming replication protocol with pgoutput format.

pub mod client;
pub mod lsn;
pub mod pgoutput;
pub mod publication;
pub mod relation_cache;
pub mod slot;
pub mod validation;

pub use client::{ReplicationStream, ReplicationStreamConfig, StreamingBatch};
pub use lsn::{format_lsn, parse_lsn};
pub use pgoutput::{PgOutputDecoder, PgOutputMessage};
pub use publication::{quote_ident, quote_table_name};
pub use relation_cache::RelationCache;
pub use slot::{ensure_slot, get_confirmed_flush_lsn, slot_exists};
pub use validation::{
    check_replication_setup, reset_replication, validate_all_tables_readable, ReplicationStatus,
    SlotStatus, PublicationStatus,
};
