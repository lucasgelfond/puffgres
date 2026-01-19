//! True push-based streaming replication using PostgreSQL's native protocol.
//!
//! This module provides push-based CDC (Change Data Capture) using the
//! PostgreSQL streaming replication protocol with pgoutput format.

pub mod client;
pub mod lsn;
pub mod pgoutput;
pub mod relation_cache;

pub use client::{ReplicationStream, ReplicationStreamConfig, StreamingBatch};
pub use lsn::{format_lsn, parse_lsn};
pub use pgoutput::{PgOutputDecoder, PgOutputMessage};
pub use relation_cache::RelationCache;
