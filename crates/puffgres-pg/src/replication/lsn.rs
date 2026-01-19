//! LSN (Log Sequence Number) utilities for PostgreSQL replication.

use crate::error::{PgError, PgResult};

/// Parse LSN from "X/Y" format to u64.
pub fn parse_lsn(lsn: &str) -> PgResult<u64> {
    let parts: Vec<&str> = lsn.split('/').collect();
    if parts.len() != 2 {
        return Err(PgError::InvalidLsn(lsn.to_string()));
    }

    let high =
        u64::from_str_radix(parts[0], 16).map_err(|_| PgError::InvalidLsn(lsn.to_string()))?;
    let low =
        u64::from_str_radix(parts[1], 16).map_err(|_| PgError::InvalidLsn(lsn.to_string()))?;

    Ok((high << 32) | low)
}

/// Format u64 LSN to "X/Y" format.
pub fn format_lsn(lsn: u64) -> String {
    let high = lsn >> 32;
    let low = lsn & 0xFFFFFFFF;
    format!("{:X}/{:X}", high, low)
}

/// Convert pgwire-replication Lsn to u64.
pub fn lsn_to_u64(lsn: pgwire_replication::Lsn) -> u64 {
    lsn.into()
}

/// Convert u64 to pgwire-replication Lsn.
pub fn u64_to_lsn(val: u64) -> pgwire_replication::Lsn {
    pgwire_replication::Lsn::from(val)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_lsn() {
        assert_eq!(parse_lsn("0/16B3748").unwrap(), 0x16B3748);
        assert_eq!(parse_lsn("1/16B3748").unwrap(), 0x100000000 + 0x16B3748);
        assert!(parse_lsn("invalid").is_err());
    }

    #[test]
    fn test_format_lsn() {
        assert_eq!(format_lsn(0x16B3748), "0/16B3748");
        assert_eq!(format_lsn(0x100000000 + 0x16B3748), "1/16B3748");
    }

    #[test]
    fn test_lsn_roundtrip() {
        let values = [0u64, 100, 0x16B3748, 0x100000000 + 0x16B3748, u64::MAX >> 1];

        for val in values {
            let formatted = format_lsn(val);
            let parsed = parse_lsn(&formatted).unwrap();
            assert_eq!(val, parsed, "Roundtrip failed for {}", val);
        }
    }
}
