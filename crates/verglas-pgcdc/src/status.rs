//! The serde status contract the control plane surfaces for a CDC job.
//!
//! A drain tick returns a [`CdcJobStatus`]: the slot and publication it drained,
//! the confirmed watermark LSN, whether it performed a full-table resync, and a
//! per-table [`CdcTableStatus`]. Field names are snake_case — this is a shared
//! wire contract read by the control plane, so the spelling is pinned.

use serde::{Deserialize, Serialize};

/// The lifecycle state of one CDC table.
pub mod state {
    /// Streaming incremental changes normally.
    pub const STREAMING: &str = "streaming";
    /// Performing (or having just performed) a full-table resync.
    pub const RESYNC: &str = "resync";
    /// Blocked on an incompatible schema change; not appending until resolved.
    pub const SCHEMA_PENDING: &str = "schema_pending";
    /// In error; the last append or decode failed.
    pub const ERROR: &str = "error";
}

/// Per-table CDC status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CdcTableStatus {
    /// The fully-qualified table (`namespace.table`).
    pub table: String,
    /// The last LSN appended for this table.
    pub last_lsn: i64,
    /// When the last change for this table committed (ISO 8601, or empty).
    pub last_committed_at: String,
    /// Rows appended for this table this run.
    pub rows_appended: u64,
    /// Values that failed to parse and were written as null this run.
    pub parse_errors: u64,
    /// The table's lifecycle state (see [`state`]).
    pub state: String,
    /// A human-readable error, when `state` is `error` or `schema_pending`.
    pub error: Option<String>,
}

impl CdcTableStatus {
    /// A streaming-state status for `table` with no rows yet.
    pub fn streaming(table: impl Into<String>) -> Self {
        CdcTableStatus {
            table: table.into(),
            last_lsn: 0,
            last_committed_at: String::new(),
            rows_appended: 0,
            parse_errors: 0,
            state: state::STREAMING.to_owned(),
            error: None,
        }
    }
}

/// Per-job CDC status: the slot/publication, the confirmed watermark, whether a
/// resync happened, and every table's status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CdcJobStatus {
    /// The replication slot drained.
    pub slot: String,
    /// The publication drained.
    pub publication: String,
    /// The confirmed watermark LSN after this tick (the slot's durable cursor).
    pub confirmed_lsn: i64,
    /// Whether this tick performed a full-table resync (a fresh slot).
    pub resynced: bool,
    /// Per-table status.
    pub tables: Vec<CdcTableStatus>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_status_round_trips() {
        let status = CdcJobStatus {
            slot: "verglas_cdc".to_owned(),
            publication: "verglas_cdc".to_owned(),
            confirmed_lsn: 42,
            resynced: true,
            tables: vec![CdcTableStatus {
                table: "pg_analytics.public_orders".to_owned(),
                last_lsn: 42,
                last_committed_at: "2026-08-02T00:00:00Z".to_owned(),
                rows_appended: 3,
                parse_errors: 1,
                state: state::STREAMING.to_owned(),
                error: None,
            }],
        };
        let json = serde_json::to_string(&status).expect("ser");
        // snake_case field names are pinned for the control-plane contract.
        assert!(json.contains("\"confirmed_lsn\":42"));
        assert!(json.contains("\"last_committed_at\""));
        assert!(json.contains("\"rows_appended\":3"));
        let back: CdcJobStatus = serde_json::from_str(&json).expect("de");
        assert_eq!(back, status);
    }

    #[test]
    fn table_status_error_is_optional() {
        let ok = CdcTableStatus::streaming("pg_analytics.public_t");
        let json = serde_json::to_string(&ok).expect("ser");
        assert!(json.contains("\"error\":null"));
        let back: CdcTableStatus = serde_json::from_str(&json).expect("de");
        assert_eq!(back, ok);
    }
}
