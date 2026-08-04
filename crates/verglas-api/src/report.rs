//! Serializable result shapes shared by daemon, SDK, engine, and CLI.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// One column of a table schema, as reported by create, append, and show.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FieldInfo {
    /// Iceberg field id.
    pub id: i32,
    /// Column name.
    pub name: String,
    /// Iceberg type label.
    pub r#type: String,
    /// Whether the column is required.
    pub required: bool,
}

/// Result of `verglas table create`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateReport {
    /// Created dotted table name.
    pub table: String,
    /// Always `create`.
    pub operation: String,
    /// Table schema.
    pub schema: Vec<FieldInfo>,
    /// Partition columns in order.
    pub partition_by: Vec<String>,
    /// Rows written by the initial append.
    pub records_added: u64,
    /// Parquet data files written.
    pub data_files_added: u64,
    /// Snapshot produced by the initial append.
    pub snapshot_id: Option<i64>,
}

/// Result of `verglas table append`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendReport {
    /// Appended dotted table name.
    pub table: String,
    /// Always `append`.
    pub operation: String,
    /// Rows written.
    pub records_added: u64,
    /// Parquet data files written.
    pub data_files_added: u64,
    /// Snapshot produced by the append.
    pub snapshot_id: Option<i64>,
}

/// A table identifier in list output.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableRef {
    /// Dotted namespace.
    pub namespace: String,
    /// Table name.
    pub name: String,
}

/// Result of `verglas table list`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListReport {
    /// Matching tables in stable order.
    pub tables: Vec<TableRef>,
}

/// Result of `verglas table show`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShowReport {
    /// Dotted table name.
    pub table: String,
    /// Current schema.
    pub schema: Vec<FieldInfo>,
    /// Partition columns in order.
    pub partition_by: Vec<String>,
    /// Total live rows when available.
    pub row_count: Option<u64>,
    /// Total live data files when available.
    pub file_count: Option<u64>,
    /// Total live data bytes when available.
    pub size_bytes: Option<u64>,
    /// Current snapshot id.
    pub current_snapshot_id: Option<i64>,
}

/// One snapshot in history output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotInfo {
    /// Snapshot id.
    pub snapshot_id: i64,
    /// Parent snapshot id.
    pub parent_snapshot_id: Option<i64>,
    /// Commit time in epoch milliseconds.
    pub timestamp_ms: i64,
    /// Commit time in RFC 3339.
    pub timestamp: String,
    /// Snapshot operation.
    pub operation: String,
    /// Snapshot summary properties.
    pub summary: BTreeMap<String, String>,
}

/// Result of `verglas table history`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryReport {
    /// Dotted table name.
    pub table: String,
    /// Snapshots oldest first.
    pub snapshots: Vec<SnapshotInfo>,
}

/// Result of a JSON SQL query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryReport {
    /// Result columns in order.
    pub columns: Vec<String>,
    /// Result rows keyed by column name.
    pub rows: Vec<serde_json::Map<String, serde_json::Value>>,
    /// Number of returned rows.
    pub row_count: usize,
}

/// A plan-based memory estimate for one query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryMemoryEstimate {
    /// Estimated bytes read from cache or backing storage.
    pub scan_bytes: u64,
    /// Estimated operator working-set bytes beyond the scan.
    pub working_set_bytes: u64,
    /// Fixed query-engine runtime allowance.
    pub base_overhead_bytes: u64,
    /// Total initial memory grant requested for the query.
    pub estimated_total_bytes: u64,
    /// Whether the estimate contains heuristic terms.
    pub approximate: bool,
    /// Human-readable limitations and unresolved-statistics diagnostics.
    pub notes: Vec<String>,
}

/// The result of compacting one table during a compaction pass.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactReport {
    /// Dotted table name.
    pub table: String,
    /// Bin-pack groups durably committed.
    pub groups_committed: u64,
    /// Existing data files rewritten.
    pub input_data_files: u64,
    /// Replacement data files produced.
    pub output_data_files: u64,
    /// Records read from rewritten files.
    pub input_records: u64,
    /// Records written to replacement files.
    pub output_records: u64,
    /// Old paths removed from the live table state.
    pub removed_files: Vec<String>,
    /// New paths added to the live table state.
    pub added_files: Vec<String>,
    /// Whether any rewrite applied the table sort order.
    pub sorted: bool,
    /// Mergeable undersized files remaining after the pass.
    pub undersized_remaining: u64,
    /// Whether the pass stopped on its wall-clock budget.
    pub budget_bounded: bool,
    /// Snapshot produced by the last committed group.
    pub snapshot_id: Option<i64>,
    /// Old snapshots expired after compaction.
    pub snapshots_expired: u64,
    /// Unreachable data files deleted after snapshot expiry.
    pub orphan_files_deleted: u64,
}

impl CompactReport {
    /// Returns whether this report committed at least one rewrite group.
    pub fn did_compact(&self) -> bool {
        self.groups_committed > 0
    }

    /// Returns whether the pass rewrote files or performed metadata cleanup.
    pub fn did_anything(&self) -> bool {
        self.did_compact() || self.snapshots_expired > 0 || self.orphan_files_deleted > 0
    }
}

/// The result of one compaction pass across the catalog.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionReport {
    /// Tables listed before the pass ended.
    pub tables_scanned: u64,
    /// Reports for tables changed by the pass.
    pub compacted: Vec<CompactReport>,
    /// Per-table failures as dotted name and diagnostic.
    pub failures: Vec<(String, String)>,
    /// Total bin-pack groups committed.
    pub groups_committed: u64,
    /// Total mergeable undersized files remaining.
    pub undersized_remaining: u64,
    /// Whether the pass stopped on its wall-clock budget.
    pub budget_bounded: bool,
    /// Total snapshots expired.
    pub snapshots_expired: u64,
    /// Total unreachable files deleted.
    pub orphan_files_deleted: u64,
}

impl CompactionReport {
    /// Returns the total number of existing data files rewritten.
    pub fn files_removed(&self) -> u64 {
        self.compacted
            .iter()
            .map(|report| report.input_data_files)
            .sum()
    }
}
