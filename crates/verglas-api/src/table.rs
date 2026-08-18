//! Table data-plane request and response contracts.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// One column in a table contract.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ColumnSpec {
    /// Column name.
    pub name: String,
    /// Arrow type name accepted by the Verglas table API.
    #[serde(rename = "type")]
    pub type_name: String,
    /// Whether the column accepts nulls.
    #[serde(default = "default_true")]
    pub nullable: bool,
}

impl ColumnSpec {
    /// Creates a required column.
    pub fn required(name: impl Into<String>, type_name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            type_name: type_name.into(),
            nullable: false,
        }
    }

    /// Creates a nullable column.
    pub fn nullable(name: impl Into<String>, type_name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            type_name: type_name.into(),
            nullable: true,
        }
    }
}

/// One field in a table partition specification.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionSpec {
    /// Source column transformed into a partition value.
    pub source: String,
    /// Iceberg transform name.
    pub transform: String,
}

impl PartitionSpec {
    /// Creates an identity partition field.
    pub fn identity(source: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            transform: "identity".to_owned(),
        }
    }

    /// Creates a month partition field.
    pub fn month(source: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            transform: "month".to_owned(),
        }
    }
}

/// Exact schema and partition contract required by a caller.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableDefinition {
    /// Columns in table order.
    pub schema: Vec<ColumnSpec>,
    /// Partition fields in partition-spec order.
    #[serde(default)]
    pub partitions: Vec<PartitionSpec>,
}

/// The result of creating a table.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateTableResponse {
    /// Dotted table name.
    pub table: String,
    /// Column names in schema order.
    pub columns: Vec<String>,
}

/// Result of idempotently ensuring an exact table contract.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnsureTableResponse {
    /// Whether this request created the table.
    pub created: bool,
    /// Exact contract now stored in the catalog.
    pub definition: TableDefinition,
}

/// JSON-row commit request.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommitRequest {
    /// Rows keyed by column name.
    pub rows: Vec<Value>,
    /// Optional replay-safe commit identity.
    #[serde(default)]
    pub idempotency_key: Option<String>,
}

/// Result of appending rows to a table.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommitResponse {
    /// Snapshot produced or replayed.
    pub snapshot_id: String,
    /// Rows committed by the original request.
    pub rows_committed: u64,
    /// Opaque cursor at the committed snapshot.
    pub watermark: String,
    /// Whether the request replayed an existing idempotency key.
    pub idempotent: bool,
}

/// Result of an async `mode=append` ingest ack: rows passed schema validation
/// and are durably journaled, but not yet committed to the Iceberg table. A
/// background task performs the real commit; this response carries no
/// snapshot id because none exists yet — poll `/snapshot` or `/delta`, or
/// pass `wait=true`/`commit=sync` on the request to get the committed
/// snapshot id synchronously instead.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IngestAckResponse {
    /// Rows accepted after schema validation and journaled durably. Rows that
    /// fail schema validation are rejected synchronously and are not counted
    /// here — the whole request fails instead, so this is always the full
    /// request's row count on success.
    pub successful_rows: u64,
    /// Always `false`: the commit has not happened yet when this is returned.
    pub committed: bool,
}

/// Current table snapshot summary.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotResponse {
    /// Current snapshot or an empty string before the first commit.
    pub snapshot_id: String,
    /// Opaque cursor at the current snapshot.
    pub watermark: String,
    /// Live records in the current snapshot.
    pub record_count: u64,
}

/// One page of current table rows.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RowsResponse {
    /// Rows keyed by column name.
    pub rows: Vec<Value>,
    /// Cursor for the next page, absent at the end.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// Rows committed after an earlier watermark.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeltaResponse {
    /// Rows added after the requested watermark.
    pub rows: Vec<Value>,
    /// Current tip watermark.
    pub watermark: String,
}

/// Defaults omitted nullability to nullable for TypeScript compatibility.
fn default_true() -> bool {
    true
}
