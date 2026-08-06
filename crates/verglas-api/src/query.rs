//! Typed request contract for SQL execution through the query role.

use serde::{Deserialize, Serialize};

/// A positional SQL parameter carried separately from the statement text.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum QueryParameter {
    /// SQL `NULL`; the surrounding expression supplies its inferred type.
    Null,
    /// Boolean value.
    Boolean(bool),
    /// Signed 64-bit integer value.
    Int64(i64),
    /// Unsigned 64-bit integer value.
    Uint64(u64),
    /// IEEE 754 double-precision value.
    Float64(f64),
    /// UTF-8 string value.
    String(String),
    /// RFC 3339 timestamp value.
    Timestamp(String),
    /// ISO 8601 calendar date (`YYYY-MM-DD`).
    Date(String),
}

/// Optional time-travel pin for one table in a query.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct QueryAt {
    /// Snapshot id, epoch-millisecond timestamp, or RFC 3339 timestamp.
    pub reference: String,
    /// Dotted table identifier to pin.
    pub table: String,
}

/// Request body accepted by `POST /v1/query`.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct QueryRequest {
    /// SQL statement to execute.
    pub sql: String,
    /// Positional values for `?` placeholders, in statement order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<QueryParameter>,
    /// Optional snapshot or timestamp pin.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at: Option<QueryAt>,
}
