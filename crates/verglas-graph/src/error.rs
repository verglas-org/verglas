//! The one error type for the graph engine.
//!
//! Wraps the table-engine error (`verglas_iceberg::AgentError`) and the raw
//! Iceberg error, and adds the graph-specific failures: a malformed row read
//! back from a graph table, and a corrupt adjacency blob. One enum so a caller
//! renders a single clear line.

use thiserror::Error;

/// A failure in a graph-engine operation.
#[derive(Debug, Error)]
pub enum GraphError {
    /// A table or query operation in the underlying engine failed.
    #[error(transparent)]
    Engine(#[from] verglas_iceberg::AgentError),

    /// A raw Iceberg catalog/table/IO operation failed.
    #[error("iceberg error: {0}")]
    Iceberg(#[from] iceberg::Error),

    /// A row read back from a graph table was missing a required column or had
    /// the wrong shape. Names the column and the reason.
    #[error("graph row in `{table}` is malformed: {detail}")]
    MalformedRow {
        /// The dotted table the row came from.
        table: String,
        /// What was wrong with the row.
        detail: String,
    },

    /// The serialized adjacency index could not be decoded (truncated blob,
    /// bad magic, or an out-of-range reference). The index is treated as absent
    /// and the caller falls back to a table scan.
    #[error("adjacency index blob is corrupt: {0}")]
    CorruptIndex(String),
}

/// The graph engine's result alias.
pub type Result<T> = std::result::Result<T, GraphError>;
