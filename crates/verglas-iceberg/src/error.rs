//! Error type shared across the agent crate.
//!
//! One error enum spans ingest, catalog, write, inspect, and query so the CLI
//! can render a single clear line and pick an exit code. Each variant names the
//! thing that failed in the agent's own vocabulary; the underlying library
//! error is carried for detail.

use thiserror::Error;

/// A failure in one of the agent-facing table or query operations.
#[derive(Debug, Error)]
pub enum AgentError {
    /// The `namespace.name` table identifier was not a single dot-separated
    /// namespace and table name.
    #[error("`{0}` is not a valid `namespace.name` table identifier")]
    BadIdent(String),

    /// The source file's extension named no supported format.
    #[error("cannot infer a format for `{0}`: expected a .csv, .parquet, or .jsonl file")]
    UnknownFormat(String),

    /// A source file could not be read or its schema inferred.
    #[error("failed to read `{path}`: {detail}")]
    Ingest {
        /// The offending source path.
        path: String,
        /// What went wrong, in plain terms.
        detail: String,
    },

    /// The data being appended does not match the table schema. Names the
    /// mismatched column so the operator can fix the source.
    #[error(
        "column `{column}` is not compatible with table `{table}`: {detail}. \
         The append was rejected; the table is unchanged."
    )]
    SchemaMismatch {
        /// The table being appended to.
        table: String,
        /// The column that does not line up.
        column: String,
        /// Why the column is incompatible.
        detail: String,
    },

    /// Zero-config resolution could not reach the local server and no explicit
    /// connection flags or environment were provided.
    #[error(
        "no connection configured and the local server at {endpoint} is unreachable: {detail}. \
         Pass --catalog-uri/--endpoint or start the Docker server."
    )]
    NoConnection {
        /// The admin endpoint that was probed.
        endpoint: String,
        /// The underlying reason it could not be reached.
        detail: String,
    },

    /// The resolved connection was missing a field the operation needs.
    #[error("incomplete connection: {0}")]
    IncompleteConnection(String),

    /// An Iceberg catalog or table operation failed.
    #[error("iceberg error: {0}")]
    Iceberg(#[from] iceberg::Error),

    /// A query could not be planned or executed.
    #[error("query failed: {0}")]
    Query(String),

    /// An SDK table-API request was malformed: an unparseable paging cursor, or
    /// a `since` watermark that names no snapshot of the table.
    #[error("{0}")]
    TableApi(String),

    /// A compaction rewrite could not be carried out: a data file could not be
    /// read or rewritten, the row count changed, or the REPLACE snapshot could
    /// not be built or committed.
    #[error("compaction failed: {0}")]
    Compaction(String),
}

/// The crate's result alias.
pub type Result<T> = std::result::Result<T, AgentError>;
