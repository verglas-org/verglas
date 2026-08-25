//! Errors returned by the narrow Iceberg Sink and Catalog capabilities.

use thiserror::Error;

/// A failure that leaves the Iceberg catalog and Sink contract unchanged.
#[derive(Debug, Error)]
pub enum AgentError {
    /// A dotted table identifier was malformed.
    #[error("`{0}` is not a valid `namespace.name` table identifier")]
    BadIdent(String),

    /// An Iceberg catalog, table, or file operation failed.
    #[error("iceberg error: {0}")]
    Iceberg(#[from] iceberg::Error),

    /// A Sink request or table contract was invalid.
    #[error("invalid Sink request: {0}")]
    InvalidRequest(String),

    /// A row or schema could not be represented by the target table.
    #[error("schema mismatch for `{table}` column `{column}`: {detail}")]
    SchemaMismatch {
        /// The table receiving the rows.
        table: String,
        /// The offending column or row marker.
        column: String,
        /// The exact incompatibility.
        detail: String,
    },
}

/// Result alias for Sink and Catalog operations.
pub type Result<T> = std::result::Result<T, AgentError>;
