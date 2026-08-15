//! The error type for client-side Iceberg ingest (#145).
//!
//! One enum spans identifier parsing, source reading, and the catalog/write
//! path so a CLI caller can render one clear line. `SchemaMismatch` always
//! names the offending column — the append is rejected before any data file
//! is written, and the table is left unchanged.

use thiserror::Error;

/// A failure creating a table, appending to one, or reading a source file for
/// client-side Iceberg ingest.
#[derive(Debug, Error)]
pub enum IngestError {
    /// The `namespace.name` table identifier was not a single dot-separated
    /// namespace and table name.
    #[error("`{0}` is not a valid `namespace.name` table identifier")]
    BadIdent(String),

    /// The source file's extension named no supported format.
    #[error("cannot infer a format for `{0}`: expected a .csv, .parquet, or .jsonl file")]
    UnknownFormat(String),

    /// A source file could not be read or its schema inferred.
    #[error("failed to read `{path}`: {detail}")]
    Read {
        /// The offending source path.
        path: String,
        /// What went wrong, in plain terms.
        detail: String,
    },

    /// The data being appended does not match the table schema. Names the
    /// mismatched column so the caller can fix the source.
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

    /// A partition column named by the caller is not in the source schema.
    #[error("partition column `{0}` is not in the schema")]
    UnknownPartitionColumn(String),

    /// An Iceberg catalog, FileIO, or table operation failed.
    #[error("iceberg error: {0}")]
    Iceberg(#[from] iceberg::Error),

    /// The catalog connection was missing a field the operation needs (e.g. no
    /// `[catalog]` configured).
    #[error("incomplete catalog configuration: {0}")]
    IncompleteConfig(String),
}

/// This crate's ingest result alias.
pub type Result<T> = std::result::Result<T, IngestError>;
