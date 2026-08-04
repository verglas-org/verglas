//! Errors for the vector-index engine.

use thiserror::Error;

/// The result type for the vector engine.
pub type Result<T> = std::result::Result<T, VectorError>;

/// Everything that can go wrong building, serializing, or querying a Vamana
/// index, or attaching it to an Iceberg snapshot.
#[derive(Debug, Error)]
pub enum VectorError {
    /// A query or insert vector did not match the index dimensionality.
    #[error("dimension mismatch: index is {expected}-d, got {got}-d")]
    DimMismatch {
        /// The index's dimensionality.
        expected: usize,
        /// The offending vector's dimensionality.
        got: usize,
    },

    /// A vector was empty or the requested dimension was zero.
    #[error("invalid dimension: {0}")]
    InvalidDim(usize),

    /// A serialized blob was truncated, had a bad magic, or an unknown version.
    #[error("corrupt vamana blob: {0}")]
    CorruptBlob(String),

    /// The embedding column named for the index was absent or not a supported
    /// vector type. Not an anomalous state to paper over — the index cannot be
    /// built and the caller must fix the schema/field.
    #[error("embedding field error: {0}")]
    Field(String),

    /// The table has no identity column to key vectors by.
    #[error("no id column: {0}")]
    NoIdColumn(String),

    /// The requested table snapshot has no attached Vamana index.
    #[error("no Vamana index for {table}.{field} at snapshot {snapshot:?}")]
    IndexNotFound {
        /// The source table.
        table: String,
        /// The indexed field.
        field: String,
        /// The exact requested snapshot, or `None` when the table is empty.
        snapshot: Option<i64>,
    },

    /// The disposable decoded-index cache was poisoned.
    #[error("vector serving cache: {0}")]
    Cache(String),

    /// A Puffin container read/write failed.
    #[error("puffin: {0}")]
    Puffin(String),

    /// An Iceberg operation (scan, delta, load) failed.
    #[error("iceberg: {0}")]
    Iceberg(#[from] iceberg::Error),

    /// A `verglas-iceberg` tables-API operation (delta, rows, snapshot) failed.
    #[error("tables-api: {0}")]
    TablesApi(#[from] verglas_iceberg::AgentError),

    /// Arrow decoding of a vector column failed.
    #[error("arrow: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
}
