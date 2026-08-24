//! Error values returned by the Durable Object engine.

/// Result type used throughout the Durable Object engine.
pub type Result<T> = std::result::Result<T, Error>;

/// A transaction, storage, or query failure.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Arrow rejected a schema, batch, or deterministic IPC encoding.
    #[error("Arrow operation failed: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
    /// DataFusion failed while producing a storage stream or execution plan.
    #[error("DataFusion operation failed: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),
    /// SQLite failed while updating or recovering one replica pager.
    #[error("SQLite replica operation failed: {0}")]
    Sqlite(#[from] rusqlite::Error),
    /// Vamana rejected vector schema, dimensions, or mutation state.
    #[error("vector projection failed: {0}")]
    Vector(#[from] verglas_vector::VectorError),
    /// A graph mutation does not match the configured edge schema.
    #[error("graph projection failed: {0}")]
    GraphProjection(String),
    /// Derived Parquet, Iceberg, or Puffin publication failed verification.
    #[error("materialization failed: {0}")]
    Materialization(String),
    /// The requested table does not exist in this Durable Object.
    #[error("unknown Durable Object table: {0}")]
    UnknownTable(String),
    /// The transaction belongs to a different Durable Object.
    #[error("transaction belongs to DO {actual}, expected {expected}")]
    WrongDo {
        /// Durable Object expected by this engine.
        expected: String,
        /// Durable Object carried by the transaction.
        actual: String,
    },
    /// The commit authority refused or failed the canonical transaction.
    #[error("commit authority failed: {0}")]
    Authority(String),
    /// The managed transaction archive failed or returned an invalid receipt.
    #[error("transaction archive failed: {0}")]
    Archive(String),
    /// A commit receipt violated the monotonic state-machine contract.
    #[error("invalid commit receipt: {0}")]
    InvalidReceipt(String),
    /// Canonical transaction bytes were truncated, corrupt, or semantically invalid.
    #[error("invalid canonical transaction envelope: {0}")]
    InvalidEnvelope(String),
    /// An object kind was configured with an incompatible durability capability.
    #[error("invalid object policy: {0}")]
    InvalidObjectPolicy(String),
    /// A committed transaction conflicts with persisted replica identity or bytes.
    #[error("replica transaction conflict: {0}")]
    ReplicaConflict(String),
    /// A replica watermark or apply sequence violated contiguous ordering.
    #[error("replica sequence violation: {0}")]
    ReplicaSequence(String),
    /// The requested snapshot is newer than the applied commit sequence.
    #[error("snapshot {requested} is ahead of applied sequence {applied}")]
    SnapshotAhead {
        /// Requested commit sequence.
        requested: u64,
        /// Highest locally applied commit sequence.
        applied: u64,
    },
    /// This initial storage implementation cannot evaluate the supplied predicate.
    #[error("predicate pushdown is not implemented: {0}")]
    UnsupportedPredicate(String),
    /// A projection named a column outside the registered table schema.
    #[error("projection column {index} is outside schema width {width}")]
    InvalidProjection {
        /// Invalid zero-based column index.
        index: usize,
        /// Number of columns in the table schema.
        width: usize,
    },
    /// A synchronization primitive was poisoned by a panicking owner.
    #[error("Durable Object state lock was poisoned")]
    Poisoned,
}
