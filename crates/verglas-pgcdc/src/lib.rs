//! Turn PostgreSQL logical replication (pgoutput) into Iceberg change-log
//! tables — the CDC runner behind Verglas Cloud's zero-ETL feature.
//!
//! The pipeline is a small set of composable layers:
//!
//! - [`pgoutput`]: a from-scratch decoder for the pgoutput logical-replication
//!   protocol v1 — the primary tested surface.
//! - [`pgtype`]: PostgreSQL type oid (+ typmod) to [`arrow_schema::DataType`].
//! - [`schema`]: the reserved-prefixed change-row schema and the schema diff
//!   that drives evolution decisions.
//! - [`rows`]: decoded pgoutput tuples to an Arrow [`arrow_array::RecordBatch`],
//!   with total, non-panicking text parsing.
//! - [`iceberg_sink`]: a thin wrapper over `verglas-iceberg` — catalog open,
//!   table create, change-row append, and add-column evolution.
//! - [`runner`]: the drain-tick control flow, with Postgres and Iceberg behind
//!   small traits so it is unit-testable with fakes.
//! - [`status`]: the serde status contract the control plane surfaces.
//!
//! Live Postgres and Iceberg IO sit behind [`runner::PgSource`] and
//! [`runner::Sink`]; the drain-tick logic (resync-on-missing-slot,
//! advance-after-append, parse-error accounting) is exercised with in-memory
//! fakes and needs no live infrastructure to test.

pub mod iceberg_sink;
pub mod pgoutput;
pub mod pgtype;
pub mod rows;
pub mod runner;
pub mod schema;
pub mod status;

use thiserror::Error;

/// The one error type the CDC runner surfaces.
#[derive(Debug, Error)]
pub enum CdcError {
    /// A pgoutput message failed to decode.
    #[error("pgoutput decode: {0}")]
    Decode(#[from] pgoutput::DecodeError),
    /// An Arrow batch could not be assembled (a structural schema mismatch).
    #[error("arrow: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
    /// An Iceberg spec/catalog operation failed.
    #[error("iceberg: {0}")]
    Iceberg(#[from] iceberg::Error),
    /// The Iceberg engine (create/append) failed.
    #[error("iceberg engine: {0}")]
    Engine(#[from] verglas_iceberg::AgentError),
    /// A Postgres query failed.
    #[error("postgres: {0}")]
    Postgres(#[from] sqlx::Error),
    /// A relation carried an incompatible schema change (a column's type
    /// changed); the table is marked `schema_pending` and left unappended.
    #[error("schema change on {table}.{column} is incompatible; table left in schema_pending")]
    SchemaPending {
        /// The table the incompatible change was seen on.
        table: String,
        /// The column whose mapped type changed.
        column: String,
    },
    /// A control-flow or configuration error.
    #[error("{0}")]
    Message(String),
}

/// The crate result alias.
pub type Result<T> = std::result::Result<T, CdcError>;
