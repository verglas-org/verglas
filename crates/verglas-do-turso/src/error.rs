//! Error types for the Turso Durable Object store.
//!
//! Errors preserve the underlying Turso, filesystem, and JSON failures while
//! giving callers stable validation and publication categories.

use thiserror::Error;

/// Failure returned by a Turso Durable Object operation.
#[derive(Debug, Error)]
pub enum Error {
    /// Reports an embedded Turso database failure.
    #[error("turso database error: {0}")]
    Turso(#[from] turso::Error),
    /// Reports a local sidecar filesystem failure.
    #[error("local Turso sidecar I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// Reports JSON serialization or deserialization failure.
    #[error("JSON conversion failed: {0}")]
    Json(#[from] serde_json::Error),
    /// Reports an invalid reserved-table schema.
    #[error("reserved Turso schema is invalid: {0}")]
    InvalidSchema(String),
    /// Reports tenant SQL that attempts to access reserved or internal tables.
    #[error("SQL is not allowed for Durable Object tenant storage: {0}")]
    InvalidSql(String),
    /// Reports a checkpoint that could not acquire every WAL frame.
    #[error("embedded Turso WAL checkpoint remained busy ({0})")]
    CheckpointBusy(i64),
    /// Reports a checkpoint that returned no completion status.
    #[error("embedded Turso WAL checkpoint returned no status row")]
    CheckpointResultMissing,
    /// Reports a transaction that was already finished or moved.
    #[error("Turso event transaction is no longer active")]
    EventFinished,
    /// Reports an enabled outbox without an injected Stream binding.
    #[error("outbox publication requires an injected Stream appender")]
    OutboxUnavailable,
    /// Reports shutdown attempted while committed Stream work remains unpublished.
    #[error("shutdown fence found committed outbox work")]
    ShutdownOutboxPending,
    /// Reports an outbox lease owner mismatch.
    #[error("outbox lease owner does not match the current inflight row")]
    OutboxLeaseMismatch,
    /// Reports an unexpired inflight outbox row that applies backpressure.
    #[error("outbox publication is inflight and has not reached its lease expiry")]
    OutboxInFlight,
    /// Reports a failed internal Stream append acknowledgement.
    #[error("internal Stream append failed: {0}")]
    StreamAppend(String),
    /// Reports a value that cannot be represented honestly as JSON.
    #[error("SQL value cannot be represented as JSON: {0}")]
    JsonValue(String),
}

/// Result alias for Turso Durable Object operations.
pub type Result<T> = std::result::Result<T, Error>;
