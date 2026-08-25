//! Turso-backed Durable Object storage primitives.
//!
//! This crate owns the one serialized Turso database, Worker reserved tables,
//! event transactions, JSON-row SQL conversion, and transactional Stream outbox.

mod error;
mod outbox;
mod rows;
mod schema;
mod store;

pub use error::{Error, Result};
pub use outbox::{OutboxKey, OutboxRecord, StreamAppender, StreamAppenderHandle};
pub use schema::{
    EVENT_SEQUENCE_TABLE, OUTBOX_TABLE, WORKER_ALARM_TABLE, WORKER_ATTACHMENTS_TABLE,
    WORKER_KV_TABLE,
};
pub use store::{TursoEvent, TursoStore};
