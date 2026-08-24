//! Transactional Stream outbox identities and binding seam.
//!
//! The outbox is a publication buffer only. Stream ingestion and deduplication
//! remain owned by the injected Stream binding rather than this crate.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

use crate::error::Result;

/// Deterministic identity for one selected event record.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct OutboxKey {
    /// Durable Object identity that selected the record.
    pub source_do_id: String,
    /// Event sequence that selected the record.
    pub event_sequence: u64,
    /// Zero-based record position within the event.
    pub record_index: u32,
}

impl OutboxKey {
    /// Creates one deterministic outbox key from event identity and position.
    pub fn new(source_do_id: impl Into<String>, event_sequence: u64, record_index: u32) -> Self {
        Self {
            source_do_id: source_do_id.into(),
            event_sequence,
            record_index,
        }
    }

    /// Returns the stable Stream deduplication identity for this record.
    pub fn event_id(&self) -> String {
        format!(
            "{}:{}:{}",
            self.source_do_id, self.event_sequence, self.record_index
        )
    }
}

/// One JSON record held in the transactional outbox.
#[derive(Clone, Debug, PartialEq)]
pub struct OutboxRecord {
    /// Deterministic record identity.
    pub key: OutboxKey,
    /// JSON payload handed to the Stream binding.
    pub payload: Value,
}

impl OutboxRecord {
    /// Creates an outbox record without assigning a second storage identity.
    pub fn new(key: OutboxKey, payload: Value) -> Self {
        Self { key, payload }
    }

    /// Returns the deterministic identity sent to Stream for deduplication.
    pub fn event_id(&self) -> String {
        self.key.event_id()
    }
}

/// Narrow cross-product binding used to deliver committed outbox records.
#[async_trait]
pub trait StreamAppender: Send + Sync {
    /// Appends records and returns only after Stream durable acknowledgement.
    async fn append(&self, records: Vec<OutboxRecord>) -> Result<()>;
}

/// Shared Stream appender handle injected by the runtime binding layer.
pub type StreamAppenderHandle = Arc<dyn StreamAppender>;
