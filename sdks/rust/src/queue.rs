//! Fenced delivery types for declared PostgreSQL-backed queue deployments.
//! Queue handles speak to the access edge, which resolves and wakes the queue container.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Result of atomically appending one message batch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueEnqueueResult {
    /// Stable ordered positions assigned by PostgreSQL.
    pub positions: Vec<i64>,
}

/// Opaque proof that a consumer owns one delivery generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueReceipt {
    /// Stable queue position.
    pub position: i64,
    /// Consumer process that owns the lease.
    pub owner: String,
    /// Monotonic generation fencing prior deliveries.
    pub generation: u64,
}

/// One exclusively leased queue message.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueDelivery {
    /// Stable queue position.
    pub position: i64,
    /// Caller-supplied JSON message.
    pub payload: Value,
    /// Receipt required for acknowledgement.
    pub receipt: QueueReceipt,
    /// RFC 3339 deadline after which redelivery is permitted.
    pub expires_at: String,
}

/// Bounded exclusive deliveries returned by one poll.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueuePollResult {
    /// Messages claimed under distinct fenced receipts.
    pub deliveries: Vec<QueueDelivery>,
}
