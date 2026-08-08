//! Queue wire types for the `/v1/queues/...` data-plane routes.
//!
//! A queue is a durable ordered log of JSON rows consumed by named consumer
//! groups. These structs are the transport contract the Rust SDK and TypeScript
//! SDK share with the server.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Result of appending rows onto a queue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueEnqueueResult {
    /// Rows appended by this call.
    pub enqueued: u64,
    /// The position one past the last appended record.
    pub end_position: u64,
}

/// One record returned by a queue poll.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueueRecord {
    /// Stable global position in the queue.
    pub position: u64,
    /// Row payload.
    pub row: Value,
}

/// A page polled for one consumer group.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueuePollResult {
    /// Records at or after the group's watermark, in order.
    pub records: Vec<QueueRecord>,
    /// The group's current watermark (acked-through position).
    pub watermark: u64,
}

/// Result of advancing a consumer group's watermark.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueAckResult {
    /// The group's watermark after the monotone ack.
    pub watermark: u64,
}
