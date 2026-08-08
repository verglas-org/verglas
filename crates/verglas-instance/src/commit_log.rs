//! The transaction seam: where a cache instance records a committed write
//! through the shared transaction layer.
//!
//! A cache instance (a ring member) is the local S3 surface a pipeline writes
//! through. When a write it owns becomes durable at the origin, the instance
//! appends a [`CommitRecord`] to a [`CommitLog`]. In a multi-node ring, cache
//! instances can funnel these records through a PostgreSQL quorum so the
//! commit order is agreed across the ring before any reader observes it; on a
//! single self-hosted node there is no quorum to reach, so the default
//! [`LocalCommitLog`] assigns a monotonic local sequence and never blocks.
//!
//! This module owns only the **trait and the local default**. A quorum
//! implementation (for example PG-WAL) can plug in behind the same trait —
//! see [`CommitLog`] for the exact contract. Keeping the seam here, independent
//! of the write-back tier, lets the single-node lakehouse run without a quorum
//! and lets a multi-node deployment add one without a source change to the
//! cache or the server.

use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use verglas_core::node::NodeId;

/// A committed-write record a cache instance appends to the shared transaction
/// layer when a write it owns becomes durable at the origin.
///
/// The record names *what* became durable (the origin object and its content
/// identity) and *who/where* recorded it (the ring member and the ring epoch it
/// placed the write under), so the transaction layer can order commits across
/// the ring and detect a record raced by a membership change. It carries no
/// object bytes — only the commit metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitRecord {
    /// The tenant (pod) scope the commit belongs to. On a single node this is
    /// the local deployment id; in a multi-node ring it is the tenant whose cache
    /// instances share one transaction layer.
    pub tenant: String,
    /// The logical table (or write-back prefix) the committed object belongs
    /// to. The transaction layer orders commits per table.
    pub table: String,
    /// The origin object key made durable by this write.
    pub object_key: String,
    /// The object's content identity (ETag) at the moment it became durable.
    /// Distinguishes a re-commit of the same key from a new version.
    pub etag: String,
    /// The object's size in bytes, for the transaction layer's accounting.
    pub bytes: u64,
    /// The ring member that owned the write and is recording the commit.
    pub node_id: NodeId,
    /// The ring epoch the write was placed under. A quorum implementation
    /// rejects (or re-homes) a record whose epoch is stale relative to the
    /// agreed membership, so a commit never lands against a torn ring.
    pub ring_epoch: u64,
}

/// How durability of a recorded commit was established.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuorumKind {
    /// No quorum was reached because none is required — a single-node
    /// deployment (the [`LocalCommitLog`] default). Read-your-writes is
    /// local-only and immediate.
    Local,
    /// The record was committed to a quorum of the shared transaction layer
    /// (e.g. a PostgreSQL quorum across the tenant's three cache instances).
    Quorum,
}

/// The acknowledgement the transaction layer returns once a record is durable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitAck {
    /// The commit's assigned sequence in the transaction layer. Monotonic and
    /// gap-free per tenant; a reader waits behind a sequence for
    /// read-your-writes across the ring.
    pub sequence: u64,
    /// How durability was established for this record.
    pub quorum: QuorumKind,
}

/// A failure recording a commit through the transaction layer.
#[derive(Debug, thiserror::Error)]
pub enum CommitLogError {
    /// The transaction layer could not be reached (transport-level). The caller
    /// retries or falls back to write-through per its own policy; the local
    /// default never returns this.
    #[error("commit-log transport unavailable: {0}")]
    Unavailable(String),
    /// The transaction layer refused the record — a stale ring epoch, a tenant
    /// mismatch, or a quorum that could not be formed. Distinct from
    /// [`Self::Unavailable`]: retrying the same record unchanged will not help.
    #[error("commit rejected by the transaction layer: {0}")]
    Rejected(String),
}

/// The point where a cache instance records a committed write through the
/// shared transaction layer.
///
/// # Contract for an implementation (the seam the PG-WAL work plugs into)
///
/// - [`record`](CommitLog::record) must be durable-on-return per the
///   implementation's quorum policy: when it returns `Ok`, the commit is
///   agreed and will survive the loss of a minority of the quorum. It assigns
///   the returned [`CommitAck::sequence`] — monotonic and gap-free per tenant.
/// - [`durable_watermark`](CommitLog::durable_watermark) returns the highest
///   sequence this instance can prove durable across the quorum; a reader waits
///   behind it for read-your-writes. It must be monotonic and never exceed the
///   highest `record` sequence the quorum has agreed.
/// - [`is_quorum`](CommitLog::is_quorum) reports whether this log reaches a
///   real quorum. The local default returns `false`; a PG-backed log returns
///   `true`. The server logs it at startup so the operator sees which mode is
///   live.
///
/// The trait is object-safe (`Arc<dyn CommitLog>`) so the server holds one and
/// the concrete log — local or quorum — is chosen once at construction.
#[async_trait]
pub trait CommitLog: Send + Sync + 'static {
    /// Records a committed write. Returns once the record is durable per the
    /// implementation's quorum policy, with the assigned sequence.
    async fn record(&self, record: CommitRecord) -> Result<CommitAck, CommitLogError>;

    /// The highest commit sequence this instance can prove durable — the read
    /// barrier a reader waits behind for read-your-writes across the ring.
    async fn durable_watermark(&self) -> Result<u64, CommitLogError>;

    /// Whether this log reaches a real quorum. Defaults to `false` (the
    /// single-node, no-quorum case); a quorum implementation overrides it.
    fn is_quorum(&self) -> bool {
        false
    }
}

/// The single-node, no-quorum default: assigns a monotonic local sequence and
/// never blocks on peers.
///
/// This is what lets a self-hosted single node run a full lakehouse for free —
/// read-your-writes is local and immediate because there is no other reader in
/// the ring to agree with. A multi-node deployment swaps this for a PG-backed quorum log
/// behind the same [`CommitLog`] trait; nothing else changes.
#[derive(Debug, Default)]
pub struct LocalCommitLog {
    /// The last assigned sequence. Starts at 0; the first commit is sequence 1.
    sequence: AtomicU64,
}

impl LocalCommitLog {
    /// Builds a fresh local commit log at sequence 0.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl CommitLog for LocalCommitLog {
    /// Assigns the next local sequence and returns immediately. The record's
    /// fields are accepted as-is: a single node cannot disagree with itself
    /// about ring epoch or ownership, so there is nothing to reject.
    async fn record(&self, _record: CommitRecord) -> Result<CommitAck, CommitLogError> {
        let sequence = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(CommitAck {
            sequence,
            quorum: QuorumKind::Local,
        })
    }

    /// The last locally-assigned sequence; on one node it is immediately
    /// durable, so the watermark is simply the newest commit.
    async fn durable_watermark(&self) -> Result<u64, CommitLogError> {
        Ok(self.sequence.load(Ordering::SeqCst))
    }

    /// The local default reaches no quorum.
    fn is_quorum(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record() -> CommitRecord {
        CommitRecord {
            tenant: "single".into(),
            table: "t".into(),
            object_key: "warehouse/t/data/0.parquet".into(),
            etag: "abc".into(),
            bytes: 1024,
            node_id: NodeId::new("single"),
            ring_epoch: 0,
        }
    }

    #[tokio::test]
    async fn local_log_assigns_monotonic_gap_free_sequences() {
        let log = LocalCommitLog::new();
        let a = log
            .record(record())
            .await
            .expect("local record never errors");
        let b = log
            .record(record())
            .await
            .expect("local record never errors");
        assert_eq!(a.sequence, 1);
        assert_eq!(b.sequence, 2);
        assert_eq!(a.quorum, QuorumKind::Local);
        assert!(!log.is_quorum());
    }

    #[tokio::test]
    async fn local_watermark_tracks_the_newest_commit() {
        let log = LocalCommitLog::new();
        assert_eq!(
            log.durable_watermark()
                .await
                .expect("local watermark never errors"),
            0
        );
        log.record(record())
            .await
            .expect("local record never errors");
        log.record(record())
            .await
            .expect("local record never errors");
        assert_eq!(
            log.durable_watermark()
                .await
                .expect("local watermark never errors"),
            2
        );
    }
}
