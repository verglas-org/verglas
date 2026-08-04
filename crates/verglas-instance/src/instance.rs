//! The cache instance: a first-class, independently-deployable ring member.
//!
//! A [`CacheInstance`] is the coordination surface of one verglas node in a
//! ring — its identity, its ring membership (placement + read-from-peer), and
//! its transaction seam (where a committed write is recorded through the shared
//! transaction layer). It is what the fleet's host-agent launches three of per
//! tenant, and what the local daemon builds one of; the same type, chosen
//! defaults apart.
//!
//! It deliberately does **not** own the cache engine or the S3 surface: those
//! are wired around it (the engine's storage factory points at the cache; the
//! S3 frontend serves through the engine). Keeping the engine out is the locked
//! constraint — no hard cache→engine dependency backward — and it keeps this
//! composition a small, testable coordination object rather than a god-struct.

use std::sync::Arc;

use verglas_core::node::NodeId;

use crate::commit_log::{CommitAck, CommitLog, CommitLogError, CommitRecord, LocalCommitLog};
use crate::ring::{LocalRing, RingMembership};

/// A verglas cache instance: one ring member's identity, ring membership, and
/// transaction seam.
///
/// Construct it with [`single_node`](CacheInstance::single_node) for the
/// self-hosted, run-for-free default (in-process ring, no-quorum commit log),
/// or with [`new`](CacheInstance::new) to supply a clustered ring and a
/// quorum-backed commit log from the fleet side.
#[derive(Clone)]
pub struct CacheInstance {
    /// Ring membership: placement and read-from-peer.
    ring: Arc<dyn RingMembership>,
    /// The transaction seam: where committed writes are recorded.
    commit_log: Arc<dyn CommitLog>,
}

impl CacheInstance {
    /// Assembles an instance from a ring membership and a commit log. This is
    /// the seam the fleet uses: pass a clustered [`RingMembership`] and a
    /// PG-backed [`CommitLog`] and the instance is a quorum ring member; pass
    /// the local defaults and it is a single node.
    pub fn new(ring: Arc<dyn RingMembership>, commit_log: Arc<dyn CommitLog>) -> Self {
        Self { ring, commit_log }
    }

    /// The self-hosted default: a single in-process ring member with a
    /// no-quorum commit log. Everything a single node needs to run a full
    /// lakehouse, with no cluster, no quorum, and no external transaction layer.
    pub fn single_node(node_id: NodeId) -> Self {
        Self::new(
            Arc::new(LocalRing::single(node_id)),
            Arc::new(LocalCommitLog::new()),
        )
    }

    /// This instance's identity in the ring.
    pub fn node_id(&self) -> NodeId {
        self.ring.node_id()
    }

    /// The ring membership: placement and read-from-peer.
    pub fn ring(&self) -> &Arc<dyn RingMembership> {
        &self.ring
    }

    /// The transaction seam.
    pub fn commit_log(&self) -> &Arc<dyn CommitLog> {
        &self.commit_log
    }

    /// Whether this instance records commits through a real quorum (fleet) or
    /// the local no-quorum default (single node). Reported at daemon startup so
    /// the operator sees which mode is live.
    pub fn is_quorum(&self) -> bool {
        self.commit_log.is_quorum()
    }

    /// Records a committed write through the transaction seam. Called when a
    /// write this instance owns becomes durable at the origin; the fleet's
    /// quorum log blocks until the record is agreed across the ring, the local
    /// default returns immediately with a monotonic sequence.
    pub async fn record_commit(&self, record: CommitRecord) -> Result<CommitAck, CommitLogError> {
        self.commit_log.record(record).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit_log::QuorumKind;
    use verglas_core::CacheKey;

    #[tokio::test]
    async fn single_node_instance_owns_its_keys_and_records_locally() {
        let instance = CacheInstance::single_node(NodeId::new("single"));
        let key = CacheKey {
            bucket: "b".into(),
            key: "warehouse/t/data/0.parquet".into(),
        };
        assert_eq!(instance.node_id(), NodeId::new("single"));
        assert_eq!(instance.ring().owner(&key), NodeId::new("single"));
        assert!(!instance.is_quorum());

        let ack = instance
            .record_commit(CommitRecord {
                tenant: "single".into(),
                table: "t".into(),
                object_key: "warehouse/t/data/0.parquet".into(),
                etag: "abc".into(),
                bytes: 10,
                node_id: instance.node_id(),
                ring_epoch: instance.ring().epoch(),
            })
            .await
            .expect("single-node record never errors");
        assert_eq!(ack.sequence, 1);
        assert_eq!(ack.quorum, QuorumKind::Local);
    }
}
