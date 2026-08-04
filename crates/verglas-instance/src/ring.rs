//! The ring-membership interface: how one cache instance, as a ring member,
//! decides which shard (key) each instance owns and pulls a block from the peer
//! that holds it.
//!
//! Three cache instances per tenant form a ring. Placement is
//! rendezvous-hashed key ownership (the pod-wide wire commitment in
//! [`verglas_core::ring`]); read-from-peer is the transport that lets an owner
//! warm a missing block from a peer that still holds it instead of refetching
//! from the origin. This crate owns the **interface plus a working local
//! default** ([`LocalRing`]): a single in-process member that owns every key and
//! has no peers, so a self-hosted node runs the full read path for free. The
//! fleet's remote transport (gossip membership + peer RPC, and later RDMA)
//! implements the same [`RingMembership`] trait — transport-agnostic, exactly
//! like the catalog change-feed pattern — and plugs in without a call-site
//! change.
//!
//! The interface deliberately does **not** own the cache engine: cache-pathed
//! I/O stays a wiring property (where the engine's storage factory points), so
//! this crate never depends on the engine backward. It provides placement and
//! peer-read; the instance composition ([`crate::CacheInstance`]) hands those to
//! whatever engine the host wires up.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use verglas_core::CacheKey;
use verglas_core::node::NodeId;
use verglas_core::ring::{RendezvousRing, Ring};

/// A failure pulling a block from a peer. Placement itself never fails (the ring
/// is total), so this covers only the read-from-peer transport.
#[derive(Debug, thiserror::Error)]
pub enum RingError {
    /// The peer could not be reached (transport-level). The caller degrades to
    /// a backend fill — slow is acceptable, wrong is never.
    #[error("peer unavailable: {0}")]
    PeerUnavailable(String),
}

/// The interface by which a cache instance participates in the ring: placement
/// (which member owns a key) and read-from-peer (pull a block a peer holds).
///
/// # Contract for an implementation (the seam the host-agent's transport plugs
/// into)
///
/// - [`owner`](RingMembership::owner) is total and deterministic: every member
///   computes the same owner for the same key and member set. It is the only
///   method on the read hot path.
/// - [`fetch_from_peer`](RingMembership::fetch_from_peer) returns `Ok(Some)` on
///   a peer hit for the exact key/version, `Ok(None)` when no peer holds it
///   (degrade to a backend fill), and `Err` only for transport failures. The
///   local default has no peers and always returns `Ok(None)`.
/// - [`epoch`](RingMembership::epoch) bumps on every membership change so an
///   in-flight operation can detect a ring that moved under it.
///
/// Object-safe (`Arc<dyn RingMembership>`) so the instance holds one and the
/// concrete ring — local or clustered — is chosen once at construction.
#[async_trait]
pub trait RingMembership: Send + Sync + 'static {
    /// This instance's identity in the ring.
    fn node_id(&self) -> NodeId;

    /// The home member for `key`. Total and deterministic.
    fn owner(&self, key: &CacheKey) -> NodeId;

    /// A point-in-time snapshot of the current member set, in no order.
    fn members(&self) -> Vec<NodeId>;

    /// The ring epoch: a counter bumped on every membership change.
    fn epoch(&self) -> u64;

    /// Whether this instance owns `key` — the steady-state "serve it locally"
    /// check derived from [`owner`](RingMembership::owner).
    fn owns(&self, key: &CacheKey) -> bool {
        self.owner(key) == self.node_id()
    }

    /// Pulls `key`'s block from the peer that holds it, before a backend fill,
    /// during an ownership transition. `Ok(None)` when no peer has it.
    async fn fetch_from_peer(&self, key: &CacheKey) -> Result<Option<Bytes>, RingError>;

    /// Whether this is an architecturally single-node ring (no peers, ever).
    /// Defaults to `false`; [`LocalRing`] overrides it. Mirrors
    /// [`verglas_write`-style single-node detection] so the write-back tier
    /// and the ring agree on "genuinely one node."
    fn is_single_node(&self) -> bool {
        false
    }
}

/// The working local default: a single in-process member that owns every key
/// and has no peers.
///
/// This is the self-hosted, run-for-free case. Placement is the rendezvous ring
/// of one from [`verglas_core::ring`], so a single node computes exactly the
/// ownership a larger pod would for its own keys — no code path assumes "local"
/// instead of `owner(key)`, which is what lets the fleet's clustered ring drop
/// in unchanged. Read-from-peer is always `Ok(None)`: one node has no peer to
/// warm from, so every miss is a backend fill.
pub struct LocalRing {
    /// This node's identity.
    node_id: NodeId,
    /// The single-member rendezvous ring backing placement.
    ring: RendezvousRing,
}

impl LocalRing {
    /// Builds the single-node ring for `node_id`. Infallible: the one-member
    /// ring is always non-empty, so `owner` is total with no error to handle.
    pub fn single(node_id: NodeId) -> Self {
        Self {
            ring: RendezvousRing::single(node_id.clone()),
            node_id,
        }
    }
}

#[async_trait]
impl RingMembership for LocalRing {
    fn node_id(&self) -> NodeId {
        self.node_id.clone()
    }

    fn owner(&self, key: &CacheKey) -> NodeId {
        self.ring.owner(key)
    }

    fn members(&self) -> Vec<NodeId> {
        self.ring.members()
    }

    fn epoch(&self) -> u64 {
        self.ring.epoch()
    }

    /// One node, no peers: every miss degrades to a backend fill.
    async fn fetch_from_peer(&self, _key: &CacheKey) -> Result<Option<Bytes>, RingError> {
        Ok(None)
    }

    /// The genuine single-node case.
    fn is_single_node(&self) -> bool {
        true
    }
}

/// Adapts any key-ownership [`Ring`] into a [`RingMembership`] for placement,
/// with read-from-peer supplied separately (or absent).
///
/// The daemon builds this over the same live gossip ring it already serves from
/// ([`verglas_cluster::LiveRing`]), so instance-level placement is identical to
/// today's read path — no divergence between "the ring the engine uses" and
/// "the ring the instance reports." The fleet layers its remote peer transport
/// on top by implementing [`RingMembership`] directly (or wrapping this); until
/// then, `fetch_from_peer` degrades to a backend fill, which is always correct.
pub struct RingAdapter {
    /// This node's identity.
    node_id: NodeId,
    /// The key-ownership ring (single or clustered), behind a trait object so
    /// the adapter is agnostic to which implementation backs it.
    ring: Arc<dyn Ring + Send + Sync>,
}

impl RingAdapter {
    /// Wraps `ring` as this instance's placement source under `node_id`.
    pub fn new(node_id: NodeId, ring: Arc<dyn Ring + Send + Sync>) -> Self {
        Self { node_id, ring }
    }
}

#[async_trait]
impl RingMembership for RingAdapter {
    fn node_id(&self) -> NodeId {
        self.node_id.clone()
    }

    fn owner(&self, key: &CacheKey) -> NodeId {
        self.ring.owner(key)
    }

    fn members(&self) -> Vec<NodeId> {
        self.ring.members()
    }

    fn epoch(&self) -> u64 {
        self.ring.epoch()
    }

    /// No peer transport wired at this layer: degrade to a backend fill. The
    /// fleet overrides this by implementing [`RingMembership`] with its remote
    /// transport.
    async fn fetch_from_peer(&self, _key: &CacheKey) -> Result<Option<Bytes>, RingError> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(bucket: &str, k: &str) -> CacheKey {
        CacheKey {
            bucket: bucket.into(),
            key: k.into(),
        }
    }

    #[tokio::test]
    async fn local_ring_owns_every_key_and_has_no_peers() {
        let ring = LocalRing::single(NodeId::new("single"));
        let k = key("b", "warehouse/t/data/0.parquet");
        assert_eq!(ring.owner(&k), NodeId::new("single"));
        assert!(ring.owns(&k));
        assert_eq!(ring.members(), vec![NodeId::new("single")]);
        assert!(ring.is_single_node());
        assert_eq!(
            ring.fetch_from_peer(&k)
                .await
                .expect("local fetch never errors"),
            None
        );
    }

    #[tokio::test]
    async fn adapter_delegates_placement_to_the_wrapped_ring() {
        let inner = Arc::new(RendezvousRing::single(NodeId::new("n0")));
        let ring = RingAdapter::new(NodeId::new("n0"), inner);
        let k = key("b", "k");
        assert_eq!(ring.owner(&k), NodeId::new("n0"));
        assert!(ring.owns(&k));
        assert!(!ring.is_single_node()); // adapter makes no single-node claim
        assert_eq!(
            ring.fetch_from_peer(&k)
                .await
                .expect("local fetch never errors"),
            None
        );
    }
}
