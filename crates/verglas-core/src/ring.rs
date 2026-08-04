//! Key-ownership ring: which node in a pod is the home for a cache key.
//!
//! This is the cluster-of-one constraint from issue #17: M1 runs a single
//! node, but every read/fill path must still ask the ring who owns a key —
//! no code path may assume a key is locally owned. The implementation is
//! rendezvous (highest-random-weight) hashing: on membership change only
//! ~1/N of keys change owner, with no token or vnode state to manage.

use std::error::Error;
use std::fmt;

use xxhash_rust::xxh3::Xxh3;

use crate::CacheKey;
use crate::node::NodeId;

/// Maps cache keys to their home node within a pod.
///
/// `owner` is total: a ring always has at least one member, so every key has
/// exactly one home node. All lookup, fill, singleflight, and invalidation
/// paths key their work by `owner(key)`, never by "local" — that is what lets
/// M2's real ring (#28) and peer RPC (#29) land without touching call sites.
pub trait Ring {
    /// Returns the home node for `key`. Deterministic: every node in a pod
    /// computes the same owner for the same key and member set.
    fn owner(&self, key: &CacheKey) -> NodeId;

    /// Returns a snapshot of the current member set, in no particular order.
    ///
    /// An owned snapshot rather than a borrow (#28): the live ring backing a
    /// running pod swaps its membership atomically as gossip (#27) reports
    /// joins and departures, so there is no stable slice to hand back a
    /// borrow into — a caller gets a consistent point-in-time copy instead.
    /// Never called from the read hot path (only `owner` is); the clone cost
    /// is paid only by the topology/admin surfaces that enumerate members.
    fn members(&self) -> Vec<NodeId>;

    /// Returns the ring's current epoch: a counter bumped every time the
    /// member set changes (#28).
    ///
    /// In-flight operations capture the epoch they resolved ownership under
    /// and can detect that a membership change has since raced them, without
    /// enumerating which keys moved. Fixed-membership rings never change, so
    /// the default is a constant `0`; only the live ring (#27/#28) overrides.
    fn epoch(&self) -> u64 {
        0
    }

    /// Returns the peer `self_id` should pull `key`'s block from before a
    /// backend fill during an ownership transition — the block's likely
    /// previous holder — or `None` when there is no such peer (#30/#31).
    ///
    /// This is the warm-from-peers hook: when `self_id` owns `key` (per
    /// [`Ring::owner`]) but misses it in its local tiers, the pod's *other*
    /// nodes may still hold the hot copy, because ownership only just moved to
    /// `self_id` (a node joined and took the key, or a node started draining and
    /// shed it). Pulling the block over the fast peer network instead of
    /// re-fetching it from the backend is what keeps a join's hit-rate crater
    /// and a drain's dip shallow. The rendezvous ranking names the previous
    /// holder for free: it is the highest-scoring node other than `self_id`.
    ///
    /// Fixed-membership rings (a cluster of one, the mock rendezvous ring) never
    /// transition, so the default is `None`; only the live gossip ring (#28)
    /// overrides it. A `None` (or an unreachable donor) degrades to a backend
    /// fill — never an error (slow is acceptable; wrong is never).
    fn warm_donor(&self, _key: &CacheKey, _self_id: &NodeId) -> Option<WarmDonor> {
        None
    }

    /// Whether `self_id` should answer a peer-fetch request for `key` from its
    /// local tiers (#29/#31).
    ///
    /// The steady-state rule is "owners only": a node serves a peer only the
    /// keys it owns, so owners stay the single intra-pod source and local
    /// replicas never fan out into chains. The one exception is a **draining**
    /// node (#31): it has shed ownership but must keep serving the blocks it
    /// still holds so its successors can warm from it — it is a donor on its way
    /// out. The default has no notion of draining, so it is the plain ownership
    /// check; only the live ring (#28) widens it for a draining self.
    fn should_serve_peer(&self, key: &CacheKey, self_id: &NodeId) -> bool {
        self.owner(key) == *self_id
    }
}

/// A peer a key's owner may warm a missing block from before a backend fill,
/// during an ownership transition (#30/#31).
///
/// `draining` distinguishes the two transitions, which the owner gates
/// differently:
///
/// - `draining == true`: ownership just *left* the donor (it is leaving the
///   pod, #31), so the donor holds the hot copy of a key the owner has only
///   just inherited. The owner pulls unconditionally — this is the whole point
///   of a graceful drain (the donor serves until its keys are re-owned warm).
/// - `draining == false`: the donor is a healthy incumbent and the owner is the
///   *newcomer* (#30). The owner pulls only while it is still warming after a
///   join, so a long-settled pod pays no extra peer hop on a genuinely cold
///   miss (the incumbent would miss too).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WarmDonor {
    /// The peer to ask for the block.
    pub node: NodeId,
    /// Whether the donor is leaving the pod (draining), which makes the pull
    /// unconditional rather than gated on the owner's warm window.
    pub draining: bool,
}

/// Computes the rendezvous hash of `key` on `node` — the pod-wide wire
/// commitment every ring implementation scores with.
///
/// Cross-node compatibility commitment: every node in a pod must compute
/// bit-identical hashes or they disagree about ownership, so the hash
/// function *and* its input framing are a wire commitment shared by all
/// members and by the weighted live ring (#28). We commit to XXH3-64 (the
/// finalized XXH3 from upstream xxHash, via the `xxhash-rust` crate) over
/// length-prefixed `(bucket, key, node)` fields; length prefixes keep
/// adjacent fields from aliasing (e.g. ("ab","c") vs ("a","bc")). Per the
/// prototype rules in AGENTS.md there is deliberately NO version tag or
/// negotiation around this choice: changing the hash or framing is a
/// flag-day break for a pod, which is acceptable pre-release. This function
/// is the extension-point marker.
pub fn rendezvous_hash(key: &CacheKey, node: &NodeId) -> u64 {
    let mut hasher = Xxh3::new();
    for field in [key.bucket.as_str(), key.key.as_str(), node.as_str()] {
        hasher.update(&(field.len() as u64).to_le_bytes());
        hasher.update(field.as_bytes());
    }
    hasher.digest()
}

/// Error building a ring from an invalid member set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RingError {
    /// A ring must have at least one member, otherwise `owner` has no answer.
    EmptyMembership,
}

impl fmt::Display for RingError {
    /// Renders the error for logs; the variant carries the whole story.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyMembership => f.write_str("a ring requires at least one member"),
        }
    }
}

impl Error for RingError {}

/// Rendezvous-hash (highest-random-weight) ring over a fixed member list.
///
/// For key `k`, every member computes `score(k, node)`; the highest score
/// owns the key. Scores depend only on `(key, node)`, so ownership is
/// independent of member order and, on membership change, only the keys
/// scored highest by the departed/arrived node move (~1/N of the keyspace).
///
/// M1 constructs this with exactly one member; the member set is fixed at
/// construction. #28 replaces the fixed list with live gossip membership but
/// keeps this scoring function.
#[derive(Debug, Clone)]
pub struct RendezvousRing {
    /// Member nodes. Invariant: non-empty (enforced by the constructors).
    members: Vec<NodeId>,
}

impl RendezvousRing {
    /// Builds a ring over `members`; fails on an empty member set so that
    /// [`Ring::owner`] can be total.
    pub fn new(members: Vec<NodeId>) -> Result<Self, RingError> {
        if members.is_empty() {
            return Err(RingError::EmptyMembership);
        }
        Ok(Self { members })
    }

    /// Builds the M1 cluster-of-one ring: a single member that owns every
    /// key. Infallible, so the single-node boot path has no error to handle.
    pub fn single(node: NodeId) -> Self {
        Self {
            members: vec![node],
        }
    }

    /// Computes the rendezvous score of `key` on `node`: the shared
    /// [`rendezvous_hash`] with no weighting.
    ///
    /// This ring is deliberately unweighted — every member has equal claim, so
    /// the score is the raw hash and `owner` picks the maximum. Weighted
    /// rendezvous (#28's capacity-proportional ownership) lives in the live
    /// ring, which transforms this same hash into `-weight / ln(u)`; with
    /// equal weights that transform is monotonic in the hash, so this
    /// unweighted ring is exactly the equal-weight degenerate case and both
    /// agree key-for-key.
    fn score(key: &CacheKey, node: &NodeId) -> u64 {
        rendezvous_hash(key, node)
    }
}

impl Ring for RendezvousRing {
    /// Returns the member with the highest rendezvous score for `key`.
    ///
    /// A 64-bit score tie is broken by node-id order so that all nodes agree
    /// even in that (astronomically unlikely) case.
    fn owner(&self, key: &CacheKey) -> NodeId {
        self.members
            .iter()
            .max_by_key(|node| (Self::score(key, node), *node))
            .cloned()
            // Non-empty by construction (both constructors enforce it), so
            // `max_by_key` always yields a node.
            .expect("ring membership is non-empty by construction")
    }

    /// Returns a copy of the member list this ring was built over. Fixed at
    /// construction, so the snapshot is always the full membership.
    fn members(&self) -> Vec<NodeId> {
        self.members.clone()
    }
}
