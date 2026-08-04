//! The live weighted rendezvous ring (#28): `owner(key)` over the pod's live,
//! capacity-weighted membership, recomputed as gossip (#27) reports change.
//!
//! Rendezvous (highest-random-weight) hashing gives us three properties for
//! free: no token/vnode state to manage, uniform distribution, and — the one
//! that matters operationally — a join or leave moves only the keys that node
//! scores highest for (~its share), never a global reshuffle. Weighting makes a
//! node's share proportional to its advertised capacity so heterogeneous pods
//! (an 800 GB node beside a 6 TB one) balance correctly.
//!
//! The member set lives behind an [`arc_swap::ArcSwap`] so a gossip change
//! swaps it atomically under the read hot path: `owner` loads the current
//! snapshot lock-free, and an in-flight lookup either sees the old set or the
//! new one, never a torn one. The engine holds a `LiveRing` by value (it is a
//! cheap `Arc` handle) and never rebuilds — a membership blip must not flush
//! the cache.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arc_swap::ArcSwap;

use verglas_core::CacheKey;
use verglas_core::node::NodeId;
use verglas_core::ring::{RingError, WarmDonor, rendezvous_hash};

/// A pod member and its rendezvous weight.
///
/// The weight is the node's advertised capacity (its NVMe budget, #28): the
/// share of the keyspace a node owns is proportional to it. Weights need no
/// common unit or normalization — only their ratios matter to the scoring.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WeightedMember {
    /// The member's pod-wide identity.
    pub id: NodeId,
    /// The member's rendezvous weight (advertised capacity). Invariant:
    /// non-zero — a zero-weight member would own no keyspace, so it is not a
    /// member (enforced at snapshot construction, see [`RingSnapshot::new`]).
    pub weight: u64,
}

impl WeightedMember {
    /// Builds a weighted member. A weight of `0` is clamped to `1` so a
    /// mis-advertised node still participates rather than silently vanishing
    /// from the ring (slow is acceptable; a black hole is not).
    pub fn new(id: NodeId, weight: u64) -> Self {
        Self {
            id,
            weight: weight.max(1),
        }
    }
}

/// A pod member with its weight *and* its lifecycle disposition for the ring:
/// whether it is draining (#31). This is the input the gossip agent feeds the
/// ring on every membership change; it is richer than [`WeightedMember`]
/// because a draining node is excluded from ownership but kept as a warm donor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RingMember {
    /// The member's pod-wide identity.
    pub id: NodeId,
    /// The member's rendezvous weight (advertised capacity).
    pub weight: u64,
    /// Whether the member is draining: it no longer takes ownership (its keys
    /// move to successors) but still serves the blocks it holds, as a donor its
    /// successors warm from (#31).
    pub draining: bool,
}

impl RingMember {
    /// Builds an owning (non-draining) member — the common case and the shape
    /// the plain-`WeightedMember` constructors reduce to.
    pub fn active(id: NodeId, weight: u64) -> Self {
        Self {
            id,
            weight: weight.max(1),
            draining: false,
        }
    }

    /// Builds a draining member: kept in the ring as a warm donor but excluded
    /// from ownership (#31).
    pub fn draining(id: NodeId, weight: u64) -> Self {
        Self {
            id,
            weight: weight.max(1),
            draining: true,
        }
    }
}

/// One member inside a [`RingSnapshot`]: its weight and whether it is draining.
#[derive(Debug, Clone)]
struct DonorMember {
    /// The member's identity.
    id: NodeId,
    /// The member's rendezvous weight.
    weight: u64,
    /// Whether the member is draining (a warm donor, never an owner).
    draining: bool,
}

/// An immutable point-in-time member set the ring scores against. Swapped
/// wholesale on every membership change so readers never see a partial update.
///
/// It keeps two views of one member set: `owners`, the nodes that take a share
/// of the keyspace (everyone not draining, so a drain sheds ownership), and
/// `donors`, every live member including draining ones, which `warm_donor`
/// ranks to name a block's previous holder during a transition (#30/#31).
#[derive(Debug)]
struct RingSnapshot {
    /// The owning members (draining nodes excluded). Invariant: non-empty
    /// (checked at construction), so `owner` is total.
    owners: Vec<WeightedMember>,
    /// Every live member with its draining flag, for `warm_donor`. Superset of
    /// `owners`; non-empty whenever `owners` is.
    donors: Vec<DonorMember>,
}

impl RingSnapshot {
    /// Builds a snapshot from members carrying their draining disposition.
    ///
    /// Owners are the non-draining members; a pod that is *entirely* draining
    /// (every member leaving at once — not a state we drive, but representable)
    /// falls back to owning over the draining set so `owner` still answers
    /// rather than emptying the ring. Rejects an empty member set.
    fn new(members: Vec<RingMember>) -> Result<Self, RingError> {
        if members.is_empty() {
            return Err(RingError::EmptyMembership);
        }
        let donors: Vec<DonorMember> = members
            .iter()
            .map(|m| DonorMember {
                id: m.id.clone(),
                weight: m.weight.max(1),
                draining: m.draining,
            })
            .collect();
        let mut owners: Vec<WeightedMember> = donors
            .iter()
            .filter(|m| !m.draining)
            .map(|m| WeightedMember::new(m.id.clone(), m.weight))
            .collect();
        // Every member draining at once: keep ownership answerable off the
        // whole set rather than returning an empty (unownable) ring.
        if owners.is_empty() {
            owners = donors
                .iter()
                .map(|m| WeightedMember::new(m.id.clone(), m.weight))
                .collect();
        }
        Ok(Self { owners, donors })
    }

    /// Builds a snapshot from plain weighted members, none draining — the
    /// pre-drain shape [`LiveRing::new`]/`update` construct.
    fn from_weighted(members: Vec<WeightedMember>) -> Result<Self, RingError> {
        Self::new(
            members
                .into_iter()
                .map(|m| RingMember {
                    id: m.id,
                    weight: m.weight,
                    draining: false,
                })
                .collect(),
        )
    }

    /// Returns the highest-weighted-random-weight owner of `key`.
    ///
    /// For each member we map the pod-wide [`rendezvous_hash`] into `u ∈ (0,1)`
    /// and score it `weight * (-1 / ln u)` — the standard weighted-rendezvous
    /// transform (Jason Resch): monotonic in the hash, so equal weights reduce
    /// to plain highest-hash, and larger weights lift the whole score curve so
    /// ownership probability scales with weight. The max score wins; an
    /// (astronomically unlikely) score tie breaks by node id so all nodes
    /// agree even then. Scored over `owners`, so a draining node owns nothing.
    fn owner(&self, key: &CacheKey) -> NodeId {
        self.owners
            .iter()
            .map(|m| (weighted_score(rendezvous_hash(key, &m.id), m.weight), &m.id))
            .max_by(|(sa, ia), (sb, ib)| sa.total_cmp(sb).then_with(|| ia.cmp(ib)))
            .map(|(_, id)| id.clone())
            // Non-empty by construction, so `max_by` always yields a member.
            .expect("ring membership is non-empty by construction")
    }

    /// Names the peer `self_id` should warm `key` from before a backend fill
    /// during a transition, ranking over `donors` (see [`Ring::warm_donor`]).
    ///
    /// The rendezvous ranking of every live member names the previous holder
    /// directly. Split the other members by score relative to `self_id`:
    ///
    /// - A member scoring **above** `self_id` can only be a draining node — a
    ///   non-draining node above `self_id` would itself be the owner. That is
    ///   the drain successor case: ownership just moved from it to `self_id`, so
    ///   it holds the hot copy and the owner pulls unconditionally
    ///   (`draining: true`).
    /// - Otherwise the highest member scoring **below** `self_id` is the plain
    ///   previous owner from before `self_id` joined (`draining: false`), which
    ///   the owner pulls from only while it is still warming.
    ///
    /// `None` when `self_id` is the sole member (nobody to warm from).
    fn warm_donor(&self, key: &CacheKey, self_id: &NodeId) -> Option<WarmDonor> {
        let scored = |id: &NodeId, weight: u64| weighted_score(rendezvous_hash(key, id), weight);
        // `self_id`'s own score, needed to split the field into above/below.
        let self_weight = self.donors.iter().find(|m| &m.id == self_id)?.weight;
        let self_key = (scored(self_id, self_weight), self_id.clone());

        let mut best_above: Option<(f64, NodeId)> = None;
        let mut best_below: Option<(f64, NodeId)> = None;
        for m in &self.donors {
            if &m.id == self_id {
                continue;
            }
            let entry = (scored(&m.id, m.weight), m.id.clone());
            // Same total order `owner` uses (score, then id) so above/below is
            // consistent with who actually owns the key.
            let above_self = (entry.0, &entry.1) > (self_key.0, &self_key.1);
            let slot = if above_self {
                &mut best_above
            } else {
                &mut best_below
            };
            let better = match slot {
                Some((bs, bid)) => (entry.0, &entry.1) > (*bs, bid),
                None => true,
            };
            if better {
                *slot = Some(entry);
            }
        }
        if let Some((_, node)) = best_above {
            // Above the owner ⇒ a draining predecessor shedding this key: pull
            // unconditionally, it is exactly the drain donor (#31).
            return Some(WarmDonor {
                node,
                draining: true,
            });
        }
        best_below.map(|(_, node)| WarmDonor {
            node,
            // A healthy incumbent: warm-window-gated (#30), never unconditional.
            draining: false,
        })
    }

    /// Whether `self_id` should answer a peer-fetch for `key` from its local
    /// tiers (#29/#30/#31).
    ///
    /// The peer-serve set for a key is exactly two nodes: its current `owner`,
    /// and the single donor the owner would warm from ([`RingSnapshot::warm_donor`]
    /// from the *owner's* view) — the block's designated previous holder during
    /// a transition. That covers both a join (the newcomer owns; the incumbent
    /// runner-up serves) and a drain (the successor owns; the leaving node
    /// serves), while a plain replica — held by neither the owner nor its one
    /// designated donor — is refused, so peer serving never fans out into
    /// A→B→C chains.
    fn should_serve_peer(&self, key: &CacheKey, self_id: &NodeId) -> bool {
        let owner = self.owner(key);
        if owner == *self_id {
            return true;
        }
        self.warm_donor(key, &owner).map(|d| d.node).as_ref() == Some(self_id)
    }
}

/// Maps a rendezvous hash to a weighted score `weight * (-1 / ln u)`.
///
/// `u` is the hash placed in the open interval `(0, 1)` (the `+ 0.5` keeps it
/// off both endpoints, so `ln u` is defined and non-zero). `-1 / ln u` is
/// positive and increasing in `u`; scaling by `weight` makes a heavier node
/// win more keys in exact proportion to its weight.
fn weighted_score(hash: u64, weight: u64) -> f64 {
    // 2^64 as f64; the mantissa loses the low bits of `hash`, which only
    // perturbs the uniform mapping negligibly.
    const SPACE: f64 = 18_446_744_073_709_551_616.0;
    let u = (hash as f64 + 0.5) / SPACE;
    (weight as f64) * (-1.0 / u.ln())
}

/// The pod's live, weighted ownership ring.
///
/// A cheap `Arc` handle (clone = one refcount bump) so the cache engine can
/// hold it by value and the cluster agent can hold a second handle to push
/// membership updates into the same underlying state.
#[derive(Clone)]
pub struct LiveRing {
    /// Shared state so engine and agent handles observe the same swaps.
    inner: Arc<Inner>,
}

/// The state shared by every [`LiveRing`] handle.
struct Inner {
    /// The current member set, swapped atomically on membership change.
    snapshot: ArcSwap<RingSnapshot>,
    /// Monotonic membership-change counter (#28's ring epoch). Bumped on every
    /// successful [`LiveRing::update`] so in-flight operations can detect that
    /// the membership moved under them.
    epoch: AtomicU64,
}

impl LiveRing {
    /// Builds a live ring over an initial weighted member set (none draining).
    /// Fails on an empty set so `owner` is total.
    pub fn new(members: Vec<WeightedMember>) -> Result<Self, RingError> {
        let snapshot = RingSnapshot::from_weighted(members)?;
        Ok(Self {
            inner: Arc::new(Inner {
                snapshot: ArcSwap::from_pointee(snapshot),
                epoch: AtomicU64::new(0),
            }),
        })
    }

    /// Builds the degenerate single-member ring: one node owns every key. This
    /// is the N=1 case a server boots with before (or without) gossip — the
    /// weight is irrelevant when there is only one member.
    pub fn single(node: NodeId, weight: u64) -> Self {
        // Infallible: a one-element vector is never empty.
        let snapshot = RingSnapshot::from_weighted(vec![WeightedMember::new(node, weight)])
            .expect("a one-member set is non-empty");
        Self {
            inner: Arc::new(Inner {
                snapshot: ArcSwap::from_pointee(snapshot),
                epoch: AtomicU64::new(0),
            }),
        }
    }

    /// Atomically replaces the member set (none draining) and bumps the epoch,
    /// so every subsequent `owner` routes on the new membership. Rejects an
    /// empty set (leaving the ring and epoch untouched) — a pod with zero live
    /// members is not a state the ring represents; the caller keeps serving the
    /// last known membership until gossip reports a live one.
    pub fn update(&self, members: Vec<WeightedMember>) -> Result<(), RingError> {
        let snapshot = RingSnapshot::from_weighted(members)?;
        self.store(snapshot);
        Ok(())
    }

    /// Atomically replaces the member set from members carrying their draining
    /// disposition (#31) and bumps the epoch. This is the gossip agent's entry
    /// point: a draining member is kept in the ring as a warm donor but sheds
    /// ownership to its successors. Rejects an empty set like [`LiveRing::update`].
    pub fn update_states(&self, members: Vec<RingMember>) -> Result<(), RingError> {
        let snapshot = RingSnapshot::new(members)?;
        self.store(snapshot);
        Ok(())
    }

    /// Swaps in a validated snapshot and bumps the epoch — the shared tail of
    /// both update paths.
    fn store(&self, snapshot: RingSnapshot) {
        self.inner.snapshot.store(Arc::new(snapshot));
        self.inner.epoch.fetch_add(1, Ordering::Relaxed);
    }
}

impl verglas_core::ring::Ring for LiveRing {
    /// Returns the current owner of `key`, loading the live snapshot lock-free.
    fn owner(&self, key: &CacheKey) -> NodeId {
        self.inner.snapshot.load().owner(key)
    }

    /// Returns a snapshot of the current owning member ids, in no particular
    /// order. Draining members are excluded — they own nothing (#31).
    fn members(&self) -> Vec<NodeId> {
        self.inner
            .snapshot
            .load()
            .owners
            .iter()
            .map(|m| m.id.clone())
            .collect()
    }

    /// Returns the current ring epoch (membership-change generation, #28).
    fn epoch(&self) -> u64 {
        self.inner.epoch.load(Ordering::Relaxed)
    }

    /// Names the warm donor for `key` from `self_id`'s view (#30/#31), loading
    /// the live snapshot lock-free — see [`RingSnapshot::warm_donor`].
    fn warm_donor(&self, key: &CacheKey, self_id: &NodeId) -> Option<WarmDonor> {
        self.inner.snapshot.load().warm_donor(key, self_id)
    }

    /// A key's peer-serve set is its owner plus the one donor the owner warms
    /// from (#29/#30/#31), loading the live snapshot lock-free — see
    /// [`RingSnapshot::should_serve_peer`].
    fn should_serve_peer(&self, key: &CacheKey, self_id: &NodeId) -> bool {
        self.inner.snapshot.load().should_serve_peer(key, self_id)
    }
}
