//! Node identity and metadata as published over gossip (#27).
//!
//! Each node gossips a small, fixed set of facts about itself — its addresses,
//! its capacity, and its lifecycle state — and every other node reads them to
//! build the pod's membership view and the weighted ring. The chitchat
//! key/value encoding here is a wire commitment shared by all nodes in a pod;
//! per the prototype rules there is deliberately no version negotiation on it
//! (this comment is the extension-point marker for the day gossip skew must interoperate).

use std::net::SocketAddr;

use chitchat::{ChitchatId, NodeState as ChitchatNodeState};

use verglas_core::node::NodeId;

/// Gossip key for a node's advertised peer-fetch address (#29 extension point).
pub(crate) const KEY_ADVERTISE_ADDR: &str = "verglas.advertise_addr";
/// Gossip key for a node's loopback admin API address.
pub(crate) const KEY_ADMIN_ADDR: &str = "verglas.admin_addr";
/// Gossip key for a node's advertised capacity, which is its ring weight.
pub(crate) const KEY_CAPACITY_BYTES: &str = "verglas.capacity_bytes";
/// Gossip key for a node's lifecycle state.
pub(crate) const KEY_STATE: &str = "verglas.state";

/// A node's lifecycle state within the pod (WHITEPAPER §3.5, addendum to #27).
///
/// Only `active` participates in normal ownership. `donor`/`takeover` pin ring
/// ownership to a slot during a successor-takeover upgrade — a donor is *not* a
/// failure and must not trigger rebalance — and `draining`/`dead` are on their
/// way out. The takeover choreography that drives the non-`active` states is
/// #79's scope; this enum is the vocabulary gossip carries so the ring can tell
/// a planned handoff from a crash.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeState {
    /// Serving and taking a normal weighted share of the keyspace.
    Active,
    /// A read-only predecessor feeding its successor during takeover.
    Donor,
    /// A successor that has inherited a slot and is warming from its donor.
    Takeover,
    /// Leaving gracefully; ownership is migrating away.
    Draining,
    /// Failed or removed; owns nothing.
    Dead,
}

impl NodeState {
    /// The wire string for this state.
    pub fn as_str(self) -> &'static str {
        match self {
            NodeState::Active => "active",
            NodeState::Donor => "donor",
            NodeState::Takeover => "takeover",
            NodeState::Draining => "draining",
            NodeState::Dead => "dead",
        }
    }

    /// Parses a wire string, defaulting an unknown/absent value to `Active` —
    /// a node that gossips no state is assumed to be serving normally (slow is
    /// acceptable, a misparse that black-holes a node is not).
    pub fn from_wire(value: Option<&str>) -> Self {
        match value {
            Some("donor") => NodeState::Donor,
            Some("takeover") => NodeState::Takeover,
            Some("draining") => NodeState::Draining,
            Some("dead") => NodeState::Dead,
            _ => NodeState::Active,
        }
    }
}

/// The full published metadata for one pod member (#27).
///
/// Built from a node's [`ChitchatId`] (its identity, generation, and gossip
/// address) plus the key/values it publishes. `capacity_bytes` doubles as the
/// node's rendezvous weight, so a bigger node owns proportionally more keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeMeta {
    /// Pod-wide node identity.
    pub node_id: NodeId,
    /// Pod (gossip cluster) identity this node belongs to.
    pub pod_id: String,
    /// Restart generation: bumped each start so a restarted node is
    /// distinguishable from a stale record of its previous life.
    pub generation: u64,
    /// The address this node gossips on.
    pub gossip_addr: SocketAddr,
    /// The address peers fetch owned bytes from (#29). `None` if unadvertised.
    pub advertise_addr: Option<SocketAddr>,
    /// The node's loopback admin API address. `None` if unadvertised.
    pub admin_addr: Option<SocketAddr>,
    /// Advertised capacity in bytes — the node's rendezvous weight (#28).
    pub capacity_bytes: u64,
    /// Lifecycle state (§3.5).
    pub state: NodeState,
}

impl NodeMeta {
    /// Reconstructs a member's metadata from its gossip state.
    ///
    /// `pod_id` is passed in (it is the shared cluster id, gossiped once as the
    /// chitchat `cluster_id` rather than per node). A malformed or absent
    /// capacity reads as `0`, which the ring clamps to a minimum weight so the
    /// node still participates rather than silently owning nothing.
    pub fn from_gossip(chitchat_id: &ChitchatId, state: &ChitchatNodeState, pod_id: &str) -> Self {
        let advertise_addr = state
            .get(KEY_ADVERTISE_ADDR)
            .and_then(|v| v.parse::<SocketAddr>().ok());
        let admin_addr = state
            .get(KEY_ADMIN_ADDR)
            .and_then(|v| v.parse::<SocketAddr>().ok());
        let capacity_bytes = state
            .get(KEY_CAPACITY_BYTES)
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        NodeMeta {
            node_id: NodeId::new(&*chitchat_id.node_id),
            pod_id: pod_id.to_owned(),
            generation: chitchat_id.generation_id,
            gossip_addr: chitchat_id.gossip_advertise_addr,
            advertise_addr,
            admin_addr,
            capacity_bytes,
            state: NodeState::from_wire(state.get(KEY_STATE)),
        }
    }
}
