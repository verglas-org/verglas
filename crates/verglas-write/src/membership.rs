//! Live pod membership for placement and quorum decisions (#180).
//!
//! Write-back evaluates "can the pod place `w` fragments on distinct live
//! nodes?" per write against the current gossip view, not against config. This
//! trait is that view; [`AgentMembership`] reads it from the cluster agent's
//! failure detector, and [`SingleNodeMembership`] is the no-cluster server
//! (always one node, so write-back always degrades to write-through).

use std::sync::Arc;

use verglas_cluster::{ClusterAgent, NodeState};
use verglas_core::node::NodeId;

/// The current live pod view a write-back placement decides against.
pub trait LiveMembership: Send + Sync + 'static {
    /// This node's id.
    fn self_id(&self) -> NodeId;

    /// Every node the failure detector currently considers alive, including
    /// self. These are the nodes a fragment may be placed on.
    fn live_nodes(&self) -> Vec<NodeId>;

    /// A monotonically-changing membership epoch. A change means the live set
    /// may have changed and dirty objects should be checked for repair.
    fn epoch(&self) -> u64;

    /// True only for a deployment that is *architecturally* one node — a
    /// self-deployed server with no pod, never any peers (#286). This is not the
    /// same as a multi-node pod that has merely degraded to one live member: that
    /// pod keeps the safe write-through fallback so it never silently drops the
    /// redundancy it normally provides, whereas a genuine single-node deployment
    /// has no redundancy to drop and instead fast-acks from local durability
    /// (degenerate `k=1, m=0, w=1` coding — one fragment fsynced to local NVMe).
    /// Defaults to `false`; only [`SingleNodeMembership`] overrides it.
    fn is_single_node(&self) -> bool {
        false
    }
}

/// Live membership from the cluster agent: the gossip failure detector's live
/// set (excluding `Dead`), and the ring epoch that bumps on every change.
pub struct AgentMembership {
    /// The pod's cluster agent.
    agent: Arc<ClusterAgent>,
}

impl AgentMembership {
    /// Wraps the cluster agent.
    pub fn new(agent: Arc<ClusterAgent>) -> Self {
        Self { agent }
    }
}

impl LiveMembership for AgentMembership {
    /// The agent's own node id.
    fn self_id(&self) -> NodeId {
        self.agent.node_id().clone()
    }

    /// Live members: every gossip member the detector has not marked `Dead`.
    fn live_nodes(&self) -> Vec<NodeId> {
        self.agent
            .members()
            .into_iter()
            .filter(|m| m.state != NodeState::Dead)
            .map(|m| m.node_id)
            .collect()
    }

    /// The ring epoch, bumped by the agent on every membership change.
    fn epoch(&self) -> u64 {
        use verglas_core::ring::Ring;
        self.agent.ring().epoch()
    }
}

/// The single-node server's membership: one node, epoch fixed. A one-node
/// deployment can never reach a fragment quorum of `w > 1`, so instead of the
/// synchronous write-through fallback it fast-acks from local durability (#286):
/// write-back degenerates to `k=1, m=0, w=1` — the single object fragment is
/// fsynced to local NVMe and, with the fsynced journal, is the ack; propagation
/// to the origin then runs in the background exactly as §6 specifies. The
/// deployment story may put the buffer directory on a replicated block volume,
/// but the server does not know or care — durability here is "one local disk
/// until propagation completes," disclosed in the write-back docs.
pub struct SingleNodeMembership {
    /// The lone node's id.
    node_id: NodeId,
}

impl SingleNodeMembership {
    /// Builds the single-node view for `node_id`.
    pub fn new(node_id: NodeId) -> Self {
        Self { node_id }
    }
}

impl LiveMembership for SingleNodeMembership {
    /// The lone node.
    fn self_id(&self) -> NodeId {
        self.node_id.clone()
    }

    /// Just this node.
    fn live_nodes(&self) -> Vec<NodeId> {
        vec![self.node_id.clone()]
    }

    /// Fixed: a single-node pod never changes membership.
    fn epoch(&self) -> u64 {
        0
    }

    /// This is the genuine one-node deployment: fast-ack from local durability
    /// rather than degrade to write-through (#286).
    fn is_single_node(&self) -> bool {
        true
    }
}
