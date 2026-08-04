//! The cluster agent: runs chitchat gossip and keeps the live ring in step
//! with membership (#27).
//!
//! A [`ClusterAgent`] publishes this node's identity and capacity, discovers
//! peers from the seed list, and runs a background task that — on every
//! membership change chitchat reports — rebuilds the pod's member view and
//! pushes it into a [`LiveRing`] so ownership reroutes without rebuilding the
//! cache engine. No consensus: the view is eventually-consistent gossip, and a
//! transient disagreement costs at most a redundant backend fill.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use chitchat::transport::{Transport, UdpTransport};
use chitchat::{
    ChitchatHandle, ChitchatId, FailureDetectorConfig, NodeState as ChitchatNodeState,
    spawn_chitchat,
};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use verglas_core::node::NodeId;

use crate::member::{
    KEY_ADMIN_ADDR, KEY_ADVERTISE_ADDR, KEY_CAPACITY_BYTES, KEY_STATE, NodeMeta, NodeState,
};
use crate::ring::{LiveRing, RingMember};

/// Fatal errors spawning or configuring the cluster agent.
#[derive(Debug, thiserror::Error)]
pub enum ClusterError {
    /// The gossip transport could not bind or the initial handshake failed.
    #[error("failed to start gossip on {addr}: {reason}")]
    Gossip {
        /// The gossip listen address that failed.
        addr: std::net::SocketAddr,
        /// The underlying chitchat/transport error, rendered.
        reason: String,
    },
}

/// Resolved configuration for one gossip node.
///
/// Built either directly (tests) or from the server's `[cluster]` config via
/// [`AgentConfig::from_config`]. Durations are explicit so tests can tune fast
/// convergence and production can tune for stability.
#[derive(Debug, Clone)]
pub struct AgentConfig {
    /// This node's stable pod-wide identity.
    pub node_id: String,
    /// The pod (gossip cluster) id; nodes converge only within a shared pod.
    pub pod_id: String,
    /// Restart generation, distinguishing a fresh instance from a stale record.
    pub generation: u64,
    /// The address this node binds and advertises for gossip.
    pub gossip_addr: std::net::SocketAddr,
    /// The peer-fetch address advertised to peers (#29 extension point).
    pub advertise_addr: Option<std::net::SocketAddr>,
    /// The loopback admin API address advertised for `verglas status` (#62).
    pub admin_addr: Option<std::net::SocketAddr>,
    /// Advertised capacity in bytes — this node's rendezvous weight (#28).
    pub capacity_bytes: u64,
    /// Seed gossip addresses (host:port or DNS) to contact on join.
    pub seeds: Vec<String>,
    /// How often this node gossips.
    pub gossip_interval: Duration,
    /// The failure-detection window: the assumed heartbeat interval the
    /// phi-accrual detector accrues against, so a silent node is dropped within
    /// a small multiple of it.
    pub suspicion: Duration,
}

impl AgentConfig {
    /// Derives an agent config from the server's `[cluster]` config plus the
    /// cache budget (the default weight) and admin port (advertised for the
    /// CLI). The rendezvous weight is the explicit `weight` if set, else the
    /// configured NVMe budget — capacity *is* the weight. The generation is the
    /// process start time in seconds, so a restart outranks its stale record.
    ///
    /// Assumes the config already passed `Config::validate` (addresses parse).
    pub fn from_config(
        cluster: &verglas_core::config::Cluster,
        cache: &verglas_core::config::Cache,
        admin_port: u16,
    ) -> Result<Self, ClusterError> {
        let gossip_addr = cluster
            .gossip_addr
            .parse::<std::net::SocketAddr>()
            .map_err(|e| ClusterError::Gossip {
                // Best-effort placeholder addr for the error; parse failed.
                addr: "0.0.0.0:0".parse().expect("literal addr"),
                reason: format!("invalid gossip_addr `{}`: {e}", cluster.gossip_addr),
            })?;
        let advertise_addr = cluster.advertise_addr.as_ref().and_then(|a| a.parse().ok());
        let generation = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        Ok(AgentConfig {
            node_id: cluster
                .node_id
                .clone()
                .unwrap_or_else(|| cluster.gossip_addr.clone()),
            pod_id: cluster.pod_id.clone(),
            generation,
            gossip_addr,
            advertise_addr,
            admin_addr: Some(
                format!("127.0.0.1:{admin_port}")
                    .parse()
                    .expect("loopback admin addr"),
            ),
            capacity_bytes: cluster.weight.unwrap_or(cache.capacity_bytes.0),
            seeds: cluster.seeds.clone(),
            gossip_interval: Duration::from_millis(500),
            suspicion: Duration::from_secs(cluster.suspicion_secs),
        })
    }

    /// The gossip key/values this node publishes about itself.
    fn initial_key_values(&self) -> Vec<(String, String)> {
        let mut kvs = vec![
            (
                KEY_CAPACITY_BYTES.to_owned(),
                self.capacity_bytes.to_string(),
            ),
            (KEY_STATE.to_owned(), NodeState::Active.as_str().to_owned()),
        ];
        if let Some(addr) = self.advertise_addr {
            kvs.push((KEY_ADVERTISE_ADDR.to_owned(), addr.to_string()));
        }
        if let Some(addr) = self.admin_addr {
            kvs.push((KEY_ADMIN_ADDR.to_owned(), addr.to_string()));
        }
        kvs
    }
}

/// A running gossip node and the live ring it feeds.
pub struct ClusterAgent {
    /// The chitchat server handle (owns the gossip task).
    handle: ChitchatHandle,
    /// The ring the background task keeps in step with membership.
    ring: LiveRing,
    /// The latest membership snapshot, swapped by the background task in the
    /// same step it updates the ring so `members()` and `ring` never disagree.
    members: Arc<ArcSwap<Vec<NodeMeta>>>,
    /// The membership-watch task; aborted on shutdown.
    watcher_task: JoinHandle<()>,
    /// This node's identity.
    node_id: NodeId,
}

impl ClusterAgent {
    /// Spawns gossip over `transport` (real `UdpTransport` in the server, an
    /// in-process `ChannelTransport` in tests) and starts the membership-watch
    /// task. The ring begins as a cluster-of-one (self only) and widens as
    /// peers are discovered.
    pub async fn spawn(
        config: AgentConfig,
        transport: &dyn Transport,
    ) -> Result<Self, ClusterError> {
        let node_id = NodeId::new(config.node_id.as_str());
        let chitchat_id = ChitchatId::new(
            config.node_id.clone(),
            config.generation,
            config.gossip_addr,
        );
        // Failure-detection tuning: the detector accrues against the suspicion
        // window until real heartbeat samples (at gossip_interval) refine it,
        // so a silent node is flagged within a small multiple of `suspicion`.
        let failure_detector_config = FailureDetectorConfig::new(
            8.0,
            1_000,
            config.suspicion.max(config.gossip_interval),
            config.suspicion,
            config.suspicion,
        );
        let chitchat_config = chitchat::ChitchatConfig {
            chitchat_id,
            cluster_id: config.pod_id.clone(),
            gossip_interval: config.gossip_interval,
            listen_addr: config.gossip_addr,
            seed_nodes: config.seeds.clone(),
            failure_detector_config,
            marked_for_deletion_grace_period: Duration::from_secs(3_600),
            catchup_callback: None,
            extra_liveness_predicate: None,
        };

        let handle = spawn_chitchat(chitchat_config, config.initial_key_values(), transport)
            .await
            .map_err(|e| ClusterError::Gossip {
                addr: config.gossip_addr,
                reason: e.to_string(),
            })?;

        // Seed the ring and snapshot with self, so ownership is answerable
        // immediately (before the first gossip round widens membership).
        let ring = LiveRing::single(node_id.clone(), config.capacity_bytes.max(1));
        let self_meta = NodeMeta {
            node_id: node_id.clone(),
            pod_id: config.pod_id.clone(),
            generation: config.generation,
            gossip_addr: config.gossip_addr,
            advertise_addr: config.advertise_addr,
            admin_addr: config.admin_addr,
            capacity_bytes: config.capacity_bytes,
            state: NodeState::Active,
        };
        let members = Arc::new(ArcSwap::from_pointee(vec![self_meta]));

        let watcher = handle.with_chitchat(|c| c.live_nodes_watcher()).await;
        let watcher_task = tokio::spawn(watch_membership(
            watcher,
            ring.clone(),
            Arc::clone(&members),
            config.pod_id.clone(),
            node_id.clone(),
        ));

        Ok(ClusterAgent {
            handle,
            ring,
            members,
            watcher_task,
            node_id,
        })
    }

    /// Spawns gossip over the real UDP transport — the server's entry point.
    pub async fn spawn_udp(config: AgentConfig) -> Result<Self, ClusterError> {
        Self::spawn(config, &UdpTransport).await
    }

    /// A clone of the live ring handle to hand to the cache engine. The engine
    /// and agent share one underlying ring, so membership updates the agent
    /// applies are visible to the engine's ownership lookups at once.
    pub fn ring(&self) -> LiveRing {
        self.ring.clone()
    }

    /// This node's pod-wide identity.
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// The current live membership view (self plus every peer the failure
    /// detector considers alive), consistent with the ring. Reads a lock-free
    /// snapshot — cheap enough to call from an admin request handler.
    pub fn members(&self) -> Vec<NodeMeta> {
        self.members.load().as_ref().clone()
    }

    /// A peer-address resolver over this agent's live membership, for the
    /// peer-fetch client (#29). It reads the same lock-free snapshot the
    /// watcher swaps, so the addresses it returns follow membership changes
    /// with no extra wiring.
    pub fn resolver(&self) -> Arc<dyn crate::peer::PeerResolver> {
        Arc::new(crate::peer::GossipResolver {
            members: Arc::clone(&self.members),
        })
    }

    /// Publishes a new lifecycle state for this node over gossip (#31).
    ///
    /// Peers observe the change on their next gossip round and the ring
    /// reroutes: entering [`NodeState::Draining`] sheds this node's ownership to
    /// its successors while it keeps serving as a donor (its `local_block`
    /// endpoint stays open via [`crate::ring::LiveRing`]'s `should_serve_peer`).
    /// This node's own membership watcher sees the change too, so it stops
    /// counting itself an owner. Awaiting returns once the self key-value is set
    /// locally; propagation is asynchronous by gossip's nature.
    pub async fn set_state(&self, state: NodeState) {
        self.handle
            .with_chitchat(|chitchat| {
                chitchat.self_node_state().set(KEY_STATE, state.as_str());
            })
            .await;
    }

    /// Stops the membership-watch task and shuts gossip down cleanly (peers
    /// learn of the departure faster than by failure detection).
    pub async fn shutdown(self) {
        self.watcher_task.abort();
        let _ = self.handle.shutdown().await;
    }
}

/// Rebuilds the member view and ring on every membership change chitchat
/// reports, and logs joins/departures.
///
/// Runs until the watch channel closes (the chitchat server stopped). Each
/// change: derive the live members from the watched map, update the ring
/// (dropping `dead`-stated nodes from ownership), then publish the snapshot —
/// ring first so a reader that sees the new `members()` also sees the rerouted
/// ring.
async fn watch_membership(
    mut watcher: watch::Receiver<BTreeMap<ChitchatId, ChitchatNodeState>>,
    ring: LiveRing,
    members: Arc<ArcSwap<Vec<NodeMeta>>>,
    pod_id: String,
    self_id: NodeId,
) {
    let mut known: BTreeSet<String> = BTreeSet::new();
    loop {
        {
            let map = watcher.borrow_and_update().clone();
            let metas: Vec<NodeMeta> = map
                .iter()
                .map(|(id, state)| NodeMeta::from_gossip(id, state, &pod_id))
                .collect();

            // A draining node (#31) stays in the ring as a warm donor but owns
            // nothing (its keys move to successors); every other non-dead node
            // owns its weighted share. Dead nodes leave the ring entirely.
            let ring_members: Vec<RingMember> = metas
                .iter()
                .filter(|m| m.state != NodeState::Dead)
                .map(|m| {
                    if m.state == NodeState::Draining {
                        RingMember::draining(m.node_id.clone(), m.capacity_bytes)
                    } else {
                        RingMember::active(m.node_id.clone(), m.capacity_bytes)
                    }
                })
                .collect();
            // An empty set can only mean the watcher briefly reported no live
            // nodes; keep serving the last membership rather than emptying the
            // ring (update rejects empty anyway).
            let _ = ring.update_states(ring_members);

            log_transitions(&self_id, &mut known, &metas);
            members.store(Arc::new(metas));
        }
        if watcher.changed().await.is_err() {
            break;
        }
    }
}

/// Logs the joins and departures between the last known member set and the
/// current one, from this node's point of view.
fn log_transitions(self_id: &NodeId, known: &mut BTreeSet<String>, metas: &[NodeMeta]) {
    let current: BTreeSet<String> = metas
        .iter()
        .map(|m| m.node_id.as_str().to_owned())
        .collect();
    for joined in current.difference(known) {
        eprintln!("verglas-cluster [{self_id}] member joined: {joined}");
    }
    for left in known.difference(&current) {
        eprintln!("verglas-cluster [{self_id}] member left: {left}");
    }
    *known = current;
}
