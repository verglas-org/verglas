//! Shared fragment-ring wiring for block FLUSH and Neon WAL (#13/#382).
//!
//! The cache node's read ownership is a deliberate cluster-of-one (see
//! [`crate::serve`]), but object PUT, block FLUSH, and the embedded safekeeper
//! write over the fleet ring. This module constructs their one shared transport,
//! membership view, fragment store, and listener. It:
//!
//! - learns the ring from the environment (the same env-driven shape the block
//!   NBD/S3 addresses use — no new config-file schema);
//! - builds the flush plane ([`RingWriteback`]) over the SAME chunk store the
//!   device registry stages into (the plane's barrier and the devices' staging
//!   must be one store);
//! - serves cache-owner reads, materialized-page placements, and fragment RPC
//!   through the ring plane's listener — bound like `:8333`/`:8335`, with no
//!   new authz in v1;
//!   tenant isolation is the existing VXLAN model, exactly as the NBD plane's
//!   stance (a shared cluster secret is honoured if the env sets one, but none is
//!   required);
//! - runs the takeover pass that completes a drain a crashed originator left
//!   behind.
//!
//! With no ring configured (`VERGLAS_RING_PEERS` unset or naming fewer than three
//! nodes) this module does nothing and the block tier stays single-node: FLUSH is
//! the synchronous R2 barrier, byte-identical to before the write-back plane
//! existed. That is topology-driven, not a config knob.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use bytes::Bytes;
use sha2::Digest;
use verglas_block::{
    FragmentTransport, LiveMembership, LocalFragmentStore, PeerFragmentTransport, RingWriteback,
};
use verglas_cluster::peer::{
    FragmentHandlers, FragmentShardStream, LocalBlockFn, LocalBlockStoreFn, PeerClient,
    PeerResolver, PeerServer,
};
use verglas_cluster::{
    FragmentClient, FragmentIoError, FragmentKey, FragmentRecord, LoadedFragment,
};
use verglas_cluster::{RaftRpcRegistry, StaticRaftAddressResolver};
use verglas_consensus::{PayloadError, PayloadRepresentation, RepresentationTransport, RequestId};
use verglas_core::activity::{ActivityPlane, ActivityTracker};
use verglas_core::node::NodeId;
use verglas_core::ring::RendezvousRing;

use crate::VERSION;
use crate::blockdev::DeviceRegistry;

/// The default fragment-plane listen address. The ring's boxes place block-flush
/// shards on this node here; bound like the S3/NBD planes, overridable with
/// `VERGLAS_RING_ADDR`. Not a config knob — one fixed port, one listener.
const DEFAULT_RING_ADDR: &str = "0.0.0.0:8336";

/// How often each node scans its held drain descriptors for one past its lease
/// and completes the drain a crashed originator left behind. A fixed constant,
/// the same shape as the object tier's repair/scrub intervals.
const TAKEOVER_INTERVAL: Duration = Duration::from_secs(5);

/// The handles that keep the ring plane alive for the process lifetime: the
/// fragment peer server and the takeover loop. Dropping them stops the plane.
pub struct RingPlane {
    /// Stable identity of this cache node in the ring.
    self_id: NodeId,
    /// Read-cache ownership shared by every node in this static fleet ring.
    read_ring: RendezvousRing,
    /// Peer transport used for owner lookups and materialized-page placement.
    read_client: PeerClient,
    /// Shared local/peer fragment transport.
    transport: Arc<dyn FragmentTransport>,
    /// Shared view of live fragment holders.
    membership: Arc<dyn LiveMembership>,
    /// One live ceiling for block, WAL, and object fragments. The disk monitor
    /// grants only unused cache capacity; reducing it never evicts dirty data.
    fragment_ceiling: Arc<AtomicU64>,
    /// Local fragments currently protected by durability state.
    local: LocalFragmentStore,
    /// Multi-Raft RPC registry served on the shared peer listener.
    raft_registry: RaftRpcRegistry,
    /// Explicit committed-voter address map; gossip never changes it.
    raft_addresses: StaticRaftAddressResolver,
    /// Shared secret presented by internal Raft clients.
    raft_secret: String,
    /// Shared payload peer identities and coding slots for committed voter allocations.
    payload_peers: Arc<RwLock<PayloadPeers>>,
    /// The fragment RPC listener peers place shards through.
    _peer_server: PeerServer,
    /// The background takeover loop.
    _takeover: tokio::task::JoinHandle<()>,
}

impl RingPlane {
    /// Returns this process's stable read-cache identity.
    pub fn node_id(&self) -> NodeId {
        self.self_id.clone()
    }

    /// Returns the static rendezvous ownership map for ordinary cache data.
    pub fn read_ring(&self) -> RendezvousRing {
        self.read_ring.clone()
    }

    /// Returns the cache-only peer transport.
    pub fn read_client(&self) -> PeerClient {
        self.read_client.clone()
    }

    /// Returns the fragment transport shared by block FLUSH and WAL.
    pub fn transport(&self) -> Arc<dyn FragmentTransport> {
        Arc::clone(&self.transport)
    }

    /// Returns the live ring view shared by block FLUSH and WAL.
    pub fn membership(&self) -> Arc<dyn LiveMembership> {
        Arc::clone(&self.membership)
    }

    /// Returns a deterministic Neon numeric id derived from the fleet node id.
    pub fn safekeeper_id(&self) -> u64 {
        self.self_id
            .as_str()
            .bytes()
            .fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
                (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
            })
    }

    /// Returns the current number of configured live fragment holders.
    pub fn node_count(&self) -> usize {
        self.membership.live_nodes().len()
    }

    /// Returns the local registry used when creating independent Raft groups.
    pub fn raft_registry(&self) -> RaftRpcRegistry {
        self.raft_registry.clone()
    }

    /// Returns the explicit numeric voter address resolver.
    pub fn raft_addresses(&self) -> StaticRaftAddressResolver {
        self.raft_addresses.clone()
    }

    /// Registers an explicit peer address before that node is admitted as a Raft learner.
    pub fn register_raft_address(
        &self,
        voter: u64,
        address: std::net::SocketAddr,
    ) -> Result<(), &'static str> {
        self.raft_addresses.register(voter, address)
    }

    /// Registers a candidate for fragment representation transfer before learner catch-up.
    pub fn register_payload_peer(&self, voter: u64, node_id: String) -> Result<(), &'static str> {
        self.validate_voter_identity(voter, &node_id)?;
        self.payload_peers
            .write()
            .map_err(|_| "payload peer map is poisoned")?
            .nodes
            .insert(voter, NodeId::new(node_id));
        Ok(())
    }

    /// Atomically publishes slots for a committed uniform voter allocation.
    pub fn set_payload_voters(&self, voters: &[u64]) -> Result<(), &'static str> {
        let mut peers = self
            .payload_peers
            .write()
            .map_err(|_| "payload peer map is poisoned")?;
        if voters.iter().any(|voter| !peers.nodes.contains_key(voter)) {
            return Err("payload voter lacks node identity");
        }
        peers.slots = voters
            .iter()
            .enumerate()
            .map(|(slot, voter)| (*voter, slot))
            .collect();
        Ok(())
    }

    /// Validates a stable node identity against its supplied Raft voter id.
    pub fn validate_voter_identity(&self, voter: u64, node_id: &str) -> Result<(), &'static str> {
        if node_id.is_empty() || numeric_node_id(&NodeId::new(node_id)) != voter {
            return Err("candidate node identity does not match voter id");
        }
        Ok(())
    }

    /// Returns the internal peer secret used by consensus RPCs.
    pub fn raft_secret(&self) -> &str {
        &self.raft_secret
    }

    /// Returns the explicit numeric voter ordering used for coding slots.
    pub fn consensus_voters(&self) -> Vec<u64> {
        let nodes = self.membership.live_nodes();
        canonical_voter_ids(nodes.iter())
    }

    /// Returns distributed consensus payload I/O over the existing fragment plane.
    pub fn consensus_payload_transport(&self) -> Arc<dyn RepresentationTransport> {
        Arc::new(RingPayloadTransport {
            transport: Arc::clone(&self.transport),
            peers: Arc::clone(&self.payload_peers),
        })
    }

    /// Bytes of dirty fragment data held on this node.
    pub fn fragment_used_bytes(&self) -> u64 {
        self.local.used_bytes()
    }

    /// Publishes a new admission ceiling. A ceiling below current usage only
    /// refuses new fragments; acknowledged dirty fragments are never evicted.
    pub fn set_fragment_ceiling(&self, bytes: u64) {
        self.fragment_ceiling.store(bytes, Ordering::Release);
    }
}

/// Wires the block-flush write-back plane onto `registry` from the environment,
/// or returns `None` for a single-node deployment (no ring peers), leaving the
/// block tier on the synchronous barrier. Called once at startup, before serving.
///
/// `secret` is the shared cluster secret to honour on the fragment plane, if the
/// env sets one; v1 requires none (VXLAN isolation, mirroring the NBD plane).
pub async fn setup(
    cache_dir: &std::path::Path,
    capacity_bytes: u64,
    registry: &DeviceRegistry,
    page_cache: crate::page_cache::PageCacheSlot,
    activity: ActivityTracker,
) -> Result<Option<RingPlane>, Box<dyn std::error::Error>> {
    let peers = match env_var("VERGLAS_RING_PEERS") {
        Some(raw) => resolve_peers_until_complete(&raw).await?,
        None => Vec::new(),
    };
    // A production cache ring needs at least three boxes; fewer is a
    // single-node deployment and the block tier stays on the synchronous barrier.
    if peers.len() < 3 {
        eprintln!(
            "verglas-cache-node {VERSION} fragment ring disabled: at least three VERGLAS_RING_PEERS are required"
        );
        return Ok(None);
    }

    let Some(self_id) = env_var("VERGLAS_NODE_ID").map(|s| NodeId::new(s.as_str())) else {
        eprintln!(
            "verglas-cache-node {VERSION} fragment ring disabled: VERGLAS_RING_PEERS is set but VERGLAS_NODE_ID is not — cannot tell which ring member this node is"
        );
        return Ok(None);
    };
    if !peers.iter().any(|(id, _)| *id == self_id) {
        eprintln!(
            "verglas-cache-node {VERSION} fragment ring disabled: VERGLAS_NODE_ID `{}` is not among VERGLAS_RING_PEERS",
            self_id.as_str()
        );
        return Ok(None);
    }

    let secret = env_var("VERGLAS_CLUSTER_SECRET");
    let raft_secret = secret.clone().unwrap_or_default();
    let raft_registry = RaftRpcRegistry::new(raft_secret.clone());
    let raft_addresses = StaticRaftAddressResolver::new(
        peers
            .iter()
            .map(|(node, address)| (numeric_node_id(node), *address))
            .collect(),
    );

    // The fragment store this node holds block and WAL ring shards in — kept
    // under its own subdir so it never collides with the read cache.
    let fragment_ceiling = Arc::new(AtomicU64::new(capacity_bytes));
    let local = LocalFragmentStore::with_dynamic_ceiling(
        cache_dir.join("fragment-ring"),
        Arc::clone(&fragment_ceiling),
    );
    // The peer RPC client + transport: self-directed placements go to the local
    // store, everything else over the fragment RPC to the resolved peer address.
    let resolver: Arc<dyn PeerResolver> = Arc::new(RingResolver::new(&peers));
    let read_client = PeerClient::new(
        Arc::clone(&resolver),
        secret.clone(),
        Duration::from_millis(50),
        Duration::from_millis(100),
    );
    let read_ring = RendezvousRing::new(peers.iter().map(|(id, _)| id.clone()).collect())?;
    let client = FragmentClient::new(
        Arc::clone(&resolver),
        secret.clone(),
        Duration::from_millis(500),
        Duration::from_secs(30),
    );
    let transport: Arc<dyn FragmentTransport> = Arc::new(PeerFragmentTransport::new(
        self_id.clone(),
        local.clone(),
        client,
    ));

    let membership: Arc<dyn LiveMembership> = Arc::new(StaticMembership {
        self_id: self_id.clone(),
        live: peers.iter().map(|(id, _)| id.clone()).collect(),
    });
    let payload_peers = Arc::new(RwLock::new(canonical_payload_peers(&peers)));

    // The plane MUST code over the same chunk store the registry stages into.
    let ring = RingWriteback::new(
        Arc::clone(&transport),
        Arc::clone(&membership),
        local.clone(),
        registry.chunk_store(),
    );
    registry.attach_ring(Arc::clone(&ring));

    // Serve both the clean read-cache owner protocol and the durable fragment
    // protocol on the ring listener. The deferred slot returns a clean miss or
    // placement error until cache recovery completes.
    let ring_addr: SocketAddr = env_var("VERGLAS_RING_ADDR")
        .unwrap_or_else(|| DEFAULT_RING_ADDR.to_owned())
        .parse()?;
    let source_slot = Arc::clone(&page_cache);
    let source: LocalBlockFn = Arc::new(move |block| {
        let slot = Arc::clone(&source_slot);
        Box::pin(async move {
            let engine = slot.get()?;
            engine.local_block(&block).await
        })
    });
    let store_slot = page_cache;
    let store: LocalBlockStoreFn = Arc::new(move |block, value| {
        let slot = Arc::clone(&store_slot);
        Box::pin(async move {
            let engine = slot
                .get()
                .ok_or_else(|| "cache recovery is not complete".to_owned())?;
            engine
                .put_local_materialized_block(block, value)
                .map_err(|error| error.to_string())
        })
    });
    let peer_server = PeerServer::bind_with_store_fragments_and_router(
        ring_addr,
        secret,
        source,
        store,
        fragment_handlers(local.clone(), activity),
        raft_registry.router(),
    )
    .await?;
    eprintln!(
        "verglas-cache-node {VERSION} block-ring fragment plane listening on http://{} ({} peers, RS quorum write-back)",
        peer_server.local_addr(),
        peers.len()
    );

    // Complete any drain a crashed originator left behind, on a fixed interval.
    let takeover = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(TAKEOVER_INTERVAL);
        loop {
            ticker.tick().await;
            ring.run_takeover_pass().await;
        }
    });

    Ok(Some(RingPlane {
        self_id,
        read_ring,
        read_client,
        transport,
        membership,
        fragment_ceiling,
        local,
        raft_registry,
        raft_addresses,
        raft_secret,
        payload_peers,
        _peer_server: peer_server,
        _takeover: takeover,
    }))
}

/// Maps the stable deployment node name to its consensus voter identity.
fn numeric_node_id(node: &NodeId) -> u64 {
    node.as_str()
        .bytes()
        .fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
        })
}

/// Maps consensus representations onto the cache ring's fsynced fragment RPC.
struct PayloadPeers {
    nodes: HashMap<u64, NodeId>,
    slots: HashMap<u64, usize>,
}

/// Returns the one stable numeric voter order used by Raft persistence and payload coding.
fn canonical_voter_ids<'a>(nodes: impl IntoIterator<Item = &'a NodeId>) -> Vec<u64> {
    let mut voters = nodes.into_iter().map(numeric_node_id).collect::<Vec<_>>();
    voters.sort_unstable();
    voters
}

/// Maps every peer to the slot assigned by the canonical persisted voter order.
fn canonical_payload_peers(peers: &[(NodeId, SocketAddr)]) -> PayloadPeers {
    let nodes = peers
        .iter()
        .map(|(node, _)| (numeric_node_id(node), node.clone()))
        .collect::<HashMap<_, _>>();
    let slots = canonical_voter_ids(peers.iter().map(|(node, _)| node))
        .into_iter()
        .enumerate()
        .map(|(slot, voter)| (voter, slot))
        .collect();
    PayloadPeers { nodes, slots }
}

struct RingPayloadTransport {
    transport: Arc<dyn FragmentTransport>,
    peers: Arc<RwLock<PayloadPeers>>,
}

#[async_trait::async_trait]
impl RepresentationTransport for RingPayloadTransport {
    async fn store(
        &self,
        voter: u64,
        representation: PayloadRepresentation,
    ) -> Result<(), PayloadError> {
        let node = self
            .peers
            .read()
            .map_err(|_| PayloadError::Transport("payload peer map is poisoned".to_owned()))?
            .nodes
            .get(&voter)
            .cloned()
            .ok_or(PayloadError::UnknownHolder)?;
        let group = representation.group.as_bytes();
        let mut bytes = Vec::with_capacity(97 + group.len() + representation.bytes.len());
        bytes.extend_from_slice(&(group.len() as u32).to_be_bytes());
        bytes.extend_from_slice(group);
        bytes.extend_from_slice(&representation.hash);
        bytes.extend_from_slice(&representation.configuration_generation.to_be_bytes());
        bytes.push(match representation.mode {
            verglas_consensus::ReplicationMode::Coded => 0,
            verglas_consensus::ReplicationMode::Complete => 1,
        });
        bytes.extend_from_slice(&representation.term.to_be_bytes());
        bytes.extend_from_slice(&representation.index.to_be_bytes());
        bytes.extend_from_slice(&representation.request.as_u128().to_be_bytes());
        bytes.extend_from_slice(&(representation.length as u64).to_be_bytes());
        bytes.extend_from_slice(&(representation.slot as u64).to_be_bytes());
        bytes.extend_from_slice(&representation.bytes);
        self.transport
            .place(
                &node,
                FragmentRecord::new(
                    consensus_fragment_key(
                        representation.hash,
                        &representation.group,
                        representation.configuration_generation,
                        representation.request,
                        representation.slot,
                    ),
                    Bytes::from(bytes),
                ),
            )
            .await
            .map_err(|error| PayloadError::Transport(error.to_string()))
    }

    async fn load(
        &self,
        voter: u64,
        hash: [u8; 32],
        group: &str,
        configuration_generation: u64,
        request: RequestId,
    ) -> Result<Option<PayloadRepresentation>, PayloadError> {
        let (node, slot) = {
            let peers = self
                .peers
                .read()
                .map_err(|_| PayloadError::Transport("payload peer map is poisoned".to_owned()))?;
            let node = peers
                .nodes
                .get(&voter)
                .cloned()
                .ok_or(PayloadError::UnknownHolder)?;
            let slot = *peers.slots.get(&voter).ok_or(PayloadError::UnknownHolder)?;
            (node, slot)
        };
        let loaded = self
            .transport
            .load(
                &node,
                &consensus_fragment_key(hash, group, configuration_generation, request, slot),
            )
            .await
            .map_err(|error| PayloadError::Transport(error.to_string()))?;
        loaded
            .map(|record| decode_representation(hash, record))
            .transpose()
    }

    async fn delete(
        &self,
        voter: u64,
        hash: [u8; 32],
        group: &str,
        configuration_generation: u64,
        request: RequestId,
        slot: usize,
    ) -> Result<(), PayloadError> {
        let node = self
            .peers
            .read()
            .map_err(|_| PayloadError::Transport("payload peer map is poisoned".to_owned()))?
            .nodes
            .get(&voter)
            .cloned()
            .ok_or(PayloadError::UnknownHolder)?;
        self.transport
            .delete(
                &node,
                &consensus_fragment_key(hash, group, configuration_generation, request, slot),
            )
            .await
            .map_err(|error| PayloadError::Transport(error.to_string()))
    }
}

/// Builds the immutable fragment identity shared by staging and reconstruction.
fn consensus_fragment_key(
    hash: [u8; 32],
    group: &str,
    configuration_generation: u64,
    request: RequestId,
    slot: usize,
) -> FragmentKey {
    let group_hash = sha2::Sha256::digest(group.as_bytes());
    FragmentKey {
        object_id: format!(
            "consensus/{}/{}/{}/{:032x}",
            hex::encode(hash),
            hex::encode(group_hash),
            configuration_generation,
            request.as_u128()
        ),
        index: slot,
    }
}

/// Validates one loaded ring fragment before exposing its representation.
fn decode_representation(
    hash: [u8; 32],
    loaded: LoadedFragment,
) -> Result<PayloadRepresentation, PayloadError> {
    if !loaded.is_healthy() || loaded.bytes.len() < 69 {
        return Err(PayloadError::CorruptRepresentation);
    }
    let group_len = u32::from_be_bytes(
        loaded.bytes[0..4]
            .try_into()
            .map_err(|_| PayloadError::CorruptRepresentation)?,
    ) as usize;
    let base = 4 + group_len;
    let fixed = base + 32 + 8 + 1 + 8 + 8;
    if loaded.bytes.len() < fixed + 32 {
        return Err(PayloadError::CorruptRepresentation);
    }
    let group = String::from_utf8(loaded.bytes[4..4 + group_len].to_vec())
        .map_err(|_| PayloadError::CorruptRepresentation)?;
    let stored_hash: [u8; 32] = loaded.bytes[base..base + 32]
        .try_into()
        .map_err(|_| PayloadError::CorruptRepresentation)?;
    if stored_hash != hash {
        return Err(PayloadError::CorruptRepresentation);
    }
    let configuration_generation = u64::from_be_bytes(
        loaded.bytes[base + 32..base + 40]
            .try_into()
            .map_err(|_| PayloadError::CorruptRepresentation)?,
    );
    let mode = match loaded.bytes[base + 40] {
        0 => verglas_consensus::ReplicationMode::Coded,
        1 => verglas_consensus::ReplicationMode::Complete,
        _ => return Err(PayloadError::CorruptRepresentation),
    };
    let term = u64::from_be_bytes(
        loaded.bytes[base + 41..base + 49]
            .try_into()
            .map_err(|_| PayloadError::CorruptRepresentation)?,
    );
    let index = u64::from_be_bytes(
        loaded.bytes[base + 49..base + 57]
            .try_into()
            .map_err(|_| PayloadError::CorruptRepresentation)?,
    );
    let request = RequestId::from_u128(u128::from_be_bytes(
        loaded.bytes[fixed..fixed + 16]
            .try_into()
            .map_err(|_| PayloadError::CorruptRepresentation)?,
    ));
    let length = u64::from_be_bytes(
        loaded.bytes[fixed + 16..fixed + 24]
            .try_into()
            .map_err(|_| PayloadError::CorruptRepresentation)?,
    ) as usize;
    let slot = u64::from_be_bytes(
        loaded.bytes[fixed + 24..fixed + 32]
            .try_into()
            .map_err(|_| PayloadError::CorruptRepresentation)?,
    ) as usize;
    Ok(PayloadRepresentation {
        term,
        index,
        group,
        configuration_generation,
        mode,
        hash,
        request,
        length,
        slot,
        bytes: Bytes::copy_from_slice(&loaded.bytes[fixed + 32..]),
    })
}

/// Waits for every declared peer to resolve during a bounded startup window.
/// Fly process DNS briefly removes a name while a Machine is replaced; starting
/// a smaller static ring in that window permanently changes EC geometry and can
/// advertise a safekeeper that can never acknowledge the configured quorum.
async fn resolve_peers_until_complete(
    raw: &str,
) -> Result<Vec<(NodeId, SocketAddr)>, std::io::Error> {
    const ATTEMPTS: usize = 150;
    const RETRY_DELAY: Duration = Duration::from_millis(200);

    let mut last_error = None;
    for attempt in 1..=ATTEMPTS {
        match resolve_peers(raw).await {
            Ok(peers) => return Ok(peers),
            Err(error) => {
                if attempt == 1 || attempt % 25 == 0 {
                    eprintln!(
                        "verglas-cache-node {VERSION} waiting for complete fragment ring (attempt {attempt}/{ATTEMPTS}): {error}"
                    );
                }
                last_error = Some(error);
                tokio::time::sleep(RETRY_DELAY).await;
            }
        }
    }
    Err(last_error.unwrap_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "VERGLAS_RING_PEERS contains no resolvable entries",
        )
    }))
}

/// Resolves every `id=host:port` entry in `VERGLAS_RING_PEERS` exactly once.
/// A configured member is never silently discarded: static membership and EC
/// geometry must be identical on every process in the ring.
async fn resolve_peers(raw: &str) -> Result<Vec<(NodeId, SocketAddr)>, std::io::Error> {
    let mut peers = Vec::new();
    for entry in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        match entry.split_once('=') {
            Some((id, addr)) => {
                let addr = addr.trim();
                let resolved = match addr.parse::<SocketAddr>() {
                    Ok(addr) => Some(addr),
                    Err(_) => match tokio::net::lookup_host(addr).await {
                        Ok(addrs) => {
                            let addrs: Vec<_> = addrs.collect();
                            addrs
                                .iter()
                                .find(|candidate| candidate.is_ipv4())
                                .or_else(|| addrs.first())
                                .copied()
                        }
                        Err(error) => {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::AddrNotAvailable,
                                format!("cannot resolve ring peer `{entry}`: {error}"),
                            ));
                        }
                    },
                };
                if let Some(addr) = resolved {
                    peers.push((NodeId::new(id.trim()), addr));
                }
            }
            None => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("malformed ring peer `{entry}` (want id=host:port)"),
                ));
            }
        }
    }
    Ok(peers)
}

/// An environment variable, empty treated as absent.
fn env_var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.trim().is_empty())
}

/// Wires the local fragment store behind the peer server's fragment handlers, so
/// a peer coordinator can place, load, delete, and headroom-check block-flush
/// shards on this node. Mirrors verglas-cache-node's object-tier `fragment_handlers`.
fn fragment_handlers(store: LocalFragmentStore, activity: ActivityTracker) -> FragmentHandlers {
    let store_put = store.clone();
    let store_stream = store.clone();
    let store_get = store.clone();
    let store_del = store.clone();
    let store_room = store.clone();
    let store_list = store;
    let store_activity = activity.clone();
    let stream_activity = activity.clone();
    let load_activity = activity.clone();
    let delete_activity = activity.clone();
    let room_activity = activity.clone();
    let list_activity = activity;
    FragmentHandlers {
        store: Arc::new(move |record: FragmentRecord| {
            let store = store_put.clone();
            let activity = store_activity.clone();
            Box::pin(async move {
                let _guard = activity
                    .try_begin(ActivityPlane::Fragment)
                    .map_err(|_| FragmentIoError::Io("cache node is fenced".to_owned()))?;
                await_fragment_store(tokio::task::spawn_blocking(move || {
                    store.store_fragment(&record)
                }))
                .await
            })
        }),
        store_stream: Arc::new(move |key: FragmentKey, shards| {
            let store = store_stream.clone();
            let activity = stream_activity.clone();
            Box::pin(async move {
                let _guard = activity
                    .try_begin(ActivityPlane::Fragment)
                    .map_err(|_| FragmentIoError::Io("cache node is fenced".to_owned()))?;
                stream_into_store(store, key, shards).await
            })
        }),
        load: Arc::new(move |key: FragmentKey| {
            let store = store_get.clone();
            let activity = load_activity.clone();
            Box::pin(async move {
                let _guard = activity
                    .try_begin(ActivityPlane::Fragment)
                    .map_err(|_| FragmentIoError::Io("cache node is fenced".to_owned()))?;
                await_fragment_store(tokio::task::spawn_blocking(move || {
                    store.load_fragment(&key)
                }))
                .await
            })
        }),
        delete: Arc::new(move |key: FragmentKey| {
            let store = store_del.clone();
            let activity = delete_activity.clone();
            Box::pin(async move {
                let _guard = activity
                    .try_begin(ActivityPlane::Fragment)
                    .map_err(|_| FragmentIoError::Io("cache node is fenced".to_owned()))?;
                await_fragment_store(tokio::task::spawn_blocking(move || {
                    store.delete_fragment(&key)
                }))
                .await
            })
        }),
        headroom: Arc::new(move |bytes: u64| {
            let store = store_room.clone();
            let activity = room_activity.clone();
            Box::pin(async move {
                let Ok(_guard) = activity.try_begin(ActivityPlane::Fragment) else {
                    return false;
                };
                store.has_headroom(bytes)
            })
        }),
        list_prefix: Arc::new(move |prefix: String| {
            let store = store_list.clone();
            let activity = list_activity.clone();
            Box::pin(async move {
                let Ok(_guard) = activity.try_begin(ActivityPlane::Fragment) else {
                    return Vec::new();
                };
                tokio::task::spawn_blocking(move || {
                    store
                        .list_fragment_keys()
                        .into_iter()
                        .filter(|key| key.object_id.starts_with(&prefix))
                        .collect()
                })
                .await
                .unwrap_or_default()
            })
        }),
    }
}

/// Streams a fragment's shards into `store`, committing once the stream ends. A
/// budget refusal or IO error aborts the write (the temp file is cleaned on
/// drop), so the peer answers 500 and the coordinator counts it against quorum.
async fn stream_into_store(
    store: LocalFragmentStore,
    key: FragmentKey,
    shards: FragmentShardStream,
) -> Result<(), FragmentIoError> {
    stream_into_store_with_commit(store, key, shards, |writer| writer.commit()).await
}

/// Relays an asynchronous upload to a bounded channel while a blocking worker
/// owns the fragment writer and its durability barrier. The HTTP handler awaits
/// the worker, so it cannot acknowledge before the file and directory fsyncs.
async fn stream_into_store_with_commit<F>(
    store: LocalFragmentStore,
    key: FragmentKey,
    mut shards: FragmentShardStream,
    commit: F,
) -> Result<(), FragmentIoError>
where
    F: FnOnce(verglas_cluster::FragmentWriter) -> Result<(), FragmentIoError> + Send + 'static,
{
    use futures::StreamExt;
    /// Distinguishes a verified end of an HTTP body from a dropped relay.
    enum UploadMessage {
        /// Carries one received body shard to the blocking writer.
        Shard(Bytes),
        /// Permits the blocking writer to make the completed fragment live.
        Complete,
        /// Stops the blocking writer and discards its temporary fragment.
        Abort(FragmentIoError),
    }

    // This is deliberately small: it bounds per-upload heap use to a few
    // stripes while the blocking worker is behind NVMe latency.
    const FRAGMENT_UPLOAD_CHANNEL_CAPACITY: usize = 4;
    let (sender, mut receiver) =
        tokio::sync::mpsc::channel::<UploadMessage>(FRAGMENT_UPLOAD_CHANNEL_CAPACITY);
    let worker = tokio::task::spawn_blocking(move || {
        let mut writer = store.open_fragment(&key)?;
        loop {
            match receiver.blocking_recv() {
                Some(UploadMessage::Shard(shard)) => writer.append(&shard)?,
                Some(UploadMessage::Complete) => return commit(writer),
                Some(UploadMessage::Abort(error)) => return Err(error),
                None => {
                    return Err(FragmentIoError::Io(
                        "fragment upload relay stopped before a complete body".to_owned(),
                    ));
                }
            }
        }
    });

    while let Some(shard) = shards.next().await {
        match shard {
            Ok(shard) => {
                if sender.send(UploadMessage::Shard(shard)).await.is_err() {
                    return Err(FragmentIoError::Io(
                        "fragment writer stopped before upload completed".to_owned(),
                    ));
                }
            }
            Err(error) => {
                if sender.send(UploadMessage::Abort(error)).await.is_err() {
                    return Err(FragmentIoError::Io(
                        "fragment writer stopped before upload completed".to_owned(),
                    ));
                }
                return await_fragment_store(worker).await;
            }
        }
    }
    if sender.send(UploadMessage::Complete).await.is_err() {
        return Err(FragmentIoError::Io(
            "fragment writer stopped before upload completed".to_owned(),
        ));
    }
    await_fragment_store(worker).await
}

/// Converts a blocking fragment-store result back into the handler's error
/// domain, including a worker panic or cancellation as a durable-write failure.
async fn await_fragment_store<T: Send + 'static>(
    task: tokio::task::JoinHandle<Result<T, FragmentIoError>>,
) -> Result<T, FragmentIoError> {
    task.await
        .map_err(|error| FragmentIoError::Io(format!("fragment storage worker stopped: {error}")))?
}

/// Resolves a ring node id to its fragment-plane address from the static peer
/// map. A `None` (unknown node) makes the fragment RPC fail, which the flush
/// coordinator counts against quorum — never a silent wrong placement.
struct RingResolver {
    /// The node → fragment-plane address map.
    map: HashMap<NodeId, SocketAddr>,
}

impl RingResolver {
    /// Builds a resolver over the parsed peer list.
    fn new(peers: &[(NodeId, SocketAddr)]) -> Self {
        Self {
            map: peers.iter().cloned().collect(),
        }
    }
}

impl PeerResolver for RingResolver {
    fn resolve(&self, node: &NodeId) -> Option<SocketAddr> {
        self.map.get(node).copied()
    }
}

/// A static live-membership view over the configured ring peers. The fleet is a
/// fixed set of boxes, so membership is the parsed peer list — no gossip failure
/// detector in v1. A down peer is discovered when its shard placement RPC fails,
/// which drops the flush to the synchronous barrier for that FLUSH; a
/// failure-detector-driven live view (excluding a dead box up front) is the
/// obvious refinement and an extension point, not built here.
struct StaticMembership {
    /// This node's id.
    self_id: NodeId,
    /// Every ring member.
    live: Vec<NodeId>,
}

impl LiveMembership for StaticMembership {
    fn self_id(&self) -> NodeId {
        self.self_id.clone()
    }

    fn live_nodes(&self) -> Vec<NodeId> {
        self.live.clone()
    }

    fn epoch(&self) -> u64 {
        // Static membership never changes within a run.
        1
    }

    fn is_single_node(&self) -> bool {
        self.live.len() <= 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A slow fragment commit must not monopolize the async runtime that also
    /// serves Raft votes and health checks.
    #[tokio::test(flavor = "current_thread")]
    async fn fragment_commit_leaves_the_runtime_responsive() {
        let directory = tempfile::tempdir().expect("fragment directory");
        let store = LocalFragmentStore::new(directory.path());
        let key = FragmentKey {
            object_id: "slow-durable-fragment".to_owned(),
            index: 0,
        };
        let (commit_started, started) = tokio::sync::oneshot::channel();
        let upload = tokio::spawn(stream_into_store_with_commit(
            store,
            key,
            Box::pin(futures::stream::iter([Ok(bytes::Bytes::from_static(
                b"fragment",
            ))])),
            move |writer| {
                let _ = commit_started.send(());
                std::thread::sleep(Duration::from_millis(200));
                writer.commit()
            },
        ));

        started.await.expect("blocking commit started");
        assert!(
            !upload.is_finished(),
            "the HTTP upload must still wait for its durable commit"
        );
        tokio::time::timeout(Duration::from_millis(50), tokio::task::yield_now())
            .await
            .expect("a Raft vote or health handler remains schedulable");
        upload
            .await
            .expect("upload task")
            .expect("durable fragment commit");
    }

    /// Cancelling a body relay after one received shard must drop its temporary
    /// writer instead of turning the relay's closed channel into a commit.
    #[tokio::test]
    async fn cancelled_fragment_stream_discards_its_partial_writer() {
        use futures::StreamExt;

        let directory = tempfile::tempdir().expect("fragment directory");
        let store = LocalFragmentStore::new(directory.path());
        let key = FragmentKey {
            object_id: "cancelled-fragment-stream".to_owned(),
            index: 0,
        };
        let upload = tokio::spawn(stream_into_store(
            store.clone(),
            key.clone(),
            Box::pin(
                futures::stream::iter([Ok(bytes::Bytes::from_static(b"partial"))])
                    .chain(futures::stream::pending()),
            ),
        ));
        tokio::time::sleep(Duration::from_millis(20)).await;
        upload.abort();
        let _ = upload.await;

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if store.used_bytes() == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("cancelled relay releases its fragment reservation");
        assert!(
            store
                .load_fragment(&key)
                .expect("inspect fragment")
                .is_none(),
            "a cancelled relay must not publish a partial fragment"
        );
    }

    #[test]
    fn consensus_fragment_identity_separates_equal_bodies_from_distinct_requests() {
        let hash = [7; 32];
        let first = consensus_fragment_key(hash, "warehouse/a", 1, RequestId::from_u128(1), 2);
        let retry = consensus_fragment_key(hash, "warehouse/a", 1, RequestId::from_u128(1), 2);
        let distinct = consensus_fragment_key(hash, "warehouse/a", 1, RequestId::from_u128(2), 2);
        let reconfigured =
            consensus_fragment_key(hash, "warehouse/a", 2, RequestId::from_u128(1), 2);

        assert_eq!(first, retry);
        assert_ne!(first, distinct);
        assert_ne!(first, reconfigured);
    }

    /// Restored Raft voters and live ring peers must assign every node the same
    /// payload slot regardless of the order in which Fly supplied peer names.
    #[test]
    fn consensus_payload_slots_use_canonical_voter_order() {
        let peers = vec![
            (
                NodeId::new("stack-cache-0"),
                "127.0.0.1:8336".parse().expect("address"),
            ),
            (
                NodeId::new("stack-cache-1"),
                "127.0.0.1:8337".parse().expect("address"),
            ),
            (
                NodeId::new("stack-cache-2"),
                "127.0.0.1:8338".parse().expect("address"),
            ),
            (
                NodeId::new("stack-cache-3"),
                "127.0.0.1:8339".parse().expect("address"),
            ),
        ];
        let payload_peers = canonical_payload_peers(&peers);
        let mut restored_voters = peers
            .iter()
            .map(|(node, _)| numeric_node_id(node))
            .collect::<Vec<_>>();
        restored_voters.sort_unstable();

        assert_eq!(
            canonical_voter_ids(peers.iter().map(|(node, _)| node)),
            restored_voters
        );
        for (slot, voter) in restored_voters.into_iter().enumerate() {
            assert_eq!(payload_peers.slots.get(&voter), Some(&slot));
        }
    }

    #[tokio::test]
    async fn ring_peers_accept_ip_addresses_and_dns_names() {
        let peers = resolve_peers("cache-0=127.0.0.1:8336,cache-1=localhost:8337")
            .await
            .expect("complete ring resolves");

        assert_eq!(peers.len(), 2);
        assert_eq!(peers[0].0, NodeId::new("cache-0"));
        assert_eq!(peers[1].0, NodeId::new("cache-1"));
        assert_eq!(peers[1].1.port(), 8337);
    }

    #[tokio::test]
    async fn ring_peers_reject_an_incomplete_topology() {
        let error = resolve_peers(
            "cache-0=127.0.0.1:8336,cache-1=missing.invalid:8336,cache-2=127.0.0.1:8338",
        )
        .await
        .expect_err("an unresolved declared member must not be discarded");

        assert!(error.to_string().contains("cache-1"), "error={error}");
    }

    #[tokio::test]
    async fn admission_fence_rejects_new_fragment_placements() {
        let dir = tempfile::tempdir().expect("fragment dir");
        let activity = ActivityTracker::new();
        let _generation = activity.fence();
        let handlers = fragment_handlers(LocalFragmentStore::new(dir.path()), activity.clone());
        let record = FragmentRecord::new(
            FragmentKey {
                object_id: "fenced-object".to_owned(),
                index: 0,
            },
            bytes::Bytes::from_static(b"fragment"),
        );

        let error = (handlers.store)(record)
            .await
            .expect_err("fenced placement");
        assert!(error.to_string().contains("fenced"));
        assert_eq!(activity.snapshot().accepted, 0);
        assert!(activity.snapshot().idle);
    }
}
