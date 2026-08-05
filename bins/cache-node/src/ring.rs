//! Shared fragment-ring wiring for block FLUSH and Neon WAL (#13/#382).
//!
//! The cache node's object serving path is a deliberate cluster-of-one (see
//! [`crate::serve`]), but block FLUSH and the embedded safekeeper both write over
//! the fleet ring. This module constructs their one shared transport,
//! membership view, fragment store, and listener. It:
//!
//! - learns the ring from the environment (the same env-driven shape the block
//!   NBD/S3 addresses use — no new config-file schema);
//! - builds the flush plane ([`RingWriteback`]) over the SAME chunk store the
//!   device registry stages into (the plane's barrier and the devices' staging
//!   must be one store);
//! - serves the fragment RPC endpoints peers place shards through, on the ring
//!   plane's own listener — bound like `:8333`/`:8335`, with no new authz in v1;
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
use std::sync::Arc;
use std::time::Duration;

use verglas_block::{
    FragmentTransport, LiveMembership, LocalFragmentStore, PeerFragmentTransport, RingWriteback,
};
use verglas_cluster::peer::{
    FragmentHandlers, FragmentShardStream, LocalBlockFn, PeerResolver, PeerServer,
};
use verglas_cluster::{FragmentClient, FragmentIoError, FragmentKey, FragmentRecord};
use verglas_core::node::NodeId;

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
    /// Shared local/peer fragment transport.
    transport: Arc<dyn FragmentTransport>,
    /// Shared view of live fragment holders.
    membership: Arc<dyn LiveMembership>,
    /// The fragment RPC listener peers place shards through.
    _peer_server: PeerServer,
    /// The background takeover loop.
    _takeover: tokio::task::JoinHandle<()>,
}

impl RingPlane {
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
}

/// Wires the block-flush write-back plane onto `registry` from the environment,
/// or returns `None` for a single-node deployment (no ring peers), leaving the
/// block tier on the synchronous barrier. Called once at startup, before serving.
///
/// `secret` is the shared cluster secret to honour on the fragment plane, if the
/// env sets one; v1 requires none (VXLAN isolation, mirroring the NBD plane).
pub async fn setup(
    cache_dir: &std::path::Path,
    registry: &DeviceRegistry,
) -> Result<Option<RingPlane>, Box<dyn std::error::Error>> {
    let peers = match env_var("VERGLAS_RING_PEERS") {
        Some(raw) => parse_peers(&raw),
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

    // The fragment store this node holds block and WAL ring shards in — kept
    // under its own subdir so it never collides with the read cache.
    let local = LocalFragmentStore::new(cache_dir.join("fragment-ring"));

    // The peer RPC client + transport: self-directed placements go to the local
    // store, everything else over the fragment RPC to the resolved peer address.
    let resolver: Arc<dyn PeerResolver> = Arc::new(RingResolver::new(&peers));
    let client = FragmentClient::new(
        resolver,
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

    // The plane MUST code over the same chunk store the registry stages into.
    let ring = RingWriteback::new(
        Arc::clone(&transport),
        Arc::clone(&membership),
        local.clone(),
        registry.chunk_store(),
    );
    registry.attach_ring(Arc::clone(&ring));

    // Serve the fragment endpoints peers place shards through. The block-fetch
    // source is a no-op — the ring plane serves only fragments.
    let ring_addr: SocketAddr = env_var("VERGLAS_RING_ADDR")
        .unwrap_or_else(|| DEFAULT_RING_ADDR.to_owned())
        .parse()?;
    let peer_server = PeerServer::bind_with_fragments(
        ring_addr,
        secret,
        noop_block_source(),
        fragment_handlers(local),
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
        transport,
        membership,
        _peer_server: peer_server,
        _takeover: takeover,
    }))
}

/// Parses `VERGLAS_RING_PEERS` — `id=host:port` entries, comma-separated —
/// skipping any malformed entry with a warning rather than failing startup.
fn parse_peers(raw: &str) -> Vec<(NodeId, SocketAddr)> {
    let mut peers = Vec::new();
    for entry in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        match entry.split_once('=') {
            Some((id, addr)) => match addr.trim().parse::<SocketAddr>() {
                Ok(addr) => peers.push((NodeId::new(id.trim()), addr)),
                Err(error) => eprintln!(
                    "verglas-cache-node {VERSION} ignoring malformed ring peer `{entry}`: {error}"
                ),
            },
            None => eprintln!(
                "verglas-cache-node {VERSION} ignoring malformed ring peer `{entry}` (want id=host:port)"
            ),
        }
    }
    peers
}

/// An environment variable, empty treated as absent.
fn env_var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.trim().is_empty())
}

/// A no-op block-fetch source: the ring plane serves only fragment endpoints, so
/// every block request is a clean miss.
fn noop_block_source() -> LocalBlockFn {
    Arc::new(|_block| Box::pin(async { None }))
}

/// Wires the local fragment store behind the peer server's fragment handlers, so
/// a peer coordinator can place, load, delete, and headroom-check block-flush
/// shards on this node. Mirrors verglas-server's object-tier `fragment_handlers`.
fn fragment_handlers(store: LocalFragmentStore) -> FragmentHandlers {
    let store_put = store.clone();
    let store_stream = store.clone();
    let store_get = store.clone();
    let store_del = store.clone();
    let store_room = store;
    FragmentHandlers {
        store: Arc::new(move |record: FragmentRecord| {
            let store = store_put.clone();
            Box::pin(async move { store.store_fragment(&record) })
        }),
        store_stream: Arc::new(move |key: FragmentKey, shards| {
            let store = store_stream.clone();
            Box::pin(async move { stream_into_store(&store, &key, shards).await })
        }),
        load: Arc::new(move |key: FragmentKey| {
            let store = store_get.clone();
            Box::pin(async move { store.load_fragment(&key) })
        }),
        delete: Arc::new(move |key: FragmentKey| {
            let store = store_del.clone();
            Box::pin(async move { store.delete_fragment(&key) })
        }),
        headroom: Arc::new(move |bytes: u64| {
            let store = store_room.clone();
            Box::pin(async move { store.has_headroom(bytes) })
        }),
    }
}

/// Streams a fragment's shards into `store`, committing once the stream ends. A
/// budget refusal or IO error aborts the write (the temp file is cleaned on
/// drop), so the peer answers 500 and the coordinator counts it against quorum.
async fn stream_into_store(
    store: &LocalFragmentStore,
    key: &FragmentKey,
    mut shards: FragmentShardStream,
) -> Result<(), FragmentIoError> {
    use futures::StreamExt;
    let mut writer = store.open_fragment(key)?;
    while let Some(shard) = shards.next().await {
        writer.append(&shard)?;
    }
    writer.commit()
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
