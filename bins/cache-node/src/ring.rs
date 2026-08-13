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
//! With one configured node, this module supplies the embedded safekeeper's
//! local staging transport but leaves the block tier on its synchronous origin
//! barrier. Three or more nodes also enable erasure-coded block write-back.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use verglas_block::{
    FragmentTransport, LiveMembership, LocalFragmentStore, PeerFragmentTransport, RingWriteback,
};
use verglas_cluster::peer::{
    FragmentHandlers, FragmentShardStream, LocalBlockFn, LocalBlockStoreFn, PeerClient,
    PeerResolver, PeerServer,
};
use verglas_cluster::{FragmentClient, FragmentIoError, FragmentKey, FragmentRecord};
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
/// or returns `None` when no node membership is configured. One member leaves
/// the block tier on the synchronous origin barrier while still providing the
/// embedded safekeeper transport. Called once at startup, before serving.
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
        Some(raw) => resolve_peers(&raw).await,
        None => Vec::new(),
    };
    if peers.is_empty() {
        eprintln!(
            "verglas-cache-node {VERSION} fragment plane disabled: VERGLAS_RING_PEERS is empty"
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

    // The plane MUST code over the same chunk store the registry stages into.
    let block_ring = if peers.len() >= 3 {
        let ring = RingWriteback::new(
            Arc::clone(&transport),
            Arc::clone(&membership),
            local.clone(),
            registry.chunk_store(),
        );
        registry.attach_ring(Arc::clone(&ring));
        Some(ring)
    } else {
        None
    };

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
    let peer_server = PeerServer::bind_with_store_and_fragments(
        ring_addr,
        secret,
        source,
        store,
        fragment_handlers(local.clone(), activity),
    )
    .await?;
    eprintln!(
        "verglas-cache-node {VERSION} fragment plane listening on http://{} ({} nodes)",
        peer_server.local_addr(),
        peers.len()
    );

    // Complete any drain a crashed originator left behind, on a fixed interval.
    let takeover = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(TAKEOVER_INTERVAL);
        loop {
            ticker.tick().await;
            if let Some(ring) = &block_ring {
                ring.run_takeover_pass().await;
            }
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
        _peer_server: peer_server,
        _takeover: takeover,
    }))
}

/// Parses `VERGLAS_RING_PEERS` — `id=host:port` entries, comma-separated —
/// skipping any malformed entry with a warning rather than failing startup.
async fn resolve_peers(raw: &str) -> Vec<(NodeId, SocketAddr)> {
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
                            eprintln!(
                                "verglas-cache-node {VERSION} ignoring unresolvable ring peer `{entry}`: {error}"
                            );
                            None
                        }
                    },
                };
                if let Some(addr) = resolved {
                    peers.push((NodeId::new(id.trim()), addr));
                }
            }
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

/// Wires the local fragment store behind the peer server's fragment handlers, so
/// a peer coordinator can place, load, delete, and headroom-check block-flush
/// shards on this node. Mirrors verglas-server's object-tier `fragment_handlers`.
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
                store.append_batch(&[record])
            })
        }),
        store_stream: Arc::new(move |key: FragmentKey, shards| {
            let store = store_stream.clone();
            let activity = stream_activity.clone();
            Box::pin(async move {
                let _guard = activity
                    .try_begin(ActivityPlane::Fragment)
                    .map_err(|_| FragmentIoError::Io("cache node is fenced".to_owned()))?;
                stream_into_store(&store, &key, shards).await
            })
        }),
        load: Arc::new(move |key: FragmentKey| {
            let store = store_get.clone();
            let activity = load_activity.clone();
            Box::pin(async move {
                let _guard = activity
                    .try_begin(ActivityPlane::Fragment)
                    .map_err(|_| FragmentIoError::Io("cache node is fenced".to_owned()))?;
                store.load_fragment(&key)
            })
        }),
        delete: Arc::new(move |key: FragmentKey| {
            let store = store_del.clone();
            let activity = delete_activity.clone();
            Box::pin(async move {
                let _guard = activity
                    .try_begin(ActivityPlane::Fragment)
                    .map_err(|_| FragmentIoError::Io("cache node is fenced".to_owned()))?;
                store.delete_fragment(&key)
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
                store
                    .list_fragment_keys()
                    .into_iter()
                    .filter(|key| key.object_id.starts_with(&prefix))
                    .collect()
            })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn ring_peers_accept_ip_addresses_and_dns_names() {
        let peers = resolve_peers("cache-0=127.0.0.1:8336,cache-1=localhost:8337").await;

        assert_eq!(peers.len(), 2);
        assert_eq!(peers[0].0, NodeId::new("cache-0"));
        assert_eq!(peers[1].0, NodeId::new("cache-1"));
        assert_eq!(peers[1].1.port(), 8337);
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
