//! Ring write-back acceptance tests (the block-flush erasure-coded write-back).
//!
//! These map to the pipeline's acceptance criteria:
//! - a single-node ring degenerates to the synchronous origin barrier;
//! - a multi-node FLUSH acks on a ring quorum, not on an origin round-trip (proven
//!   with a failing backend that cannot serve the ack);
//! - an originator crash mid-drain is completed by a peer that reconstructs the
//!   sealed flush from the surviving fragments (one shard lost) and finishes the
//!   origin barrier, committing the manifest version exactly once;
//! - a ring that cannot place its quorum falls back to the synchronous barrier.
//!
//! The transport is faked over real per-node [`LocalFragmentStore`]s (not an
//! in-memory map), so the takeover pass exercises the true descriptor
//! enumeration path. The origin backend is a controllable in-memory
//! [`ObjectBackend`] whose PUTs can be made to fail on demand.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use verglas_block::chunk::{CHUNK_SIZE_BYTES, ChunkHash};
use verglas_block::{
    BackendError, ChunkStore, FragmentTransport, LiveMembership, LocalFragmentStore, Manifest,
    ObjectBackend, RingWriteback, TransportError,
};
use verglas_cluster::fragments::{FragmentIoError, FragmentKey, FragmentRecord, LoadedFragment};
use verglas_core::node::NodeId;
use verglas_writeback::transport::ShardStream;

// ---- controllable in-memory origin backend --------------------------------------

/// An in-memory object backend whose PUTs can be toggled to fail, standing in
/// for the origin during the ack-vs-drain window. GETs and existence checks always work,
/// so a drain that runs while PUTs fail simply cannot commit.
#[derive(Default)]
struct FlakyBackend {
    /// The stored objects.
    objects: Mutex<HashMap<String, Bytes>>,
    /// When true, every PUT fails.
    fail_puts: AtomicBool,
    /// Count of successful PUTs, for asserting the drain actually ran.
    puts: AtomicUsize,
}

impl FlakyBackend {
    /// A fresh backend that accepts PUTs.
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Toggles whether PUTs fail.
    fn set_fail(&self, fail: bool) {
        self.fail_puts.store(fail, Ordering::SeqCst);
    }

    /// The committed manifest version for `device_id`, read from its `latest`
    /// pointer — `None` when the device has no committed manifest.
    fn latest_version(&self, device_id: &str) -> Option<u64> {
        let objects = self.objects.lock().expect("lock");
        let pointer = objects.get(&format!("manifests/{device_id}/latest"))?;
        String::from_utf8_lossy(pointer).trim().parse().ok()
    }

    /// Whether an object exists at `key`.
    fn has(&self, key: &str) -> bool {
        self.objects.lock().expect("lock").contains_key(key)
    }
}

#[async_trait]
impl ObjectBackend for FlakyBackend {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<(), BackendError> {
        if self.fail_puts.load(Ordering::SeqCst) {
            return Err(BackendError::Store {
                key: key.to_owned(),
                source: object_store::Error::Generic {
                    store: "FlakyBackend",
                    source: "injected put failure".into(),
                },
            });
        }
        self.puts.fetch_add(1, Ordering::SeqCst);
        self.objects
            .lock()
            .expect("lock")
            .insert(key.to_owned(), bytes);
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>, BackendError> {
        Ok(self.objects.lock().expect("lock").get(key).cloned())
    }

    async fn exists(&self, key: &str) -> Result<bool, BackendError> {
        Ok(self.objects.lock().expect("lock").contains_key(key))
    }
}

// ---- fake transport over real per-node fragment stores ----------------------

/// A fragment transport over one real [`LocalFragmentStore`] per node, so shards
/// and descriptors persist on disk exactly as they would on a peer's NVMe and the
/// takeover pass can enumerate them. A killed node loses its store (a box loss).
struct MemoryTransport {
    /// One fragment store per node id.
    stores: HashMap<String, LocalFragmentStore>,
    /// Nodes marked down: placement/headroom fail, loads miss.
    dead: Mutex<HashSet<String>>,
}

impl MemoryTransport {
    /// A transport over the given nodes, each with a store under `root/<node>`.
    fn new(root: &std::path::Path, nodes: &[&str]) -> Arc<Self> {
        let stores = nodes
            .iter()
            .map(|n| ((*n).to_owned(), LocalFragmentStore::new(root.join(n))))
            .collect();
        Arc::new(Self {
            stores,
            dead: Mutex::new(HashSet::new()),
        })
    }

    /// The store for `node`.
    fn store(&self, node: &str) -> &LocalFragmentStore {
        self.stores.get(node).expect("known node")
    }

    /// Marks a node down: its fragments become unreachable (a box loss).
    fn kill(&self, node: &str) {
        self.dead.lock().expect("lock").insert(node.to_owned());
    }

    /// Whether `node` is down.
    fn is_dead(&self, node: &str) -> bool {
        self.dead.lock().expect("lock").contains(node)
    }
}

#[async_trait]
impl FragmentTransport for MemoryTransport {
    async fn has_headroom(&self, node: &NodeId, _bytes: u64) -> bool {
        !self.is_dead(node.as_str())
    }

    async fn place(&self, node: &NodeId, record: FragmentRecord) -> Result<(), TransportError> {
        if self.is_dead(node.as_str()) {
            return Err(TransportError::Local(FragmentIoError::Io(format!(
                "{} is down",
                node.as_str()
            ))));
        }
        self.store(node.as_str()).store_fragment(&record)?;
        Ok(())
    }

    async fn place_stream(
        &self,
        _node: &NodeId,
        _key: FragmentKey,
        _shards: ShardStream,
    ) -> Result<(), TransportError> {
        // The block flush plane encodes the whole sealed bundle and uses `place`,
        // never the streaming placement path.
        Err(TransportError::Local(FragmentIoError::Io(
            "block flush plane does not use place_stream".to_owned(),
        )))
    }

    async fn load(
        &self,
        node: &NodeId,
        key: &FragmentKey,
    ) -> Result<Option<LoadedFragment>, TransportError> {
        if self.is_dead(node.as_str()) {
            return Ok(None);
        }
        Ok(self.store(node.as_str()).load_fragment(key)?)
    }

    async fn list(&self, node: &NodeId, prefix: &str) -> Result<Vec<FragmentKey>, TransportError> {
        if self.is_dead(node.as_str()) {
            return Ok(Vec::new());
        }
        Ok(self
            .store(node.as_str())
            .list_fragment_keys()
            .into_iter()
            .filter(|key| key.object_id.starts_with(prefix))
            .collect())
    }

    async fn delete(&self, node: &NodeId, key: &FragmentKey) -> Result<(), TransportError> {
        if self.is_dead(node.as_str()) {
            return Ok(());
        }
        self.store(node.as_str()).delete_fragment(key)?;
        Ok(())
    }
}

// ---- fake membership --------------------------------------------------------

/// A static live-membership view: a fixed node set, optionally the degenerate
/// single-node deployment.
struct FakeMembership {
    /// This node's id.
    self_id: NodeId,
    /// The live node set.
    live: Vec<NodeId>,
    /// Whether this is the architecturally-single-node deployment.
    single: bool,
}

impl FakeMembership {
    /// A membership for `self_id` over `nodes`, multi-node unless `single`.
    fn new(self_id: &str, nodes: &[&str], single: bool) -> Arc<Self> {
        Arc::new(Self {
            self_id: NodeId::new(self_id),
            live: nodes.iter().map(|n| NodeId::new(*n)).collect(),
            single,
        })
    }
}

impl LiveMembership for FakeMembership {
    fn self_id(&self) -> NodeId {
        self.self_id.clone()
    }
    fn live_nodes(&self) -> Vec<NodeId> {
        self.live.clone()
    }
    fn epoch(&self) -> u64 {
        1
    }
    fn is_single_node(&self) -> bool {
        self.single
    }
}

// ---- harness ----------------------------------------------------------------

/// A per-node ring write-back plane plus its chunk store, over a shared backend
/// and transport.
struct Node {
    /// The flush write-back plane for this node.
    ring: Arc<RingWriteback>,
    /// This node's chunk store (local NVMe + shared backend).
    store: ChunkStore,
}

/// Builds a node's ring plane and chunk store. `lease_ms == 0` makes every drain
/// immediately takeover-eligible so a peer takeover can be driven in a test.
#[allow(clippy::too_many_arguments)]
async fn build_node(
    root: &std::path::Path,
    node: &str,
    all_nodes: &[&str],
    single: bool,
    backend: Arc<dyn ObjectBackend>,
    transport: Arc<dyn FragmentTransport>,
    frag_stores: &Arc<MemoryTransport>,
    lease_ms: u64,
) -> Node {
    let store = ChunkStore::open(root.join(format!("nvme-{node}")), backend)
        .await
        .expect("chunk store");
    let membership = FakeMembership::new(node, all_nodes, single);
    let local = frag_stores.store(node).clone();
    let ring = RingWriteback::with_lease(
        transport,
        membership,
        local,
        store.clone(),
        Duration::from_millis(lease_ms),
    );
    Node { ring, store }
}

/// Stages one chunk of `byte`-filled bytes into `store` and returns a single-
/// chunk manifest at `version` referencing it, plus the sealed dirty set.
async fn one_chunk_flush(
    store: &ChunkStore,
    device_id: &str,
    version: u64,
    byte: u8,
) -> (Manifest, Vec<(ChunkHash, Bytes)>, Bytes) {
    let payload = Bytes::from(vec![byte; CHUNK_SIZE_BYTES as usize]);
    let hash = store.stage(payload.clone()).await.expect("stage");
    let manifest = Manifest {
        device_id: device_id.to_owned(),
        version,
        size_bytes: CHUNK_SIZE_BYTES,
        chunk_size_bytes: CHUNK_SIZE_BYTES,
        chunks: vec![Some(hash.to_hex())],
    };
    let dirty = store
        .undurable_chunks(&[hash])
        .await
        .expect("undurable chunks");
    (manifest, dirty, payload)
}

// ---- tests ------------------------------------------------------------------

/// A single-node ring degenerates to the synchronous origin barrier: the flush
/// commits the chunk and manifest to the origin before it returns, and places no ring
/// fragments at all. This is topology-driven, not a config choice.
#[tokio::test]
async fn single_node_ring_degenerates_to_synchronous_barrier() {
    let dir = tempfile::tempdir().expect("tempdir");
    let flaky = FlakyBackend::new();
    let backend: Arc<dyn ObjectBackend> = Arc::clone(&flaky) as Arc<dyn ObjectBackend>;
    let transport = MemoryTransport::new(dir.path(), &["solo"]);
    let node = build_node(
        dir.path(),
        "solo",
        &["solo"],
        true,
        Arc::clone(&backend),
        Arc::clone(&transport) as Arc<dyn FragmentTransport>,
        &transport,
        30_000,
    )
    .await;

    let (manifest, dirty, _payload) = one_chunk_flush(&node.store, "solo-dev", 1, 0xAB).await;
    node.ring
        .flush("solo-dev", manifest, dirty)
        .await
        .expect("flush acks");

    // Synchronous: the manifest is durable at the origin the instant the flush returns.
    assert_eq!(
        flaky.latest_version("solo-dev"),
        Some(1),
        "single-node flush commits synchronously to the origin"
    );
    // No peers to code across: no fragment was ever placed.
    assert_eq!(
        transport.store("solo").list_fragment_keys().len(),
        0,
        "the degenerate ring places no fragments"
    );
}

/// A multi-node FLUSH acks on the ring quorum, not on the origin: with the backend
/// refusing every PUT, the flush still returns Ok and the fragments plus
/// descriptors are on the ring, while the origin holds no manifest. The ack cost is a
/// ring RTT; the origin barrier is deferred to the drain.
#[tokio::test]
async fn flush_acks_on_quorum_not_on_origin() {
    let dir = tempfile::tempdir().expect("tempdir");
    let flaky = FlakyBackend::new();
    flaky.set_fail(true); // the origin is unavailable for the whole test
    let backend: Arc<dyn ObjectBackend> = Arc::clone(&flaky) as Arc<dyn ObjectBackend>;
    let nodes = ["a", "b", "c"];
    let transport = MemoryTransport::new(dir.path(), &nodes);
    let node = build_node(
        dir.path(),
        "a",
        &nodes,
        false,
        Arc::clone(&backend),
        Arc::clone(&transport) as Arc<dyn FragmentTransport>,
        &transport,
        30_000,
    )
    .await;

    let (manifest, dirty, _payload) = one_chunk_flush(&node.store, "dev", 1, 0x11).await;
    node.ring
        .flush("dev", manifest, dirty)
        .await
        .expect("flush acks despite the origin being down");

    // The ack landed even though the origin cannot serve a single PUT.
    assert_eq!(flaky.latest_version("dev"), None, "no manifest reached the origin");
    // RS(2,1) over 3 nodes: each node holds exactly one data/parity fragment plus
    // a full descriptor copy (the sentinel index).
    for n in nodes {
        let keys = transport.store(n).list_fragment_keys();
        assert_eq!(keys.len(), 2, "node {n}: one fragment + one descriptor");
    }
}

/// Originator crash mid-drain: the flush acks on the quorum, then the origin is briefly
/// down so the originator's own drain cannot complete; the originator's box is
/// lost (its fragment gone); a surviving peer's takeover pass reconstructs the
/// sealed flush from the two remaining fragments (one shard lost), completes the
/// origin barrier, and commits the manifest version exactly once.
#[tokio::test]
async fn originator_crash_peer_reconstructs_and_drains_exactly_once() {
    let dir = tempfile::tempdir().expect("tempdir");
    let flaky = FlakyBackend::new();
    flaky.set_fail(true);
    let backend: Arc<dyn ObjectBackend> = Arc::clone(&flaky) as Arc<dyn ObjectBackend>;
    let nodes = ["a", "b", "c"];
    let transport = MemoryTransport::new(dir.path(), &nodes);
    let tdyn = Arc::clone(&transport) as Arc<dyn FragmentTransport>;

    // lease_ms = 0 so a takeover is immediately eligible once the originator is
    // presumed dead.
    let origin = build_node(
        dir.path(),
        "a",
        &nodes,
        false,
        Arc::clone(&backend),
        Arc::clone(&tdyn),
        &transport,
        0,
    )
    .await;
    let peer_b = build_node(
        dir.path(),
        "b",
        &nodes,
        false,
        Arc::clone(&backend),
        Arc::clone(&tdyn),
        &transport,
        0,
    )
    .await;

    let (manifest, dirty, payload) = one_chunk_flush(&origin.store, "dev", 1, 0x7C).await;
    let chunk_key = ChunkHash::of(&payload).object_key();
    origin
        .ring
        .flush("dev", manifest, dirty)
        .await
        .expect("flush acks");
    // Let the originator's background drain attempt (and fail against the down origin).
    tokio::task::yield_now().await;
    assert_eq!(
        flaky.latest_version("dev"),
        None,
        "drain blocked while the origin is down"
    );

    // The origin comes back; the originator's box is lost before it could re-drain.
    flaky.set_fail(false);
    transport.kill("a");

    // A surviving peer takes the drain over: reconstruct from b + c (a's shard is
    // gone — one shard lost), PUT the chunk, commit version 1.
    peer_b.ring.run_takeover_pass().await;

    assert_eq!(
        flaky.latest_version("dev"),
        Some(1),
        "peer completed the origin barrier"
    );
    assert!(flaky.has(&chunk_key), "the flushed chunk reached the origin");

    // Exactly once: another pass (or the same one again) never advances the
    // version, and the ring holds have been released.
    peer_b.ring.run_takeover_pass().await;
    assert_eq!(
        flaky.latest_version("dev"),
        Some(1),
        "version is exactly once"
    );
    assert_eq!(
        transport.store("b").list_fragment_keys().len(),
        0,
        "b released its fragment and descriptor after the barrier"
    );
    assert_eq!(
        transport.store("c").list_fragment_keys().len(),
        0,
        "c released its fragment and descriptor after the barrier"
    );
}

/// A block device attached on a single-node ring flushes exactly like the
/// pre-write-back synchronous path: written bytes are durable at the origin after FLUSH
/// and read back from a fresh attach over the same backend. Confirms the device
/// integration keeps the degenerate-ring behaviour byte-identical.
#[tokio::test]
async fn block_device_on_single_node_ring_reads_back_after_flush() {
    use verglas_block::BlockDevice;

    let dir = tempfile::tempdir().expect("tempdir");
    let flaky = FlakyBackend::new();
    let backend: Arc<dyn ObjectBackend> = Arc::clone(&flaky) as Arc<dyn ObjectBackend>;
    let transport = MemoryTransport::new(dir.path(), &["solo"]);
    let node = build_node(
        dir.path(),
        "solo",
        &["solo"],
        true,
        Arc::clone(&backend),
        Arc::clone(&transport) as Arc<dyn FragmentTransport>,
        &transport,
        30_000,
    )
    .await;

    // The device and the ring share one chunk store (as the cache node wires
    // them), so a chunk the device stages is the chunk the ring's barrier makes
    // durable.
    let dev = BlockDevice::ensure_on_ring("vd0", 4096, node.store.clone(), Arc::clone(&node.ring))
        .await
        .expect("ensure on ring");
    dev.write_at(512, b"hello ring").await.expect("write");
    assert_eq!(dev.flush().await.expect("flush"), 1);

    // A fresh attach over the same backend reads the flushed bytes back (they are
    // durable at the origin synchronously on a single-node ring).
    let store2 = ChunkStore::open(dir.path().join("dev-nvme-b"), backend)
        .await
        .expect("store b");
    let dev2 = BlockDevice::open("vd0", store2).await.expect("open");
    assert_eq!(
        dev2.read_at(512, 10).await.expect("read"),
        Bytes::from_static(b"hello ring")
    );
}

/// A ring that cannot place its quorum (a peer refuses placement) falls back to
/// the synchronous origin barrier — the always-safe path — committing the manifest
/// to the origin before the flush returns and leaving no orphaned fragments.
#[tokio::test]
async fn quorum_short_ring_falls_back_to_synchronous_barrier() {
    let dir = tempfile::tempdir().expect("tempdir");
    let flaky = FlakyBackend::new(); // the origin is up, so the sync barrier can commit
    let backend: Arc<dyn ObjectBackend> = Arc::clone(&flaky) as Arc<dyn ObjectBackend>;
    let nodes = ["a", "b", "c"];
    let transport = MemoryTransport::new(dir.path(), &nodes);
    // Node c is down, so only 2 of the 3 required fragments can be placed (w=3).
    transport.kill("c");
    let node = build_node(
        dir.path(),
        "a",
        &nodes,
        false,
        Arc::clone(&backend),
        Arc::clone(&transport) as Arc<dyn FragmentTransport>,
        &transport,
        30_000,
    )
    .await;

    let (manifest, dirty, _payload) = one_chunk_flush(&node.store, "dev", 1, 0x22).await;
    node.ring
        .flush("dev", manifest, dirty)
        .await
        .expect("flush falls back and commits");

    assert_eq!(
        flaky.latest_version("dev"),
        Some(1),
        "quorum-short flush committed via the synchronous barrier"
    );
    // The partial EC attempt was cleaned up: no fragments left on the live nodes.
    assert_eq!(transport.store("a").list_fragment_keys().len(), 0);
    assert_eq!(transport.store("b").list_fragment_keys().len(), 0);
}
