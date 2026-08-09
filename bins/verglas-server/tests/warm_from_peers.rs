//! In-process live proof for warm-from-peers on ownership movement (#30 join,
//! #31 drain): real cache engines wired by the real peer RPC over loopback,
//! sharing one live rendezvous ring, exercising the acceptance criteria with
//! the counters as evidence.
//!
//! - **Join (#30):** an incumbent holds a key warm; a node joins and inherits
//!   ownership of it; the joiner, in its warm window, pulls the hot block from
//!   the incumbent over the peer network — one `peer_hit`, zero backend GETs.
//!   With the warm window closed the same joiner backend-fills, proving the
//!   gate keeps a settled pod hop-free (the counter contrast).
//! - **Drain (#31):** a node owns a key warm, then drains; ownership shifts to
//!   its successor, which reads the key and warms it from the *draining donor*
//!   — one `peer_hit`, zero backend GETs, the read never errors (slow success,
//!   never wrong), and the draining node owns nothing afterwards.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures::TryStreamExt;
use object_store::memory::InMemory;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStoreExt, PutPayload};

use verglas_cache::HybridCacheEngine;
use verglas_cluster::peer::LocalBlockFn;
use verglas_cluster::{
    LiveRing, PeerClient, PeerServer, RingMember, StaticResolver, WeightedMember,
};
use verglas_core::CacheKey;
use verglas_core::config::{ByteSize, Cache as CacheConfig};
use verglas_core::node::NodeId;
use verglas_core::ring::Ring;
use verglas_s3::{BackendStore, ObjectRead, PassthroughRead, ReadRange};

/// A backend read that counts GETs before delegating — the ground truth for
/// "zero backend GETs on the warming node".
struct CountingRead {
    inner: PassthroughRead,
    gets: Arc<AtomicU64>,
}

impl verglas_s3::ObjectRead for CountingRead {
    async fn get(
        &self,
        key: &CacheKey,
        range: ReadRange,
    ) -> Result<verglas_s3::ObjectGet, verglas_s3::ReadError> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        self.inner.get(key, range).await
    }

    async fn head(&self, key: &CacheKey) -> Result<verglas_s3::ObjectMeta, verglas_s3::ReadError> {
        self.inner.head(key).await
    }
}

/// A cache engine over a counting backend, sharing a live ring and a peer client.
type Engine = HybridCacheEngine<CountingRead, PeerClient, LiveRing>;

/// Builds an engine for `node` over the shared `store`, `ring`, and `peers`.
/// Returns the engine, its backend GET counter, and a scratch dir kept alive.
async fn build_engine(
    node: NodeId,
    store: Arc<InMemory>,
    ring: LiveRing,
    peers: PeerClient,
) -> (Engine, Arc<AtomicU64>, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("temp dir");
    let gets = Arc::new(AtomicU64::new(0));
    let backend = CountingRead {
        inner: PassthroughRead::new(BackendStore::single("managed-lakehouse", "lake", store)),
        gets: Arc::clone(&gets),
    };
    let cache = CacheConfig {
        dir: dir.path().to_path_buf(),
        capacity_bytes: ByteSize(64 * 1024 * 1024),
        dram_bytes: ByteSize(128 * 1024 * 1024),
        data_block_bytes: ByteSize(8 * 1024 * 1024),
        admission: Default::default(),
        meta_fraction: 0.05,
        mutable_mapping_ttl_secs: 5,
        warming: Default::default(),
        prefetch: Default::default(),
        writeback: Default::default(),
    };
    let engine = HybridCacheEngine::new(backend, peers, ring, node, &cache)
        .await
        .expect("build engine");
    (engine, gets, dir)
}

/// Wires a peer server to `engine`'s cache-only `local_block` — the donor side.
fn donor_source(engine: Engine) -> LocalBlockFn {
    Arc::new(move |bk| {
        let engine = engine.clone();
        Box::pin(async move { engine.local_block(&bk).await })
    })
}

/// Reads a whole object through an engine, collecting the streamed body.
async fn read_all(engine: &Engine, key: &CacheKey) -> Bytes {
    let got = engine.get(key, ReadRange::Full).await.expect("read ok");
    let mut buf = BytesMut::new();
    let mut stream = got.body;
    while let Some(chunk) = stream.try_next().await.expect("stream chunk") {
        buf.extend_from_slice(&chunk);
    }
    buf.freeze()
}

/// A deterministic single-block payload.
fn block_bytes(seed: u64) -> Bytes {
    (0..3 * 1024 * 1024)
        .map(|i| ((i as u64 + seed) % 251) as u8)
        .collect::<Vec<u8>>()
        .into()
}

/// Finds an object key the `target` node owns under `ring`, seeded into `store`.
async fn seed_key_owned_by(store: &InMemory, ring: &LiveRing, target: &NodeId) -> CacheKey {
    for i in 0..10_000u64 {
        let key = CacheKey {
            storage_binding_id: "managed-lakehouse".to_owned(),
            bucket: "lake".to_owned(),
            key: format!("warehouse/table/data/part-{i:05}.parquet"),
        };
        if ring.owner(&key) == *target {
            store
                .put(
                    &ObjPath::from(key.key.clone()),
                    PutPayload::from(block_bytes(i)),
                )
                .await
                .expect("seed");
            return key;
        }
    }
    panic!("no key owned by {target} in the sampled space");
}

/// Builds a peer client that resolves `owner` to `addr` with a tight budget.
fn client_to(owner: NodeId, addr: std::net::SocketAddr) -> PeerClient {
    PeerClient::new(
        Arc::new(StaticResolver::with(owner, addr)),
        None,
        Duration::from_millis(50),
        Duration::from_millis(500),
    )
}

/// #30 — a joiner warms its newly-owned keys from the incumbent that held them,
/// not from the backend: one peer hit, zero backend fills, zero backend GETs.
#[tokio::test(flavor = "multi_thread")]
async fn joiner_warms_owned_keys_from_the_previous_owner_not_the_backend() {
    let store = Arc::new(InMemory::new());
    let incumbent = NodeId::new("node-a");
    let joiner = NodeId::new("node-d");

    // The post-join ring: both nodes present. Pick a key the joiner will own.
    let joined_ring = LiveRing::new(vec![
        WeightedMember::new(incumbent.clone(), 1),
        WeightedMember::new(joiner.clone(), 1),
    ])
    .expect("non-empty");
    let key = seed_key_owned_by(&store, &joined_ring, &joiner).await;
    let want = read_key(&store, &key).await;

    // The incumbent boots as a cluster-of-one that owns the key and warms it.
    let incumbent_ring =
        LiveRing::new(vec![WeightedMember::new(incumbent.clone(), 1)]).expect("non-empty");
    let (incumbent_engine, _ig, _id) = build_engine(
        incumbent.clone(),
        Arc::clone(&store),
        incumbent_ring.clone(),
        PeerClient::disabled(),
    )
    .await;
    assert_eq!(
        read_all(&incumbent_engine, &key).await,
        want,
        "incumbent warms it"
    );

    // The joiner arrives: gossip widens the incumbent's ring to include it, so
    // the incumbent no longer owns the key but still holds it — as the donor.
    incumbent_ring
        .update(vec![
            WeightedMember::new(incumbent.clone(), 1),
            WeightedMember::new(joiner.clone(), 1),
        ])
        .expect("non-empty");
    assert_eq!(
        joined_ring.owner(&key),
        joiner,
        "the joiner now owns the key"
    );

    let server = PeerServer::bind(
        "127.0.0.1:0".parse().expect("addr"),
        None,
        donor_source(incumbent_engine.clone()),
    )
    .await
    .expect("bind donor");

    // The joiner, in its warm window, reads the key it now owns.
    let peers = client_to(incumbent.clone(), server.local_addr());
    let (joiner_engine, joiner_gets, _jd) = build_engine(
        joiner.clone(),
        Arc::clone(&store),
        joined_ring.clone(),
        peers,
    )
    .await;
    joiner_engine.begin_warming(Duration::from_secs(60));
    assert!(joiner_engine.is_warming(), "the join warm window is open");

    joiner_engine.head(&key).await.expect("resolve mapping");
    let got = read_all(&joiner_engine, &key).await;
    assert_eq!(got, want, "the joiner serves the right bytes");

    let snap = joiner_engine.counters().snapshot();
    assert_eq!(snap.peer_hits, 1, "the owned block came from the peer");
    assert_eq!(snap.backend_fills, 0, "warm-from-peers, not a cold fill");
    assert_eq!(
        joiner_gets.load(Ordering::Relaxed),
        0,
        "the joiner issued zero backend GETs"
    );

    // The incumbent metered the block it donated (#46 intra-pod amplification).
    assert_eq!(incumbent_engine.counters().snapshot().peer_served_blocks, 1);

    server.shutdown().await;
}

/// #30 gate — the same joiner, with its warm window closed, backend-fills the
/// key it owns rather than paying a wasted peer hop. This is what keeps a
/// long-settled pod hop-free; contrast it with the test above.
#[tokio::test(flavor = "multi_thread")]
async fn joiner_without_a_warm_window_backend_fills() {
    let store = Arc::new(InMemory::new());
    let incumbent = NodeId::new("node-a");
    let joiner = NodeId::new("node-d");
    let ring = LiveRing::new(vec![
        WeightedMember::new(incumbent.clone(), 1),
        WeightedMember::new(joiner.clone(), 1),
    ])
    .expect("non-empty");
    let key = seed_key_owned_by(&store, &ring, &joiner).await;
    let want = read_key(&store, &key).await;

    // No peer server needed: the closed window must skip the peer rung entirely.
    let peers = client_to(incumbent, "127.0.0.1:9".parse().expect("addr"));
    let (joiner_engine, joiner_gets, _jd) =
        build_engine(joiner.clone(), Arc::clone(&store), ring, peers).await;
    // is_warming() is false — begin_warming was never called.
    assert!(!joiner_engine.is_warming());

    joiner_engine.head(&key).await.expect("resolve mapping");
    assert_eq!(read_all(&joiner_engine, &key).await, want);
    let snap = joiner_engine.counters().snapshot();
    assert_eq!(snap.peer_hits, 0, "no peer hop while not warming");
    assert_eq!(
        snap.peer_errors, 0,
        "the peer rung was skipped, not attempted"
    );
    assert!(
        snap.backend_fills > 0,
        "an unwarmed owned miss backend-fills"
    );
    assert!(joiner_gets.load(Ordering::Relaxed) > 0);
}

/// #31 — a draining node keeps serving its shed keys, so the successor that
/// inherits them warms from the leaving donor: one peer hit, zero backend
/// fills, the read never errors, and the draining node owns nothing after.
#[tokio::test(flavor = "multi_thread")]
async fn drain_successor_warms_shed_keys_from_the_draining_donor() {
    let store = Arc::new(InMemory::new());
    let leaver = NodeId::new("node-leaver");
    let successor = NodeId::new("node-successor");

    // The active pod: both own their share. Pick a key the leaver owns.
    let ring = LiveRing::new(vec![
        WeightedMember::new(leaver.clone(), 1),
        WeightedMember::new(successor.clone(), 1),
    ])
    .expect("non-empty");
    let key = seed_key_owned_by(&store, &ring, &leaver).await;
    let want = read_key(&store, &key).await;

    // The leaver warms the key it owns, then a peer server exposes it as donor.
    let (leaver_engine, _lg, _ld) = build_engine(
        leaver.clone(),
        Arc::clone(&store),
        ring.clone(),
        PeerClient::disabled(),
    )
    .await;
    assert_eq!(
        read_all(&leaver_engine, &key).await,
        want,
        "leaver warms it"
    );
    let server = PeerServer::bind(
        "127.0.0.1:0".parse().expect("addr"),
        None,
        donor_source(leaver_engine.clone()),
    )
    .await
    .expect("bind donor");

    // Drain: gossip marks the leaver draining on the (shared) ring. Ownership
    // shifts to the successor; the leaver stays a donor for what it holds.
    ring.update_states(vec![
        RingMember::draining(leaver.clone(), 1),
        RingMember::active(successor.clone(), 1),
    ])
    .expect("non-empty");
    assert!(!ring.members().contains(&leaver), "the leaver owns nothing");
    assert_eq!(
        ring.owner(&key),
        successor,
        "the successor inherited the key"
    );

    // The successor reads the shed key. It is NOT warming (a drain does not warm
    // the successor via a window) — the draining donor is pulled unconditionally.
    let peers = client_to(leaver.clone(), server.local_addr());
    let (successor_engine, successor_gets, _sd) =
        build_engine(successor.clone(), Arc::clone(&store), ring.clone(), peers).await;
    assert!(!successor_engine.is_warming());

    successor_engine.head(&key).await.expect("resolve mapping");
    let got = read_all(&successor_engine, &key).await;
    assert_eq!(
        got, want,
        "zero client errors: the read serves the right bytes"
    );

    let snap = successor_engine.counters().snapshot();
    assert_eq!(
        snap.peer_hits, 1,
        "the shed block came from the draining donor"
    );
    assert_eq!(
        snap.backend_fills, 0,
        "the drain shed warmth, not cold-started"
    );
    assert_eq!(
        successor_gets.load(Ordering::Relaxed),
        0,
        "the successor issued zero backend GETs"
    );
    assert_eq!(leaver_engine.counters().snapshot().peer_served_blocks, 1);

    server.shutdown().await;
}

/// Reads an object's bytes directly from the store, for the expected value.
async fn read_key(store: &InMemory, key: &CacheKey) -> Bytes {
    store
        .get(&ObjPath::from(key.key.clone()))
        .await
        .expect("get")
        .bytes()
        .await
        .expect("bytes")
}
