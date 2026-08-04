//! In-process integration tests for peer-fetch (#29): two cache engines wired
//! by the real RPC pair over loopback TCP, exercising the acceptance criteria.
//!
//! - a block cached on the owner (node A) is served to a requester (node B) via
//!   peer fetch — byte-identical, one `peer_hit` on B, zero backend GETs on B;
//! - a pod-wide miss: B peer-fetches, the owner also lacks it, so B backend-
//!   fills locally (the ladder continues, never errors);
//! - a dead owner: B falls back to a backend fill inside the timeout budget —
//!   slow success, not an error;
//! - a stale ETag never serves: after the object is overwritten, a request for
//!   the old-ETag block misses at the peer and B fills the new version.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use futures::TryStreamExt;
use object_store::memory::InMemory;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStoreExt, PutPayload};

use verglas_cache::HybridCacheEngine;
use verglas_cluster::peer::LocalBlockFn;
use verglas_cluster::{LiveRing, PeerClient, PeerServer, StaticResolver, WeightedMember};
use verglas_core::CacheKey;
use verglas_core::config::{ByteSize, Cache as CacheConfig};
use verglas_core::node::NodeId;
use verglas_s3::{
    BackendStore, ObjectGet, ObjectMeta, ObjectRead, PassthroughRead, ReadError, ReadRange,
};

/// A deterministic payload: byte `i` is `(i + seed) % 251`.
fn pattern(seed: u64, len: usize) -> Bytes {
    (0..len)
        .map(|i| ((i as u64 + seed) % 251) as u8)
        .collect::<Vec<u8>>()
        .into()
}

/// An `ObjectRead` that counts GET/HEAD calls before delegating — the ground
/// truth for "zero backend GETs on the requester".
struct CountingRead<R> {
    inner: R,
    gets: Arc<AtomicU64>,
    heads: Arc<AtomicU64>,
}

impl<R: ObjectRead> ObjectRead for CountingRead<R> {
    async fn get(&self, key: &CacheKey, range: ReadRange) -> Result<ObjectGet, ReadError> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        self.inner.get(key, range).await
    }

    async fn head(&self, key: &CacheKey) -> Result<ObjectMeta, ReadError> {
        self.heads.fetch_add(1, Ordering::Relaxed);
        self.inner.head(key).await
    }
}

/// A cache engine over a counting backend, plus the GET counter.
type Engine = HybridCacheEngine<CountingRead<PassthroughRead>, PeerClient, LiveRing>;

/// Builds an engine for `node` over the shared `store`, the given `ring`, and
/// `peers`. Returns the engine, its backend GET counter, and a scratch dir kept
/// alive for the engine's lifetime.
async fn build_engine(
    node: NodeId,
    store: Arc<InMemory>,
    ring: LiveRing,
    peers: PeerClient,
) -> (Engine, Arc<AtomicU64>, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("temp dir");
    let gets = Arc::new(AtomicU64::new(0));
    let backend = CountingRead {
        inner: PassthroughRead::new(BackendStore::single("lake", store)),
        gets: Arc::clone(&gets),
        heads: Arc::new(AtomicU64::new(0)),
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
fn local_block_source(engine: Engine) -> LocalBlockFn {
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

/// A two-member ring and the owner/requester split for `key`: the ring decides
/// which node owns the key; the other is the requester that must peer-fetch.
fn owner_and_requester(a: &NodeId, b: &NodeId, key: &CacheKey) -> (NodeId, NodeId, LiveRing) {
    use verglas_core::ring::Ring;
    let ring = LiveRing::new(vec![
        WeightedMember::new(a.clone(), 1),
        WeightedMember::new(b.clone(), 1),
    ])
    .expect("non-empty ring");
    let owner = ring.owner(key);
    let requester = if &owner == a { b.clone() } else { a.clone() };
    (owner, requester, ring)
}

#[tokio::test(flavor = "multi_thread")]
async fn block_cached_on_owner_is_served_to_requester_over_peer_fetch() {
    let store = Arc::new(InMemory::new());
    let body = pattern(7, 3 * 1024 * 1024); // one block
    store
        .put(
            &ObjPath::from("hot.parquet"),
            PutPayload::from(body.clone()),
        )
        .await
        .expect("seed");
    let key = CacheKey {
        bucket: "lake".to_owned(),
        key: "hot.parquet".to_owned(),
    };

    let a = NodeId::new("node-a");
    let b = NodeId::new("node-b");
    let (owner, requester, ring) = owner_and_requester(&a, &b, &key);

    // The owner owns the key, so it never peer-fetches: a disabled client.
    let (owner_engine, _owner_gets, _od) = build_engine(
        owner.clone(),
        Arc::clone(&store),
        ring.clone(),
        PeerClient::disabled(),
    )
    .await;
    // Warm the block on the owner (its only backend GET for this object).
    assert_eq!(
        read_all(&owner_engine, &key).await,
        body,
        "owner serves bytes"
    );

    // Stand up the owner's peer server over its cache-only endpoint.
    let server = PeerServer::bind(
        "127.0.0.1:0".parse().expect("addr"),
        Some("pod-secret".to_owned()),
        local_block_source(owner_engine.clone()),
    )
    .await
    .expect("bind peer server");

    // The requester resolves the owner to that server, same secret.
    let resolver = StaticResolver::with(owner.clone(), server.local_addr());
    let peers = PeerClient::new(
        Arc::new(resolver),
        Some("pod-secret".to_owned()),
        Duration::from_millis(50),
        Duration::from_millis(500),
    );
    let (req_engine, req_gets, _rd) =
        build_engine(requester.clone(), Arc::clone(&store), ring, peers).await;

    // Resolve the mapping first (a backend HEAD, not a GET) so the GET runs the
    // full block ladder — the peer-metadata protocol (fetching the mapping from
    // the owner too) is a documented follow-on extension point; #29 is the block transfer.
    req_engine.head(&key).await.expect("resolve mapping");
    // Read the same key through the requester: byte-identical, served by peer.
    let got = read_all(&req_engine, &key).await;
    assert_eq!(got, body, "requester served wrong bytes");

    let snap = req_engine.counters().snapshot();
    assert_eq!(snap.peer_hits, 1, "the one block came from the peer");
    assert_eq!(
        snap.backend_fills, 0,
        "requester must not fill from the backend"
    );
    assert_eq!(
        req_gets.load(Ordering::Relaxed),
        0,
        "requester issued zero backend GETs"
    );

    // The owner metered the block it served to the peer (#46).
    let owner_snap = owner_engine.counters().snapshot();
    assert_eq!(owner_snap.peer_served_blocks, 1);
    assert_eq!(owner_snap.peer_served_bytes, body.len() as u64);

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn pod_wide_miss_falls_through_to_a_backend_fill() {
    let store = Arc::new(InMemory::new());
    let body = pattern(3, 2 * 1024 * 1024);
    store
        .put(
            &ObjPath::from("cold.parquet"),
            PutPayload::from(body.clone()),
        )
        .await
        .expect("seed");
    let key = CacheKey {
        bucket: "lake".to_owned(),
        key: "cold.parquet".to_owned(),
    };
    let a = NodeId::new("node-a");
    let b = NodeId::new("node-b");
    let (owner, requester, ring) = owner_and_requester(&a, &b, &key);

    // The owner's cache is cold (nothing warmed), so the peer will miss.
    let (owner_engine, _og, _od) = build_engine(
        owner.clone(),
        Arc::clone(&store),
        ring.clone(),
        PeerClient::disabled(),
    )
    .await;
    let server = PeerServer::bind(
        "127.0.0.1:0".parse().expect("addr"),
        None,
        local_block_source(owner_engine),
    )
    .await
    .expect("bind");

    let peers = PeerClient::new(
        Arc::new(StaticResolver::with(owner.clone(), server.local_addr())),
        None,
        Duration::from_millis(50),
        Duration::from_millis(500),
    );
    let (req_engine, req_gets, _rd) =
        build_engine(requester, Arc::clone(&store), ring, peers).await;

    req_engine.head(&key).await.expect("resolve mapping");
    let got = read_all(&req_engine, &key).await;
    assert_eq!(got, body, "a pod-wide miss still serves the right bytes");
    let snap = req_engine.counters().snapshot();
    assert_eq!(snap.peer_misses, 1, "the peer answered a clean miss");
    assert_eq!(snap.peer_hits, 0);
    assert!(
        req_gets.load(Ordering::Relaxed) > 0,
        "a pod-wide miss backend-fills at the requester"
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn dead_owner_falls_back_to_backend_within_the_budget() {
    let store = Arc::new(InMemory::new());
    let body = pattern(5, 2 * 1024 * 1024);
    store
        .put(
            &ObjPath::from("obj.parquet"),
            PutPayload::from(body.clone()),
        )
        .await
        .expect("seed");
    let key = CacheKey {
        bucket: "lake".to_owned(),
        key: "obj.parquet".to_owned(),
    };
    let a = NodeId::new("node-a");
    let b = NodeId::new("node-b");
    let (owner, requester, ring) = owner_and_requester(&a, &b, &key);

    // Resolve the owner to a dead address — nothing listens there.
    let dead = "127.0.0.1:1".parse().expect("addr");
    let peers = PeerClient::new(
        Arc::new(StaticResolver::with(owner, dead)),
        None,
        Duration::from_millis(50),
        Duration::from_millis(200),
    );
    let (req_engine, req_gets, _rd) =
        build_engine(requester, Arc::clone(&store), ring, peers).await;

    req_engine.head(&key).await.expect("resolve mapping");
    let start = Instant::now();
    let got = read_all(&req_engine, &key).await;
    let elapsed = start.elapsed();
    assert_eq!(
        got, body,
        "a dead owner still yields correct bytes (slow success)"
    );
    assert!(
        elapsed < Duration::from_secs(3),
        "a dead owner must degrade fast (was {elapsed:?})"
    );
    let snap = req_engine.counters().snapshot();
    assert_eq!(snap.peer_errors, 1, "the dead peer counted as an error");
    assert!(
        req_gets.load(Ordering::Relaxed) > 0,
        "requester backend-filled"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn stale_etag_never_serves_peer_bytes_after_overwrite() {
    let store = Arc::new(InMemory::new());
    let v1 = pattern(1, 2 * 1024 * 1024);
    let key = CacheKey {
        bucket: "lake".to_owned(),
        key: "mut.parquet".to_owned(),
    };
    store
        .put(&ObjPath::from("mut.parquet"), PutPayload::from(v1.clone()))
        .await
        .expect("seed v1");

    let a = NodeId::new("node-a");
    let b = NodeId::new("node-b");
    let (owner, requester, ring) = owner_and_requester(&a, &b, &key);

    let (owner_engine, _og, _od) = build_engine(
        owner.clone(),
        Arc::clone(&store),
        ring.clone(),
        PeerClient::disabled(),
    )
    .await;
    // Owner warms v1 into its cache (keyed by v1's ETag).
    assert_eq!(read_all(&owner_engine, &key).await, v1);
    let server = PeerServer::bind(
        "127.0.0.1:0".parse().expect("addr"),
        None,
        local_block_source(owner_engine),
    )
    .await
    .expect("bind");

    // Overwrite the object at the origin: it now has a new ETag (v2 bytes).
    let v2 = pattern(2, 2 * 1024 * 1024);
    store
        .put(&ObjPath::from("mut.parquet"), PutPayload::from(v2.clone()))
        .await
        .expect("overwrite v2");

    // The requester, resolving v2's mapping via a fresh HEAD, asks the owner
    // for the v2 block — which the owner does not have (it cached only v1) —
    // so it misses and the requester backend-fills v2. It must never receive
    // the owner's v1 bytes.
    let peers = PeerClient::new(
        Arc::new(StaticResolver::with(owner, server.local_addr())),
        None,
        Duration::from_millis(50),
        Duration::from_millis(500),
    );
    let (req_engine, req_gets, _rd) =
        build_engine(requester, Arc::clone(&store), ring, peers).await;

    req_engine.head(&key).await.expect("resolve mapping");
    let got = read_all(&req_engine, &key).await;
    assert_eq!(got, v2, "a stale peer version must never be served");
    assert_ne!(
        got, v1,
        "the old version's bytes must not leak across the pod"
    );
    let snap = req_engine.counters().snapshot();
    assert_eq!(
        snap.peer_hits, 0,
        "the stale-ETag block must not be a peer hit"
    );
    assert!(
        req_gets.load(Ordering::Relaxed) > 0,
        "requester filled v2 from backend"
    );

    server.shutdown().await;
}
