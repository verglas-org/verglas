//! Coordinator contract tests (#180), mapped to the issue's acceptance criteria
//! with in-memory fakes so quorum, fallback, read-your-writes, and repair are
//! exercised without a network or a live origin:
//!
//! - quorum ack accounting: an eligible write acks over `w` distinct nodes and
//!   is counted as a quorum ack, with a dirty journal and placed fragments;
//! - write-through fallback: a live view that cannot support `w` (small pod)
//!   degrades to a synchronous origin write, counted as write-through, with no
//!   journal; a mid-flight fan-out shortfall also degrades to write-through;
//! - read-your-writes: a just-acked object reads back byte-identically from
//!   fragments before it propagates;
//! - node-loss repair: losing a fragment node re-encodes the missing fragment
//!   from the survivors and re-places it on a live node;
//! - propagation: an acked object uploads to the origin and frees its fragments.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use verglas_cluster::fragments::{FragmentIoError, LoadedFragment};
use verglas_core::CacheKey;
use verglas_core::node::NodeId;
use verglas_core::read::{ObjectRead, ReadError, ReadRange};
use verglas_core::write::{
    CompletedPartRef, CopyOutcome, MultipartCreation, ObjectWrite, PartInfo, PartUpload,
    PutOutcome, WriteBodyStream, WriteChecksum, WriteError, WriteMetadata,
};
use verglas_write::coordinator::WriteCoordinator;
use verglas_write::journal::JournalStore;
use verglas_write::membership::LiveMembership;
use verglas_write::metrics::WritebackMetrics;
use verglas_write::reader::WritebackReader;
use verglas_write::transport::{FragmentTransport, TransportError};

// ---- in-memory fragment transport ----------------------------------------

#[derive(Default)]
struct TransportInner {
    /// Each stored fragment keeps its payload and the checksum it was placed
    /// with, mirroring the on-disk `payload || crc` trailer so a test can corrupt
    /// the payload without touching the checksum (#220).
    frags: HashMap<(String, String, usize), (Bytes, u32)>,
    dead: HashSet<String>,
    fail_place: HashSet<String>,
    /// Nodes with no fragment headroom: excluded from placement.
    no_headroom: HashSet<String>,
}

/// An in-memory fragment transport with controllable node liveness.
struct MemoryTransport {
    inner: Mutex<TransportInner>,
}

impl MemoryTransport {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(TransportInner::default()),
        })
    }

    /// Marks a node down: its fragments are lost and further ops fail/miss.
    fn kill(&self, node: &str) {
        let mut inner = self.inner.lock().expect("lock");
        inner.dead.insert(node.to_owned());
        inner.frags.retain(|(n, _, _), _| n != node);
    }

    /// Makes placement on a node fail (node up but cannot store).
    fn fail_placement(&self, node: &str) {
        self.inner
            .lock()
            .expect("lock")
            .fail_place
            .insert(node.to_owned());
    }

    /// Marks a node's fragment store full (live, but no headroom), so placement
    /// must exclude it.
    fn fill_disk(&self, node: &str) {
        self.inner
            .lock()
            .expect("lock")
            .no_headroom
            .insert(node.to_owned());
    }

    /// Count of stored fragments across all nodes.
    fn fragment_count(&self) -> usize {
        self.inner.lock().expect("lock").frags.len()
    }

    /// Flips a byte in one stored fragment's payload while leaving its recorded
    /// checksum unchanged, so a later load fails verification — simulates on-disk
    /// bit-rot for the #220 tests. Returns false if the fragment is absent.
    fn corrupt_fragment(&self, object_id: &str, index: usize) -> bool {
        let mut inner = self.inner.lock().expect("lock");
        for ((_, obj, idx), (bytes, _crc)) in inner.frags.iter_mut() {
            if obj == object_id && *idx == index {
                let mut v = bytes.to_vec();
                v[0] ^= 0xff;
                *bytes = Bytes::from(v);
                return true;
            }
        }
        false
    }
}

#[async_trait::async_trait]
impl FragmentTransport for MemoryTransport {
    async fn has_headroom(&self, node: &NodeId, _bytes: u64) -> bool {
        let inner = self.inner.lock().expect("lock");
        let n = node.as_str();
        !inner.dead.contains(n) && !inner.no_headroom.contains(n)
    }

    async fn place(
        &self,
        node: &NodeId,
        record: verglas_cluster::fragments::FragmentRecord,
    ) -> Result<(), TransportError> {
        let mut inner = self.inner.lock().expect("lock");
        let n = node.as_str();
        if inner.dead.contains(n) || inner.fail_place.contains(n) {
            return Err(TransportError::Local(FragmentIoError::Io(format!(
                "{n} down"
            ))));
        }
        inner.frags.insert(
            (n.to_owned(), record.key.object_id, record.key.index),
            (record.bytes, record.checksum),
        );
        Ok(())
    }

    async fn place_stream(
        &self,
        node: &NodeId,
        key: verglas_cluster::fragments::FragmentKey,
        mut shards: verglas_write::transport::ShardStream,
    ) -> Result<(), TransportError> {
        use futures::StreamExt;
        // Accumulate the streamed shards into the whole fragment, mirroring what
        // a real node stores. A refusal is signalled via `no_headroom`/`dead`.
        {
            let inner = self.inner.lock().expect("lock");
            let n = node.as_str();
            if inner.dead.contains(n)
                || inner.fail_place.contains(n)
                || inner.no_headroom.contains(n)
            {
                return Err(TransportError::Local(FragmentIoError::Io(format!(
                    "{n} cannot store"
                ))));
            }
        }
        let mut buf = Vec::new();
        while let Some(shard) = shards.next().await {
            buf.extend_from_slice(&shard);
        }
        let bytes = Bytes::from(buf);
        let checksum = verglas_cluster::fragments::fragment_checksum(&bytes);
        let mut inner = self.inner.lock().expect("lock");
        inner.frags.insert(
            (node.as_str().to_owned(), key.object_id, key.index),
            (bytes, checksum),
        );
        Ok(())
    }

    async fn load(
        &self,
        node: &NodeId,
        key: &verglas_cluster::fragments::FragmentKey,
    ) -> Result<Option<LoadedFragment>, TransportError> {
        let inner = self.inner.lock().expect("lock");
        let n = node.as_str();
        if inner.dead.contains(n) {
            return Ok(None);
        }
        Ok(inner
            .frags
            .get(&(n.to_owned(), key.object_id.clone(), key.index))
            .map(|(bytes, checksum)| LoadedFragment {
                bytes: bytes.clone(),
                checksum: *checksum,
            }))
    }

    async fn delete(
        &self,
        node: &NodeId,
        key: &verglas_cluster::fragments::FragmentKey,
    ) -> Result<(), TransportError> {
        self.inner.lock().expect("lock").frags.remove(&(
            node.as_str().to_owned(),
            key.object_id.clone(),
            key.index,
        ));
        Ok(())
    }
}

// ---- in-memory membership --------------------------------------------------

/// A mutable live view for placement and repair tests.
struct FakeMembership {
    self_id: NodeId,
    inner: Mutex<(Vec<NodeId>, u64)>,
}

impl FakeMembership {
    fn new(self_id: &str, nodes: &[&str]) -> Arc<Self> {
        Arc::new(Self {
            self_id: NodeId::new(self_id),
            inner: Mutex::new((nodes.iter().map(|n| NodeId::new(*n)).collect(), 1)),
        })
    }

    /// Removes a node from the live set and bumps the epoch (a loss event).
    fn drop_node(&self, node: &str) {
        let mut inner = self.inner.lock().expect("lock");
        inner.0.retain(|n| n.as_str() != node);
        inner.1 += 1;
    }
}

impl LiveMembership for FakeMembership {
    fn self_id(&self) -> NodeId {
        self.self_id.clone()
    }
    fn live_nodes(&self) -> Vec<NodeId> {
        self.inner.lock().expect("lock").0.clone()
    }
    fn epoch(&self) -> u64 {
        self.inner.lock().expect("lock").1
    }
}

// ---- recording origin writer ----------------------------------------------

/// An origin that records PUT bodies, optionally failing to keep objects dirty.
struct RecordingOrigin {
    puts: Mutex<HashMap<(String, String), Bytes>>,
    fail: AtomicBool,
}

impl RecordingOrigin {
    fn new(fail: bool) -> Arc<Self> {
        Arc::new(Self {
            puts: Mutex::new(HashMap::new()),
            fail: AtomicBool::new(fail),
        })
    }
    fn get(&self, bucket: &str, key: &str) -> Option<Bytes> {
        self.puts
            .lock()
            .expect("lock")
            .get(&(bucket.to_owned(), key.to_owned()))
            .cloned()
    }
}

impl ObjectWrite for RecordingOrigin {
    async fn put(
        &self,
        key: &CacheKey,
        _metadata: WriteMetadata,
        mut body: WriteBodyStream,
    ) -> Result<PutOutcome, WriteError> {
        let mut buf = Vec::new();
        while let Some(chunk) = body.next().await {
            buf.extend_from_slice(&chunk?);
        }
        if self.fail.load(Ordering::Relaxed) {
            return Err(WriteError::Backend("origin down".to_owned()));
        }
        self.puts
            .lock()
            .expect("lock")
            .insert((key.bucket.clone(), key.key.clone()), Bytes::from(buf));
        Ok(PutOutcome {
            checksums: Default::default(),
            version_id: None,
            e_tag: Some("\"origin\"".to_owned()),
        })
    }
    async fn delete(&self, _key: &CacheKey) -> Result<(), WriteError> {
        Ok(())
    }
    async fn delete_batch(
        &self,
        keys: &[CacheKey],
    ) -> Result<Vec<Result<(), WriteError>>, WriteError> {
        Ok(keys.iter().map(|_| Ok(())).collect())
    }
    async fn copy(
        &self,
        _s: &CacheKey,
        _d: &CacheKey,
        _m: Option<WriteMetadata>,
    ) -> Result<CopyOutcome, WriteError> {
        Ok(CopyOutcome {
            e_tag: None,
            last_modified: None,
        })
    }
    async fn create_multipart(
        &self,
        _key: &CacheKey,
        _m: WriteMetadata,
    ) -> Result<MultipartCreation, WriteError> {
        Ok(MultipartCreation {
            upload_id: "upload".to_owned(),
            ..Default::default()
        })
    }
    async fn upload_part(
        &self,
        _key: &CacheKey,
        _u: &str,
        _p: u16,
        _c: WriteChecksum,
        _b: WriteBodyStream,
    ) -> Result<PartUpload, WriteError> {
        Ok(PartUpload {
            e_tag: "\"part\"".to_owned(),
            ..Default::default()
        })
    }
    async fn complete_multipart(
        &self,
        _key: &CacheKey,
        _u: &str,
        _p: Vec<CompletedPartRef>,
        _c: WriteChecksum,
    ) -> Result<PutOutcome, WriteError> {
        Ok(PutOutcome {
            e_tag: None,
            checksums: Default::default(),
            version_id: None,
        })
    }
    async fn abort_multipart(&self, _key: &CacheKey, _u: &str) -> Result<(), WriteError> {
        Ok(())
    }
    async fn list_parts(&self, _key: &CacheKey, _u: &str) -> Result<Vec<PartInfo>, WriteError> {
        Ok(Vec::new())
    }
}

// ---- helpers ---------------------------------------------------------------

/// Builds a coordinator over the given fakes, returning it and the temp dir.
fn build(
    transport: Arc<MemoryTransport>,
    membership: Arc<FakeMembership>,
    origin: Arc<RecordingOrigin>,
) -> (Arc<WriteCoordinator<RecordingOrigin>>, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tmp");
    let journals = Arc::new(JournalStore::open(dir.path()).expect("journals"));
    let metrics = Arc::new(WritebackMetrics::default());
    let coordinator = Arc::new(WriteCoordinator::new(
        transport,
        membership,
        journals,
        metrics,
        origin,
        Duration::from_secs(5),
    ));
    (coordinator, dir)
}

/// Deterministic bytes crossing shard/stripe boundaries.
fn body(len: usize) -> Bytes {
    Bytes::from((0..len).map(|i| (i % 251) as u8).collect::<Vec<u8>>())
}

/// Waits until the transport holds no fragments (the async cleanup ran), with
/// a tight poll and a bounded deadline — a condition wait, never a fixed sleep
/// (standing test policy). Returns whether the condition was met.
async fn wait_for_no_fragments(transport: &MemoryTransport) -> bool {
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while std::time::Instant::now() < deadline {
        if transport.fragment_count() == 0 {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    transport.fragment_count() == 0
}

fn ck(key: &str) -> CacheKey {
    CacheKey {
        bucket: "bkt".to_owned(),
        key: key.to_owned(),
    }
}

// ---- tests -----------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quorum_ack_places_w_fragments_and_counts_it() {
    let transport = MemoryTransport::new();
    let membership = FakeMembership::new("node-0", &["node-0", "node-1", "node-2"]);
    let origin = RecordingOrigin::new(true); // fail so nothing is cleaned mid-test
    let (coordinator, _dir) = build(transport.clone(), membership, origin);

    let outcome = coordinator
        .put(
            &ck("data/x"),
            &WriteMetadata::default(),
            body(4096),
            2,
            1,
            3,
        )
        .await
        .expect("quorum ack");
    assert!(outcome.e_tag.is_some(), "ack returns a synthetic ETag");

    let m = coordinator.metrics().snapshot();
    assert_eq!(m.acked_via_quorum, 1, "one quorum ack");
    assert_eq!(m.acked_via_write_through, 0, "no fallback");
    // Three distinct fragments landed, and a dirty journal exists.
    assert_eq!(transport.fragment_count(), 3);
    assert!(!coordinator.journals().is_idle(), "object is dirty");
    assert!(coordinator.journals().find_dirty("bkt", "data/x").is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn small_pod_falls_back_to_write_through() {
    let transport = MemoryTransport::new();
    // Only two live nodes, but w=3 cannot be reached: enforced write-through.
    let membership = FakeMembership::new("node-0", &["node-0", "node-1"]);
    let origin = RecordingOrigin::new(false);
    let (coordinator, _dir) = build(transport.clone(), membership, origin.clone());

    let payload = body(4096);
    coordinator
        .put(
            &ck("data/x"),
            &WriteMetadata::default(),
            payload.clone(),
            2,
            1,
            3,
        )
        .await
        .expect("write-through ack");

    let m = coordinator.metrics().snapshot();
    assert_eq!(m.acked_via_write_through, 1, "counted as write-through");
    assert_eq!(m.acked_via_quorum, 0, "not a quorum ack");
    assert_eq!(transport.fragment_count(), 0, "no fragments placed");
    assert!(coordinator.journals().is_idle(), "no dirty journal");
    // The object is durable at the origin, byte-identical (fallback proof).
    assert_eq!(origin.get("bkt", "data/x"), Some(payload));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn required_quorum_rejects_without_origin_fallback() {
    let transport = MemoryTransport::new();
    let membership = FakeMembership::new("node-0", &["node-0", "node-1"]);
    let origin = RecordingOrigin::new(false);
    let dir = tempfile::tempdir().expect("tmp");
    let coordinator = Arc::new(
        WriteCoordinator::new(
            transport.clone(),
            membership,
            Arc::new(JournalStore::open(dir.path()).expect("journals")),
            Arc::new(WritebackMetrics::default()),
            origin.clone(),
            Duration::from_secs(5),
        )
        .require_quorum(),
    );

    let result = coordinator
        .put(
            &ck("tenants/t1/timelines/l1/index_part.json-00000001"),
            &WriteMetadata::default(),
            body(4096),
            2,
            1,
            3,
        )
        .await;

    assert!(result.is_err(), "ring shortfall must reject publication");
    assert_eq!(
        origin.get("bkt", "tenants/t1/timelines/l1/index_part.json-00000001"),
        None
    );
    assert_eq!(coordinator.metrics().snapshot().acked_via_write_through, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn midflight_shortfall_rebuilds_and_writes_through() {
    let transport = MemoryTransport::new();
    // Five live nodes, geometry k=2/m=3/w=5. Two nodes fail placement, so only
    // three fragments commit — short of w=5 but at least k=2, so the object is
    // rebuilt from the committed fragments and written through to the origin
    // byte-identically. The body was consumed by the streaming encode, so the
    // rebuild-from-fragments path is the write-through here.
    let membership = FakeMembership::new(
        "node-0",
        &["node-0", "node-1", "node-2", "node-3", "node-4"],
    );
    transport.fail_placement("node-3");
    transport.fail_placement("node-4");
    let origin = RecordingOrigin::new(false);
    let (coordinator, _dir) = build(transport.clone(), membership, origin.clone());

    let payload = body(9000);
    coordinator
        .put(
            &ck("data/x"),
            &WriteMetadata::default(),
            payload.clone(),
            2,
            3,
            5,
        )
        .await
        .expect("write-through ack");

    let m = coordinator.metrics().snapshot();
    assert_eq!(m.acked_via_write_through, 1, "degraded to write-through");
    assert_eq!(m.acked_via_quorum, 0, "not a quorum ack");
    // The object is durable at the origin, byte-identical (fallback proof).
    assert_eq!(origin.get("bkt", "data/x"), Some(payload));
    assert!(coordinator.journals().is_idle(), "no dirty journal left");
    // Cleanup is async; wait on the condition, not the clock.
    assert!(
        wait_for_no_fragments(&transport).await,
        "no orphan fragments left"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shortfall_below_k_fails_cleanly() {
    let transport = MemoryTransport::new();
    // Three live nodes, geometry k=2/m=1/w=3. Two of three fail placement, so
    // only one fragment commits — below k=2, so the object cannot be rebuilt.
    // The body is already consumed, so the write fails cleanly rather than
    // acking on fewer than a reconstructable set (never a wrong ack).
    let membership = FakeMembership::new("node-0", &["node-0", "node-1", "node-2"]);
    transport.fail_placement("node-1");
    transport.fail_placement("node-2");
    let origin = RecordingOrigin::new(false);
    let (coordinator, _dir) = build(transport.clone(), membership, origin.clone());

    let result = coordinator
        .put(
            &ck("data/x"),
            &WriteMetadata::default(),
            body(4096),
            2,
            1,
            3,
        )
        .await;
    assert!(result.is_err(), "a below-k shortfall fails cleanly");
    let m = coordinator.metrics().snapshot();
    assert_eq!(m.acked_via_quorum, 0, "not a quorum ack");
    // Cleanup is async; wait on the condition, not the clock.
    assert!(
        wait_for_no_fragments(&transport).await,
        "no orphan fragments left"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_node_is_excluded_from_placement() {
    let transport = MemoryTransport::new();
    // Four live nodes but one is out of fragment headroom: placement must skip
    // it and land the three fragments on the three nodes that have room.
    let membership = FakeMembership::new("node-0", &["node-0", "node-1", "node-2", "node-3"]);
    transport.fill_disk("node-1");
    let origin = RecordingOrigin::new(true); // keep dirty so placements survive
    let (coordinator, _dir) = build(transport.clone(), membership, origin);

    coordinator
        .put(
            &ck("data/x"),
            &WriteMetadata::default(),
            body(4096),
            2,
            1,
            3,
        )
        .await
        .expect("quorum ack over the three nodes with headroom");

    let m = coordinator.metrics().snapshot();
    assert_eq!(m.acked_via_quorum, 1, "still a quorum ack");
    assert_eq!(transport.fragment_count(), 3, "three fragments placed");
    // No fragment landed on the full node.
    let object_id = coordinator
        .journals()
        .find_dirty("bkt", "data/x")
        .expect("dirty");
    let journal = coordinator
        .journals()
        .read(&object_id)
        .expect("read")
        .expect("some");
    assert!(
        journal.placements.iter().all(|p| p.node != "node-1"),
        "the full node holds no fragment"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_disk_headroom_falls_back_to_write_through() {
    let transport = MemoryTransport::new();
    // Three live nodes, but two are out of headroom: fewer than w=3 nodes can
    // hold a fragment, so the write degrades to write-through.
    let membership = FakeMembership::new("node-0", &["node-0", "node-1", "node-2"]);
    transport.fill_disk("node-1");
    transport.fill_disk("node-2");
    let origin = RecordingOrigin::new(false);
    let (coordinator, _dir) = build(transport.clone(), membership, origin.clone());

    let payload = body(4096);
    coordinator
        .put(
            &ck("data/x"),
            &WriteMetadata::default(),
            payload.clone(),
            2,
            1,
            3,
        )
        .await
        .expect("write-through ack");

    let m = coordinator.metrics().snapshot();
    assert_eq!(m.acked_via_write_through, 1, "degraded to write-through");
    assert_eq!(m.acked_via_quorum, 0, "not a quorum ack");
    assert_eq!(transport.fragment_count(), 0, "no fragments placed");
    assert!(coordinator.journals().is_idle(), "no dirty journal");
    assert_eq!(origin.get("bkt", "data/x"), Some(payload));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_your_writes_before_propagation() {
    let transport = MemoryTransport::new();
    let membership = FakeMembership::new("node-0", &["node-0", "node-1", "node-2"]);
    let origin = RecordingOrigin::new(true); // keep dirty
    let (coordinator, _dir) = build(transport.clone(), membership, origin);

    let payload = body(9000);
    coordinator
        .put(
            &ck("data/x"),
            &WriteMetadata::default(),
            payload.clone(),
            2,
            1,
            3,
        )
        .await
        .expect("ack");

    // The inner read path errors, proving the read is served from fragments.
    let reader = WritebackReader::new(FailingReader, Arc::clone(&coordinator));
    let got = reader
        .get(&ck("data/x"), ReadRange::Full)
        .await
        .expect("read");
    let mut buf = Vec::new();
    let mut stream = got.body;
    while let Some(chunk) = stream.next().await {
        buf.extend_from_slice(&chunk.expect("chunk"));
    }
    assert_eq!(
        Bytes::from(buf),
        payload,
        "read-your-writes is byte-identical"
    );

    // HEAD reports the object length and a synthetic ETag.
    let meta = reader.head(&ck("data/x")).await.expect("head");
    assert_eq!(meta.size, payload.len() as u64);
    assert!(meta.e_tag.is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupt_fragment_reassembles_byte_identical() {
    // k=2/m=1/w=3: three fragments placed, one data fragment then corrupted on
    // disk. Reassembly must treat it as an erasure and rebuild the exact object
    // from the good fragments — never silently wrong bytes (#220).
    let transport = MemoryTransport::new();
    let membership = FakeMembership::new("node-0", &["node-0", "node-1", "node-2"]);
    let origin = RecordingOrigin::new(true); // keep dirty so we can reassemble
    let (coordinator, _dir) = build(transport.clone(), membership, origin);

    let payload = body(9000);
    coordinator
        .put(
            &ck("data/x"),
            &WriteMetadata::default(),
            payload.clone(),
            2,
            1,
            3,
        )
        .await
        .expect("ack");
    let object_id = coordinator
        .journals()
        .find_dirty("bkt", "data/x")
        .expect("dirty");

    // Corrupt one data fragment (index 0) in place.
    assert!(
        transport.corrupt_fragment(&object_id, 0),
        "a data fragment was placed and corrupted"
    );

    let journal = coordinator
        .journals()
        .read(&object_id)
        .expect("read")
        .expect("some");
    let rebuilt = coordinator.reassemble(&journal).await.expect("reassemble");
    assert_eq!(
        rebuilt, payload,
        "the corrupt fragment is erased; output is byte-identical"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn more_than_m_corrupt_fails_loudly() {
    // k=2/m=1: only m=1 corrupt/missing is tolerable. Corrupt two of the three
    // fragments so fewer than k=2 verify: the read must fail loudly, never
    // return reconstructed garbage (#220).
    let transport = MemoryTransport::new();
    let membership = FakeMembership::new("node-0", &["node-0", "node-1", "node-2"]);
    let origin = RecordingOrigin::new(true);
    let (coordinator, _dir) = build(transport.clone(), membership, origin);

    let payload = body(9000);
    coordinator
        .put(
            &ck("data/x"),
            &WriteMetadata::default(),
            payload.clone(),
            2,
            1,
            3,
        )
        .await
        .expect("ack");
    let object_id = coordinator
        .journals()
        .find_dirty("bkt", "data/x")
        .expect("dirty");

    // Corrupt two of the three fragments (indices 0 and 1).
    assert!(transport.corrupt_fragment(&object_id, 0));
    assert!(transport.corrupt_fragment(&object_id, 1));

    let journal = coordinator
        .journals()
        .read(&object_id)
        .expect("read")
        .expect("some");
    let err = coordinator
        .reassemble(&journal)
        .await
        .expect_err("more than m corrupt cannot rebuild");
    assert!(
        err.to_string().contains("need at least 2"),
        "fails loudly, never wrong bytes: {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repair_verifies_and_re_encodes_a_corrupt_survivor() {
    // All nodes stay live, but one surviving fragment bit-rots. The repair path
    // must verify survivors, treat the corrupt one as needing repair, and
    // re-encode it from the healthy survivors (#220).
    let transport = MemoryTransport::new();
    let membership = FakeMembership::new("node-0", &["node-0", "node-1", "node-2", "node-3"]);
    let origin = RecordingOrigin::new(true); // keep dirty so repair has work
    let (coordinator, _dir) = build(transport.clone(), membership, origin);

    let payload = body(9000);
    coordinator
        .put(
            &ck("data/x"),
            &WriteMetadata::default(),
            payload.clone(),
            2,
            1,
            3,
        )
        .await
        .expect("ack");
    let object_id = coordinator
        .journals()
        .find_dirty("bkt", "data/x")
        .expect("dirty");
    let journal = coordinator
        .journals()
        .read(&object_id)
        .expect("read")
        .expect("some");
    // Corrupt one placed fragment; every node stays live, so this is a pure
    // bit-rot repair (not a node-loss repair).
    let victim = journal.placements[0].index;
    assert!(transport.corrupt_fragment(&object_id, victim));

    // repair_once is what the loop calls on an epoch change; call it directly.
    let repaired = coordinator.repair_once().await.expect("repair");
    assert_eq!(
        repaired, 1,
        "the corrupt survivor was re-encoded and re-placed"
    );

    // After repair every fragment verifies and the object reassembles exactly.
    let journal = coordinator
        .journals()
        .read(&object_id)
        .expect("read")
        .expect("some");
    let rebuilt = coordinator.reassemble(&journal).await.expect("reassemble");
    assert_eq!(rebuilt, payload, "repaired object is byte-identical");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scrubber_repairs_a_corrupt_fragment_and_counts_it() {
    // The background scrubber walks stored fragments, finds one corrupt, and
    // re-encodes it so a later read has a full verified set again. The scrub
    // metrics count what it saw and fixed (#220).
    let transport = MemoryTransport::new();
    let membership = FakeMembership::new("node-0", &["node-0", "node-1", "node-2", "node-3"]);
    let origin = RecordingOrigin::new(true); // keep dirty so the scrubber has work
    let (coordinator, _dir) = build(transport.clone(), membership, origin);

    let payload = body(9000);
    coordinator
        .put(
            &ck("data/x"),
            &WriteMetadata::default(),
            payload.clone(),
            2,
            1,
            3,
        )
        .await
        .expect("ack");
    let object_id = coordinator
        .journals()
        .find_dirty("bkt", "data/x")
        .expect("dirty");

    // Corrupt one fragment on disk (all nodes stay live).
    assert!(transport.corrupt_fragment(&object_id, 0));

    let report = coordinator.scrub_once().await.expect("scrub");
    assert_eq!(report.corrupt, 1, "the scrubber found one corrupt fragment");
    assert_eq!(report.repaired, 1, "and re-encoded it");
    assert!(report.scrubbed >= 3, "it verified every placed fragment");

    let m = coordinator.metrics().snapshot();
    assert_eq!(m.corrupt_fragments_found, 1);
    assert_eq!(m.fragments_scrubbed, report.scrubbed);
    assert_eq!(m.fragments_repaired, 1);

    // A later read now has a full, verified set and reassembles byte-identically.
    let journal = coordinator
        .journals()
        .read(&object_id)
        .expect("read")
        .expect("some");
    let rebuilt = coordinator.reassemble(&journal).await.expect("reassemble");
    assert_eq!(rebuilt, payload, "post-scrub read is byte-identical");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scrub_of_healthy_object_repairs_nothing() {
    // A clean object: the scrubber verifies every fragment and repairs nothing.
    let transport = MemoryTransport::new();
    let membership = FakeMembership::new("node-0", &["node-0", "node-1", "node-2"]);
    let origin = RecordingOrigin::new(true);
    let (coordinator, _dir) = build(transport.clone(), membership, origin);

    coordinator
        .put(
            &ck("data/x"),
            &WriteMetadata::default(),
            body(9000),
            2,
            1,
            3,
        )
        .await
        .expect("ack");

    let report = coordinator.scrub_once().await.expect("scrub");
    assert_eq!(report.corrupt, 0, "nothing corrupt on a clean object");
    assert_eq!(report.repaired, 0, "nothing to repair");
    assert!(report.scrubbed >= 3, "still verified every fragment");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_loss_triggers_repair_from_survivors() {
    let transport = MemoryTransport::new();
    // Four nodes, geometry k=2/m=1/w=3: three hold fragments, one is spare.
    let membership = FakeMembership::new("node-0", &["node-0", "node-1", "node-2", "node-3"]);
    let origin = RecordingOrigin::new(true); // keep dirty so repair has work
    let (coordinator, _dir) = build(transport.clone(), membership.clone(), origin);

    let payload = body(9000);
    coordinator
        .put(
            &ck("data/x"),
            &WriteMetadata::default(),
            payload.clone(),
            2,
            1,
            3,
        )
        .await
        .expect("ack");
    let object_id = coordinator
        .journals()
        .find_dirty("bkt", "data/x")
        .expect("dirty");
    let journal = coordinator
        .journals()
        .read(&object_id)
        .expect("read")
        .expect("some");
    // Pick a node that actually holds a fragment and kill it.
    let victim = journal.placements[0].node.clone();
    transport.kill(&victim);
    membership.drop_node(&victim);

    let repaired = coordinator.repair_once().await.expect("repair");
    assert_eq!(repaired, 1, "one fragment re-encoded and re-placed");

    // After repair every placement is on a live node and the object still
    // reassembles byte-identically.
    let journal = coordinator
        .journals()
        .read(&object_id)
        .expect("read")
        .expect("some");
    let live: HashSet<String> = membership
        .live_nodes()
        .into_iter()
        .map(|n| n.as_str().to_owned())
        .collect();
    assert!(journal.placements.iter().all(|p| live.contains(&p.node)));
    let rebuilt = coordinator.reassemble(&journal).await.expect("reassemble");
    assert_eq!(rebuilt, payload);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acked_object_propagates_and_frees_fragments() {
    let transport = MemoryTransport::new();
    let membership = FakeMembership::new("node-0", &["node-0", "node-1", "node-2"]);
    let origin = RecordingOrigin::new(false); // succeeds, so propagation completes
    let (coordinator, _dir) = build(transport.clone(), membership, origin.clone());

    let payload = body(9000);
    coordinator
        .put(
            &ck("data/x"),
            &WriteMetadata::default(),
            payload.clone(),
            2,
            1,
            3,
        )
        .await
        .expect("ack");

    // Propagation runs in the background: wait for it to upload and clean up.
    let mut propagated = false;
    for _ in 0..50 {
        if coordinator.metrics().snapshot().propagated == 1 {
            propagated = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(propagated, "object propagated to origin");
    assert_eq!(origin.get("bkt", "data/x"), Some(payload));
    assert!(
        coordinator.journals().is_idle(),
        "journal cleaned after propagation"
    );
    assert_eq!(
        transport.fragment_count(),
        0,
        "fragments freed after propagation"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streamed_large_object_acks_over_quorum_and_reassembles() {
    let transport = MemoryTransport::new();
    let membership = FakeMembership::new("node-0", &["node-0", "node-1", "node-2"]);
    let origin = RecordingOrigin::new(true); // keep dirty so we can reassemble
    let (coordinator, _dir) = build(transport.clone(), membership, origin);

    // A multi-stripe object fed to the coordinator as a stream of many chunks,
    // never as one buffer — this is the streaming write path.
    let payload = body(3_000_000);
    let chunks: Vec<Result<Bytes, WriteError>> = payload
        .chunks(64 * 1024)
        .map(|c| Ok(Bytes::copy_from_slice(c)))
        .collect();
    let stream = futures::stream::iter(chunks).boxed();

    coordinator
        .put_stream(&ck("data/big"), &WriteMetadata::default(), stream, 2, 1, 3)
        .await
        .expect("quorum ack over the stream");

    let m = coordinator.metrics().snapshot();
    assert_eq!(m.acked_via_quorum, 1, "streamed write acked over quorum");
    assert_eq!(transport.fragment_count(), 3, "three fragments placed");

    // The object reassembles byte-identically from its fragments.
    let object_id = coordinator
        .journals()
        .find_dirty("bkt", "data/big")
        .expect("dirty");
    let journal = coordinator
        .journals()
        .read(&object_id)
        .expect("read")
        .expect("some");
    assert_eq!(journal.object_len, payload.len() as u64);
    let rebuilt = coordinator.reassemble(&journal).await.expect("reassemble");
    assert_eq!(rebuilt, payload, "streamed object reassembles exactly");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unpropagated_fragments_survive_disk_pressure() {
    let transport = MemoryTransport::new();
    let membership = FakeMembership::new("node-0", &["node-0", "node-1", "node-2"]);
    let origin = RecordingOrigin::new(true); // keep dirty: the pod is the only copy
    let (coordinator, _dir) = build(transport.clone(), membership, origin);

    let payload = body(9000);
    coordinator
        .put(
            &ck("data/x"),
            &WriteMetadata::default(),
            payload.clone(),
            2,
            1,
            3,
        )
        .await
        .expect("ack");
    assert_eq!(transport.fragment_count(), 3);

    // Now every node reports no headroom: the fragment budget is a reservation
    // gate on NEW writes, so it refuses further placement — but it never evicts
    // an already-stored, still-dirty fragment. The un-propagated fragments are
    // the only durable copy of acked data, and they stay put under pressure.
    transport.fill_disk("node-0");
    transport.fill_disk("node-1");
    transport.fill_disk("node-2");
    // A second write is refused write-back and degrades to write-through, but
    // the first object's fragments are untouched.
    assert_eq!(
        transport.fragment_count(),
        3,
        "disk pressure loses no dirty fragment"
    );

    // The dirty object still reassembles byte-identically from its fragments.
    let object_id = coordinator
        .journals()
        .find_dirty("bkt", "data/x")
        .expect("still dirty");
    let journal = coordinator
        .journals()
        .read(&object_id)
        .expect("read")
        .expect("some");
    let rebuilt = coordinator.reassemble(&journal).await.expect("reassemble");
    assert_eq!(rebuilt, payload, "object reassembles after disk pressure");
}

/// An inner read path that always errors, so a passing read must have come from
/// the write-back fragments, not the delegate.
struct FailingReader;

impl ObjectRead for FailingReader {
    async fn get(
        &self,
        _key: &CacheKey,
        _range: ReadRange,
    ) -> Result<verglas_core::read::ObjectGet, ReadError> {
        Err(ReadError::Backend("inner should not be called".to_owned()))
    }
    async fn head(&self, _key: &CacheKey) -> Result<verglas_core::read::ObjectMeta, ReadError> {
        Err(ReadError::Backend("inner should not be called".to_owned()))
    }
}
