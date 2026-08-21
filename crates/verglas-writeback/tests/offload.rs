//! Object offload stream contract tests (#164 §4), mapped to the frozen
//! objective's acceptance criteria with in-memory fakes:
//!
//! - `small_objects_accumulate_then_flush_as_one_pack_on_threshold`: objects
//!   under the size limit accumulate; crossing the limit flushes the whole
//!   batch as one origin PUT, and every packed key reads back byte-identical
//!   through the pack index.
//! - `drain_flushes_a_partial_stream_and_second_drain_is_a_noop`: an explicit
//!   drain flushes a still-under-threshold stream; a second drain on the now
//!   empty stream issues no further origin PUT.
//! - `object_over_the_limit_bypasses_accumulation`: an object at or above the
//!   size limit uploads alone, at its own key, never inside a pack.
//! - `deleting_a_packed_key_stops_it_resolving`: a delete after a flush
//!   removes the key's pack index entry.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use verglas_cluster::fragments::LoadedFragment;
use verglas_core::CacheKey;
use verglas_core::node::NodeId;
use verglas_core::read::{BodyStream, ObjectGet, ObjectMeta, ObjectRead, ReadError, ReadRange};
use verglas_core::write::{
    CompletedPartRef, CopyOutcome, MultipartCreation, ObjectWrite, PartInfo, PartUpload,
    PutOutcome, WriteBodyStream, WriteChecksum, WriteError, WriteMetadata,
};
use verglas_writeback::coordinator::WriteCoordinator;
use verglas_writeback::journal::JournalStore;
use verglas_writeback::membership::LiveMembership;
use verglas_writeback::metrics::WritebackMetrics;
use verglas_writeback::reader::WritebackReader;
use verglas_writeback::transport::{FragmentTransport, TransportError};
use verglas_writeback::{ConsensusCommitter, ObjectCommit, PackCommit, StagedObject, StagedPack};

// ---- in-memory fragment transport ------------------------------------------

#[derive(Default)]
struct TransportInner {
    frags: HashMap<(String, String, usize), (Bytes, u32)>,
}

struct MemoryTransport {
    inner: Mutex<TransportInner>,
}

impl MemoryTransport {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(TransportInner::default()),
        })
    }

    fn fragment_count(&self) -> usize {
        self.inner.lock().expect("lock").frags.len()
    }
}

#[async_trait::async_trait]
impl FragmentTransport for MemoryTransport {
    async fn has_headroom(&self, _node: &NodeId, _bytes: u64) -> bool {
        true
    }

    async fn place(
        &self,
        node: &NodeId,
        record: verglas_cluster::fragments::FragmentRecord,
    ) -> Result<(), TransportError> {
        self.inner.lock().expect("lock").frags.insert(
            (
                node.as_str().to_owned(),
                record.key.object_id,
                record.key.index,
            ),
            (record.bytes, record.checksum),
        );
        Ok(())
    }

    async fn place_stream(
        &self,
        node: &NodeId,
        key: verglas_cluster::fragments::FragmentKey,
        mut shards: verglas_writeback::transport::ShardStream,
    ) -> Result<(), TransportError> {
        let mut buf = Vec::new();
        while let Some(shard) = shards.next().await {
            buf.extend_from_slice(&shard);
        }
        let bytes = Bytes::from(buf);
        let checksum = verglas_cluster::fragments::fragment_checksum(&bytes);
        self.inner.lock().expect("lock").frags.insert(
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
        Ok(self
            .inner
            .lock()
            .expect("lock")
            .frags
            .get(&(node.as_str().to_owned(), key.object_id.clone(), key.index))
            .map(|(bytes, checksum)| LoadedFragment {
                bytes: bytes.clone(),
                checksum: *checksum,
            }))
    }

    async fn list(
        &self,
        _node: &NodeId,
        _prefix: &str,
    ) -> Result<Vec<verglas_cluster::fragments::FragmentKey>, TransportError> {
        Ok(Vec::new())
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

// ---- fixed three-node membership -------------------------------------------

struct FixedMembership {
    self_id: NodeId,
    nodes: Vec<NodeId>,
}

impl FixedMembership {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            self_id: NodeId::new("node-0"),
            nodes: vec![
                NodeId::new("node-0"),
                NodeId::new("node-1"),
                NodeId::new("node-2"),
            ],
        })
    }
}

impl LiveMembership for FixedMembership {
    fn self_id(&self) -> NodeId {
        self.self_id.clone()
    }
    fn live_nodes(&self) -> Vec<NodeId> {
        self.nodes.clone()
    }
    fn epoch(&self) -> u64 {
        1
    }
}

// ---- an origin that is both the write and read path, backed by one map ----

/// Stands in for the real S3 origin on both sides of the write-back tier: the
/// same bucket a flush uploads a pack into is the bucket a read-your-writes
/// GET falls through to once a key is no longer dirty, exactly as in
/// production.
struct RecordingOrigin {
    puts: Mutex<HashMap<(String, String), Bytes>>,
    put_count: AtomicUsize,
}

impl RecordingOrigin {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            puts: Mutex::new(HashMap::new()),
            put_count: AtomicUsize::new(0),
        })
    }

    fn get_bytes(&self, bucket: &str, key: &str) -> Option<Bytes> {
        self.puts
            .lock()
            .expect("lock")
            .get(&(bucket.to_owned(), key.to_owned()))
            .cloned()
    }

    fn put_count(&self) -> usize {
        self.put_count.load(Ordering::SeqCst)
    }

    fn keys(&self) -> Vec<String> {
        self.puts
            .lock()
            .expect("lock")
            .keys()
            .map(|(_, key)| key.clone())
            .collect()
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
        self.puts
            .lock()
            .expect("lock")
            .insert((key.bucket.clone(), key.key.clone()), Bytes::from(buf));
        self.put_count.fetch_add(1, Ordering::SeqCst);
        Ok(PutOutcome {
            checksums: Default::default(),
            version_id: None,
            e_tag: Some("\"origin\"".to_owned()),
        })
    }
    async fn delete(&self, key: &CacheKey) -> Result<(), WriteError> {
        self.puts
            .lock()
            .expect("lock")
            .remove(&(key.bucket.clone(), key.key.clone()));
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

impl ObjectRead for RecordingOrigin {
    async fn get(&self, key: &CacheKey, range: ReadRange) -> Result<ObjectGet, ReadError> {
        let Some(bytes) = self.get_bytes(&key.bucket, &key.key) else {
            return Err(ReadError::NoSuchKey);
        };
        let size = bytes.len() as u64;
        let served = match range {
            ReadRange::Full => 0..size,
            ReadRange::From(first) => first..size,
            ReadRange::Bounded(first, last) => first..last.saturating_add(1).min(size),
            ReadRange::Suffix(length) => size.saturating_sub(length.min(size))..size,
        };
        let start = served.start as usize;
        let end = served.end as usize;
        Ok(ObjectGet {
            meta: ObjectMeta {
                size,
                e_tag: Some("\"origin\"".to_owned()),
                ..Default::default()
            },
            range: served,
            body: once_body(bytes.slice(start..end)),
            served_from: verglas_core::read::TierCell::new(),
        })
    }

    async fn head(&self, key: &CacheKey) -> Result<ObjectMeta, ReadError> {
        let Some(bytes) = self.get_bytes(&key.bucket, &key.key) else {
            return Err(ReadError::NoSuchKey);
        };
        Ok(ObjectMeta {
            size: bytes.len() as u64,
            e_tag: Some("\"origin\"".to_owned()),
            ..Default::default()
        })
    }
}

fn once_body(bytes: Bytes) -> BodyStream {
    futures::stream::once(async move { Ok(bytes) }).boxed()
}

// ---- consensus test double --------------------------------------------------

struct TestCommitter;

#[async_trait::async_trait]
impl ConsensusCommitter for TestCommitter {
    async fn commit(&self, staged: StagedObject) -> Result<ObjectCommit, String> {
        if staged.placements.is_empty() {
            return Err("empty certificate".to_owned());
        }
        Ok(ObjectCommit { index: 1 })
    }

    async fn commit_pack(&self, staged: StagedPack) -> Result<PackCommit, String> {
        if staged.entries.is_empty() {
            return Err("empty pack".to_owned());
        }
        Ok(PackCommit { index: 1 })
    }
}

// ---- helpers ----------------------------------------------------------------

fn ck(key: &str) -> CacheKey {
    CacheKey {
        storage_binding_id: "default".to_owned(),
        bucket: "bkt".to_owned(),
        key: key.to_owned(),
    }
}

fn body(len: usize) -> Bytes {
    Bytes::from((0..len).map(|i| (i % 251) as u8).collect::<Vec<u8>>())
}

/// Builds a coordinator with the given offload size limit.
fn build(
    size_limit_bytes: u64,
) -> (
    Arc<WriteCoordinator<RecordingOrigin>>,
    Arc<RecordingOrigin>,
    Arc<MemoryTransport>,
    tempfile::TempDir,
) {
    let transport = MemoryTransport::new();
    let membership = FixedMembership::new();
    let origin = RecordingOrigin::new();
    let dir = tempfile::tempdir().expect("tmp");
    let journals = Arc::new(JournalStore::open(dir.path()).expect("journals"));
    let metrics = Arc::new(WritebackMetrics::default());
    let coordinator = Arc::new(WriteCoordinator::new(
        transport.clone(),
        membership,
        journals,
        metrics,
        origin.clone(),
        Arc::new(TestCommitter),
        verglas_writeback::WritebackThresholds {
            ack_deadline: Duration::from_secs(5),
            offload_size_limit_bytes: size_limit_bytes,
        },
    ));
    (coordinator, origin, transport, dir)
}

/// Polls a condition with a bounded deadline instead of a fixed sleep
/// (standing test policy). 20 s: the condition can depend on a CPU-bound
/// Reed-Solomon reassemble in a background task, which can take much longer
/// than its own work under host CPU contention. The loop still returns
/// immediately once the condition is met.
async fn wait_until(mut condition: impl FnMut() -> bool) -> bool {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while std::time::Instant::now() < deadline {
        if condition() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    condition()
}

/// Retries an explicit drain until it observes a flush. The append to a
/// binding's offload stream happens in the ack's spawned background
/// continuation, so a single drain issued immediately after `put()` returns
/// can race ahead of that append and find nothing to flush; draining is a
/// no-op in that case, so retrying is safe and never double-flushes.
async fn drain_until_flushed(
    coordinator: &WriteCoordinator<RecordingOrigin>,
    origin: &RecordingOrigin,
    storage_binding_id: &str,
    bucket: &str,
) {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while origin.put_count() == 0 && std::time::Instant::now() < deadline {
        coordinator.drain_offload(storage_binding_id, bucket).await;
        if origin.put_count() == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

/// Reads a key end to end through the reader and returns its bytes.
async fn read_back(
    reader: &WritebackReader<Arc<RecordingOrigin>, RecordingOrigin>,
    key: &str,
) -> Bytes {
    let got = reader.get(&ck(key), ReadRange::Full).await.expect("read");
    let mut buf = Vec::new();
    let mut stream = got.body;
    while let Some(chunk) = stream.next().await {
        buf.extend_from_slice(&chunk.expect("chunk"));
    }
    Bytes::from(buf)
}

// ---- tests -----------------------------------------------------------------

/// Two objects under a 100-byte limit accumulate; the second append crosses
/// the threshold and flushes both as one origin PUT (the pack). Both keys
/// then read back byte-identical through the pack index, and their fragments
/// are freed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn small_objects_accumulate_then_flush_as_one_pack_on_threshold() {
    let (coordinator, origin, transport, _dir) = build(100);
    let payload_a = body(60);
    let payload_b = body(70);

    coordinator
        .put(
            &ck("small/a"),
            &WriteMetadata::default(),
            payload_a.clone(),
            2,
            1,
            3,
        )
        .await
        .expect("ack a");
    coordinator
        .put(
            &ck("small/b"),
            &WriteMetadata::default(),
            payload_b.clone(),
            2,
            1,
            3,
        )
        .await
        .expect("ack b");

    // Crossing 100 bytes (60 + 70) triggers the flush automatically; wait for
    // it rather than sleeping a fixed amount.
    assert!(
        wait_until(|| origin.put_count() >= 1).await,
        "threshold crossing flushed a pack"
    );
    assert_eq!(
        origin.put_count(),
        1,
        "exactly one origin PUT for both objects"
    );
    let pack_keys = origin.keys();
    assert_eq!(pack_keys.len(), 1, "one packed S3 object exists");
    assert!(
        pack_keys[0].starts_with("_verglas/packs/"),
        "pack lives under a reserved prefix: {}",
        pack_keys[0]
    );

    assert!(
        wait_until(|| coordinator.journals().is_idle()).await,
        "both journals cleaned after the flush"
    );
    assert!(
        wait_until(|| transport.fragment_count() == 0).await,
        "fragments freed after the flush"
    );

    let reader = WritebackReader::new(Arc::clone(&origin), Arc::clone(&coordinator));
    assert_eq!(read_back(&reader, "small/a").await, payload_a);
    assert_eq!(read_back(&reader, "small/b").await, payload_b);
}

/// A stream well under its size limit never flushes on its own; an explicit
/// drain flushes the partial buffer as one pack, and a second drain on the
/// now-empty stream issues no further origin PUT.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drain_flushes_a_partial_stream_and_second_drain_is_a_noop() {
    let (coordinator, origin, _transport, _dir) = build(1_000_000);
    let payload = body(50);

    coordinator
        .put(
            &ck("partial/x"),
            &WriteMetadata::default(),
            payload.clone(),
            2,
            1,
            3,
        )
        .await
        .expect("ack");

    // Give the ack continuation a chance to append to the stream before
    // asserting nothing has flushed yet.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(origin.put_count(), 0, "far under the limit: no auto-flush");
    assert!(!coordinator.journals().is_idle(), "still dirty, unflushed");

    drain_until_flushed(&coordinator, &origin, "default", "bkt").await;
    assert_eq!(
        origin.put_count(),
        1,
        "explicit drain flushes the partial buffer"
    );
    assert!(
        wait_until(|| coordinator.journals().is_idle()).await,
        "journal cleaned after drain"
    );

    // A second drain on the now-empty stream is a no-op: no new origin PUT.
    coordinator.drain_offload("default", "bkt").await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        origin.put_count(),
        1,
        "second drain on an empty stream is a no-op"
    );

    let reader = WritebackReader::new(Arc::clone(&origin), Arc::clone(&coordinator));
    assert_eq!(read_back(&reader, "partial/x").await, payload);
}

/// An object at or above the size limit bypasses accumulation entirely: it
/// uploads alone, at its own origin key, never packed with anything else.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn object_over_the_limit_bypasses_accumulation() {
    let (coordinator, origin, _transport, _dir) = build(10);
    let payload = body(4096);

    coordinator
        .put(
            &ck("big/one"),
            &WriteMetadata::default(),
            payload.clone(),
            2,
            1,
            3,
        )
        .await
        .expect("ack");

    assert!(
        wait_until(|| origin.put_count() == 1).await,
        "the oversized object propagates on its own"
    );
    assert_eq!(
        origin.get_bytes("bkt", "big/one"),
        Some(payload),
        "uploaded at its own key, not packed"
    );
    assert!(
        wait_until(|| coordinator.journals().is_idle()).await,
        "journal cleaned after the direct upload"
    );
}

/// After a flush, deleting a packed key removes its pack index entry so a
/// later read no longer resolves to the (still-present) pack object.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deleting_a_packed_key_stops_it_resolving() {
    let (coordinator, origin, _transport, _dir) = build(1_000_000);
    let payload = body(20);
    coordinator
        .put(&ck("del/x"), &WriteMetadata::default(), payload, 2, 1, 3)
        .await
        .expect("ack");
    // Well under the limit: an explicit drain is what packs it.
    drain_until_flushed(&coordinator, &origin, "default", "bkt").await;
    assert_eq!(origin.put_count(), 1, "flushed");
    assert!(
        coordinator
            .pack_index()
            .resolve("default", "bkt", "del/x")
            .is_some(),
        "packed"
    );

    coordinator.delete(&ck("del/x")).await.expect("delete");
    assert!(
        coordinator
            .pack_index()
            .resolve("default", "bkt", "del/x")
            .is_none(),
        "delete removes the pack index entry"
    );
}
