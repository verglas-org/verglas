//! Single-node write-back + commit barrier contract tests (#286).
//!
//! These map to the issue's acceptance criteria, with in-memory fakes so the
//! fast-ack path, backpressure, the commit barrier's ordering, and
//! recovery-gates-serving are exercised without a network or a live origin. The
//! real crash-durability evidence (kill -9 the server between ack and flush,
//! restart, segment reaches S3) lives in `tests/writeback-recovery/run.sh`.
//!
//! - `single_node_fast_acks_from_local_durability`: a one-node deployment acks a
//!   PUT with the origin unreachable — durability is the local buffer, not an
//!   origin round-trip (ack-implies-durable, local half).
//! - `full_buffer_rejects_when_origin_down_never_silent_drop` /
//!   `full_buffer_backpressures_through_origin_when_up`: at capacity the write
//!   backpressures — write-through to the origin when it is up, a clear error
//!   when it is down — never a silent drop.
//! - `commit_barrier_waits_for_referenced_propagation`: a commit awaits its
//!   referenced data file's propagation and proceeds only after it lands
//!   (barrier ordering).
//! - `commit_barrier_refuses_when_origin_unreachable`: with the origin down the
//!   barrier times out with a clear error, the commit is refused, and the
//!   buffered data stays put with its propagation still in flight.
//! - `recovery_gates_serving_on_replay`: after a restart that recovers a dirty
//!   journal, a commit is gated on the replay of that segment to the origin.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use tempfile::TempDir;
use verglas_cluster::fragments::{FragmentIoError, FragmentKey, FragmentRecord, LoadedFragment};
use verglas_core::CacheKey;
use verglas_core::node::NodeId;
use verglas_core::write::{
    CompletedPartRef, CopyOutcome, MultipartCreation, ObjectWrite, PartInfo, PartUpload,
    PutOutcome, WriteBodyStream, WriteChecksum, WriteError, WriteMetadata,
};
use verglas_write::barrier::{BarrierError, CommitBarrier, JournalBarrier};
use verglas_write::coordinator::WriteCoordinator;
use verglas_write::journal::JournalStore;
use verglas_write::membership::SingleNodeMembership;
use verglas_write::metrics::WritebackMetrics;
use verglas_write::transport::{FragmentTransport, TransportError};
use verglas_write::{ConsensusCommitter, ObjectCommit, StagedObject};

const SELF: &str = "solo-node";

// ---- in-memory fragment transport (fragments persist across a simulated crash
// because the store Arc is reused, standing in for durable local NVMe) --------

#[derive(Default)]
struct TransportInner {
    frags: HashMap<(String, usize), (Bytes, u32)>,
    no_headroom: bool,
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

    fn set_no_headroom(&self, full: bool) {
        self.inner.lock().expect("lock").no_headroom = full;
    }

    fn fragment_count(&self) -> usize {
        self.inner.lock().expect("lock").frags.len()
    }
}

#[async_trait::async_trait]
impl FragmentTransport for MemoryTransport {
    async fn has_headroom(&self, _node: &NodeId, _bytes: u64) -> bool {
        !self.inner.lock().expect("lock").no_headroom
    }

    async fn place(&self, _node: &NodeId, record: FragmentRecord) -> Result<(), TransportError> {
        let mut inner = self.inner.lock().expect("lock");
        if inner.no_headroom {
            return Err(TransportError::Local(FragmentIoError::Io("full".into())));
        }
        inner.frags.insert(
            (record.key.object_id, record.key.index),
            (record.bytes, record.checksum),
        );
        Ok(())
    }

    async fn place_stream(
        &self,
        _node: &NodeId,
        key: FragmentKey,
        mut shards: verglas_write::transport::ShardStream,
    ) -> Result<(), TransportError> {
        {
            let inner = self.inner.lock().expect("lock");
            if inner.no_headroom {
                return Err(TransportError::Local(FragmentIoError::Io("full".into())));
            }
        }
        let mut buf = Vec::new();
        while let Some(shard) = shards.next().await {
            buf.extend_from_slice(&shard);
        }
        let bytes = Bytes::from(buf);
        let checksum = verglas_cluster::fragments::fragment_checksum(&bytes);
        self.inner
            .lock()
            .expect("lock")
            .frags
            .insert((key.object_id, key.index), (bytes, checksum));
        Ok(())
    }

    async fn load(
        &self,
        _node: &NodeId,
        key: &FragmentKey,
    ) -> Result<Option<LoadedFragment>, TransportError> {
        Ok(self
            .inner
            .lock()
            .expect("lock")
            .frags
            .get(&(key.object_id.clone(), key.index))
            .map(|(bytes, checksum)| LoadedFragment {
                bytes: bytes.clone(),
                checksum: *checksum,
            }))
    }

    async fn list(&self, _node: &NodeId, prefix: &str) -> Result<Vec<FragmentKey>, TransportError> {
        Ok(self
            .inner
            .lock()
            .expect("lock")
            .frags
            .keys()
            .filter(|(object_id, _)| object_id.starts_with(prefix))
            .map(|(object_id, index)| FragmentKey {
                object_id: object_id.clone(),
                index: *index,
            })
            .collect())
    }

    async fn delete(&self, _node: &NodeId, key: &FragmentKey) -> Result<(), TransportError> {
        self.inner
            .lock()
            .expect("lock")
            .frags
            .remove(&(key.object_id.clone(), key.index));
        Ok(())
    }
}

// ---- a gated origin: fails while blocked, records puts once released --------

struct GatedOrigin {
    blocked: AtomicBool,
    puts: Mutex<HashMap<(String, String), Bytes>>,
}

impl GatedOrigin {
    fn new(blocked: bool) -> Arc<Self> {
        Arc::new(Self {
            blocked: AtomicBool::new(blocked),
            puts: Mutex::new(HashMap::new()),
        })
    }
    fn release(&self) {
        self.blocked.store(false, Ordering::SeqCst);
    }
    fn get(&self, bucket: &str, key: &str) -> Option<Bytes> {
        self.puts
            .lock()
            .expect("lock")
            .get(&(bucket.to_owned(), key.to_owned()))
            .cloned()
    }
}

impl ObjectWrite for GatedOrigin {
    async fn put(
        &self,
        key: &CacheKey,
        _metadata: WriteMetadata,
        mut body: WriteBodyStream,
    ) -> Result<PutOutcome, WriteError> {
        if self.blocked.load(Ordering::SeqCst) {
            return Err(WriteError::Backend("origin unreachable".into()));
        }
        let mut buf = Vec::new();
        while let Some(chunk) = body.next().await {
            buf.extend_from_slice(&chunk?);
        }
        self.puts
            .lock()
            .expect("lock")
            .insert((key.bucket.clone(), key.key.clone()), Bytes::from(buf));
        Ok(PutOutcome {
            e_tag: Some("\"origin\"".into()),
            checksums: Default::default(),
            version_id: None,
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
    // The write-back path never delegates copy/multipart to the origin in these
    // tests; a plain error keeps the fake honest without a panic macro.
    async fn copy(
        &self,
        _s: &CacheKey,
        _d: &CacheKey,
        _m: Option<WriteMetadata>,
    ) -> Result<CopyOutcome, WriteError> {
        Err(unused("copy"))
    }
    async fn create_multipart(
        &self,
        _k: &CacheKey,
        _m: WriteMetadata,
    ) -> Result<MultipartCreation, WriteError> {
        Err(unused("create_multipart"))
    }
    async fn upload_part(
        &self,
        _k: &CacheKey,
        _u: &str,
        _p: u16,
        _c: WriteChecksum,
        _b: WriteBodyStream,
    ) -> Result<PartUpload, WriteError> {
        Err(unused("upload_part"))
    }
    async fn complete_multipart(
        &self,
        _k: &CacheKey,
        _u: &str,
        _p: Vec<CompletedPartRef>,
        _c: WriteChecksum,
    ) -> Result<PutOutcome, WriteError> {
        Err(unused("complete_multipart"))
    }
    async fn abort_multipart(&self, _k: &CacheKey, _u: &str) -> Result<(), WriteError> {
        Err(unused("abort_multipart"))
    }
    async fn list_parts(&self, _k: &CacheKey, _u: &str) -> Result<Vec<PartInfo>, WriteError> {
        Err(unused("list_parts"))
    }
}

/// A uniform error for the origin operations these tests never exercise.
fn unused(op: &str) -> WriteError {
    WriteError::Backend(format!("{op} is not exercised by the #286 tests"))
}

// ---- helpers ---------------------------------------------------------------

fn ck(key: &str) -> CacheKey {
    CacheKey {
        storage_binding_id: "default".to_owned(),
        bucket: "bkt".into(),
        key: key.into(),
    }
}

fn body(len: usize) -> Bytes {
    Bytes::from((0..len).map(|i| (i % 251) as u8).collect::<Vec<u8>>())
}

/// Test consensus accepts staged immutable object certificates.
struct TestCommitter;

#[async_trait::async_trait]
impl ConsensusCommitter for TestCommitter {
    /// Commits only certificates that include their local durable fragment.
    async fn commit(&self, staged: StagedObject) -> Result<ObjectCommit, String> {
        if staged.placements.is_empty() {
            return Err("empty certificate".to_owned());
        }
        Ok(ObjectCommit { index: 1 })
    }
}

/// A single-node coordinator over the given transport, journal dir, and origin.
/// The configured pod geometry (k=4,m=2,w=5) is what a clustered deployment
/// would use; the single-node path degenerates it to (1,0,1) internally.
fn single_node_coordinator(
    transport: Arc<MemoryTransport>,
    journals: Arc<JournalStore>,
    origin: Arc<GatedOrigin>,
    metrics: Arc<WritebackMetrics>,
) -> Arc<WriteCoordinator<GatedOrigin>> {
    let membership = Arc::new(SingleNodeMembership::new(NodeId::new(SELF)));
    Arc::new(WriteCoordinator::new(
        transport,
        membership,
        journals,
        metrics,
        origin,
        Arc::new(TestCommitter),
        Duration::from_secs(2),
    ))
}

const POD_K: usize = 4;
const POD_M: usize = 2;
const POD_W: usize = 5;

// ---- tests -----------------------------------------------------------------

/// A one-node deployment acks a PUT while the origin is unreachable: the ack is
/// the local fragment + journal fsync, not an origin round-trip. The object is
/// left dirty (buffered) for background propagation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_node_fast_acks_from_local_durability() {
    let dir = TempDir::new().expect("tmp");
    let transport = MemoryTransport::new();
    let journals = Arc::new(JournalStore::open(dir.path()).expect("open"));
    let origin = GatedOrigin::new(true); // origin down for the whole test
    let metrics = Arc::new(WritebackMetrics::default());
    let coord = single_node_coordinator(
        Arc::clone(&transport),
        Arc::clone(&journals),
        Arc::clone(&origin),
        Arc::clone(&metrics),
    );

    let out = coord
        .put(
            &ck("data/f1"),
            &WriteMetadata::default(),
            body(4096),
            POD_K,
            POD_M,
            POD_W,
        )
        .await
        .expect("single-node PUT acks locally even with the origin down");
    assert!(
        out.e_tag.is_some(),
        "the local ack carries a synthetic etag"
    );

    // Durable locally: one fragment placed, and the object is dirty (buffered).
    assert_eq!(
        transport.fragment_count(),
        1,
        "one local fragment (k=1,m=0)"
    );
    assert!(
        journals.find_dirty("default", "bkt", "data/f1").is_some(),
        "acked-but-unpropagated object is dirty in the journal"
    );
    // Counted as a durable ack, not a write-through: no origin round-trip.
    let snap = metrics.snapshot();
    assert_eq!(snap.acked_via_quorum, 1, "local-durability ack");
    assert_eq!(
        snap.acked_via_write_through, 0,
        "no origin write on the ack path"
    );
}

/// At capacity with the origin down, the write is refused with a clear error —
/// never a silent drop and never an ack it cannot back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_buffer_rejects_when_origin_down_never_silent_drop() {
    let dir = TempDir::new().expect("tmp");
    let transport = MemoryTransport::new();
    transport.set_no_headroom(true); // buffer full
    let journals = Arc::new(JournalStore::open(dir.path()).expect("open"));
    let origin = GatedOrigin::new(true); // origin also down
    let metrics = Arc::new(WritebackMetrics::default());
    let coord = single_node_coordinator(
        Arc::clone(&transport),
        Arc::clone(&journals),
        Arc::clone(&origin),
        Arc::clone(&metrics),
    );

    let err = coord
        .put(
            &ck("data/f1"),
            &WriteMetadata::default(),
            body(4096),
            POD_K,
            POD_M,
            POD_W,
        )
        .await
        .expect_err("a full buffer with the origin down must refuse the write");
    assert!(
        matches!(err, WriteError::Backend(_)),
        "clear error: {err:?}"
    );
    assert!(
        journals.is_idle(),
        "nothing acked, nothing buffered — no silent drop"
    );
    assert_eq!(transport.fragment_count(), 0);
}

/// A full single-node buffer refuses admission even when the origin is reachable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_buffer_refuses_without_an_origin_fallback() {
    let dir = TempDir::new().expect("tmp");
    let transport = MemoryTransport::new();
    transport.set_no_headroom(true);
    let journals = Arc::new(JournalStore::open(dir.path()).expect("open"));
    let origin = GatedOrigin::new(false); // origin up
    let metrics = Arc::new(WritebackMetrics::default());
    let coord = single_node_coordinator(
        Arc::clone(&transport),
        Arc::clone(&journals),
        Arc::clone(&origin),
        Arc::clone(&metrics),
    );

    coord
        .put(
            &ck("data/f1"),
            &WriteMetadata::default(),
            body(4096),
            POD_K,
            POD_M,
            POD_W,
        )
        .await
        .expect_err("a full buffer cannot use the origin as a second authority");
    assert_eq!(origin.get("bkt", "data/f1"), None, "origin was not written");
    let snap = metrics.snapshot();
    assert_eq!(
        snap.acked_via_write_through, 0,
        "no origin fallback acknowledgement"
    );
    assert!(journals.is_idle(), "refusal leaves nothing buffered");
}

/// A commit awaits its referenced data file's propagation: it does not proceed
/// while the file is still buffered, and returns once the origin has it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_barrier_waits_for_referenced_propagation() {
    let dir = TempDir::new().expect("tmp");
    let transport = MemoryTransport::new();
    let journals = Arc::new(JournalStore::open(dir.path()).expect("open"));
    let origin = GatedOrigin::new(true); // hold propagation
    let metrics = Arc::new(WritebackMetrics::default());
    let coord = single_node_coordinator(
        Arc::clone(&transport),
        Arc::clone(&journals),
        Arc::clone(&origin),
        Arc::clone(&metrics),
    );

    coord
        .put(
            &ck("data/f1"),
            &WriteMetadata::default(),
            body(2048),
            POD_K,
            POD_M,
            POD_W,
        )
        .await
        .expect("fast-ack");
    assert!(
        journals.find_dirty("default", "bkt", "data/f1").is_some(),
        "buffered, not yet at origin"
    );

    let barrier = JournalBarrier::new(Arc::clone(&journals));
    // The commit blocks while the file is dirty...
    let quick = barrier
        .await_referenced(&[ck("data/f1")], Duration::from_millis(80))
        .await;
    assert!(
        matches!(quick, Err(BarrierError::Timeout { .. })),
        "blocks while buffered"
    );

    // ...release the origin; the background propagation lands the file...
    origin.release();
    // ...and now the barrier crosses, only after the referenced file is durable.
    let crossed = barrier
        .await_referenced(&[ck("data/f1")], Duration::from_secs(5))
        .await
        .expect("barrier crosses once the referenced file reaches the origin");
    assert_eq!(crossed.awaited, 1, "it waited on the one referenced file");
    assert_eq!(
        origin.get("bkt", "data/f1"),
        Some(body(2048)),
        "file is at the origin"
    );
    assert!(journals.is_idle(), "propagation marked it clean");
}

/// With the origin unreachable, the commit barrier times out with a clear error,
/// the commit is refused, and the buffered data stays put — its propagation is
/// never abandoned on the wall clock (transport-level-only retry).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_barrier_refuses_when_origin_unreachable() {
    let dir = TempDir::new().expect("tmp");
    let transport = MemoryTransport::new();
    let journals = Arc::new(JournalStore::open(dir.path()).expect("open"));
    let origin = GatedOrigin::new(true); // never released
    let metrics = Arc::new(WritebackMetrics::default());
    let coord = single_node_coordinator(
        Arc::clone(&transport),
        Arc::clone(&journals),
        Arc::clone(&origin),
        Arc::clone(&metrics),
    );

    coord
        .put(
            &ck("data/f1"),
            &WriteMetadata::default(),
            body(1024),
            POD_K,
            POD_M,
            POD_W,
        )
        .await
        .expect("fast-ack");

    let barrier = JournalBarrier::new(Arc::clone(&journals));
    let err = barrier
        .await_all_dirty(Duration::from_millis(150))
        .await
        .expect_err("origin down: the commit is refused");
    match err {
        BarrierError::Timeout { pending, .. } => assert_eq!(pending, 1),
    }
    // The data is still buffered and dirty: nothing was dropped or abandoned.
    assert!(
        journals.find_dirty("default", "bkt", "data/f1").is_some(),
        "data safe, still in flight"
    );
    assert!(
        origin.get("bkt", "data/f1").is_none(),
        "never published to a down origin"
    );
}

/// After a restart that recovers a dirty journal from disk, a commit is gated on
/// the replay of that recovered segment to the origin: the barrier does not
/// cross until recovery propagation lands the file.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_gates_serving_on_replay() {
    let dir = TempDir::new().expect("tmp");
    let transport = MemoryTransport::new(); // fragments persist across the "crash"
    let origin = GatedOrigin::new(true); // origin down through the crash
    let metrics = Arc::new(WritebackMetrics::default());

    // Phase 1: ack a write, then "crash" before propagation (drop the coordinator
    // without ever releasing the origin). The journal is fsynced to `dir`.
    {
        let journals = Arc::new(JournalStore::open(dir.path()).expect("open"));
        let coord = single_node_coordinator(
            Arc::clone(&transport),
            Arc::clone(&journals),
            Arc::clone(&origin),
            Arc::clone(&metrics),
        );
        coord
            .put(
                &ck("data/f1"),
                &WriteMetadata::default(),
                body(4096),
                POD_K,
                POD_M,
                POD_W,
            )
            .await
            .expect("fast-ack");
        assert!(journals.find_dirty("default", "bkt", "data/f1").is_some());
        // Process death: dropping the coordinator does NOT cancel its spawned
        // propagation task (it holds its own Arc), so without shutdown() the
        // "crashed" run's retry loop would keep running and race the phase-2
        // recovery replay — whichever won, the loser's JournalStore kept a
        // stale dirty index and the barrier below timed out flakily.
        coord.shutdown();
    }

    // Phase 2: restart. Reopening the journal store rebuilds the dirty index from
    // disk — this is boot recovery, and serving of commits is gated on it.
    let journals = Arc::new(JournalStore::open(dir.path()).expect("reopen"));
    assert!(
        journals.find_dirty("default", "bkt", "data/f1").is_some(),
        "recovery rebuilt the dirty segment from the fsynced journal"
    );
    let coord = single_node_coordinator(
        Arc::clone(&transport),
        Arc::clone(&journals),
        Arc::clone(&origin),
        Arc::clone(&metrics),
    );
    coord.resume_propagation(); // recovery replay begins (blocked on the down origin)

    let barrier = JournalBarrier::new(Arc::clone(&journals));
    // A commit issued right after restart is gated: the recovered segment is not
    // at the origin yet.
    assert!(
        barrier
            .await_all_dirty(Duration::from_millis(80))
            .await
            .is_err(),
        "commit gated on recovery replay"
    );

    // Recovery completes once the origin returns; the barrier then crosses.
    origin.release();
    barrier
        .await_all_dirty(Duration::from_secs(5))
        .await
        .expect("barrier crosses once recovery replays the segment to the origin");
    assert_eq!(
        origin.get("bkt", "data/f1"),
        Some(body(4096)),
        "the recovered segment reached the origin"
    );
    assert!(journals.is_idle());
}
