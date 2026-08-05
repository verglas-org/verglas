//! Contract tests for the EC quorum append log (#372, sequencing step 2),
//! mapped to the append-log contract with in-memory fakes so append -> ack ->
//! read-back -> flush -> drop -> recover, plus truncation and fencing, are
//! exercised without a network or a live S3.
//!
//! - `append_acks_over_quorum_and_reads_back_the_tail`: appends ack over `w`
//!   distinct nodes, ranges are contiguous, and the un-flushed tail reads back
//!   byte-identically from fragments (contract §1, §2, §3).
//! - `below_quorum_append_fails_and_leaves_the_tail_unmoved`: a placement that
//!   cannot reach `w` fails cleanly with no fragments and no tail movement — never
//!   a sub-quorum ack (§1, §7).
//! - `flush_drains_to_s3_then_drops_local_fragments`: flush writes the segment to
//!   S3, advances the watermark, and only then frees the fragments; the range
//!   still reads back, now from S3 (§4).
//! - `read_spans_a_flushed_segment_and_the_ec_tail`: a read across the flush
//!   boundary stitches S3 bytes and EC-tail bytes (§3, §4).
//! - `recovers_the_tail_from_fragments_after_a_node_loss`: after a restart with a
//!   node lost within tolerance, the tail rebuilds from the survivors (§3, §7).
//! - `recovers_from_s3_after_a_full_flush`: after a full flush and losing every
//!   fragment, a restart serves the whole log from S3 (§4, the turn-off test).
//! - `truncate_below_the_flush_watermark_drops_segments`: truncation forgets
//!   flushed segments and deletes their S3 objects (§5).
//! - `truncate_beyond_the_flush_watermark_is_refused`: truncating un-flushed
//!   bytes is refused (§5).
//! - `fencing_rejects_a_stale_writer`: a fenced writer's append is rejected; the
//!   new epoch's append is accepted (§6).

use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::sync::Arc;
use std::sync::Mutex;

use bytes::Bytes;
use futures::StreamExt;
use verglas_cluster::fragments::{FragmentIoError, FragmentKey, FragmentRecord, LoadedFragment};
use verglas_core::CacheKey;
use verglas_core::node::NodeId;
use verglas_core::read::{
    BodyStream, ObjectGet, ObjectMeta, ObjectRead, ReadError, ReadRange, TierCell,
};
use verglas_core::write::{
    CompletedPartRef, CopyOutcome, MultipartCreation, ObjectWrite, PartInfo, PartUpload,
    PutOutcome, WriteBodyStream, WriteChecksum, WriteError, WriteMetadata,
};
use verglas_safekeeper::{AppendError, AppendGeometry, AppendLog, EcAppendLog, Epoch, Lsn};
use verglas_safekeeper::{FragmentTransport, LiveMembership, TransportError};

// ---- in-memory fragment transport (fragments persist across a simulated crash
// because the store Arc is reused, standing in for durable NVMe) ---------------

#[derive(Default)]
struct TransportInner {
    frags: HashMap<(String, String, usize), (Bytes, u32)>,
    dead: HashSet<String>,
    fail_place: HashSet<String>,
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

    /// Marks a node down: its fragments are lost and further ops miss/fail.
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

    fn fragment_count(&self) -> usize {
        self.inner.lock().expect("lock").frags.len()
    }
}

#[async_trait::async_trait]
impl FragmentTransport for MemoryTransport {
    async fn has_headroom(&self, node: &NodeId, _bytes: u64) -> bool {
        !self
            .inner
            .lock()
            .expect("lock")
            .dead
            .contains(node.as_str())
    }

    async fn place(&self, node: &NodeId, record: FragmentRecord) -> Result<(), TransportError> {
        let mut inner = self.inner.lock().expect("lock");
        let n = node.as_str();
        if inner.dead.contains(n) || inner.fail_place.contains(n) {
            return Err(TransportError::Local(FragmentIoError::Io(format!(
                "{n} cannot store"
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
        _node: &NodeId,
        _key: FragmentKey,
        _shards: verglas_write::transport::ShardStream,
    ) -> Result<(), TransportError> {
        // The append log encodes each buffered append and uses `place`, never the
        // streaming placement path.
        Err(TransportError::Local(FragmentIoError::Io(
            "append log does not use place_stream".to_owned(),
        )))
    }

    async fn load(
        &self,
        node: &NodeId,
        key: &FragmentKey,
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

    async fn delete(&self, node: &NodeId, key: &FragmentKey) -> Result<(), TransportError> {
        self.inner.lock().expect("lock").frags.remove(&(
            node.as_str().to_owned(),
            key.object_id.clone(),
            key.index,
        ));
        Ok(())
    }
}

// ---- in-memory membership --------------------------------------------------

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

// ---- in-memory S3 (ObjectRead + ObjectWrite) -------------------------------

struct MemStore {
    objects: Mutex<HashMap<(String, String), Bytes>>,
}

impl MemStore {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            objects: Mutex::new(HashMap::new()),
        })
    }
    fn object_count(&self) -> usize {
        self.objects.lock().expect("lock").len()
    }
}

fn ok_body(bytes: Bytes) -> BodyStream {
    futures::stream::once(async move { Ok(bytes) }).boxed()
}

impl ObjectRead for MemStore {
    async fn get(&self, key: &CacheKey, _range: ReadRange) -> Result<ObjectGet, ReadError> {
        let bytes = self
            .objects
            .lock()
            .expect("lock")
            .get(&(key.bucket.clone(), key.key.clone()))
            .cloned()
            .ok_or(ReadError::NoSuchKey)?;
        let size = bytes.len() as u64;
        Ok(ObjectGet {
            meta: ObjectMeta {
                size,
                e_tag: Some("\"mem\"".to_owned()),
                ..Default::default()
            },
            range: Range {
                start: 0,
                end: size,
            },
            body: ok_body(bytes),
            served_from: TierCell::new(),
        })
    }

    async fn head(&self, key: &CacheKey) -> Result<ObjectMeta, ReadError> {
        let objects = self.objects.lock().expect("lock");
        let bytes = objects
            .get(&(key.bucket.clone(), key.key.clone()))
            .ok_or(ReadError::NoSuchKey)?;
        Ok(ObjectMeta {
            size: bytes.len() as u64,
            e_tag: Some("\"mem\"".to_owned()),
            ..Default::default()
        })
    }
}

impl ObjectWrite for MemStore {
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
        self.objects
            .lock()
            .expect("lock")
            .insert((key.bucket.clone(), key.key.clone()), Bytes::from(buf));
        Ok(PutOutcome {
            e_tag: Some("\"mem\"".to_owned()),
            checksums: Default::default(),
            version_id: None,
        })
    }
    async fn delete(&self, key: &CacheKey) -> Result<(), WriteError> {
        self.objects
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
        _k: &CacheKey,
        _m: WriteMetadata,
    ) -> Result<MultipartCreation, WriteError> {
        Ok(MultipartCreation {
            upload_id: "u".to_owned(),
            ..Default::default()
        })
    }
    async fn upload_part(
        &self,
        _k: &CacheKey,
        _u: &str,
        _p: u16,
        _c: WriteChecksum,
        _b: WriteBodyStream,
    ) -> Result<PartUpload, WriteError> {
        Ok(PartUpload {
            e_tag: "\"p\"".to_owned(),
            ..Default::default()
        })
    }
    async fn complete_multipart(
        &self,
        _k: &CacheKey,
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
    async fn abort_multipart(&self, _k: &CacheKey, _u: &str) -> Result<(), WriteError> {
        Ok(())
    }
    async fn list_parts(&self, _k: &CacheKey, _u: &str) -> Result<Vec<PartInfo>, WriteError> {
        Ok(Vec::new())
    }
}

// ---- helpers ---------------------------------------------------------------

/// Deterministic bytes crossing shard/stripe boundaries.
fn bytes(n: usize) -> Bytes {
    Bytes::from((0..n).map(|i| (i % 251) as u8).collect::<Vec<u8>>())
}

/// Builds an append log over the given fakes in `dir`.
fn build(
    store: Arc<MemStore>,
    transport: Arc<MemoryTransport>,
    membership: Arc<FakeMembership>,
    dir: &std::path::Path,
    geometry: AppendGeometry,
) -> EcAppendLog<MemStore> {
    EcAppendLog::open(
        store, "wal-bkt", "wal", transport, membership, dir, geometry,
    )
    .expect("open append log")
}

fn geom() -> AppendGeometry {
    AppendGeometry::new(2, 1, 3).expect("geometry")
}

// ---- tests -----------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn append_acks_over_quorum_and_reads_back_the_tail() {
    let dir = tempfile::tempdir().expect("tmp");
    let store = MemStore::new();
    let transport = MemoryTransport::new();
    let membership = FakeMembership::new("n0", &["n0", "n1", "n2"]);
    let log = build(store, transport.clone(), membership, dir.path(), geom());

    let a = bytes(4096);
    let b = bytes(2048);
    let r1 = log
        .append(Epoch(0), Lsn(0), a.clone())
        .await
        .expect("append a");
    let r2 = log
        .append(Epoch(0), r1.end, b.clone())
        .await
        .expect("append b");

    // Ranges are contiguous and monotonic.
    assert_eq!(r1.start, Lsn(0));
    assert_eq!(r1.end, Lsn(4096));
    assert_eq!(r2.start, r1.end, "the next append starts at the old tail");
    assert_eq!(r2.end, Lsn(4096 + 2048));
    assert_eq!(log.tail(), Lsn(4096 + 2048));

    // w=3 fragments per append landed on distinct nodes.
    assert_eq!(transport.fragment_count(), 6);

    // The un-flushed tail reads back byte-identically, whole and sub-range.
    let whole = log.read(Lsn(0), log.tail()).await.expect("read whole");
    let mut expect = a.to_vec();
    expect.extend_from_slice(&b);
    assert_eq!(whole, Bytes::from(expect));

    let mid = log.read(Lsn(4000), Lsn(4100)).await.expect("read mid");
    let mut expect_mid = a[4000..].to_vec();
    expect_mid.extend_from_slice(&b[..(4100 - 4096)]);
    assert_eq!(
        mid,
        Bytes::from(expect_mid),
        "a read across the append seam"
    );

    assert_eq!(log.flushed_through(), Lsn(0), "nothing flushed yet");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_append_preserves_the_postgres_start_lsn() {
    let dir = tempfile::tempdir().expect("tmp");
    let store = MemStore::new();
    let transport = MemoryTransport::new();
    let membership = FakeMembership::new("n0", &["n0", "n1", "n2"]);
    let log = build(store, transport, membership, dir.path(), geom());

    let start = Lsn(0x16B6_0A10);
    let payload = bytes(4096);
    let appended = log
        .append(Epoch(0), start, payload.clone())
        .await
        .expect("append at the timeline's PostgreSQL LSN");

    assert_eq!(appended.start, start);
    assert_eq!(appended.end, start.advance(payload.len() as u64));
    assert_eq!(log.read(start, appended.end).await.expect("read"), payload);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exact_replay_is_idempotent_and_places_no_new_fragments() {
    let dir = tempfile::tempdir().expect("tmp");
    let store = MemStore::new();
    let transport = MemoryTransport::new();
    let membership = FakeMembership::new("n0", &["n0", "n1", "n2"]);
    let log = build(store, transport.clone(), membership, dir.path(), geom());
    let payload = bytes(4096);

    let first = log
        .append(Epoch(0), Lsn(0), payload.clone())
        .await
        .expect("first append");
    let fragments = transport.fragment_count();
    let replay = log
        .append(Epoch(0), Lsn(0), payload)
        .await
        .expect("idempotent replay");

    assert_eq!(replay, first);
    assert_eq!(log.tail(), first.end);
    assert_eq!(transport.fragment_count(), fragments);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partial_overlap_is_validated_and_only_the_suffix_is_appended() {
    let dir = tempfile::tempdir().expect("tmp");
    let store = MemStore::new();
    let transport = MemoryTransport::new();
    let membership = FakeMembership::new("n0", &["n0", "n1", "n2"]);
    let log = build(store, transport.clone(), membership, dir.path(), geom());
    let payload = bytes(4096);
    log.append(Epoch(0), Lsn(0), payload.clone())
        .await
        .expect("first append");

    let mut replay_and_suffix = payload[2048..].to_vec();
    replay_and_suffix.extend_from_slice(&bytes(1024));
    let appended = log
        .append(Epoch(0), Lsn(2048), Bytes::from(replay_and_suffix))
        .await
        .expect("validated overlap and new suffix");

    assert_eq!(appended.start, Lsn(2048));
    assert_eq!(appended.end, Lsn(5120));
    assert_eq!(log.tail(), Lsn(5120));
    assert_eq!(transport.fragment_count(), 6, "one new EC append");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conflicting_overlap_is_rejected_without_moving_the_tail() {
    let dir = tempfile::tempdir().expect("tmp");
    let store = MemStore::new();
    let transport = MemoryTransport::new();
    let membership = FakeMembership::new("n0", &["n0", "n1", "n2"]);
    let log = build(store, transport, membership, dir.path(), geom());
    let payload = bytes(4096);
    log.append(Epoch(0), Lsn(0), payload)
        .await
        .expect("first append");

    let err = log
        .append(Epoch(0), Lsn(2048), Bytes::from_static(b"different WAL"))
        .await
        .expect_err("conflicting WAL must fail");

    assert!(matches!(err, AppendError::ConflictingWal { at: Lsn(2048) }));
    assert_eq!(log.tail(), Lsn(4096));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_gap_is_rejected_without_moving_the_tail() {
    let dir = tempfile::tempdir().expect("tmp");
    let store = MemStore::new();
    let transport = MemoryTransport::new();
    let membership = FakeMembership::new("n0", &["n0", "n1", "n2"]);
    let log = build(store, transport, membership, dir.path(), geom());
    log.append(Epoch(0), Lsn(0), bytes(4096))
        .await
        .expect("first append");

    let err = log
        .append(Epoch(0), Lsn(5000), bytes(32))
        .await
        .expect_err("gap must fail");

    assert!(matches!(
        err,
        AppendError::WalGap {
            expected: Lsn(4096),
            presented: Lsn(5000)
        }
    ));
    assert_eq!(log.tail(), Lsn(4096));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn below_quorum_append_fails_and_leaves_the_tail_unmoved() {
    let dir = tempfile::tempdir().expect("tmp");
    let store = MemStore::new();
    let transport = MemoryTransport::new();
    // Two of three nodes refuse placement: only one fragment can land, short of
    // w=3, so the append is not durable and must fail cleanly.
    transport.fail_placement("n1");
    transport.fail_placement("n2");
    let membership = FakeMembership::new("n0", &["n0", "n1", "n2"]);
    let log = build(store, transport.clone(), membership, dir.path(), geom());

    let err = log
        .append(Epoch(0), Lsn(0), bytes(4096))
        .await
        .expect_err("below-quorum append fails");
    assert!(
        matches!(err, AppendError::QuorumUnavailable { needed: 3, .. }),
        "not a sub-quorum ack: {err}"
    );
    assert_eq!(log.tail(), Lsn(0), "the tail did not move");
    assert_eq!(
        transport.fragment_count(),
        0,
        "partial placement cleaned up"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_drains_to_s3_then_drops_local_fragments() {
    let dir = tempfile::tempdir().expect("tmp");
    let store = MemStore::new();
    let transport = MemoryTransport::new();
    let membership = FakeMembership::new("n0", &["n0", "n1", "n2"]);
    let log = build(
        store.clone(),
        transport.clone(),
        membership,
        dir.path(),
        geom(),
    );

    let payload = bytes(9000);
    log.append(Epoch(0), Lsn(0), payload.clone())
        .await
        .expect("append");
    assert_eq!(transport.fragment_count(), 3);

    let watermark = log.flush().await.expect("flush");
    assert_eq!(watermark, log.tail(), "flush watermark reaches the tail");
    assert_eq!(log.flushed_through(), log.tail());
    assert_eq!(
        transport.fragment_count(),
        0,
        "local fragments dropped after S3 confirmed"
    );
    assert_eq!(store.object_count(), 1, "one segment object in S3");

    // The range still reads back, now served from S3.
    let got = log.read(Lsn(0), log.tail()).await.expect("read from s3");
    assert_eq!(got, payload, "flushed range reads back byte-identically");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_spans_a_flushed_segment_and_the_ec_tail() {
    let dir = tempfile::tempdir().expect("tmp");
    let store = MemStore::new();
    let transport = MemoryTransport::new();
    let membership = FakeMembership::new("n0", &["n0", "n1", "n2"]);
    let log = build(store, transport.clone(), membership, dir.path(), geom());

    let a = bytes(5000);
    log.append(Epoch(0), Lsn(0), a.clone())
        .await
        .expect("append a");
    log.flush().await.expect("flush a"); // a is now in S3
    let b = bytes(3000);
    log.append(Epoch(0), Lsn(5000), b.clone())
        .await
        .expect("append b"); // b is the EC tail

    let whole = log.read(Lsn(0), log.tail()).await.expect("read across");
    let mut expect = a.to_vec();
    expect.extend_from_slice(&b);
    assert_eq!(
        whole,
        Bytes::from(expect),
        "a read stitches the flushed S3 segment and the EC tail"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovers_the_tail_from_fragments_after_a_node_loss() {
    let dir = tempfile::tempdir().expect("tmp");
    let store = MemStore::new();
    let transport = MemoryTransport::new();
    let membership = FakeMembership::new("n0", &["n0", "n1", "n2"]);

    let payload = bytes(9000);
    {
        let log = build(
            store.clone(),
            transport.clone(),
            membership.clone(),
            dir.path(),
            geom(),
        );
        log.append(Epoch(0), Lsn(0), payload.clone())
            .await
            .expect("append");
        // drop the log: simulated process death before any flush
    }

    // Lose one fragment node — within the m=1 tolerance.
    transport.kill("n1");
    membership.drop_node("n1");

    // Restart: reopen the log from the fsynced manifest over the same fragments.
    let log = build(store, transport, membership, dir.path(), geom());
    assert_eq!(
        log.tail(),
        Lsn(9000),
        "manifest recovered the tail position"
    );
    let got = log
        .read(Lsn(0), Lsn(9000))
        .await
        .expect("tail rebuilds from the surviving fragments");
    assert_eq!(
        got, payload,
        "recovered tail is byte-identical after a node loss"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovers_from_s3_after_a_full_flush() {
    let dir = tempfile::tempdir().expect("tmp");
    let store = MemStore::new();
    let transport = MemoryTransport::new();
    let membership = FakeMembership::new("n0", &["n0", "n1", "n2"]);

    let payload = bytes(7000);
    {
        let log = build(
            store.clone(),
            transport.clone(),
            membership.clone(),
            dir.path(),
            geom(),
        );
        log.append(Epoch(0), Lsn(0), payload.clone())
            .await
            .expect("append");
        log.flush().await.expect("flush");
    }
    // Lose every fragment node: the buffer is gone entirely. The log must still
    // recover, because a fully-flushed log lives in S3 alone (the turn-off test).
    transport.kill("n0");
    transport.kill("n1");
    transport.kill("n2");
    assert_eq!(transport.fragment_count(), 0);

    let log = build(store, transport, membership, dir.path(), geom());
    assert_eq!(
        log.flushed_through(),
        Lsn(7000),
        "recovered flush watermark"
    );
    let got = log.read(Lsn(0), Lsn(7000)).await.expect("read from S3");
    assert_eq!(got, payload, "the whole log recovers from S3");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn truncate_below_the_flush_watermark_drops_segments() {
    let dir = tempfile::tempdir().expect("tmp");
    let store = MemStore::new();
    let transport = MemoryTransport::new();
    let membership = FakeMembership::new("n0", &["n0", "n1", "n2"]);
    let log = build(
        store.clone(),
        transport.clone(),
        membership,
        dir.path(),
        geom(),
    );

    // Two segments: append, flush (seals segment 0), append, flush (segment 1).
    let a = bytes(4000);
    log.append(Epoch(0), Lsn(0), a).await.expect("append a");
    log.flush().await.expect("flush a");
    let split = log.tail();
    let b = bytes(3000);
    log.append(Epoch(0), split, b.clone())
        .await
        .expect("append b");
    log.flush().await.expect("flush b");
    assert_eq!(store.object_count(), 2, "two segment objects in S3");

    // Truncate away everything below the split (all of segment 0).
    log.truncate(split).await.expect("truncate");
    assert_eq!(
        store.object_count(),
        1,
        "the dropped segment's S3 object was deleted"
    );

    // The truncated range is no longer readable; the kept range still is.
    let err = log
        .read(Lsn(0), split)
        .await
        .expect_err("truncated range is gone");
    assert!(matches!(err, AppendError::OutOfRange { .. }), "{err}");
    let kept = log.read(split, log.tail()).await.expect("kept range reads");
    assert_eq!(
        kept, b,
        "the surviving segment still reads byte-identically"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn truncate_beyond_the_flush_watermark_is_refused() {
    let dir = tempfile::tempdir().expect("tmp");
    let store = MemStore::new();
    let transport = MemoryTransport::new();
    let membership = FakeMembership::new("n0", &["n0", "n1", "n2"]);
    let log = build(store, transport, membership, dir.path(), geom());

    log.append(Epoch(0), Lsn(0), bytes(4000))
        .await
        .expect("append");
    // Nothing flushed, so any positive truncate is beyond the watermark.
    let err = log
        .truncate(log.tail())
        .await
        .expect_err("cannot truncate un-flushed bytes");
    assert!(
        matches!(err, AppendError::TruncateBeyondFlush { .. }),
        "refused: {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fencing_rejects_a_stale_writer() {
    let dir = tempfile::tempdir().expect("tmp");
    let store = MemStore::new();
    let transport = MemoryTransport::new();
    let membership = FakeMembership::new("n0", &["n0", "n1", "n2"]);
    let log = build(store, transport, membership, dir.path(), geom());

    log.append(Epoch(0), Lsn(0), bytes(1000))
        .await
        .expect("epoch 0 append");

    // A new writer fences to epoch 1.
    log.fence(Epoch(1)).await.expect("fence to epoch 1");
    assert_eq!(log.epoch(), Epoch(1));

    // The stale writer (epoch 0) is now fenced out.
    let err = log
        .append(Epoch(0), Lsn(1000), bytes(1000))
        .await
        .expect_err("stale writer is fenced");
    assert!(matches!(err, AppendError::Fenced { .. }), "{err}");

    // The new writer (epoch 1) appends fine.
    log.append(Epoch(1), Lsn(1000), bytes(1000))
        .await
        .expect("new writer appends under the current epoch");

    // Fencing must advance: a non-greater epoch is refused.
    let err = log
        .fence(Epoch(1))
        .await
        .expect_err("a non-advancing fence is refused");
    assert!(matches!(err, AppendError::StaleFence { .. }), "{err}");
}
