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

use bytes::{BufMut, Bytes, BytesMut};
use futures::StreamExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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
use verglas_safekeeper::server::SafekeeperServer;
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

    fn wal_fragment_count(&self) -> usize {
        self.inner
            .lock()
            .expect("lock")
            .frags
            .keys()
            .filter(|(_, _, index)| *index < 1024)
            .count()
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
    assert_eq!(transport.wal_fragment_count(), 6);

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
    let fragments = transport.wal_fragment_count();
    let replay = log
        .append(Epoch(0), Lsn(0), payload)
        .await
        .expect("idempotent replay");

    assert_eq!(replay, first);
    assert_eq!(log.tail(), first.end);
    assert_eq!(transport.wal_fragment_count(), fragments);
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
    assert_eq!(transport.wal_fragment_count(), 6, "one new EC append");
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
        transport.wal_fragment_count(),
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
    assert_eq!(transport.wal_fragment_count(), 3);

    let watermark = log.flush().await.expect("flush");
    assert_eq!(watermark, log.tail(), "flush watermark reaches the tail");
    assert_eq!(log.flushed_through(), log.tail());
    assert_eq!(
        transport.wal_fragment_count(),
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
async fn replacement_coordinator_recovers_without_the_failed_nodes_manifest() {
    let original_dir = tempfile::tempdir().expect("original tmp");
    let replacement_dir = tempfile::tempdir().expect("replacement tmp");
    let store = MemStore::new();
    let transport = MemoryTransport::new();
    let membership = FakeMembership::new("n0", &["n0", "n1", "n2"]);
    let payload = bytes(9000);

    {
        let log = build(
            store.clone(),
            transport.clone(),
            membership.clone(),
            original_dir.path(),
            geom(),
        );
        log.append(Epoch(0), Lsn(0x1000), payload.clone())
            .await
            .expect("quorum append");
    }

    transport.kill("n0");
    membership.drop_node("n0");
    let replacement = build(store, transport, membership, replacement_dir.path(), geom());
    assert_eq!(replacement.tail(), Lsn(0), "replacement starts empty");
    assert!(
        replacement
            .recover_from_ring()
            .await
            .expect("recover descriptor from surviving holders")
    );
    assert_eq!(replacement.tail(), Lsn(0x1000 + 9000));
    assert_eq!(
        replacement
            .read(Lsn(0x1000), replacement.tail())
            .await
            .expect("reassemble on replacement"),
        payload
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
    assert_eq!(transport.wal_fragment_count(), 0);

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

/// Sends one PostgreSQL v3 startup packet with the supplied parameters.
async fn send_pg_startup(stream: &mut tokio::net::TcpStream, params: &[(&str, &str)]) {
    let mut payload = BytesMut::new();
    payload.put_u32(196_608);
    for (key, value) in params {
        payload.put_slice(key.as_bytes());
        payload.put_u8(0);
        payload.put_slice(value.as_bytes());
        payload.put_u8(0);
    }
    payload.put_u8(0);
    stream
        .write_u32((payload.len() + 4) as u32)
        .await
        .expect("startup length");
    stream.write_all(&payload).await.expect("startup payload");
}

/// Sends one tagged PostgreSQL frontend message.
async fn send_pg_message(stream: &mut tokio::net::TcpStream, tag: u8, payload: &[u8]) {
    stream.write_u8(tag).await.expect("frontend tag");
    stream
        .write_u32((payload.len() + 4) as u32)
        .await
        .expect("frontend length");
    stream.write_all(payload).await.expect("frontend payload");
}

/// Reads one tagged PostgreSQL backend message.
async fn read_pg_message(stream: &mut tokio::net::TcpStream) -> (u8, Bytes) {
    let tag = stream.read_u8().await.expect("backend tag");
    let len = stream.read_u32().await.expect("backend length") as usize;
    let mut payload = vec![0_u8; len - 4];
    stream
        .read_exact(&mut payload)
        .await
        .expect("backend payload");
    (tag, Bytes::from(payload))
}

/// Drains startup messages through ReadyForQuery.
async fn finish_pg_startup(stream: &mut tokio::net::TcpStream) {
    loop {
        let (tag, _) = read_pg_message(stream).await;
        if tag == b'Z' {
            return;
        }
    }
}

/// Appends one NUL-terminated protocol string.
fn put_neon_cstr(frame: &mut BytesMut, value: &str) {
    frame.put_slice(value.as_bytes());
    frame.put_u8(0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn neon_wal_push_is_acked_and_served_back_over_physical_replication() {
    const TENANT: &str = "0123456789abcdef0123456789abcdef";
    const TIMELINE: &str = "fedcba9876543210fedcba9876543210";
    let dir = tempfile::tempdir().expect("tmp");
    let store = MemStore::new();
    let transport = MemoryTransport::new();
    let membership = FakeMembership::new("n0", &["n0", "n1", "n2"]);
    let server = SafekeeperServer::new(
        41,
        store.clone(),
        "wal-bkt",
        "neon",
        transport.clone(),
        membership,
        dir.path(),
        geom(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address = listener.local_addr().expect("address");
    let server_task = tokio::spawn(server.serve(listener));

    let mut proposer = tokio::net::TcpStream::connect(address)
        .await
        .expect("connect proposer");
    send_pg_startup(&mut proposer, &[("user", "cloud_admin")]).await;
    finish_pg_startup(&mut proposer).await;
    send_pg_message(
        &mut proposer,
        b'Q',
        b"START_WAL_PUSH (proto_version '3', allow_timeline_creation 'true')\0",
    )
    .await;
    assert_eq!(read_pg_message(&mut proposer).await.0, b'W');

    let mut greeting = BytesMut::new();
    greeting.put_u8(b'g');
    put_neon_cstr(&mut greeting, TENANT);
    put_neon_cstr(&mut greeting, TIMELINE);
    greeting.put_u32(7);
    greeting.put_u32(1);
    greeting.put_u64(41);
    put_neon_cstr(&mut greeting, "127.0.0.1");
    greeting.put_u16(address.port());
    greeting.put_u32(0);
    greeting.put_u32(160_000);
    greeting.put_u64(0x1122_3344_5566_7788);
    greeting.put_u32(16 * 1024 * 1024);
    send_pg_message(&mut proposer, b'd', &greeting).await;
    let (tag, response) = read_pg_message(&mut proposer).await;
    assert_eq!(tag, b'd');
    assert_eq!(response[0], b'g');

    let mut vote = BytesMut::new();
    vote.put_u8(b'v');
    vote.put_u32(7);
    vote.put_u64(1);
    send_pg_message(&mut proposer, b'd', &vote).await;
    let (_, vote_response) = read_pg_message(&mut proposer).await;
    assert_eq!(vote_response[0], b'v');
    assert_eq!(vote_response[13], 1, "vote granted");

    let start = 0x1000_u64;
    let mut elected = BytesMut::new();
    elected.put_u8(b'e');
    elected.put_u32(7);
    elected.put_u64(1);
    elected.put_u64(start);
    elected.put_u32(1);
    elected.put_u64(1);
    elected.put_u64(start);
    send_pg_message(&mut proposer, b'd', &elected).await;

    let wal = Bytes::from_static(b"byte-identical-postgres-wal");
    let end = start + wal.len() as u64;
    let mut append = BytesMut::new();
    append.put_u8(b'a');
    append.put_u32(7);
    append.put_u64(1);
    append.put_u64(start);
    append.put_u64(end);
    append.put_u64(end);
    append.put_u64(start);
    append.put_slice(&wal);
    send_pg_message(&mut proposer, b'd', &append).await;
    let (_, append_response) = read_pg_message(&mut proposer).await;
    assert_eq!(append_response[0], b'a');
    assert_eq!(
        u64::from_be_bytes(append_response[13..21].try_into().expect("flush lsn")),
        end,
        "flush_lsn advances only after the EC append"
    );

    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while store.object_count() == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("background WAL drain reached object storage");
    assert_eq!(
        transport.wal_fragment_count(),
        0,
        "EC fragments drop only after the object-store write"
    );

    let mut replica = tokio::net::TcpStream::connect(address)
        .await
        .expect("connect replica");
    let options = format!("-c tenant_id={TENANT} -c timeline_id={TIMELINE}");
    send_pg_startup(
        &mut replica,
        &[("user", "cloud_admin"), ("options", &options)],
    )
    .await;
    finish_pg_startup(&mut replica).await;
    send_pg_message(
        &mut replica,
        b'Q',
        b"START_REPLICATION PHYSICAL 0/00001000 (term='1')\0",
    )
    .await;
    assert_eq!(read_pg_message(&mut replica).await.0, b'W');
    let (tag, xlog_data) = read_pg_message(&mut replica).await;
    assert_eq!(tag, b'd');
    assert_eq!(xlog_data[0], b'w');
    assert_eq!(&xlog_data[25..], &wal[..]);

    server_task.abort();
}
