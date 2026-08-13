//! Standalone write-through durability contract tests.
//!
//! One cache node is not a quorum. These tests prove that it never acknowledges
//! local fragment or journal state and returns success only after the origin
//! accepts the complete object.

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
use verglas_write::coordinator::WriteCoordinator;
use verglas_write::journal::JournalStore;
use verglas_write::membership::SingleNodeMembership;
use verglas_write::metrics::WritebackMetrics;
use verglas_write::transport::{FragmentTransport, TransportError};

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

/// A single-node coordinator over the given transport, journal dir, and origin.
/// The configured pod geometry is intentionally irrelevant in standalone mode.
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
        Duration::from_secs(2),
    ))
}

const POD_K: usize = 4;
const POD_M: usize = 2;
const POD_W: usize = 5;

// ---- tests -----------------------------------------------------------------

/// Standalone writes never acknowledge local cache state as durable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_node_requires_origin_durability_before_ack() {
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

    let error = coord
        .put(
            &ck("data/f1"),
            &WriteMetadata::default(),
            body(4096),
            POD_K,
            POD_M,
            POD_W,
        )
        .await
        .expect_err("standalone PUT must fail while the origin is unavailable");
    assert!(matches!(error, WriteError::Backend(_)), "{error:?}");

    // Local cache state is never the standalone durability authority.
    assert_eq!(transport.fragment_count(), 0, "no local fragment is acked");
    assert!(
        journals.find_dirty("default", "bkt", "data/f1").is_none(),
        "a failed origin write leaves no dirty write-back record"
    );
    let snap = metrics.snapshot();
    assert_eq!(snap.acked_via_quorum, 0, "no local quorum exists");
    assert_eq!(snap.acked_via_write_through, 1, "origin path attempted");
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

/// Local headroom does not affect standalone origin write-through.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_writes_through_when_origin_is_available() {
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
        .expect("write-through to a reachable origin succeeds");
    assert_eq!(
        origin.get("bkt", "data/f1"),
        Some(body(4096)),
        "landed at origin"
    );
    let snap = metrics.snapshot();
    assert_eq!(
        snap.acked_via_write_through, 1,
        "standalone ACK follows synchronous write-through"
    );
    assert!(journals.is_idle(), "write-through leaves nothing buffered");
    assert_eq!(transport.fragment_count(), 0, "no EC state was created");
}
