//! Single-node degenerate-geometry tests for the append log (#372 step 2, the
//! free/single-box case built on #286's shape). A one-node deployment runs
//! `k=1, m=0, w=1`: one fragment fsynced to local NVMe plus the fsynced manifest
//! record is the ack, no origin round-trip, async S3 flush behind it.
//!
//! - `single_node_fast_acks_with_the_origin_absent`: an append acks and reads
//!   back from the one local fragment while nothing is in S3 (local durability).
//! - `single_node_recovers_the_tail_across_a_restart`: after a crash the tail
//!   rebuilds from the local fragment (no redundancy, so the fragment must
//!   survive — it does, on durable local storage).
//! - `single_node_flush_then_recover_from_s3`: after a flush the fragment is
//!   dropped and the log recovers wholly from S3.

use std::collections::HashMap;
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
use verglas_safekeeper::{
    AppendGeometry, AppendLog, EcAppendLog, Epoch, FragmentTransport, Lsn, SingleNodeMembership,
    TransportError,
};

const SELF: &str = "solo";

// ---- local fragment store (persists across a simulated crash) --------------

struct MemoryTransport {
    frags: Mutex<HashMap<(String, usize), (Bytes, u32)>>,
}

impl MemoryTransport {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            frags: Mutex::new(HashMap::new()),
        })
    }
    fn wal_fragment_count(&self) -> usize {
        self.frags
            .lock()
            .expect("lock")
            .keys()
            .filter(|(_, index)| *index < 1024)
            .count()
    }
}

#[async_trait::async_trait]
impl FragmentTransport for MemoryTransport {
    async fn has_headroom(&self, _node: &NodeId, _bytes: u64) -> bool {
        true
    }
    async fn place(&self, _node: &NodeId, record: FragmentRecord) -> Result<(), TransportError> {
        self.frags.lock().expect("lock").insert(
            (record.key.object_id, record.key.index),
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
        Err(TransportError::Local(FragmentIoError::Io(
            "unused".to_owned(),
        )))
    }
    async fn load(
        &self,
        _node: &NodeId,
        key: &FragmentKey,
    ) -> Result<Option<LoadedFragment>, TransportError> {
        Ok(self
            .frags
            .lock()
            .expect("lock")
            .get(&(key.object_id.clone(), key.index))
            .map(|(bytes, checksum)| LoadedFragment {
                bytes: bytes.clone(),
                checksum: *checksum,
            }))
    }
    async fn delete(&self, _node: &NodeId, key: &FragmentKey) -> Result<(), TransportError> {
        self.frags
            .lock()
            .expect("lock")
            .remove(&(key.object_id.clone(), key.index));
        Ok(())
    }
}

// ---- in-memory S3 ----------------------------------------------------------

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

fn bytes(n: usize) -> Bytes {
    Bytes::from((0..n).map(|i| (i % 251) as u8).collect::<Vec<u8>>())
}

/// A single-node append log. The configured geometry is the pod one; the
/// single-node membership makes the log degenerate it to (1,0,1) internally.
fn build(
    store: Arc<MemStore>,
    transport: Arc<MemoryTransport>,
    dir: &std::path::Path,
) -> EcAppendLog<MemStore> {
    let membership = Arc::new(SingleNodeMembership::new(NodeId::new(SELF)));
    let geometry = AppendGeometry::new(4, 2, 5).expect("pod geometry");
    EcAppendLog::open(
        0, store, "wal-bkt", "wal", transport, membership, dir, geometry,
    )
    .expect("open")
}

// ---- tests -----------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_node_fast_acks_with_the_origin_absent() {
    let dir = tempfile::tempdir().expect("tmp");
    let store = MemStore::new();
    let transport = MemoryTransport::new();
    let log = build(store.clone(), transport.clone(), dir.path());

    let payload = bytes(4096);
    let r = log
        .append(Epoch(0), Lsn(0), payload.clone())
        .await
        .expect("single-node append acks from local durability, origin untouched");
    assert_eq!(r.start, Lsn(0));
    assert_eq!(r.end, Lsn(4096));

    // One fragment (k=1, m=0), nothing in S3.
    assert_eq!(transport.wal_fragment_count(), 1, "one local fragment");
    assert_eq!(
        store.object_count(),
        0,
        "no origin round-trip on the ack path"
    );

    // Reads back from the one local fragment.
    let got = log.read(Lsn(0), Lsn(4096)).await.expect("read tail");
    assert_eq!(got, payload);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_node_recovers_the_tail_across_a_restart() {
    let dir = tempfile::tempdir().expect("tmp");
    let store = MemStore::new();
    let transport = MemoryTransport::new(); // durable local store, survives the crash

    let payload = bytes(5000);
    {
        let log = build(store.clone(), transport.clone(), dir.path());
        log.append(Epoch(0), Lsn(0), payload.clone())
            .await
            .expect("append");
        // crash: drop the log before any flush
    }

    // Restart: reopen over the same local fragments and manifest.
    let log = build(store, transport, dir.path());
    assert_eq!(log.tail(), Lsn(5000), "manifest recovered the tail");
    let got = log
        .read(Lsn(0), Lsn(5000))
        .await
        .expect("tail rebuilds from the local fragment");
    assert_eq!(got, payload, "recovered tail is byte-identical");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_node_flush_then_recover_from_s3() {
    let dir = tempfile::tempdir().expect("tmp");
    let store = MemStore::new();
    let transport = MemoryTransport::new();

    let payload = bytes(6000);
    {
        let log = build(store.clone(), transport.clone(), dir.path());
        log.append(Epoch(0), Lsn(0), payload.clone())
            .await
            .expect("append");
        let watermark = log.flush().await.expect("flush");
        assert_eq!(watermark, Lsn(6000));
        assert_eq!(
            transport.wal_fragment_count(),
            0,
            "fragment dropped after flush"
        );
        assert_eq!(store.object_count(), 1, "segment in S3");
    }

    // Restart with the local fragment gone: recovery is wholly from S3.
    let log = build(store, transport, dir.path());
    assert_eq!(log.flushed_through(), Lsn(6000));
    let got = log.read(Lsn(0), Lsn(6000)).await.expect("read from S3");
    assert_eq!(got, payload, "flushed single-node log recovers from S3");
}
