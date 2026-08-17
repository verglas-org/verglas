//! The attachable block device: a flat byte range backed by content-addressed
//! chunks, with read/write/flush the NBD server drives.
//!
//! A device holds an in-memory chunk map (index → content hash, or unwritten).
//! Reads resolve each covering chunk (NVMe hit or backend fill) and cut the
//! requested bytes; an unwritten chunk reads as zeros without touching storage.
//! Writes are read-modify-write on the affected chunks, staged locally
//! (write-back) and left dirty. [`BlockDevice::flush`] is the durability
//! barrier: it makes every referenced chunk durable in the backend and only then
//! commits a new manifest version — so a committed version can never reference a
//! non-durable chunk. A chunk that writes back to all zeros is dropped to
//! unwritten, keeping zero regions sparse.
//!
//! Concurrency: one device serves one attached NBD client, so operations serialize
//! behind a single async lock. Correctness over throughput — the block path is
//! not the object cache's hot path.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};

use crate::chunk::{CHUNK_SIZE_BYTES, ChunkHash, chunk_len, covering_spans, is_zero};
use crate::manifest::{Manifest, ManifestError};
use crate::ring::{RingError, RingWriteback};
use crate::store::{ChunkStore, StoreError};

/// A device-level failure.
#[derive(Debug, thiserror::Error)]
pub enum DeviceError {
    /// The chunk store (NVMe or backend) failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// The manifest layer failed.
    #[error(transparent)]
    Manifest(#[from] ManifestError),
    /// The ring write-back plane failed to seal or ack a flush.
    #[error(transparent)]
    Ring(#[from] RingError),
    /// [`BlockDevice::open`] found no committed manifest for the device.
    #[error("device `{0}` has no committed manifest")]
    NotFound(String),
    /// An `ensure` requested a size that disagrees with the device's committed
    /// size. A device's size is fixed at creation; a relaunch must ask for the
    /// same size it was created with.
    #[error("device `{device_id}` exists at {existing} bytes; ensure requested {requested}")]
    SizeMismatch {
        /// The device whose size disagreed.
        device_id: String,
        /// The committed size.
        existing: u64,
        /// The size the ensure requested.
        requested: u64,
    },
}

/// A durable, cache-served virtual block device. Cheap to clone (one `Arc` bump)
/// so the NBD listener and its per-connection handler share one device.
#[derive(Clone)]
pub struct BlockDevice {
    /// Shared device state.
    inner: Arc<Inner>,
}

/// The device's shared, serialized state.
struct Inner {
    /// The device id (also the NBD export name and manifest key component).
    device_id: String,
    /// The fixed device size in bytes.
    size_bytes: u64,
    /// The chunk store backing this device.
    store: ChunkStore,
    /// The flush write-back plane, when this device is attached on a ring. Absent
    /// on a plain single-node deployment, where FLUSH is the inline synchronous
    /// origin barrier; present on the fleet ring, where FLUSH erasure-codes the
    /// sealed set across peers and acks on a quorum (the plane itself still takes
    /// the synchronous barrier for a degenerate or quorum-short ring).
    ring: Option<Arc<RingWriteback>>,
    /// The mutable serving state behind one async lock.
    state: tokio::sync::Mutex<State>,
}

/// The device's mutable state: the current chunk map, the last committed
/// version, and whether uncommitted writes are outstanding.
struct State {
    /// Chunk index → content hash, or `None` for an unwritten (sparse) chunk.
    chunks: Vec<Option<ChunkHash>>,
    /// The last committed manifest version.
    version: u64,
    /// Whether writes have landed since the last commit.
    dirty: bool,
}

impl BlockDevice {
    /// Opens `device_id` if it has a committed manifest, else creates it blank at
    /// `size_bytes` and commits version 0 so its size is durable and it is
    /// attachable from any box. If it already exists, `size_bytes` must match its
    /// committed size. This is the attach entry point the cache-node's block
    /// control endpoint calls.
    pub async fn ensure(
        device_id: impl Into<String>,
        size_bytes: u64,
        store: ChunkStore,
    ) -> Result<BlockDevice, DeviceError> {
        Self::ensure_inner(device_id, size_bytes, store, None).await
    }

    /// Like [`BlockDevice::ensure`], but attaches the device on a ring: its FLUSH
    /// erasure-codes the sealed set across the ring and acks on a quorum, draining
    /// to the origin in the background (a degenerate or quorum-short ring still takes the
    /// synchronous barrier inside the plane). This is the fleet attach entry.
    pub async fn ensure_on_ring(
        device_id: impl Into<String>,
        size_bytes: u64,
        store: ChunkStore,
        ring: Arc<RingWriteback>,
    ) -> Result<BlockDevice, DeviceError> {
        Self::ensure_inner(device_id, size_bytes, store, Some(ring)).await
    }

    /// The shared ensure body: open or blank-create the device, wiring the flush
    /// plane (`ring`) the caller chose.
    async fn ensure_inner(
        device_id: impl Into<String>,
        size_bytes: u64,
        store: ChunkStore,
        ring: Option<Arc<RingWriteback>>,
    ) -> Result<BlockDevice, DeviceError> {
        let device_id = device_id.into();
        match Manifest::load_latest(store.backend().as_ref(), &device_id).await? {
            Some(manifest) => {
                if manifest.size_bytes != size_bytes {
                    return Err(DeviceError::SizeMismatch {
                        device_id,
                        existing: manifest.size_bytes,
                        requested: size_bytes,
                    });
                }
                Ok(Self::from_manifest(store, manifest, ring))
            }
            None => {
                let manifest = Manifest::blank(&device_id, size_bytes);
                manifest.commit(store.backend().as_ref()).await?;
                Ok(Self::from_manifest(store, manifest, ring))
            }
        }
    }

    /// Opens an existing device by loading its latest manifest; errors if the
    /// device was never committed. Used where the size is not known up front.
    pub async fn open(
        device_id: impl Into<String>,
        store: ChunkStore,
    ) -> Result<BlockDevice, DeviceError> {
        let device_id = device_id.into();
        match Manifest::load_latest(store.backend().as_ref(), &device_id).await? {
            Some(manifest) => Ok(Self::from_manifest(store, manifest, None)),
            None => Err(DeviceError::NotFound(device_id)),
        }
    }

    /// Like [`BlockDevice::open`], but attaches the reopened device on `ring` so
    /// its FLUSH uses the ring write-back plane. Used when a node resolves a
    /// committed device it did not itself ensure this run (a restart).
    pub async fn open_on_ring(
        device_id: impl Into<String>,
        store: ChunkStore,
        ring: Arc<RingWriteback>,
    ) -> Result<BlockDevice, DeviceError> {
        let device_id = device_id.into();
        match Manifest::load_latest(store.backend().as_ref(), &device_id).await? {
            Some(manifest) => Ok(Self::from_manifest(store, manifest, Some(ring))),
            None => Err(DeviceError::NotFound(device_id)),
        }
    }

    /// Builds an in-memory device from a loaded manifest and the chosen flush
    /// plane.
    fn from_manifest(
        store: ChunkStore,
        manifest: Manifest,
        ring: Option<Arc<RingWriteback>>,
    ) -> BlockDevice {
        let chunks = manifest
            .chunks
            .iter()
            .map(|slot| slot.as_ref().and_then(|hex| ChunkHash::from_hex(hex)))
            .collect();
        BlockDevice {
            inner: Arc::new(Inner {
                device_id: manifest.device_id,
                size_bytes: manifest.size_bytes,
                store,
                ring,
                state: tokio::sync::Mutex::new(State {
                    chunks,
                    version: manifest.version,
                    dirty: false,
                }),
            }),
        }
    }

    /// The device id (NBD export name).
    pub fn device_id(&self) -> &str {
        &self.inner.device_id
    }

    /// The device size in bytes (the NBD export size).
    pub fn size_bytes(&self) -> u64 {
        self.inner.size_bytes
    }

    /// The last committed manifest version.
    pub async fn version(&self) -> u64 {
        self.inner.state.lock().await.version
    }

    /// Reads `len` bytes at `offset`. Bytes past the device end are not returned
    /// (the read is clamped to the device size). Unwritten ranges read as zeros.
    pub async fn read_at(&self, offset: u64, len: u64) -> Result<Bytes, DeviceError> {
        let size = self.inner.size_bytes;
        if offset >= size || len == 0 {
            return Ok(Bytes::new());
        }
        let read_end = (offset + len).min(size);
        let read_len = read_end - offset;
        let state = self.inner.state.lock().await;
        let mut out = BytesMut::with_capacity(read_len as usize);
        for span in covering_spans(offset, read_len) {
            match state.chunks.get(span.index as usize).and_then(|s| *s) {
                Some(hash) => {
                    let bytes = self.inner.store.read(&hash).await?;
                    out.extend_from_slice(&bytes[span.start..span.end]);
                }
                None => out.resize(out.len() + span.len(), 0),
            }
        }
        Ok(out.freeze())
    }

    /// Writes `data` at `offset` (read-modify-write on affected chunks). Bytes
    /// past the device end are dropped. The write is staged locally and marks the
    /// device dirty; it is not durable until [`BlockDevice::flush`]. A chunk that
    /// becomes all zeros is dropped to unwritten (sparse).
    pub async fn write_at(&self, offset: u64, data: &[u8]) -> Result<(), DeviceError> {
        let size = self.inner.size_bytes;
        if offset >= size || data.is_empty() {
            return Ok(());
        }
        let write_len = (data.len() as u64).min(size - offset);
        let mut state = self.inner.state.lock().await;
        let mut cursor = 0usize;
        for span in covering_spans(offset, write_len) {
            let logical = chunk_len(span.index, size);
            let mut buf = match state.chunks.get(span.index as usize).and_then(|s| *s) {
                Some(hash) => self.inner.store.read(&hash).await?.to_vec(),
                None => vec![0u8; logical],
            };
            let n = span.len();
            buf[span.start..span.end].copy_from_slice(&data[cursor..cursor + n]);
            cursor += n;
            let slot = if is_zero(&buf) {
                None
            } else {
                Some(self.inner.store.stage(Bytes::from(buf)).await?)
            };
            if let Some(entry) = state.chunks.get_mut(span.index as usize) {
                *entry = slot;
            }
        }
        state.dirty = true;
        Ok(())
    }

    /// Drops `len` bytes at `offset` to zeros (NBD TRIM). A fully covered chunk
    /// becomes unwritten (sparse, its object dropped); a partially covered chunk
    /// is zeroed in place. Marks the device dirty.
    pub async fn trim(&self, offset: u64, len: u64) -> Result<(), DeviceError> {
        let size = self.inner.size_bytes;
        if offset >= size || len == 0 {
            return Ok(());
        }
        let trim_len = len.min(size - offset);
        let zeros = vec![0u8; trim_len as usize];
        self.write_at(offset, &zeros).await
    }

    /// The durability barrier and version commit (NBD FLUSH / clean disconnect).
    /// Seals the dirty set into the next manifest version and makes it durable,
    /// then advances the device version. A no-op that returns the current version
    /// when nothing changed since the last flush. Returns the committed version.
    ///
    /// How "durable" is reached is the flush plane's job:
    /// - No ring plane (a plain single-node deployment): the inline synchronous
    ///   origin barrier — every referenced chunk durable, THEN the manifest — the
    ///   invariant that a committed manifest never names a non-durable chunk.
    /// - A ring plane: the sealed set (target manifest + the not-yet-durable
    ///   chunk bytes) is erasure-coded across the ring and acked on a quorum, with
    ///   the origin drain running behind the ack. The device version is advanced only
    ///   after the ack, so a failed flush never advances it.
    pub async fn flush(&self) -> Result<u64, DeviceError> {
        let mut state = self.inner.state.lock().await;
        if !state.dirty {
            return Ok(state.version);
        }
        let next_version = state.version + 1;
        let manifest = Manifest {
            device_id: self.inner.device_id.clone(),
            version: next_version,
            size_bytes: self.inner.size_bytes,
            chunk_size_bytes: CHUNK_SIZE_BYTES,
            chunks: state
                .chunks
                .iter()
                .map(|slot| slot.as_ref().map(ChunkHash::to_hex))
                .collect(),
        };
        let referenced: Vec<ChunkHash> = state.chunks.iter().flatten().copied().collect();
        match &self.inner.ring {
            None => {
                // Barrier: every referenced chunk is durable before the manifest
                // is written. A failure here returns without committing, so
                // `latest` stays on the previous version.
                self.inner.store.ensure_durable(&referenced).await?;
                manifest.commit(self.inner.store.backend().as_ref()).await?;
            }
            Some(ring) => {
                // Seal only the chunks not yet durable at the origin — exactly what a
                // drain (originator or takeover peer) must PUT — and hand the
                // sealed set to the ring plane, which acks on a quorum.
                let dirty = self.inner.store.undurable_chunks(&referenced).await?;
                ring.flush(&self.inner.device_id, manifest, dirty).await?;
            }
        }
        state.version = next_version;
        state.dirty = false;
        Ok(next_version)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::ObjectBackend;
    use crate::backend::testing::MemoryBackend;
    use crate::chunk::CHUNK_SIZE_BYTES;

    /// Opens a device over a fresh in-memory backend and temp NVMe dir.
    async fn device(
        id: &str,
        size: u64,
        backend: Arc<dyn ObjectBackend>,
    ) -> (BlockDevice, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = ChunkStore::open(dir.path(), backend).await.expect("store");
        let dev = BlockDevice::ensure(id, size, store).await.expect("ensure");
        (dev, dir)
    }

    #[tokio::test]
    async fn blank_device_reads_zeros() {
        let backend: Arc<dyn ObjectBackend> = Arc::new(MemoryBackend::new());
        let (dev, _dir) = device("d", 4096, backend).await;
        assert_eq!(
            dev.read_at(0, 4096).await.expect("read"),
            Bytes::from(vec![0u8; 4096])
        );
    }

    #[tokio::test]
    async fn write_then_read_within_a_chunk() {
        let backend: Arc<dyn ObjectBackend> = Arc::new(MemoryBackend::new());
        let (dev, _dir) = device("d", CHUNK_SIZE_BYTES, backend).await;
        dev.write_at(100, b"hello").await.expect("write");
        assert_eq!(
            dev.read_at(100, 5).await.expect("read"),
            Bytes::from_static(b"hello")
        );
        // Surrounding bytes stay zero.
        assert_eq!(
            dev.read_at(95, 5).await.expect("read"),
            Bytes::from(vec![0u8; 5])
        );
    }

    #[tokio::test]
    async fn write_spanning_a_chunk_boundary() {
        let backend: Arc<dyn ObjectBackend> = Arc::new(MemoryBackend::new());
        let (dev, _dir) = device("d", 3 * CHUNK_SIZE_BYTES, backend).await;
        let payload = vec![7u8; 40];
        dev.write_at(CHUNK_SIZE_BYTES - 20, &payload)
            .await
            .expect("write");
        assert_eq!(
            dev.read_at(CHUNK_SIZE_BYTES - 20, 40).await.expect("read"),
            Bytes::from(payload)
        );
    }

    #[tokio::test]
    async fn writing_zeros_stores_no_chunk() {
        let memory = Arc::new(MemoryBackend::new());
        let backend: Arc<dyn ObjectBackend> = memory.clone();
        let (dev, _dir) = device("d", CHUNK_SIZE_BYTES, backend).await;
        dev.write_at(0, &vec![0u8; 4096]).await.expect("write");
        dev.flush().await.expect("flush");
        assert_eq!(
            memory.count_under("blocks/"),
            0,
            "zero writes store no chunk"
        );
    }

    #[tokio::test]
    async fn overwriting_a_chunk_back_to_zero_drops_it() {
        let memory = Arc::new(MemoryBackend::new());
        let backend: Arc<dyn ObjectBackend> = memory.clone();
        let (dev, _dir) = device("d", CHUNK_SIZE_BYTES, backend).await;
        dev.write_at(0, b"data").await.expect("write");
        dev.flush().await.expect("flush");
        assert_eq!(memory.count_under("blocks/"), 1);
        // Overwrite the same bytes back to zero; the chunk becomes sparse again.
        dev.write_at(0, &[0u8; 4]).await.expect("zero");
        dev.trim(0, CHUNK_SIZE_BYTES).await.expect("trim");
        assert_eq!(
            dev.read_at(0, 4).await.expect("read"),
            Bytes::from(vec![0u8; 4])
        );
    }

    #[tokio::test]
    async fn flush_is_a_noop_when_clean() {
        let backend: Arc<dyn ObjectBackend> = Arc::new(MemoryBackend::new());
        let (dev, _dir) = device("d", 4096, backend).await;
        assert_eq!(dev.version().await, 0);
        assert_eq!(
            dev.flush().await.expect("flush"),
            0,
            "clean flush keeps the version"
        );
    }

    #[tokio::test]
    async fn flush_barrier_puts_chunks_before_the_manifest() {
        let memory = Arc::new(MemoryBackend::new());
        let backend: Arc<dyn ObjectBackend> = memory.clone();
        let (dev, _dir) = device("barrier", CHUNK_SIZE_BYTES, backend).await;
        dev.write_at(0, b"durable-first").await.expect("write");
        // The staged chunk is a full-size buffer with the written prefix;
        // reconstruct it to know its content key.
        let mut expected = vec![0u8; CHUNK_SIZE_BYTES as usize];
        expected[..b"durable-first".len()].copy_from_slice(b"durable-first");
        let chunk_hash = ChunkHash::of(&expected);
        let version = dev.flush().await.expect("flush");
        assert_eq!(version, 1);

        let order = memory.put_order();
        let chunk_key = chunk_hash.object_key();
        let manifest_key = format!("manifests/barrier/{:020}.json", 1);
        let chunk_pos = order
            .iter()
            .position(|k| *k == chunk_key)
            .expect("chunk put");
        let manifest_pos = order
            .iter()
            .position(|k| *k == manifest_key)
            .expect("manifest put");
        assert!(
            chunk_pos < manifest_pos,
            "chunk durable before manifest: {order:?}"
        );
    }

    #[tokio::test]
    async fn flush_failure_leaves_no_new_version() {
        let memory = Arc::new(MemoryBackend::new());
        let backend: Arc<dyn ObjectBackend> = memory.clone();
        let (dev, _dir) = device("failing", CHUNK_SIZE_BYTES, backend).await;
        // v0 (blank) is committed by ensure.
        assert_eq!(dev.version().await, 0);
        dev.write_at(0, b"never durable").await.expect("write");
        memory.fail_puts_under("blocks/");
        assert!(dev.flush().await.is_err(), "chunk upload fails");
        assert_eq!(
            dev.version().await,
            0,
            "no version bump on a failed barrier"
        );
        // latest still points at v0, and there is no v1 manifest.
        let loaded = Manifest::load_latest(memory.as_ref(), "failing")
            .await
            .expect("load")
            .expect("v0 present");
        assert_eq!(loaded.version, 0);
    }

    #[tokio::test]
    async fn versions_advance_and_latest_reads_newest() {
        let backend: Arc<dyn ObjectBackend> = Arc::new(MemoryBackend::new());
        let (dev, _dir) = device("v", CHUNK_SIZE_BYTES, backend).await;
        dev.write_at(0, b"one").await.expect("w1");
        assert_eq!(dev.flush().await.expect("f1"), 1);
        dev.write_at(10, b"two").await.expect("w2");
        assert_eq!(dev.flush().await.expect("f2"), 2);
    }
}
