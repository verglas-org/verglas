//! The content-addressed chunk store: an NVMe-local hit path in front of the
//! durable object backend.
//!
//! A chunk is staged locally first (write-back), served from the local NVMe
//! copy on read, and made durable in the backend only at the flush barrier.
//! Chunks are named by content ([`ChunkHash`]), so:
//! - staging the same bytes twice is idempotent (one local file, one backend
//!   object),
//! - [`ChunkStore::ensure_durable`] skips any chunk the backend already holds
//!   (dedup across devices and versions), and
//! - a chunk missed locally fills from the backend and repopulates NVMe, so a
//!   device attached on a different box reads back what another box flushed.
//!
//! Local layout: `<root>/blocks/<hex>`, one file per chunk, written temp-then-
//! rename so a crash never leaves a half-written chunk visible.
//!
//! Extension point (not built, prototype scope): the local store grows with the
//! chunks a device touches and has no eviction. Sharing the cache's one NVMe
//! budget — LRU-evicting clean, backend-durable chunks under pressure — is the
//! same first-come-first-served accounting the block cache uses (#223); wire it
//! here when the block device tier competes for NVMe with the object cache.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use bytes::Bytes;

use crate::backend::{BackendError, ObjectBackend};
use crate::chunk::{CHUNK_PREFIX, ChunkHash};

/// A chunk-store failure: either local NVMe IO or the durable backend.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// Local NVMe IO failed (staging or reading a chunk file).
    #[error("chunk store local io for `{hash}`: {source}")]
    Io {
        /// The chunk being read or written, in hex.
        hash: String,
        /// The underlying IO error.
        #[source]
        source: std::io::Error,
    },
    /// The durable backend failed.
    #[error(transparent)]
    Backend(#[from] BackendError),
    /// A chunk referenced by a read is present in neither NVMe nor the backend.
    /// A device manifest must never reference a non-durable chunk, so this is a
    /// corrupt-state signal, never served as zeros or partial bytes.
    #[error("chunk `{0}` is durable in neither NVMe nor the backend")]
    MissingChunk(String),
}

/// A content-addressed chunk store over a local NVMe directory and a durable
/// object backend. Cheap to clone (one `Arc` bump) so a device and the flush
/// path share one store.
#[derive(Clone)]
pub struct ChunkStore {
    /// Shared state behind an `Arc` so clones address one store.
    inner: Arc<Inner>,
}

/// The store's shared state.
struct Inner {
    /// Local NVMe root; chunk files live under `<root>/blocks/`.
    root: PathBuf,
    /// The durable object backend.
    backend: Arc<dyn ObjectBackend>,
    /// Chunk hashes known to be durable in the backend. A chunk enters this set
    /// when it is uploaded or when a backend `exists` check finds it, so the
    /// flush barrier does not re-upload or re-probe an already-durable chunk.
    /// A conservative cache: a hash absent here is re-checked, never wrongly
    /// assumed durable.
    durable: Mutex<HashSet<ChunkHash>>,
}

impl ChunkStore {
    /// Opens a chunk store rooted at `local_root`, creating the `blocks/`
    /// subdirectory, over the durable `backend`.
    pub async fn open(
        local_root: impl AsRef<Path>,
        backend: Arc<dyn ObjectBackend>,
    ) -> Result<ChunkStore, StoreError> {
        let root = local_root.as_ref().to_path_buf();
        let blocks_dir = root.join(CHUNK_PREFIX);
        tokio::fs::create_dir_all(&blocks_dir)
            .await
            .map_err(|source| StoreError::Io {
                hash: "<mkdir>".to_owned(),
                source,
            })?;
        Ok(ChunkStore {
            inner: Arc::new(Inner {
                root,
                backend,
                durable: Mutex::new(HashSet::new()),
            }),
        })
    }

    /// The local NVMe path a chunk's bytes live at.
    fn local_path(&self, hash: &ChunkHash) -> PathBuf {
        self.inner.root.join(CHUNK_PREFIX).join(hash.to_hex())
    }

    /// Stages `bytes` locally (write-back) and returns its content address.
    /// Idempotent: re-staging identical bytes is a no-op past the first write.
    /// The chunk is NOT durable until [`ChunkStore::ensure_durable`] runs — this
    /// is the write-back property the flush barrier closes.
    pub async fn stage(&self, bytes: Bytes) -> Result<ChunkHash, StoreError> {
        let hash = ChunkHash::of(&bytes);
        let path = self.local_path(&hash);
        if tokio::fs::try_exists(&path).await.unwrap_or(false) {
            return Ok(hash);
        }
        let tmp = self.temp_path(&hash);
        tokio::fs::write(&tmp, &bytes)
            .await
            .map_err(|source| StoreError::Io {
                hash: hash.to_hex(),
                source,
            })?;
        tokio::fs::rename(&tmp, &path)
            .await
            .map_err(|source| StoreError::Io {
                hash: hash.to_hex(),
                source,
            })?;
        Ok(hash)
    }

    /// A temp path alongside the final chunk file, for the temp-then-rename
    /// staging write. Distinct per chunk so concurrent stages do not collide.
    fn temp_path(&self, hash: &ChunkHash) -> PathBuf {
        self.inner
            .root
            .join(CHUNK_PREFIX)
            .join(format!(".{}.tmp", hash.to_hex()))
    }

    /// Reads a chunk's bytes: NVMe hit, else fill from the backend and
    /// repopulate NVMe. A chunk present in neither is [`StoreError::MissingChunk`]
    /// — never served as zeros — because a committed manifest guarantees every
    /// referenced chunk is durable.
    pub async fn read(&self, hash: &ChunkHash) -> Result<Bytes, StoreError> {
        let path = self.local_path(hash);
        match tokio::fs::read(&path).await {
            Ok(bytes) => Ok(Bytes::from(bytes)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                self.fill_from_backend(hash).await
            }
            Err(source) => Err(StoreError::Io {
                hash: hash.to_hex(),
                source,
            }),
        }
    }

    /// Fills a locally-missing chunk from the backend and repopulates NVMe. The
    /// filled chunk is known durable (it came from the backend), so it joins the
    /// durable set.
    async fn fill_from_backend(&self, hash: &ChunkHash) -> Result<Bytes, StoreError> {
        let key = hash.object_key();
        let Some(bytes) = self.inner.backend.get(&key).await? else {
            return Err(StoreError::MissingChunk(hash.to_hex()));
        };
        // Repopulate NVMe so subsequent reads hit locally. A failure to write the
        // local copy is not fatal to this read — return the bytes regardless.
        let tmp = self.temp_path(hash);
        if tokio::fs::write(&tmp, &bytes).await.is_ok() {
            let _ = tokio::fs::rename(&tmp, &self.local_path(hash)).await;
        }
        self.mark_durable(hash);
        Ok(bytes)
    }

    /// The flush barrier. Makes every chunk in `hashes` durable in the backend
    /// before returning: a chunk already durable (in the durable set, or found
    /// by a backend `exists` probe — the dedup path) is skipped; otherwise its
    /// local bytes are read and uploaded. On return, every referenced chunk is
    /// guaranteed durable, which is the precondition for writing a manifest that
    /// references them. A failure here leaves the caller free to NOT commit the
    /// manifest, preserving the invariant that a manifest never names a
    /// non-durable chunk.
    pub async fn ensure_durable(&self, hashes: &[ChunkHash]) -> Result<(), StoreError> {
        for hash in hashes {
            if self.is_known_durable(hash) {
                continue;
            }
            let key = hash.object_key();
            if self.inner.backend.exists(&key).await? {
                self.mark_durable(hash);
                continue;
            }
            let bytes = self.read_local(hash).await?;
            self.inner.backend.put(&key, bytes).await?;
            self.mark_durable(hash);
        }
        Ok(())
    }

    /// Collects the bytes of every referenced chunk that is not yet known durable
    /// in the backend, reading each from local NVMe. This is the dirty set the
    /// ring write-back plane seals into a flush bundle: exactly the chunks a
    /// drain (whether by the originator or by a peer that reconstructed the
    /// bundle) must PUT to the origin, since the already-durable chunks are at the origin already.
    /// A chunk not known durable that is actually at the origin costs one idempotent
    /// re-PUT — content addressing makes that harmless.
    pub async fn undurable_chunks(
        &self,
        referenced: &[ChunkHash],
    ) -> Result<Vec<(ChunkHash, Bytes)>, StoreError> {
        let mut out = Vec::new();
        let mut seen = HashSet::new();
        for hash in referenced {
            if self.is_known_durable(hash) || !seen.insert(*hash) {
                continue;
            }
            out.push((*hash, self.read_local(hash).await?));
        }
        Ok(out)
    }

    /// Records `hash` as durable in the backend after a drain PUT, so a later
    /// flush barrier does not re-upload or re-probe it. The public counterpart of
    /// the internal marking [`ChunkStore::ensure_durable`] does, for the ring
    /// plane's drain path which PUTs bundle chunks directly to the backend.
    pub fn note_durable(&self, hash: &ChunkHash) {
        self.mark_durable(hash);
    }

    /// Reads a chunk's bytes strictly from NVMe (no backend fill). Used by the
    /// flush barrier to upload a staged chunk; a staged chunk is always present
    /// locally, so a miss here is a real local-IO error.
    async fn read_local(&self, hash: &ChunkHash) -> Result<Bytes, StoreError> {
        tokio::fs::read(self.local_path(hash))
            .await
            .map(Bytes::from)
            .map_err(|source| StoreError::Io {
                hash: hash.to_hex(),
                source,
            })
    }

    /// Whether `hash` is recorded as durable in this process.
    fn is_known_durable(&self, hash: &ChunkHash) -> bool {
        self.inner
            .durable
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(hash)
    }

    /// Records `hash` as durable in the backend.
    fn mark_durable(&self, hash: &ChunkHash) {
        self.inner
            .durable
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(*hash);
    }

    /// The durable object backend, for the manifest writer to share one backend
    /// handle with the chunk store.
    pub fn backend(&self) -> &Arc<dyn ObjectBackend> {
        &self.inner.backend
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::testing::MemoryBackend;

    /// Stages bytes, uploads them, and reads them back through the durable path.
    #[tokio::test]
    async fn stage_ensure_durable_and_read() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend: Arc<dyn ObjectBackend> = Arc::new(MemoryBackend::new());
        let store = ChunkStore::open(dir.path(), backend.clone())
            .await
            .expect("open");

        let hash = store
            .stage(Bytes::from_static(b"chunk-bytes"))
            .await
            .expect("stage");
        store.ensure_durable(&[hash]).await.expect("durable");
        assert_eq!(
            store.read(&hash).await.expect("read"),
            Bytes::from_static(b"chunk-bytes")
        );
    }

    /// Two identical chunks upload once: content addressing dedups them in the
    /// backend even when staged separately.
    #[tokio::test]
    async fn identical_chunks_upload_once() {
        let dir = tempfile::tempdir().expect("tempdir");
        let memory = Arc::new(MemoryBackend::new());
        let backend: Arc<dyn ObjectBackend> = memory.clone();
        let store = ChunkStore::open(dir.path(), backend).await.expect("open");

        let a = store
            .stage(Bytes::from_static(b"same"))
            .await
            .expect("stage a");
        let b = store
            .stage(Bytes::from_static(b"same"))
            .await
            .expect("stage b");
        assert_eq!(a, b);
        store.ensure_durable(&[a, b]).await.expect("durable");
        assert_eq!(memory.count_under("blocks/"), 1);
    }

    /// A chunk missing from both NVMe and the backend is a hard error, never
    /// served as zeros.
    #[tokio::test]
    async fn missing_chunk_is_an_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend: Arc<dyn ObjectBackend> = Arc::new(MemoryBackend::new());
        let store = ChunkStore::open(dir.path(), backend).await.expect("open");
        let phantom = ChunkHash::of(b"never stored");
        assert!(matches!(
            store.read(&phantom).await,
            Err(StoreError::MissingChunk(_))
        ));
    }

    /// A chunk staged on one store reads back through a second store over the
    /// same backend with an empty local dir — the cross-box fill path.
    #[tokio::test]
    async fn fills_from_backend_on_a_fresh_local_store() {
        let backend: Arc<dyn ObjectBackend> = Arc::new(MemoryBackend::new());
        let box_a = tempfile::tempdir().expect("a");
        let box_b = tempfile::tempdir().expect("b");
        let store_a = ChunkStore::open(box_a.path(), backend.clone())
            .await
            .expect("open a");
        let hash = store_a
            .stage(Bytes::from_static(b"cross-box"))
            .await
            .expect("stage");
        store_a.ensure_durable(&[hash]).await.expect("durable");

        let store_b = ChunkStore::open(box_b.path(), backend)
            .await
            .expect("open b");
        assert_eq!(
            store_b.read(&hash).await.expect("read b"),
            Bytes::from_static(b"cross-box")
        );
    }
}
