//! The device manifest: one small object per device version mapping each chunk
//! index to its content hash (or "unwritten", which reads as zeros).
//!
//! Committing a new version writes the version's manifest object and then, as
//! the last step, advances a `latest` pointer object to that version. The
//! pointer swap is a single object write, so a reader sees either the old
//! version or the new one, never a torn state — that is the version-swap
//! atomicity guarantee. Backend layout, all under the device's manifest prefix:
//! - `manifests/<device_id>/<version:020>.json` — one immutable version.
//! - `manifests/<device_id>/latest` — the current version number as text.
//!
//! The flush barrier lives in [`crate::device`], not here: the caller makes
//! every referenced chunk durable BEFORE calling [`Manifest::commit`], so a
//! committed manifest can never name a non-durable chunk.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::backend::{BackendError, ObjectBackend};
use crate::chunk::{CHUNK_SIZE_BYTES, ChunkHash, chunk_count};

/// The backend key prefix device manifests live under.
const MANIFEST_PREFIX: &str = "manifests";

/// The pointer object naming the current version under a device's prefix.
const LATEST_POINTER: &str = "latest";

/// A manifest-layer failure.
#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    /// The durable backend failed.
    #[error(transparent)]
    Backend(#[from] BackendError),
    /// A manifest object could not be decoded (corrupt or truncated JSON).
    #[error("cannot decode manifest at `{key}`: {source}")]
    Decode {
        /// The manifest key that failed to decode.
        key: String,
        /// The underlying JSON error.
        #[source]
        source: serde_json::Error,
    },
    /// The `latest` pointer named a version whose manifest object is absent.
    #[error("device `{device_id}` latest points at version {version} but its manifest is missing")]
    MissingVersion {
        /// The device whose pointer is dangling.
        device_id: String,
        /// The version the pointer named.
        version: u64,
    },
}

/// One device version: its size, and the chunk-index → content-hash map. An
/// entry of `None` is an unwritten (sparse) chunk that reads back as zeros and
/// stores no object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// The device id this manifest belongs to.
    pub device_id: String,
    /// The monotonic version number. Version 0 is the blank device.
    pub version: u64,
    /// The device size in bytes.
    pub size_bytes: u64,
    /// The fixed chunk size the map is cut on. Stored so a reader validates the
    /// geometry it decodes against the one this crate uses.
    pub chunk_size_bytes: u64,
    /// Chunk index → content hash (hex), or `None` for an unwritten chunk. The
    /// vector length is `chunk_count(size_bytes)`.
    pub chunks: Vec<Option<String>>,
}

impl Manifest {
    /// A blank device manifest at version 0: correctly sized, every chunk
    /// unwritten (all reads are zeros, no stored objects).
    pub fn blank(device_id: impl Into<String>, size_bytes: u64) -> Manifest {
        let count = chunk_count(size_bytes) as usize;
        Manifest {
            device_id: device_id.into(),
            version: 0,
            size_bytes,
            chunk_size_bytes: CHUNK_SIZE_BYTES,
            chunks: vec![None; count],
        }
    }

    /// The content hash mapped at chunk `index`, or `None` if the index is
    /// unwritten or out of range.
    pub fn chunk_at(&self, index: u64) -> Option<ChunkHash> {
        self.chunks
            .get(index as usize)
            .and_then(|slot| slot.as_ref())
            .and_then(|hex| ChunkHash::from_hex(hex))
    }

    /// Every distinct chunk hash this manifest references, for the flush barrier
    /// to make durable before commit.
    pub fn referenced_chunks(&self) -> Vec<ChunkHash> {
        let mut out = Vec::new();
        for slot in self.chunks.iter().flatten() {
            if let Some(hash) = ChunkHash::from_hex(slot) {
                out.push(hash);
            }
        }
        out
    }

    /// The backend key of this manifest's version object.
    fn version_key(device_id: &str, version: u64) -> String {
        format!("{MANIFEST_PREFIX}/{device_id}/{version:020}.json")
    }

    /// The backend key of a device's `latest` pointer.
    fn latest_key(device_id: &str) -> String {
        format!("{MANIFEST_PREFIX}/{device_id}/{LATEST_POINTER}")
    }

    /// Commits this manifest as a new durable version, then advances the
    /// device's `latest` pointer to it. The caller MUST have made every
    /// referenced chunk durable first (see [`crate::device::BlockDevice::flush`]).
    /// Ordering matters: the version object is written before the pointer, so a
    /// crash between the two leaves `latest` on the previous version (the new
    /// version is orphaned, never half-visible).
    pub async fn commit(&self, backend: &dyn ObjectBackend) -> Result<(), ManifestError> {
        let body = serde_json::to_vec(self).map_err(|source| ManifestError::Decode {
            key: Self::version_key(&self.device_id, self.version),
            source,
        })?;
        backend
            .put(
                &Self::version_key(&self.device_id, self.version),
                Bytes::from(body),
            )
            .await?;
        backend
            .put(
                &Self::latest_key(&self.device_id),
                Bytes::from(self.version.to_string()),
            )
            .await?;
        Ok(())
    }

    /// Loads the current (latest-pointed) manifest for `device_id`, or `None` if
    /// the device has never been committed.
    pub async fn load_latest(
        backend: &dyn ObjectBackend,
        device_id: &str,
    ) -> Result<Option<Manifest>, ManifestError> {
        let Some(pointer) = backend.get(&Self::latest_key(device_id)).await? else {
            return Ok(None);
        };
        let version: u64 = String::from_utf8_lossy(&pointer)
            .trim()
            .parse()
            .map_err(|_| ManifestError::MissingVersion {
                device_id: device_id.to_owned(),
                version: 0,
            })?;
        let key = Self::version_key(device_id, version);
        let Some(body) = backend.get(&key).await? else {
            return Err(ManifestError::MissingVersion {
                device_id: device_id.to_owned(),
                version,
            });
        };
        let manifest = serde_json::from_slice(&body)
            .map_err(|source| ManifestError::Decode { key, source })?;
        Ok(Some(manifest))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::testing::MemoryBackend;
    use std::sync::Arc;

    #[test]
    fn blank_manifest_is_sized_and_sparse() {
        let m = Manifest::blank("dev", CHUNK_SIZE_BYTES + 1);
        assert_eq!(m.version, 0);
        assert_eq!(m.chunks.len(), 2);
        assert!(m.chunks.iter().all(Option::is_none));
        assert!(m.referenced_chunks().is_empty());
    }

    #[tokio::test]
    async fn commit_then_load_latest_round_trips() {
        let backend: Arc<dyn ObjectBackend> = Arc::new(MemoryBackend::new());
        let mut m = Manifest::blank("dev", CHUNK_SIZE_BYTES);
        let h = ChunkHash::of(b"payload");
        m.chunks[0] = Some(h.to_hex());
        m.version = 1;
        m.commit(backend.as_ref()).await.expect("commit");

        let loaded = Manifest::load_latest(backend.as_ref(), "dev")
            .await
            .expect("load")
            .expect("present");
        assert_eq!(loaded.version, 1);
        assert_eq!(loaded.chunk_at(0), Some(h));
        assert_eq!(loaded.referenced_chunks(), vec![h]);
    }

    #[tokio::test]
    async fn absent_device_has_no_latest() {
        let backend: Arc<dyn ObjectBackend> = Arc::new(MemoryBackend::new());
        assert!(
            Manifest::load_latest(backend.as_ref(), "nope")
                .await
                .expect("load")
                .is_none()
        );
    }
}
