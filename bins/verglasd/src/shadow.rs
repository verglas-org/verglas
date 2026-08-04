//! The daemon-side adapter bridging `verglas_vector`'s [`ShadowBlobStore`] seam
//! onto the cache-managed shadow store in `verglas_cache` (#95).
//!
//! The layering it protects: `verglas-vector` is engine-pure — it depends only
//! on its own [`ShadowBlobStore`] trait, never on `verglas-cache`. The concrete
//! NVMe-resident store lives in `verglas-cache`
//! ([`verglas_cache::shadow::ShadowStore`]). This adapter, owned by the daemon
//! (which already depends on both crates), is the one place the two meet, so
//! neither engine crate gains a dependency on the other.
//!
//! The bridge is a straight mapping. A vector index's
//! [`IndexKey`]`{ target, field }` becomes a
//! [`ArtifactKey`]`{ kind: VectorIndex, target, id: field }` — the shadow
//! store's key space carries the artifact kind so future tenants (graph
//! adjacency, heat, prewarm, pruning rollups) share the same store, but the
//! vector routes only ever stamp `VectorIndex`. Store errors surface as
//! [`VectorError::Store`].

use std::sync::Arc;

use async_trait::async_trait;

use verglas_cache::shadow::{ArtifactKey, ArtifactKind, ShadowStore};
use verglas_vector::error::{Result as VectorResult, VectorError};
use verglas_vector::store::{IndexKey, ShadowBlobStore, StoredBlob};

/// A [`ShadowBlobStore`] backed by the cache's [`ShadowStore`]. Every vector
/// index blob is stored under [`ArtifactKind::VectorIndex`]; the target and
/// field come straight from the [`IndexKey`].
pub struct CacheShadowStore {
    inner: Arc<ShadowStore>,
}

impl CacheShadowStore {
    /// Wraps a shared cache shadow store as the vector engine's blob store.
    pub fn new(inner: Arc<ShadowStore>) -> Self {
        CacheShadowStore { inner }
    }

    /// The cache-store key for a vector [`IndexKey`].
    fn key(index: &IndexKey) -> ArtifactKey {
        ArtifactKey::new(
            ArtifactKind::VectorIndex,
            index.target.clone(),
            index.field.clone(),
        )
    }
}

#[async_trait]
impl ShadowBlobStore for CacheShadowStore {
    async fn put(&self, key: &IndexKey, snapshot: i64, bytes: Vec<u8>) -> VectorResult<StoredBlob> {
        let blob = self
            .inner
            .put(&Self::key(key), snapshot, bytes)
            .await
            .map_err(|e| VectorError::Store(e.to_string()))?;
        Ok(StoredBlob {
            location: blob.location,
            snapshot: blob.snapshot,
            bytes: blob.bytes,
        })
    }

    async fn latest(&self, key: &IndexKey) -> VectorResult<Option<StoredBlob>> {
        let found = self
            .inner
            .latest(&Self::key(key))
            .await
            .map_err(|e| VectorError::Store(e.to_string()))?;
        Ok(found.map(|blob| StoredBlob {
            location: blob.location,
            snapshot: blob.snapshot,
            bytes: blob.bytes,
        }))
    }

    async fn get(&self, key: &IndexKey, snapshot: i64) -> VectorResult<Option<StoredBlob>> {
        let found = self
            .inner
            .get(&Self::key(key), snapshot)
            .await
            .map_err(|e| VectorError::Store(e.to_string()))?;
        Ok(found.map(|blob| StoredBlob {
            location: blob.location,
            snapshot: blob.snapshot,
            bytes: blob.bytes,
        }))
    }

    async fn snapshots(&self, key: &IndexKey) -> VectorResult<Vec<i64>> {
        self.inner
            .snapshots(&Self::key(key))
            .await
            .map_err(|e| VectorError::Store(e.to_string()))
    }
}
