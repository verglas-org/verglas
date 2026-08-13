//! The block-device subsystem: the durable chunk store, the attached-device
//! registry, and the block-control HTTP route an attach client calls.
//!
//! Workloads that attach virtual block devices are otherwise stateless; this
//! cache node is the stateful layer for their block storage (#382). Before an
//! attach client connects a kernel NBD client to this node's [`crate::nbd`]
//! listener, it POSTs `/blocks/ensure` here so the device exists at a known
//! size and is registered for the NBD export name = device id. Durability is
//! the cache's backing bucket, reached through [`verglas_block`]'s object
//! backend.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use axum::response::{IntoResponse, Response};
use axum::{Json, Router, http::StatusCode, routing::post};
use serde::{Deserialize, Serialize};
use verglas_block::{BlockDevice, ChunkStore, DeviceError, ObjectBackend, RingWriteback};

/// The subdirectory under `cache.dir` the local NVMe chunk copies live in. Kept
/// distinct from the object cache's `blocks.data`/`meta.data` foyer files so the
/// two tiers never collide on disk.
const LOCAL_SUBDIR: &str = "block-devices";

/// The attached-device registry over one shared chunk store. Cheap to clone (one
/// `Arc` bump) so the NBD listener and the control route share one registry.
#[derive(Clone)]
pub struct DeviceRegistry {
    /// Shared registry state.
    inner: Arc<Inner>,
}

/// The registry's shared state: the chunk store, the map of open devices, and —
/// once the node has learned its ring peers — the flush write-back plane every
/// device attaches on.
struct Inner {
    /// The content-addressed chunk store backing every device on this node.
    store: ChunkStore,
    /// The ring flush plane, set once at startup when the node is on a ring.
    /// Absent means a single-node deployment: FLUSH is the synchronous R2
    /// barrier, exactly as before the write-back plane existed.
    ring: OnceLock<Arc<RingWriteback>>,
    /// Devices opened on this node, keyed by device id (the NBD export name).
    devices: tokio::sync::Mutex<HashMap<String, BlockDevice>>,
}

impl DeviceRegistry {
    /// Opens the registry: roots the local chunk store under `cache_dir` and
    /// wires it to the durable object `backend` (the cache's backing bucket). The
    /// flush plane is attached separately via [`DeviceRegistry::attach_ring`] once
    /// the node's ring peers are known; until then FLUSH is the synchronous R2
    /// barrier.
    pub async fn open(
        cache_dir: &std::path::Path,
        backend: Arc<dyn ObjectBackend>,
    ) -> Result<DeviceRegistry, DeviceError> {
        let store = ChunkStore::open(cache_dir.join(LOCAL_SUBDIR), backend).await?;
        Ok(DeviceRegistry {
            inner: Arc::new(Inner {
                store,
                ring: OnceLock::new(),
                devices: tokio::sync::Mutex::new(HashMap::new()),
            }),
        })
    }

    /// The shared chunk store, so the caller can build the ring flush plane over
    /// the same store this registry's devices stage into (the plane's barrier and
    /// the devices' staging must be one store).
    pub fn chunk_store(&self) -> ChunkStore {
        self.inner.store.clone()
    }

    /// Attaches the ring flush plane. Called once at startup, before any device
    /// is ensured, when the node is on a ring. Every device attached after this
    /// routes its FLUSH through the plane (erasure-code, quorum-ack, background
    /// drain); the plane itself still takes the synchronous barrier for a
    /// degenerate or quorum-short ring.
    pub fn attach_ring(&self, ring: Arc<RingWriteback>) {
        let _ = self.inner.ring.set(ring);
    }

    /// Ensures a device exists at `size_bytes` and is registered under
    /// `device_id`, creating and committing a blank version 0 on first sight so
    /// its size is durable and it is attachable from any box. Idempotent: a
    /// re-ensure of an open device returns the open handle. Returns the device's
    /// current committed version.
    pub async fn ensure(&self, device_id: &str, size_bytes: u64) -> Result<u64, DeviceError> {
        let mut devices = self.inner.devices.lock().await;
        if let Some(existing) = devices.get(device_id) {
            return Ok(existing.version().await);
        }
        let store = self.inner.store.clone();
        let device = match self.inner.ring.get() {
            Some(ring) => {
                BlockDevice::ensure_on_ring(device_id, size_bytes, store, ring.clone()).await?
            }
            None => BlockDevice::ensure(device_id, size_bytes, store).await?,
        };
        let version = device.version().await;
        devices.insert(device_id.to_owned(), device);
        Ok(version)
    }

    /// Resolves an attached device by NBD export name (device id). Falls back to
    /// opening an already-committed device from the backend if it is not yet in
    /// the in-memory map (a node restart, say); returns `None` only when no such
    /// device was ever committed. A reopened device attaches on the ring plane
    /// when one is present, so its FLUSH uses the write-back path too.
    pub async fn get(&self, device_id: &str) -> Result<Option<BlockDevice>, DeviceError> {
        let mut devices = self.inner.devices.lock().await;
        if let Some(existing) = devices.get(device_id) {
            return Ok(Some(existing.clone()));
        }
        let store = self.inner.store.clone();
        let opened = match self.inner.ring.get() {
            Some(ring) => BlockDevice::open_on_ring(device_id, store, ring.clone()).await,
            None => BlockDevice::open(device_id, store).await,
        };
        match opened {
            Ok(device) => {
                devices.insert(device_id.to_owned(), device.clone());
                Ok(Some(device))
            }
            Err(DeviceError::NotFound(_)) => Ok(None),
            Err(other) => Err(other),
        }
    }
}

/// The `/blocks/ensure` request body: the device id and its size.
#[derive(Debug, Clone, Deserialize)]
pub struct EnsureRequest {
    /// The device id (the NBD export name; stable across relaunch).
    pub device_id: String,
    /// The device size in bytes.
    pub size_bytes: u64,
}

/// The `/blocks/ensure` response: the device id and its current committed version.
#[derive(Debug, Clone, Serialize)]
pub struct EnsureResponse {
    /// The ensured device id.
    pub device_id: String,
    /// The device's current committed manifest version.
    pub version: u64,
}

/// The block-control router, merged into the admin surface. Carries exactly
/// `POST /blocks/ensure`; the NBD data plane is a separate raw-TCP listener.
pub fn control_router(registry: DeviceRegistry) -> Router {
    Router::new().route(
        "/blocks/ensure",
        post(move |Json(req): Json<EnsureRequest>| {
            let registry = registry.clone();
            async move {
                match registry.ensure(&req.device_id, req.size_bytes).await {
                    Ok(version) => Json(EnsureResponse {
                        device_id: req.device_id,
                        version,
                    })
                    .into_response(),
                    Err(error) => ensure_error(&error),
                }
            }
        }),
    )
}

/// Renders an ensure failure as a 500 with the device error text. A failing
/// ensure is what makes a launch fail loudly when the cache cannot back the
/// device — the host agent treats a non-200 as "no durable disk, do not boot".
fn ensure_error(error: &DeviceError) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("cannot ensure block device: {error}"),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use verglas_block::ObjectStoreBackend;

    /// A registry over an in-memory object backend and temp NVMe dir.
    async fn registry() -> (DeviceRegistry, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let backend: Arc<dyn ObjectBackend> = Arc::new(ObjectStoreBackend::new(object_store));
        let registry = DeviceRegistry::open(dir.path(), backend)
            .await
            .expect("open");
        (registry, dir)
    }

    #[tokio::test]
    async fn ensure_is_idempotent_and_registers_the_device() {
        let (registry, _dir) = registry().await;
        assert_eq!(registry.ensure("dev", 4096).await.expect("ensure"), 0);
        // Second ensure returns the open handle's version, not a new device.
        assert_eq!(registry.ensure("dev", 4096).await.expect("ensure"), 0);
        assert!(registry.get("dev").await.expect("get").is_some());
    }

    #[tokio::test]
    async fn get_returns_none_for_an_unknown_device() {
        let (registry, _dir) = registry().await;
        assert!(registry.get("never").await.expect("get").is_none());
    }
}
