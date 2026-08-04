//! Durable, cache-served virtual block devices for microVMs (#382).
//!
//! Workload microVMs are stateless; the verglas cache tier is the only stateful
//! layer, and its durability is the cache's backing bucket. This crate is that
//! stateful layer for block storage: tmpfs-resident workload state (a bundled
//! database datadir, say) moves onto a cache-served block device that survives
//! relaunch and attaches from any box.
//!
//! # Shape
//! - [`chunk`] — content addressing and 2 MiB chunk geometry. A device is a flat
//!   byte range cut into fixed chunks, each named by the SHA-256 of its bytes so
//!   identical chunks dedup across devices and versions.
//! - [`backend`] — the durability seam: a minimal object backend, with an
//!   adapter over the `object_store` handle the daemon already builds per bucket.
//!   Chunks and manifests persist through the same backing bucket the cache uses,
//!   under dedicated prefixes.
//! - [`store`] — the content-addressed chunk store: an NVMe-local hit path in
//!   front of the backend, with write-back staging and the `ensure_durable`
//!   barrier.
//! - [`manifest`] — one small object per device version mapping chunk index to
//!   content hash, with an atomically-swapped `latest` pointer.
//! - [`device`] — the attachable [`BlockDevice`]: read/write/flush/trim, with the
//!   flush barrier that never commits a manifest referencing a non-durable chunk,
//!   and sparse handling so zero ranges cost no storage.
//!
//! # What this crate does NOT do (extension points, not built here)
//! - It does not speak NBD. The NBD server that exposes a [`BlockDevice`] to the
//!   Linux kernel client lives in the cache-node binary; `vhost-user-blk` is the
//!   later attach surface noted there.
//! - It does not lazy-boot a rootfs from a block device, migrate the snapshot
//!   store onto this layout, or do cross-box live attach — those build on this
//!   (verglas-cloud #89/#90) and are out of scope.
//! - It does not evict the local NVMe copies; the local store grows with touched
//!   chunks. Sharing the one NVMe budget is an accounting extension point (see
//!   [`store`]).

pub mod backend;
pub mod chunk;
pub mod device;
pub mod manifest;
pub mod ring;
pub mod store;

pub use backend::{BackendError, ObjectBackend, ObjectStoreBackend};
pub use chunk::{CHUNK_SIZE_BYTES, ChunkHash};
pub use device::{BlockDevice, DeviceError};
pub use manifest::{Manifest, ManifestError};
pub use ring::{DrainDescriptor, RingError, RingWriteback, ShardPlacement};
pub use store::{ChunkStore, StoreError};

// Re-export the substrate pieces the ring write-back plane reuses, so a consumer
// (the cache node) wires a block flush plane from one crate. The erasure codec,
// the local fragment store, the peer fragment transport, and the live-membership
// view are the SAME machinery the object write-back tier (#180/#286) and the EC
// append log (#372) are built on — this crate adds the block flush write-back
// over them, it re-implements no codec, store, or transport.
pub use verglas_cluster::fragments::LocalFragmentStore;
pub use verglas_writeback::membership::{AgentMembership, LiveMembership, SingleNodeMembership};
pub use verglas_writeback::transport::{FragmentTransport, PeerFragmentTransport, TransportError};
