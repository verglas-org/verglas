//! The block-flush write-back plane: erasure-code a device flush across the
//! cache ring, ack on a quorum, and drain to the origin in the background.
//!
//! # Why the block FLUSH is write-back, not write-through
//!
//! The synchronous flush barrier (see [`crate::device::BlockDevice::flush`] and
//! [`crate::store::ChunkStore::ensure_durable`]) makes every dirty chunk and the
//! manifest durable at the origin *before* the NBD FLUSH is acked. That is correct but
//! pays an origin round-trip on every FLUSH — the same latency the WAL quorum
//! write-back and the Iceberg commit-ack model already remove for their write
//! paths. This plane applies that model to block devices: on FLUSH the dirty set
//! is sealed, erasure-coded across the ring, and the FLUSH is acked once a
//! reconstructable quorum of fragments is fsynced on distinct peers. The drain
//! to the origin runs behind the ack; the ring copies are released only after the origin
//! barrier completes. An acked flush survives any single box loss during the
//! writeback window; the ack latency is a ring RTT, not an origin RTT.
//!
//! # The same substrate as the object write-back tier
//!
//! Nothing here is a parallel mechanism. The Reed-Solomon codec
//! ([`verglas_cache::writeback_codec`]), the local fragment store and peer
//! fragment RPC ([`verglas_cluster::fragments`], the [`FragmentTransport`]), and
//! the live-membership view ([`LiveMembership`]) are the exact machinery the
//! object write-back tier (#180/#286) and the EC append log (#372) are built on.
//! This module adds the block flush write-back — seal, EC, quorum-ack, drain,
//! and cross-node takeover — over them.
//!
//! # Geometry — ring-size aware, topology-driven, no knobs
//!
//! The geometry is derived from the live ring size, never configured. For an
//! `n`-node ring the flush is coded `RS(k = n - 1, m = 1)` and acked once all
//! `n` fragments (one per node) are durable — so a 3-box ring is `RS(2, 1)` and
//! tolerates the loss of any one box after the ack (any `k = 2` of the `3`
//! fragments reconstruct). A one-node ring (dev / free tier) has no peers to
//! code across, so the flush falls through to the existing synchronous origin
//! barrier: that is topology, not a fallback for errors and not a config knob. A
//! multi-node ring that cannot place its quorum right now (a peer is down or
//! full) also falls through to the synchronous barrier — the always-safe path,
//! mirroring the object tier's enforced write-through.
//!
//! # Exactly-once drain and cross-node takeover
//!
//! The unit that is erasure-coded is the whole sealed flush: the target manifest
//! version plus the bytes of every chunk that is not yet durable at the origin. So a
//! peer that reconstructs the fragments holds everything needed to finish the origin
//! barrier exactly as the originating node would — PUT the chunks, commit the
//! manifest version. Both are idempotent (chunks are content-addressed; a
//! manifest version object and its `latest` pointer are rewritten identically),
//! so the committed version is exactly-once regardless of who drains.
//!
//! Alongside its fragment, every shard-holder is given a full copy of a small
//! [`DrainDescriptor`] — the placement list, geometry, and a drain lease. The
//! originating node drains immediately behind the ack and, on success, deletes
//! the fragments and descriptors. If it crashes mid-drain the descriptors
//! outlive it: [`RingWriteback::run_takeover_pass`] on each surviving holder
//! finds a descriptor past its lease, reconstructs the flush, and completes the
//! drain. Duplicate takeovers are harmless because every step is idempotent, so
//! the takeover trigger is deliberately simple: lease expiry, any holder acts.
//! (A failure-detector-driven single-designate takeover is the obvious
//! refinement; it is an extension point, not built here.)
//!
//! # Version ordering — `latest` never regresses
//!
//! A device's flushes are serialized (one attached VM, one device lock), so they
//! ack in version order — but their background drains could still commit the
//! manifest `latest` pointer out of order and regress it. Each device's drains
//! therefore serialize through a per-device drain lock, and under it a drain
//! re-reads the origin's current committed version: a flush at or below what the origin already
//! holds is superseded (a newer flush drained first, or this exact one already
//! ran) and skips the commit, releasing only its ring holds. This keeps `latest`
//! monotonic for a node's own drains and for a takeover after the originator is
//! gone (the peer reads the newer version the origin already holds). The one residual
//! window is two *different* live nodes draining two versions of one device at
//! the same instant — possible only if the originator is alive but stalled past
//! a drain lease — which a conditional-PUT (compare-and-set) on the `latest`
//! object would close; that is an extension point, deferred, and its worst case
//! is a pointer regression with every version's chunks already durable, not data
//! loss.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use verglas_cache::writeback_codec::{Encoded, Fragment, Geometry, encode, reassemble};
use verglas_cluster::fragments::{FragmentKey, FragmentRecord, LocalFragmentStore};
use verglas_core::CacheKey;
use verglas_core::node::NodeId;
use verglas_core::ring::rendezvous_hash;
use verglas_writeback::{FragmentTransport, LiveMembership};

use crate::backend::BackendError;
use crate::chunk::ChunkHash;
use crate::manifest::{Manifest, ManifestError};
use crate::store::{ChunkStore, StoreError};

/// The fragment index the replicated drain descriptor is stored under. Real
/// erasure fragments occupy `0..k+m` (a handful), so this sentinel can never
/// collide with a data or parity fragment. The descriptor is a full copy on
/// each shard-holder — it is what bootstraps a takeover, so it cannot itself be
/// erasure-coded (you need the placement list before you can reassemble).
const DESCRIPTOR_INDEX: usize = usize::MAX;

/// How long after a flush ack the originating node is trusted to complete the
/// drain before any surviving shard-holder may take it over. A fixed constant,
/// not a knob: long enough that a healthy background drain always finishes
/// first, short enough that a crashed originator's flush reaches the origin promptly.
const DEFAULT_DRAIN_LEASE: Duration = Duration::from_secs(30);

/// A ring write-back failure.
#[derive(Debug, thiserror::Error)]
pub enum RingError {
    /// The chunk store (local NVMe or the origin backend) failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// The durable backend failed on a drain PUT.
    #[error(transparent)]
    Backend(#[from] BackendError),
    /// The manifest layer failed committing a drained version.
    #[error(transparent)]
    Manifest(#[from] ManifestError),
    /// The erasure codec rejected an encode, decode, or geometry.
    #[error("ring codec: {0}")]
    Codec(String),
    /// A sealed flush bundle could not be framed or parsed.
    #[error("ring bundle: {0}")]
    Bundle(String),
    /// A drain could not reconstruct the flush from the surviving fragments
    /// (more than the codec's erasure budget lost or corrupt). Fails loudly,
    /// never a partial or fabricated drain.
    #[error("ring drain: {0}")]
    Drain(String),
}

/// Which shard-holder node holds one fragment of a sealed flush.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardPlacement {
    /// Fragment index (`0..k` data, `k..k+m` parity).
    pub index: usize,
    /// The node id holding this fragment.
    pub node: String,
}

/// The replicated descriptor that lets any shard-holder complete a drain.
///
/// Written (a full copy) to every node that holds a fragment of the flush, so a
/// peer can drive the origin barrier if the originator dies. It carries only what a
/// takeover needs: the object id and geometry to reassemble, the placement list
/// to fetch fragments from, and the drain lease.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DrainDescriptor {
    /// Pod-unique id of this sealed flush's fragment set.
    pub object_id: String,
    /// The device this flush belongs to (for diagnostics and object-id shape).
    pub device_id: String,
    /// The manifest version this flush commits. Fixed here, so every drainer
    /// commits exactly this version — the exactly-once anchor.
    pub target_version: u64,
    /// Reed-Solomon data fragments.
    pub k: usize,
    /// Reed-Solomon parity fragments.
    pub m: usize,
    /// Per-fragment stripe chunk size the bundle was coded with.
    pub chunk: usize,
    /// The sealed bundle's length before stripe padding, for reassembly.
    pub bundle_len: u64,
    /// Where each fragment lives.
    pub placements: Vec<ShardPlacement>,
    /// Millisecond Unix time after which a surviving holder may take the drain
    /// over from a presumed-dead originator.
    pub lease_expiry_ms: u64,
    /// Millisecond Unix time the flush was acked.
    pub created_ms: u64,
}

/// The block-flush write-back plane. Cheap to clone-share via `Arc`.
pub struct RingWriteback {
    /// Fragment placement transport (local store + peer RPC), reused verbatim
    /// from the object write-back tier so the same peer server serves flush
    /// fragments.
    transport: Arc<dyn FragmentTransport>,
    /// Live ring view for geometry and quorum.
    membership: Arc<dyn LiveMembership>,
    /// This node's local fragment store, for enumerating descriptors during a
    /// takeover pass (the transport trait cannot list; the store can).
    local: LocalFragmentStore,
    /// The chunk store: local NVMe reads for the sync barrier, and the origin
    /// backend the drain PUTs chunks and commits the manifest to.
    store: ChunkStore,
    /// The drain lease applied to every descriptor.
    lease: Duration,
    /// Per-flush counter feeding object-id uniqueness.
    counter: AtomicU64,
    /// Per-device drain locks. A device's drains serialize through one lock so
    /// two background drains of successive versions never commit the manifest
    /// `latest` pointer out of order — the lock holder re-reads the origin's current
    /// version under the lock and skips a superseded flush. Keyed by device id.
    drain_locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl RingWriteback {
    /// Builds a ring write-back plane over the shared substrate.
    pub fn new(
        transport: Arc<dyn FragmentTransport>,
        membership: Arc<dyn LiveMembership>,
        local: LocalFragmentStore,
        store: ChunkStore,
    ) -> Arc<Self> {
        Arc::new(Self {
            transport,
            membership,
            local,
            store,
            lease: DEFAULT_DRAIN_LEASE,
            counter: AtomicU64::new(0),
            drain_locks: Mutex::new(HashMap::new()),
        })
    }

    /// Builds a plane with an explicit drain lease. For tests that drive the
    /// takeover path without waiting the production lease.
    pub fn with_lease(
        transport: Arc<dyn FragmentTransport>,
        membership: Arc<dyn LiveMembership>,
        local: LocalFragmentStore,
        store: ChunkStore,
        lease: Duration,
    ) -> Arc<Self> {
        Arc::new(Self {
            transport,
            membership,
            local,
            store,
            lease,
            counter: AtomicU64::new(0),
            drain_locks: Mutex::new(HashMap::new()),
        })
    }

    /// The flush entry the device calls with the sealed set: the target manifest
    /// version and the bytes of every chunk not yet durable at the origin. Returns once
    /// the flush is durable — on a ring quorum (ack = ring RTT, drain runs
    /// behind it), or via the synchronous origin barrier for a degenerate or
    /// quorum-short ring. The manifest version is only advanced by the caller
    /// after this returns Ok, so a failed flush never advances the device.
    pub async fn flush(
        self: &Arc<Self>,
        device_id: &str,
        manifest: Manifest,
        dirty: Vec<(ChunkHash, Bytes)>,
    ) -> Result<(), RingError> {
        let live = self.membership.live_nodes();
        // Degenerate ring (single-node deployment) or a ring degraded to one
        // live member: no peers to code across, so the flush IS the synchronous
        // origin barrier. Topology-driven, not an error fallback.
        if self.membership.is_single_node() || live.len() < 2 {
            return self.sync_barrier(&manifest).await;
        }

        let (k, m, w) = ring_geometry(live.len());
        let bundle = encode_bundle(&manifest, &dirty)?;
        let object_id = self.new_object_id(device_id, manifest.version);
        let encoded = encode(k, m, &bundle).map_err(|e| RingError::Codec(e.to_string()))?;

        // Place one fragment per node; the ack needs all `w` durable. A shortfall
        // (a peer down or full) is not an error — clean up and take the
        // always-safe synchronous barrier.
        let Some(placements) = self.place(&object_id, &encoded, w, &live).await else {
            self.release_holds(&object_id, &all_indices(k, m), &live)
                .await;
            return self.sync_barrier(&manifest).await;
        };

        let descriptor = DrainDescriptor {
            object_id: object_id.clone(),
            device_id: device_id.to_owned(),
            target_version: manifest.version,
            k,
            m,
            chunk: encoded.geometry.chunk,
            bundle_len: encoded.object_len,
            placements: placements.clone(),
            lease_expiry_ms: now_ms() + self.lease.as_millis() as u64,
            created_ms: now_ms(),
        };

        // Replicate the descriptor to every shard-holder so a takeover can start
        // from any survivor. If a holder will not take it, the flush cannot be
        // taken over from that node — fall back to the synchronous barrier rather
        // than ack a flush that a single crash could strand.
        if !self.place_descriptor(&descriptor).await {
            self.release_holds(&object_id, &all_indices(k, m), &live)
                .await;
            return self.sync_barrier(&manifest).await;
        }

        // Quorum durable and takeover-covered: this is the ack. Drain to the origin in
        // the background and release the ring holds after the origin barrier.
        let me = Arc::clone(self);
        tokio::spawn(async move {
            if let Err(error) = me.drain(&descriptor).await {
                // The background drain failed (origin down, say). The descriptors
                // stay past their lease, so a takeover pass — on this node or a
                // peer — retries it. Nothing is lost; the flush is already acked
                // and quorum-durable.
                eprintln!(
                    "block ring drain of {} failed: {error}; leaving it for a takeover pass",
                    descriptor.object_id
                );
            }
        });
        Ok(())
    }

    /// The synchronous origin barrier: make every referenced chunk durable, then
    /// commit the manifest version. Byte-identical to the pre-write-back flush,
    /// and the always-safe path for a degenerate or quorum-short ring.
    async fn sync_barrier(&self, manifest: &Manifest) -> Result<(), RingError> {
        let referenced = manifest.referenced_chunks();
        self.store.ensure_durable(&referenced).await?;
        manifest.commit(self.store.backend().as_ref()).await?;
        Ok(())
    }

    /// Places each of `encoded`'s fragments on a distinct live node (rendezvous
    /// order, headroom-filtered), returning the placements once at least `w`
    /// land, or `None` on a shortfall. A shortfall is the caller's cue to fall
    /// back to the synchronous barrier — never a sub-quorum ack.
    async fn place(
        &self,
        object_id: &str,
        encoded: &Encoded,
        w: usize,
        live: &[NodeId],
    ) -> Option<Vec<ShardPlacement>> {
        let probe = encoded
            .fragments
            .first()
            .map_or(0, |f| f.bytes.len() as u64);
        let ordered = placement_order(object_id, live);
        let mut nodes = Vec::with_capacity(ordered.len());
        for node in ordered {
            if self.transport.has_headroom(&node, probe).await {
                nodes.push(node);
            }
        }
        let mut placements = Vec::new();
        for (index, fragment) in encoded.fragments.iter().enumerate() {
            let Some(node) = nodes.get(index) else { break };
            let record = FragmentRecord::new(
                FragmentKey {
                    object_id: object_id.to_owned(),
                    index,
                },
                fragment.bytes.clone(),
            );
            if self.transport.place(node, record).await.is_ok() {
                placements.push(ShardPlacement {
                    index,
                    node: node.as_str().to_owned(),
                });
            }
        }
        if placements.len() < w {
            None
        } else {
            Some(placements)
        }
    }

    /// Replicates the descriptor (a full copy, under the sentinel index) to every
    /// shard-holder. True only when every holder accepted it, so a takeover can
    /// start from any survivor.
    async fn place_descriptor(&self, descriptor: &DrainDescriptor) -> bool {
        let bytes = match encode_descriptor(descriptor) {
            Ok(bytes) => bytes,
            Err(_) => return false,
        };
        for placement in &descriptor.placements {
            let node = NodeId::new(placement.node.as_str());
            let record = FragmentRecord::new(
                FragmentKey {
                    object_id: descriptor.object_id.clone(),
                    index: DESCRIPTOR_INDEX,
                },
                bytes.clone(),
            );
            if self.transport.place(&node, record).await.is_err() {
                return false;
            }
        }
        true
    }

    /// The per-device drain lock, created on first use. Serializes a device's
    /// drains so successive versions never commit `latest` out of order.
    fn device_lock(&self, device_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.drain_locks.lock().unwrap_or_else(|e| e.into_inner());
        Arc::clone(
            locks
                .entry(device_id.to_owned())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
        )
    }

    /// Completes the origin barrier for one sealed flush and releases its ring holds.
    /// Reconstructs the bundle from any `k` surviving fragments, PUTs its chunks
    /// to the origin (idempotent, content-addressed), commits the manifest version
    /// (idempotent), then deletes the fragments and descriptors. Safe to run
    /// concurrently on several holders and to re-run after a partial crash: every
    /// step is idempotent, so the committed version is exactly-once.
    ///
    /// A device's drains serialize through the device lock, and under it the drain
    /// re-reads the origin's current committed version: a flush whose target is at or
    /// below what the origin already holds is superseded (a newer flush drained first, or
    /// this one already ran) — it skips the PUT and commit and just releases its
    /// ring holds, so `latest` never regresses to an older version.
    pub async fn drain(&self, descriptor: &DrainDescriptor) -> Result<(), RingError> {
        let device_lock = self.device_lock(&descriptor.device_id);
        let _guard = device_lock.lock().await;

        // Superseded? Re-read the origin's current version under the device lock.
        let current = Manifest::load_latest(self.store.backend().as_ref(), &descriptor.device_id)
            .await?
            .map(|m| m.version)
            .unwrap_or(0);
        // A real flush targets version >= 1; `current == 0` (or no manifest) means
        // nothing newer is at the origin, so this branch only fires for a genuinely
        // superseded or already-committed flush.
        if descriptor.target_version <= current {
            // A newer flush already reached the origin (or this exact one did): do not
            // regress `latest`. Just release this flush's ring holds.
            self.release_all(descriptor).await;
            return Ok(());
        }

        let bundle = self.reconstruct_bundle(descriptor).await?;
        let (manifest, chunks) = decode_bundle(&bundle)?;
        // PUT the chunks first, then the manifest — the same ordering the
        // synchronous barrier keeps, so a committed manifest never names a
        // non-durable chunk even if the drain is interrupted between steps.
        for (hash, bytes) in &chunks {
            self.store
                .backend()
                .put(&hash.object_key(), bytes.clone())
                .await?;
            self.store.note_durable(hash);
        }
        manifest.commit(self.store.backend().as_ref()).await?;
        // The origin barrier is complete: release the ring copies.
        self.release_all(descriptor).await;
        Ok(())
    }

    /// Releases every ring hold for a sealed flush: its fragments on all live
    /// nodes and its descriptor copy on every shard-holder.
    async fn release_all(&self, descriptor: &DrainDescriptor) {
        let live = self.membership.live_nodes();
        let indices: Vec<usize> = descriptor.placements.iter().map(|p| p.index).collect();
        self.release_holds(&descriptor.object_id, &indices, &live)
            .await;
        self.release_descriptors(descriptor).await;
    }

    /// Reassembles the sealed bundle bytes from any `k` surviving, verified
    /// fragments named in the descriptor. Fails loudly below `k` (never a
    /// fabricated drain).
    async fn reconstruct_bundle(&self, descriptor: &DrainDescriptor) -> Result<Vec<u8>, RingError> {
        let geometry = Geometry {
            k: descriptor.k,
            m: descriptor.m,
            chunk: descriptor.chunk,
        };
        let mut fragments = Vec::new();
        for placement in &descriptor.placements {
            let node = NodeId::new(placement.node.as_str());
            let fkey = FragmentKey {
                object_id: descriptor.object_id.clone(),
                index: placement.index,
            };
            if let Ok(Some(loaded)) = self.transport.load(&node, &fkey).await {
                let fragment = Fragment {
                    index: placement.index,
                    bytes: loaded.bytes,
                    checksum: loaded.checksum,
                };
                if fragment.verify() {
                    fragments.push(fragment);
                }
            }
            if fragments.len() >= descriptor.k {
                break;
            }
        }
        reassemble(geometry, descriptor.bundle_len, &fragments)
            .map(|b| b.to_vec())
            .map_err(|e| RingError::Drain(e.to_string()))
    }

    /// Best-effort deletion of the fragments `indices` for `object_id` on every
    /// live node — the ring-hold release after the origin barrier, and the cleanup
    /// for a quorum-short EC attempt.
    async fn release_holds(&self, object_id: &str, indices: &[usize], live: &[NodeId]) {
        for node in live {
            for &index in indices {
                let _ = self
                    .transport
                    .delete(
                        node,
                        &FragmentKey {
                            object_id: object_id.to_owned(),
                            index,
                        },
                    )
                    .await;
            }
        }
    }

    /// Best-effort deletion of the descriptor copy on every shard-holder.
    async fn release_descriptors(&self, descriptor: &DrainDescriptor) {
        for placement in &descriptor.placements {
            let node = NodeId::new(placement.node.as_str());
            let _ = self
                .transport
                .delete(
                    &node,
                    &FragmentKey {
                        object_id: descriptor.object_id.clone(),
                        index: DESCRIPTOR_INDEX,
                    },
                )
                .await;
        }
    }

    /// One takeover pass over the descriptors this node holds: for every one past
    /// its drain lease, reconstruct the flush and complete the drain the presumed
    /// -dead originator did not. Wired to a background interval by the server (the
    /// same shape as the object tier's repair/scrub loops). Duplicate takeovers
    /// across holders are harmless — the drain is idempotent.
    ///
    /// Descriptors are drained in ascending `(device_id, target_version)` order so
    /// a peer that inherited several pending versions of one device commits them
    /// in order; the drain's superseded check backs this up.
    pub async fn run_takeover_pass(&self) {
        let now = now_ms();
        let mut expired: Vec<DrainDescriptor> = Vec::new();
        for key in self.local.list_fragment_keys() {
            if key.index != DESCRIPTOR_INDEX {
                continue;
            }
            let Ok(Some(loaded)) = self.local.load_fragment(&key) else {
                continue;
            };
            if !loaded.is_healthy() {
                continue;
            }
            let Ok(descriptor) = decode_descriptor(&loaded.bytes) else {
                continue;
            };
            if descriptor.lease_expiry_ms > now {
                continue; // the originator is still within its drain lease
            }
            expired.push(descriptor);
        }
        expired.sort_by(|a, b| {
            a.device_id
                .cmp(&b.device_id)
                .then(a.target_version.cmp(&b.target_version))
        });
        for descriptor in &expired {
            if let Err(error) = self.drain(descriptor).await {
                eprintln!(
                    "block ring takeover drain of {} failed: {error}; will retry next pass",
                    descriptor.object_id
                );
            }
        }
    }

    /// A pod-unique id for one sealed flush: device, target version, a local
    /// counter, and wall-clock nanos so two flushes never share fragment keys.
    fn new_object_id(&self, device_id: &str, version: u64) -> String {
        let seq = self.counter.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        format!("{device_id}/v{version:020}/{seq:016x}{nanos:032x}")
    }
}

/// The ring-size-aware geometry: `RS(n-1, 1)` acked at `w = n`. One fragment per
/// node, tolerate any single node loss after the ack. `n` is assumed `>= 2` (the
/// one-node ring takes the synchronous barrier before this is called).
fn ring_geometry(n: usize) -> (usize, usize, usize) {
    (n - 1, 1, n)
}

/// The fragment indices `0..k+m` a flush of geometry `(k, m)` occupies.
fn all_indices(k: usize, m: usize) -> Vec<usize> {
    (0..k + m).collect()
}

/// Orders `live` nodes by descending rendezvous score over `seed` — a
/// deterministic distinct placement that spreads fragments across flushes. The
/// same rendezvous machinery the cache ring and the object write-back placement
/// use.
fn placement_order(seed: &str, live: &[NodeId]) -> Vec<NodeId> {
    let key = CacheKey {
        storage_binding_id: "default".to_owned(),
        bucket: "block-writeback".to_owned(),
        key: seed.to_owned(),
    };
    let mut scored: Vec<(u64, NodeId)> = live
        .iter()
        .map(|n| (rendezvous_hash(&key, n), n.clone()))
        .collect();
    scored.sort_by(|(sa, na), (sb, nb)| sb.cmp(sa).then_with(|| na.as_str().cmp(nb.as_str())));
    scored.into_iter().map(|(_, n)| n).collect()
}

/// Millisecond Unix time.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Frames a sealed flush into bundle bytes: the target manifest (JSON) followed
/// by each not-yet-durable chunk's raw digest and bytes. Length-prefixed so it
/// decodes without a schema; the whole bundle is then erasure-coded as one unit.
fn encode_bundle(manifest: &Manifest, chunks: &[(ChunkHash, Bytes)]) -> Result<Vec<u8>, RingError> {
    let manifest_json =
        serde_json::to_vec(manifest).map_err(|e| RingError::Bundle(e.to_string()))?;
    let mut out = Vec::with_capacity(manifest_json.len() + 8);
    out.extend_from_slice(&(manifest_json.len() as u32).to_le_bytes());
    out.extend_from_slice(&manifest_json);
    out.extend_from_slice(&(chunks.len() as u32).to_le_bytes());
    for (hash, bytes) in chunks {
        out.extend_from_slice(&hash.to_bytes());
        out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(bytes);
    }
    Ok(out)
}

/// Parses bundle bytes back into the manifest and its chunk set — the inverse of
/// [`encode_bundle`]. A malformed or truncated bundle is a hard error, never a
/// partial decode.
fn decode_bundle(bytes: &[u8]) -> Result<(Manifest, Vec<(ChunkHash, Bytes)>), RingError> {
    let mut cursor = Cursor::new(bytes);
    let manifest_len = cursor.u32()? as usize;
    let manifest_bytes = cursor.take(manifest_len)?;
    let manifest: Manifest =
        serde_json::from_slice(manifest_bytes).map_err(|e| RingError::Bundle(e.to_string()))?;
    let count = cursor.u32()? as usize;
    let mut chunks = Vec::with_capacity(count);
    for _ in 0..count {
        let digest = cursor.take(32)?;
        let mut array = [0u8; 32];
        array.copy_from_slice(digest);
        let chunk_len = cursor.u32()? as usize;
        let chunk_bytes = cursor.take(chunk_len)?;
        chunks.push((
            ChunkHash::from_bytes(array),
            Bytes::copy_from_slice(chunk_bytes),
        ));
    }
    Ok((manifest, chunks))
}

/// Serializes a drain descriptor to JSON for replication to shard-holders.
fn encode_descriptor(descriptor: &DrainDescriptor) -> Result<Bytes, RingError> {
    serde_json::to_vec(descriptor)
        .map(Bytes::from)
        .map_err(|e| RingError::Bundle(e.to_string()))
}

/// Parses a replicated drain descriptor back from its JSON bytes.
fn decode_descriptor(bytes: &[u8]) -> Result<DrainDescriptor, RingError> {
    serde_json::from_slice(bytes).map_err(|e| RingError::Bundle(e.to_string()))
}

/// A tiny forward-only reader over the bundle byte frame, so a truncated bundle
/// is a clean error rather than a panic on an out-of-range slice.
struct Cursor<'a> {
    /// The bundle bytes.
    bytes: &'a [u8],
    /// The read offset.
    pos: usize,
}

impl<'a> Cursor<'a> {
    /// A reader positioned at the start of `bytes`.
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    /// Reads the next little-endian `u32`, erroring on a short bundle.
    fn u32(&mut self) -> Result<u32, RingError> {
        let raw = self.take(4)?;
        Ok(u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]))
    }

    /// Borrows the next `len` bytes, erroring on a short bundle.
    fn take(&mut self, len: usize) -> Result<&'a [u8], RingError> {
        let end = self
            .pos
            .checked_add(len)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| RingError::Bundle("bundle is truncated".to_owned()))?;
        let slice = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(slice)
    }
}
