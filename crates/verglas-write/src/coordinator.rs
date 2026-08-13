//! The write-back coordinator (#180): the per-write quorum-ack decision, the
//! background origin propagation, and node-loss repair.
//!
//! Contract enforced here:
//! - A write is acked over the fragment quorum only when `w` fragments are
//!   durable on distinct live nodes within the ack deadline. Stragglers finish
//!   in the background.
//! - If the live gossip view cannot support `w` distinct nodes, or fan-out
//!   falls short of `w` by the deadline, the write degrades to a synchronous
//!   write-through to the origin. Write-through is full durability, so it is
//!   never a sub-quorum ack and never a rejected write; the write only fails
//!   cleanly when the origin itself is unavailable.
//! - After ack the object is propagated to the origin and its fragments freed.
//! - On node loss, missing fragments are re-encoded from any surviving `k` and
//!   re-placed on live nodes.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use bytes::Bytes;
use futures::{StreamExt, stream};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard, mpsc};
use verglas_cache::writeback_codec::{
    Encoded, Fragment, Geometry, MAX_STRIPE_CHUNK, StreamingStripeEncoder, StripeEncoder,
    reassemble,
};
use verglas_cluster::fragments::{FragmentKey, FragmentRecord};
use verglas_core::CacheKey;
use verglas_core::node::NodeId;
use verglas_core::ring::rendezvous_hash;
use verglas_core::write::{ObjectWrite, PutOutcome, WriteBodyStream, WriteError, WriteMetadata};

use crate::journal::{Journal, JournalState, JournalStore, Placement};
use crate::membership::LiveMembership;
use crate::meta::{StoredMetadata, now_unix_ms};
use crate::metrics::WritebackMetrics;
use crate::transport::{DurableQuorum, FragmentTransport};

/// Base backoff between propagation retries.
const PROPAGATION_BACKOFF: Duration = Duration::from_millis(200);

/// Upper bound on fragment indices swept when cleaning up an abandoned object.
/// No configured geometry places more than this many fragments (k + m), so
/// deleting `0..this` on every live node removes every fragment an object could
/// have. A generous fixed bound keeps cleanup independent of the journal.
const MAX_FRAGMENTS_PER_OBJECT: usize = 256;

/// How the last write was acked, for transition counting.
const MODE_UNSET: u8 = 0;
/// Last write acked over a fragment quorum.
const MODE_QUORUM: u8 = 1;
/// Last write acked via write-through fallback.
const MODE_WRITE_THROUGH: u8 = 2;

/// A coordinator-level error (propagation, repair, reassembly).
#[derive(Debug, thiserror::Error)]
pub enum WritebackError {
    /// A fragment could not be reassembled (fewer than `k` survive).
    #[error("reassembly failed: {0}")]
    Reassembly(String),
    /// The journal store failed.
    #[error(transparent)]
    Journal(#[from] crate::journal::JournalError),
    /// The fragment transport failed.
    #[error(transparent)]
    Transport(#[from] crate::transport::TransportError),
    /// The codec rejected an encode or geometry.
    #[error("codec: {0}")]
    Codec(String),
    /// A durable journal is missing origin-routing identity.
    #[error("invalid journal: {0}")]
    InvalidJournal(String),
}

/// What one scrub pass saw and fixed (#220): fragments verified, found corrupt,
/// and re-encoded. Returned by [`WriteCoordinator::scrub_once`] and surfaced via
/// the write-back counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ScrubReport {
    /// Fragments whose checksum was verified this pass.
    pub scrubbed: u64,
    /// Fragments found present but failing their checksum (bit-rot).
    pub corrupt: u64,
    /// Fragments re-encoded and re-placed (corrupt or missing).
    pub repaired: u64,
}

/// Per-object repair counts, the shared result of verifying and re-encoding one
/// object's fragments (used by both repair and scrub).
#[derive(Debug, Clone, Copy, Default)]
struct ObjectRepair {
    /// Fragments on live nodes whose checksum was verified.
    scrubbed: u64,
    /// Fragments found corrupt (present but failing their checksum).
    corrupt: u64,
    /// Fragments re-encoded and re-placed.
    repaired: u64,
}

/// The write-back coordinator. Cheap to clone-share via `Arc`.
pub struct WriteCoordinator<W: ObjectWrite> {
    /// Fragment placement transport (local + peers).
    transport: Arc<dyn FragmentTransport>,
    /// Live pod view for placement and quorum.
    membership: Arc<dyn LiveMembership>,
    /// Journal store and dirty index.
    journals: Arc<JournalStore>,
    /// Write-back counters.
    metrics: Arc<WritebackMetrics>,
    /// Direct-to-origin writer for propagation and true standalone write-through.
    origin: Arc<W>,
    /// This node's id.
    self_id: NodeId,
    /// Deadline to gather `w` durable fragments before refusing the write.
    ack_deadline: Duration,
    /// Last ack mode, for counting write-back <-> write-through transitions.
    last_mode: AtomicU8,
    /// Per-write counter feeding object-id uniqueness.
    object_counter: AtomicU64,
    /// Serializes origin-visible operations for one object key. A quorum-acked
    /// PUT transfers its guard to propagation, so a later DELETE cannot race
    /// ahead of that PUT and then be resurrected by it.
    key_locks: Mutex<HashMap<CacheKey, Weak<AsyncMutex<()>>>>,
    /// Set by [`shutdown`](Self::shutdown): background propagation retry loops
    /// exit at their next wake instead of acting on a coordinator whose owner
    /// is gone. The journals stay dirty for the next run's recovery replay.
    shutdown: AtomicBool,
}

impl<W: ObjectWrite> WriteCoordinator<W> {
    /// Builds a coordinator.
    pub fn new(
        transport: Arc<dyn FragmentTransport>,
        membership: Arc<dyn LiveMembership>,
        journals: Arc<JournalStore>,
        metrics: Arc<WritebackMetrics>,
        origin: Arc<W>,
        ack_deadline: Duration,
    ) -> Self {
        let self_id = membership.self_id();
        Self {
            transport,
            membership,
            journals,
            metrics,
            origin,
            self_id,
            ack_deadline,
            last_mode: AtomicU8::new(MODE_UNSET),
            object_counter: AtomicU64::new(0),
            key_locks: Mutex::new(HashMap::new()),
            shutdown: AtomicBool::new(false),
        }
    }

    /// Stops background propagation: every in-flight retry loop exits at its
    /// next wake, leaving still-dirty journals for the next run's recovery
    /// replay (exactly what they are for). For server shutdown, and for tests
    /// that simulate process death — a dropped coordinator does not cancel its
    /// spawned tasks (they hold their own `Arc<Self>`), so without this a
    /// "crashed" coordinator's propagation keeps running and races the
    /// restarted one's recovery.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }

    /// The journal store (for the reader wrapper).
    pub fn journals(&self) -> &Arc<JournalStore> {
        &self.journals
    }

    /// The counters (for admin export and tests).
    pub fn metrics(&self) -> &Arc<WritebackMetrics> {
        &self.metrics
    }

    /// Executes a write-back PUT for `key` from a fully-buffered `body`. A thin
    /// wrapper over [`put_stream`](Self::put_stream) for callers (tests, small
    /// objects) that already hold the whole object.
    ///
    /// `k`/`m`/`w` are the resolved geometry for this key's prefix.
    pub async fn put(
        self: &Arc<Self>,
        key: &CacheKey,
        metadata: &WriteMetadata,
        body: Bytes,
        k: usize,
        m: usize,
        w: usize,
    ) -> Result<PutOutcome, WriteError> {
        let body_stream = once_body(body);
        self.put_stream(key, metadata, body_stream, k, m, w).await
    }

    /// Executes a write-back PUT for `key`, streaming `body` into the erasure
    /// encoder so only one stripe is resident regardless of object size. Returns
    /// the ack once the write is durable — on a fragment quorum, or via
    /// write-through fallback.
    ///
    /// The write-back size limit is NVMe headroom, not DRAM: a candidate node is
    /// excluded when its fragment store cannot hold the fragment, and if fewer
    /// than `w` nodes have headroom the write degrades to write-through before
    /// the body is consumed. `k`/`m`/`w` are the resolved geometry.
    pub async fn put_stream(
        self: &Arc<Self>,
        key: &CacheKey,
        metadata: &WriteMetadata,
        body: WriteBodyStream,
        k: usize,
        m: usize,
        w: usize,
    ) -> Result<PutOutcome, WriteError> {
        // Preserve completion order for operations on the same key. The guard
        // is moved into background propagation on a quorum ack; write-through
        // and failed writes release it when this function returns.
        let key_guard = self.key_lock(key).lock_owned().await;
        let live = self.membership.live_nodes();
        // One cache node is one failure domain, not a durability quorum. A
        // standalone deployment therefore acknowledges only after the origin
        // accepts the complete write and never creates dirty EC state.
        if self.membership.is_single_node() {
            return self.write_through_stream(key, metadata, body).await;
        }
        if live.len() < w {
            return Err(quorum_shortfall(w, live.len()));
        }

        let object_id = self.new_object_id(key, 0);
        // Placement nodes, ordered by rendezvous, narrowed to those with room
        // for at least one stripe chunk per fragment. The object length is not
        // yet known (it is streaming), so headroom is probed at the stripe-chunk
        // granularity; a node that fills mid-stream refuses the next shard and
        // counts against quorum. If fewer than `w` nodes have headroom, degrade
        // to write-through before consuming the body.
        let ordered = placement_order(key, &object_id, &live);
        let nodes = self
            .nodes_with_headroom(ordered, MAX_STRIPE_CHUNK as u64)
            .await;
        if nodes.len() < w {
            return Err(quorum_shortfall(w, nodes.len()));
        }

        // Stream-encode the body into per-fragment placement tasks. Each fragment
        // index gets its own shard channel and a task that streams that channel
        // to its node. The encoder feeds each stripe's shards into the channels,
        // so at most one stripe is resident.
        let geometry = match Geometry::streaming(k, m, MAX_STRIPE_CHUNK) {
            Ok(geometry) => geometry,
            Err(error) => return Err(WriteError::Backend(format!("write-back geometry: {error}"))),
        };
        let total = nodes.len().min(geometry.total());
        let placement_nodes: Vec<NodeId> = nodes.into_iter().take(total).collect();

        let mut senders: Vec<Option<mpsc::Sender<Bytes>>> = Vec::with_capacity(total);
        let (done_tx, mut done_rx) = mpsc::unbounded_channel();
        for (index, node) in placement_nodes.iter().cloned().enumerate() {
            // A small bounded channel: back-pressure keeps only a couple of
            // stripes' worth of shards in flight per fragment.
            let (shard_tx, shard_rx) = mpsc::channel::<Bytes>(2);
            senders.push(Some(shard_tx));
            let transport = Arc::clone(&self.transport);
            let done_tx = done_tx.clone();
            let fkey = FragmentKey {
                object_id: object_id.clone(),
                index,
            };
            tokio::spawn(async move {
                let stream = receiver_stream(shard_rx);
                let result = transport.place_stream(&node, fkey, stream).await;
                let _ = done_tx.send((index, node, result));
            });
        }
        drop(done_tx);

        // Drive the streaming encoder from the body. Each completed stripe hands
        // its `total` shards to the matching fragment channel. A send failure
        // means that fragment's task ended (its node refused) — the shard is
        // dropped and that fragment simply will not commit.
        let object_len = match feed_encoder(geometry, body, total, &mut senders).await {
            Ok(len) => len,
            Err(error) => {
                // The client body errored mid-stream: abandon every fragment
                // (dropping the senders ends the placement tasks; uncommitted
                // fragments are cleaned up on the store side) and surface it.
                senders.clear();
                self.drain_placements(&mut done_rx).await;
                self.spawn_object_cleanup(object_id.clone());
                return Err(error);
            }
        };
        // Close every channel so the placement tasks finish and commit.
        senders.clear();

        // Gather committed fragments until `w` land or the deadline passes.
        let deadline = tokio::time::Instant::now() + self.ack_deadline;
        let mut acked = DurableQuorum::new(w);
        loop {
            if acked.reached() {
                break;
            }
            match tokio::time::timeout_at(deadline, done_rx.recv()).await {
                Ok(Some((index, node, result))) => {
                    let _ = acked.record(result.map(|()| Placement {
                        index,
                        node: node.as_str().to_owned(),
                    }));
                }
                Ok(None) => break,
                Err(_) => break,
            }
        }

        let etag = synthetic_object_etag(&object_id, object_len);
        if acked.reached() {
            self.finish_stream_ack(
                key,
                metadata,
                &object_id,
                &etag,
                geometry,
                object_len,
                acked.into_receipts(),
                done_rx,
                key_guard,
            )
            .await
        } else {
            let placed = acked.len();
            self.drain_placements(&mut done_rx).await;
            self.spawn_object_cleanup(object_id);
            Err(quorum_shortfall(w, placed))
        }
    }

    /// Records the dirty journal for the reached quorum (with the exact acked
    /// placements), acks the client, then drains stragglers into the journal
    /// and starts propagation. The streaming path's ack.
    #[allow(clippy::too_many_arguments)]
    async fn finish_stream_ack(
        self: &Arc<Self>,
        key: &CacheKey,
        metadata: &WriteMetadata,
        object_id: &str,
        etag: &str,
        geometry: Geometry,
        object_len: u64,
        acked: Vec<Placement>,
        rx: mpsc::UnboundedReceiver<(usize, NodeId, Result<(), crate::transport::TransportError>)>,
        key_guard: OwnedMutexGuard<()>,
    ) -> Result<PutOutcome, WriteError> {
        let journal = Journal {
            object_id: object_id.to_owned(),
            storage_binding_id: key.storage_binding_id.clone(),
            bucket: key.bucket.clone(),
            key: key.key.clone(),
            metadata: StoredMetadata::from(metadata),
            object_len,
            k: geometry.k,
            m: geometry.m,
            chunk: geometry.chunk,
            etag: etag.to_owned(),
            created_ms: now_unix_ms(),
            state: JournalState::Dirty,
            propagated_ms: None,
            placements: acked,
        };
        self.journals
            .put(&journal)
            .map_err(|e| WriteError::Backend(format!("write-back journal: {e}")))?;

        WritebackMetrics::bump(&self.metrics.acked_via_quorum);
        self.record_mode(MODE_QUORUM);

        // Merge each straggler placement into the current journal (never a bulk
        // overwrite, so a concurrent repair is not clobbered), then propagate.
        let me = Arc::clone(self);
        let object_id = object_id.to_owned();
        tokio::spawn(async move {
            // Keep the key serialized from the start of the accepted PUT until
            // every straggler is recorded and propagation has finished. This is
            // what makes a subsequent
            // successful DELETE final rather than vulnerable to resurrection.
            let _key_guard = key_guard;
            let mut rx = rx;
            while let Some((index, node, result)) = rx.recv().await {
                if result.is_ok() {
                    let _ = me.journals.add_placement(
                        &object_id,
                        Placement {
                            index,
                            node: node.as_str().to_owned(),
                        },
                    );
                }
            }
            me.propagate_locked(object_id).await;
        });

        Ok(PutOutcome {
            e_tag: Some(etag.to_owned()),
            // The write-back path reassembles from fragments and does not carry
            // an origin checksum or version id at ack time (#154/#156); the
            // origin assigns them on propagation.
            checksums: Default::default(),
            version_id: None,
        })
    }

    /// Drains a placement-result channel, ignoring the outcomes. Used to let the
    /// spawned placement tasks finish after the write is abandoned.
    async fn drain_placements(
        &self,
        rx: &mut mpsc::UnboundedReceiver<(
            usize,
            NodeId,
            Result<(), crate::transport::TransportError>,
        )>,
    ) {
        while rx.recv().await.is_some() {}
    }

    /// Best-effort deletion of every fragment stored for `object_id` on every
    /// live node, for cleaning up after an abandoned or shortfall write.
    fn spawn_object_cleanup(&self, object_id: String) {
        let transport = Arc::clone(&self.transport);
        let live = self.membership.live_nodes();
        tokio::spawn(async move {
            for node in live {
                for index in 0..MAX_FRAGMENTS_PER_OBJECT {
                    let fkey = FragmentKey {
                        object_id: object_id.clone(),
                        index,
                    };
                    let _ = transport.delete(&node, &fkey).await;
                }
            }
        });
    }

    /// Filters `ordered` to the nodes whose fragment store can still hold a
    /// `fragment_len`-byte fragment, preserving the rendezvous order. Headroom
    /// is checked in parallel so the placement decision costs one pod-LAN RTT,
    /// not one per candidate.
    async fn nodes_with_headroom(&self, ordered: Vec<NodeId>, fragment_len: u64) -> Vec<NodeId> {
        let checks = ordered.into_iter().map(|node| {
            let transport = Arc::clone(&self.transport);
            async move {
                let ok = transport.has_headroom(&node, fragment_len).await;
                (node, ok)
            }
        });
        futures::future::join_all(checks)
            .await
            .into_iter()
            .filter_map(|(node, ok)| ok.then_some(node))
            .collect()
    }

    /// Synchronous write-through to the origin from a body stream: the degraded,
    /// always-safe path. The body has not been consumed, so it re-streams whole
    /// to the origin.
    async fn write_through_stream(
        &self,
        key: &CacheKey,
        metadata: &WriteMetadata,
        body: WriteBodyStream,
    ) -> Result<PutOutcome, WriteError> {
        WritebackMetrics::bump(&self.metrics.acked_via_write_through);
        self.record_mode(MODE_WRITE_THROUGH);
        self.origin.put(key, metadata.clone(), body).await
    }

    /// Counts a transition when the ack mode changes.
    fn record_mode(&self, mode: u8) {
        let previous = self.last_mode.swap(mode, Ordering::Relaxed);
        if previous != MODE_UNSET && previous != mode {
            WritebackMetrics::bump(&self.metrics.mode_transitions);
            let name = if mode == MODE_QUORUM {
                "quorum write-back"
            } else {
                "write-through"
            };
            eprintln!("writeback: mode transition to {name}");
        }
    }

    /// Returns the per-key operation lock, retaining only weak references so
    /// idle object names do not accumulate forever.
    fn key_lock(&self, key: &CacheKey) -> Arc<AsyncMutex<()>> {
        let mut locks = self
            .key_locks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        locks.retain(|_, lock| lock.strong_count() > 0);
        if let Some(lock) = locks.get(key).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(AsyncMutex::new(()));
        locks.insert(key.clone(), Arc::downgrade(&lock));
        lock
    }

    /// Deletes an object after any earlier quorum-acked PUT for the same key.
    /// The origin delete happens before the dirty journal is discarded, so a
    /// failed delete preserves the acknowledged object for retry/recovery.
    pub async fn delete(&self, key: &CacheKey) -> Result<(), WriteError> {
        let _key_guard = self.key_lock(key).lock_owned().await;
        self.origin.delete(key).await?;

        let Some(object_id) =
            self.journals
                .find_dirty(&key.storage_binding_id, &key.bucket, &key.key)
        else {
            return Ok(());
        };
        let journal = self
            .journals
            .read(&object_id)
            .map_err(|error| WriteError::Backend(format!("write-back journal: {error}")))?;
        self.journals
            .delete(&object_id)
            .map_err(|error| WriteError::Backend(format!("write-back journal: {error}")))?;
        if let Some(journal) = journal {
            for placement in journal.placements {
                let node = NodeId::new(placement.node);
                let fragment = FragmentKey {
                    object_id: object_id.clone(),
                    index: placement.index,
                };
                let _ = self.transport.delete(&node, &fragment).await;
            }
        }
        Ok(())
    }

    /// Propagates one object to the origin until it succeeds or shutdown starts.
    /// The dirty journal and EC fragments remain durable throughout an origin
    /// outage; recovery does not require a process restart.
    pub async fn propagate(self: Arc<Self>, object_id: String) {
        let Some(journal) = self.journals.read(&object_id).ok().flatten() else {
            return;
        };
        let key = CacheKey {
            storage_binding_id: journal.storage_binding_id,
            bucket: journal.bucket,
            key: journal.key,
        };
        let _key_guard = self.key_lock(&key).lock_owned().await;
        self.propagate_locked(object_id).await;
    }

    /// Propagates while the caller owns the per-key operation lock.
    async fn propagate_locked(&self, object_id: String) {
        let mut backoff = PROPAGATION_BACKOFF;
        let mut attempt = 0u64;
        loop {
            if self.shutdown.load(Ordering::SeqCst) {
                return;
            }
            attempt = attempt.saturating_add(1);
            match self.propagate_once(&object_id).await {
                Ok(()) => return,
                Err(error) => {
                    WritebackMetrics::bump(&self.metrics.propagation_failures);
                    eprintln!(
                        "writeback: propagation of {object_id} attempt {} failed: {error}",
                        attempt
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(10));
                }
            }
        }
    }

    /// Reassembles one dirty object, uploads it to the origin, marks the
    /// journal clean, and frees the fragments.
    async fn propagate_once(&self, object_id: &str) -> Result<(), WritebackError> {
        let Some(journal) = self.journals.read(object_id)? else {
            return Ok(());
        };
        if journal.state == JournalState::Clean {
            return Ok(());
        }
        let bytes = self.reassemble(&journal).await?;
        if journal.storage_binding_id.is_empty() {
            return Err(WritebackError::InvalidJournal(format!(
                "{} has no storage binding; run the explicit journal migration",
                journal.object_id
            )));
        }
        let key = CacheKey {
            storage_binding_id: journal.storage_binding_id.clone(),
            bucket: journal.bucket.clone(),
            key: journal.key.clone(),
        };
        let metadata = WriteMetadata::from(&journal.metadata);
        self.origin
            .put(&key, metadata, once_body(bytes))
            .await
            .map_err(|e| WritebackError::Reassembly(format!("origin put: {e}")))?;
        self.journals.mark_clean(object_id)?;
        // Free the fragments; they were the pod's only copy and the origin now
        // has the object. Best-effort — a leftover fragment is harmless.
        for placement in &journal.placements {
            let node = NodeId::new(placement.node.as_str());
            let fkey = FragmentKey {
                object_id: object_id.to_owned(),
                index: placement.index,
            };
            let _ = self.transport.delete(&node, &fkey).await;
        }
        let _ = self.journals.delete(object_id);
        WritebackMetrics::bump(&self.metrics.propagated);
        Ok(())
    }

    /// Reassembles the object bytes from any `k` surviving fragments named in
    /// the journal.
    pub async fn reassemble(&self, journal: &Journal) -> Result<Bytes, WritebackError> {
        let geometry = Geometry {
            k: journal.k,
            m: journal.m,
            chunk: journal.chunk,
        };
        // Load fragments, carrying each one's stored checksum into the codec
        // `Fragment` so reassembly verifies it before use (#220). A corrupt
        // fragment is dropped by the codec (treated as an erasure), so we keep
        // loading past `k` until `k` *verified* fragments are in hand.
        let mut fragments = Vec::new();
        for placement in &journal.placements {
            let node = NodeId::new(placement.node.as_str());
            let fkey = FragmentKey {
                object_id: journal.object_id.clone(),
                index: placement.index,
            };
            if let Ok(Some(loaded)) = self.transport.load(&node, &fkey).await {
                let fragment = Fragment {
                    index: placement.index,
                    bytes: loaded.bytes,
                    checksum: loaded.checksum,
                };
                // Skip a fragment that fails its checksum here so we do not stop
                // loading one short: it would be dropped at decode anyway.
                if fragment.verify() {
                    fragments.push(fragment);
                }
            }
            if fragments.len() >= journal.k {
                break;
            }
        }
        reassemble(geometry, journal.object_len, &fragments)
            .map_err(|e| WritebackError::Reassembly(e.to_string()))
    }

    /// Replays every dirty journal on startup: rebuild placement and finish the
    /// origin upload the crashed run did not.
    pub fn resume_propagation(self: &Arc<Self>) {
        for object_id in self.journals.dirty_object_ids() {
            let me = Arc::clone(self);
            tokio::spawn(async move {
                me.propagate(object_id).await;
            });
        }
    }

    /// Repairs every dirty object with a fragment that is lost (on a node the
    /// failure detector no longer considers live) OR corrupt (fails its checksum
    /// on a surviving node): re-encode those fragments from the healthy `k` and
    /// re-place them on fresh live nodes. Returns the number of fragments
    /// re-placed. Wired to gossip by the repair loop.
    ///
    /// Fragments are verified before re-encoding (#220): a survivor that has
    /// bit-rotted is not trusted as clean, so it is treated like a lost fragment
    /// and repaired. The re-encode source is [`Self::reassemble`], which itself
    /// only uses verified fragments, so a corrupt survivor never poisons the
    /// re-encode.
    pub async fn repair_once(&self) -> Result<u64, WritebackError> {
        let live: HashSet<NodeId> = self.membership.live_nodes().into_iter().collect();
        let mut repaired = 0u64;
        for object_id in self.journals.dirty_object_ids() {
            let Some(journal) = self.journals.read(&object_id)? else {
                continue;
            };
            let object = self.repair_object(&object_id, &journal, &live).await?;
            repaired += object.repaired;
        }
        WritebackMetrics::add(&self.metrics.fragments_repaired, repaired);
        Ok(repaired)
    }

    /// One full scrub pass over the dirty journals (#220): verify every stored
    /// fragment's checksum and re-encode any that are corrupt or missing before
    /// the object drops below `k`. This is the durability follow-on to the
    /// per-fragment integrity check — repair fires only on a membership change,
    /// but silent bit-rot has no membership event, so the scrubber walks the
    /// fragments on a schedule to catch it.
    ///
    /// Durability reasoning: a fragment is safe as long as no more than `m` of an
    /// object's `k + m` fragments are lost or corrupt at once. Faults accumulate
    /// independently over time, so the scrub interval must be short enough to
    /// find and repair a fault before enough others accumulate to cross `m`. We
    /// do not prove a formal bound in v1; the assumption is that the configured
    /// interval is far shorter than the mean time for `m` independent faults to
    /// pile up on one object.
    ///
    /// Reuses the same verify-and-re-encode path as [`Self::repair_once`], so a
    /// scrub also repairs node-loss gaps it happens to find.
    pub async fn scrub_once(&self) -> Result<ScrubReport, WritebackError> {
        let live: HashSet<NodeId> = self.membership.live_nodes().into_iter().collect();
        let mut report = ScrubReport::default();
        for object_id in self.journals.dirty_object_ids() {
            let Some(journal) = self.journals.read(&object_id)? else {
                continue;
            };
            let object = self.repair_object(&object_id, &journal, &live).await?;
            report.scrubbed += object.scrubbed;
            report.corrupt += object.corrupt;
            report.repaired += object.repaired;
            // Yield between objects so a long scrub never monopolizes the runtime
            // ahead of organic serving traffic (politeness, like the prefetch
            // executor).
            tokio::task::yield_now().await;
        }
        WritebackMetrics::add(&self.metrics.fragments_scrubbed, report.scrubbed);
        WritebackMetrics::add(&self.metrics.corrupt_fragments_found, report.corrupt);
        WritebackMetrics::add(&self.metrics.fragments_repaired, report.repaired);
        Ok(report)
    }

    /// Verifies one object's fragments and re-encodes the lost or corrupt ones,
    /// returning per-object counts. The shared body behind [`Self::repair_once`]
    /// and [`Self::scrub_once`]: it loads every placement on a live node and
    /// checks its checksum (#220), treats a dead-node, missing, or corrupt
    /// fragment as needing repair, and re-places the repaired fragments on fresh
    /// live nodes from the healthy `k`. Never re-encodes from an unverified
    /// fragment, and never rebuilds when fewer than `k` are healthy.
    async fn repair_object(
        &self,
        object_id: &str,
        journal: &Journal,
        live: &HashSet<NodeId>,
    ) -> Result<ObjectRepair, WritebackError> {
        let mut stats = ObjectRepair::default();
        // Partition placements into healthy survivors (live node + verified
        // checksum) and indices needing repair (dead node OR corrupt).
        let mut healthy: Vec<Placement> = Vec::new();
        let mut to_repair: HashSet<usize> = HashSet::new();
        for placement in &journal.placements {
            let node = NodeId::new(placement.node.as_str());
            if !live.contains(&node) {
                to_repair.insert(placement.index);
                continue;
            }
            let fkey = FragmentKey {
                object_id: object_id.to_owned(),
                index: placement.index,
            };
            // Every fragment on a live node is loaded and checksummed — this is
            // the scrub count.
            stats.scrubbed += 1;
            match self.transport.load(&node, &fkey).await {
                Ok(Some(loaded)) if loaded.is_healthy() => healthy.push(placement.clone()),
                Ok(Some(_)) => {
                    // Present but failing its checksum: bit-rot.
                    stats.corrupt += 1;
                    to_repair.insert(placement.index);
                }
                // Absent on a live node (or a transport error): treat as an
                // erasure needing repair, not as corruption.
                _ => {
                    to_repair.insert(placement.index);
                }
            }
        }
        if to_repair.is_empty() {
            return Ok(stats);
        }
        if healthy.len() < journal.k {
            // Fewer than k healthy fragments: loss/corruption beyond the codec's
            // tolerance. Skip and leave the journal for an operator/alert; do not
            // crash, never rebuild garbage.
            eprintln!(
                "writeback: cannot repair {object_id}: {} of k={} fragments healthy",
                healthy.len(),
                journal.k
            );
            return Ok(stats);
        }

        // Reassemble from the healthy fragments (verified inside), then re-encode
        // with the journal's exact geometry.
        let healthy_journal = Journal {
            placements: healthy.clone(),
            ..journal.clone()
        };
        let bytes = self.reassemble(&healthy_journal).await?;
        let encoded = encode_with_geometry_chunk(journal.k, journal.m, journal.chunk, &bytes)
            .map_err(WritebackError::Codec)?;

        // Candidate nodes: live, not already holding a healthy fragment.
        let occupied: HashSet<String> = healthy.iter().map(|p| p.node.clone()).collect();
        let mut candidates: Vec<NodeId> = placement_order(
            &CacheKey {
                storage_binding_id: journal.storage_binding_id.clone(),
                bucket: journal.bucket.clone(),
                key: journal.key.clone(),
            },
            object_id,
            &live.iter().cloned().collect::<Vec<_>>(),
        )
        .into_iter()
        .filter(|n| !occupied.contains(n.as_str()))
        .collect();

        let mut placements = healthy;
        let mut repair_indices: Vec<usize> = to_repair.into_iter().collect();
        repair_indices.sort_unstable();
        for index in repair_indices {
            let Some(node) = candidates.pop() else {
                break; // no spare live node; partial repair, try again later
            };
            let fragment = &encoded.fragments[index];
            self.transport
                .place(
                    &node,
                    FragmentRecord::new(
                        FragmentKey {
                            object_id: object_id.to_owned(),
                            index,
                        },
                        fragment.bytes.clone(),
                    ),
                )
                .await?;
            placements.push(Placement {
                index,
                node: node.as_str().to_owned(),
            });
            stats.repaired += 1;
        }
        self.journals.update_placements(object_id, placements)?;
        Ok(stats)
    }

    /// Generates a pod-unique object id from node, key, time, size, and a local
    /// counter.
    fn new_object_id(&self, key: &CacheKey, len: usize) -> String {
        let seq = self.object_counter.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let mut buf = Vec::new();
        buf.extend_from_slice(self.self_id.as_str().as_bytes());
        buf.push(0);
        buf.extend_from_slice(key.bucket.as_bytes());
        buf.push(0);
        buf.extend_from_slice(key.key.as_bytes());
        buf.push(0);
        buf.extend_from_slice(&nanos.to_le_bytes());
        buf.extend_from_slice(&seq.to_le_bytes());
        buf.extend_from_slice(&(len as u64).to_le_bytes());
        format!("{:032x}", xxhash_rust::xxh3::xxh3_128(&buf))
    }
}

/// Builds the stable error returned by a strict ring durability boundary.
fn quorum_shortfall(required: usize, durable: usize) -> WriteError {
    WriteError::Backend(format!(
        "write-back quorum requires {required} durable fragments; only {durable} available"
    ))
}

/// Orders `live` nodes by descending rendezvous score over the object, giving a
/// deterministic distinct placement that spreads load across objects.
fn placement_order(key: &CacheKey, object_id: &str, live: &[NodeId]) -> Vec<NodeId> {
    let placement_key = CacheKey {
        storage_binding_id: "default".to_owned(),
        bucket: key.bucket.clone(),
        key: format!("{}#writeback#{object_id}", key.key),
    };
    let mut scored: Vec<(u64, NodeId)> = live
        .iter()
        .map(|n| (rendezvous_hash(&placement_key, n), n.clone()))
        .collect();
    scored.sort_by(|(sa, na), (sb, nb)| sb.cmp(sa).then_with(|| na.as_str().cmp(nb.as_str())));
    scored.into_iter().map(|(_, n)| n).collect()
}

/// Streams `body` through the erasure encoder, sending each completed stripe's
/// shards into the matching fragment channel. Returns the true object length.
/// Only one stripe is resident, so a large object never buffers whole. A send
/// error on a channel means that fragment's placement task has ended (its node
/// refused); the shard is dropped and that fragment simply will not commit.
async fn feed_encoder(
    geometry: Geometry,
    mut body: WriteBodyStream,
    total: usize,
    senders: &mut [Option<mpsc::Sender<Bytes>>],
) -> Result<u64, WriteError> {
    let mut encoder = StreamingStripeEncoder::new(geometry);
    // A queue of stripes' shards produced by the (sync) encoder callback, then
    // sent to the channels between reads. The callback cannot await, so it
    // stages shards here and the loop flushes them.
    let mut pending: Vec<Vec<Bytes>> = Vec::new();
    while let Some(chunk) = body.next().await {
        let chunk = chunk?;
        encoder
            .push(&chunk, |shards: &[Vec<u8>]| {
                pending.push(
                    shards
                        .iter()
                        .take(total)
                        .map(|s| Bytes::copy_from_slice(s))
                        .collect(),
                );
                Ok::<(), verglas_cache::writeback_codec::CodecError>(())
            })
            .map_err(|e| WriteError::Backend(format!("write-back encode: {e}")))?;
        flush_pending(&mut pending, senders).await;
    }
    let object_len = encoder
        .finish(|shards: &[Vec<u8>]| {
            pending.push(
                shards
                    .iter()
                    .take(total)
                    .map(|s| Bytes::copy_from_slice(s))
                    .collect(),
            );
            Ok::<(), verglas_cache::writeback_codec::CodecError>(())
        })
        .map_err(|e| WriteError::Backend(format!("write-back encode: {e}")))?;
    flush_pending(&mut pending, senders).await;
    Ok(object_len)
}

/// Sends each staged stripe's shards to the matching fragment channels. A closed
/// channel (its task ended) drops the shard for that fragment.
async fn flush_pending(pending: &mut Vec<Vec<Bytes>>, senders: &mut [Option<mpsc::Sender<Bytes>>]) {
    for stripe in pending.drain(..) {
        for (index, shard) in stripe.into_iter().enumerate() {
            if let Some(Some(sender)) = senders.get(index)
                && sender.send(shard).await.is_err()
            {
                senders[index] = None;
            }
        }
    }
}

/// A synthetic, opaque ETag for a streamed object (128-bit hash of its id and
/// length, quoted like S3). The body is not buffered, so it hashes the object
/// identity rather than the bytes; the origin assigns the durable ETag on
/// propagation.
fn synthetic_object_etag(object_id: &str, object_len: u64) -> String {
    let mut buf = Vec::with_capacity(object_id.len() + 8);
    buf.extend_from_slice(object_id.as_bytes());
    buf.extend_from_slice(&object_len.to_le_bytes());
    format!("\"{:032x}\"", xxhash_rust::xxh3::xxh3_128(&buf))
}

/// Encodes `body` with a fixed geometry chunk (repair must match the journal's
/// exact chunk so the re-encoded fragments align with the surviving ones).
fn encode_with_geometry_chunk(
    k: usize,
    m: usize,
    chunk: usize,
    body: &[u8],
) -> Result<Encoded, String> {
    let geometry = Geometry::streaming(k, m, chunk).map_err(|e| e.to_string())?;
    let mut encoder = StripeEncoder::new(geometry);
    encoder.push(body).map_err(|e| e.to_string())?;
    encoder.finish().map_err(|e| e.to_string())
}

/// Turns a shard receiver into a boxed byte stream the transport places from.
fn receiver_stream(rx: mpsc::Receiver<Bytes>) -> crate::transport::ShardStream {
    stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|shard| (shard, rx))
    })
    .boxed()
}

/// Wraps `bytes` as a one-chunk write body stream.
fn once_body(bytes: Bytes) -> verglas_core::write::WriteBodyStream {
    use futures::StreamExt;
    stream::once(async move { Ok(bytes) }).boxed()
}
