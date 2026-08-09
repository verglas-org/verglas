//! [`EcAppendLog`]: the erasure-coded, quorum-acked implementation of the
//! [`AppendLog`] contract, built on the same substrate machinery as the object
//! write-back tier (#180/#286) — the codec, the local fragment store, the peer
//! fragment transport, and the live-membership view. This crate adds the
//! ordered, LSN-addressed append log over them; it re-implements none of them.
//!
//! Appends are serialized through one async mutex over the durable manifest:
//! the log is single-writer by contract, and serializing is what makes the
//! assigned LSN order the commit order. The mutex is held across fragment
//! placement so an ack is only ever handed out after its manifest record is
//! fsynced — the durability point.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use verglas_cache::writeback_codec::{Encoded, Fragment, Geometry, encode, reassemble};
use verglas_cluster::fragments::{FragmentKey, FragmentRecord};
use verglas_core::CacheKey;
use verglas_core::node::NodeId;
use verglas_core::read::{ObjectRead, ReadRange};
use verglas_core::ring::rendezvous_hash;
use verglas_core::write::{ObjectWrite, WriteBodyStream, WriteMetadata};

use crate::contract::{
    AppendError, AppendGeometry, AppendLog, Appended, Epoch, Lsn, SafekeeperState,
};
use crate::manifest::{
    AppendEntry, Manifest, ManifestStore, Placement, SegmentEntry, SegmentState,
};

/// Seal a segment and start a new one once the open one reaches this many bytes.
/// A fixed flush-granularity constant, not a tuning knob: the whole tuning
/// surface is the erasure geometry (see the crate contract, §7).
const SEGMENT_TARGET: u64 = 16 * 1024 * 1024;

/// Fragment index reserved for full-copy state descriptors. EC data fragments
/// occupy the small `0..k+m` range, so this cannot collide with WAL data.
const STATE_DESCRIPTOR_INDEX: usize = usize::MAX - 1;

/// Fragment index reserved for the replicated pointer to the latest committed
/// state descriptor.
const STATE_HEAD_INDEX: usize = usize::MAX;

/// The erasure-coded quorum append log. `S` is the S3 origin, used for the flush
/// write and for reading already-flushed ranges back; it is never on the append
/// (commit) path.
pub struct EcAppendLog<S> {
    /// Stable identity of the safekeeper whose state this log represents.
    /// Safekeepers advance and flush independently, so their replicated state
    /// keys must not collide even though they receive the same WAL stream.
    node_id: u64,
    /// Backend binding used for durable WAL objects.
    storage_binding_id: String,
    /// The S3 origin: flush target and flushed-range read source.
    store: Arc<S>,
    /// The bucket flushed segment objects live in.
    bucket: String,
    /// The key prefix flushed segment objects are written under.
    prefix: String,
    /// Fragment placement transport (local + peers) — reused verbatim from the
    /// write-back tier, so the same peer server serves append fragments.
    transport: Arc<dyn crate::FragmentTransport>,
    /// Live pod view for placement and quorum.
    membership: Arc<dyn crate::LiveMembership>,
    /// The configured multi-node geometry. A single-node deployment overrides it
    /// to the degenerate `(1, 0, 1)` per append.
    geometry: AppendGeometry,
    /// The durable manifest on disk.
    manifest_store: ManifestStore,
    /// The in-memory manifest, guarded so appends serialize.
    state: Mutex<Manifest>,
    /// Lock-free mirrors of the watermarks so the sync getters never block the
    /// append mutex. Written under the lock, after each persist.
    tail: AtomicU64,
    /// Mirror of the flush watermark.
    flushed: AtomicU64,
    /// Mirror of the writer epoch.
    epoch: AtomicU64,
}

impl<S> EcAppendLog<S>
where
    S: ObjectRead + ObjectWrite,
{
    /// Opens an append log, recovering any durable manifest under `dir` (so a
    /// restart rebuilds the tail and the flush watermark) or starting an empty
    /// log. `geometry` is the multi-node erasure geometry; a single-node
    /// deployment ignores it and runs `(1, 0, 1)`.
    // Open takes the full append-plane identity (node, store, ring, geometry).
    // Bundling would only rename the same eight inputs.
    #[allow(clippy::too_many_arguments)]
    pub fn open(
        node_id: u64,
        storage_binding_id: impl Into<String>,
        store: Arc<S>,
        bucket: impl Into<String>,
        prefix: impl Into<String>,
        transport: Arc<dyn crate::FragmentTransport>,
        membership: Arc<dyn crate::LiveMembership>,
        dir: impl AsRef<Path>,
        geometry: AppendGeometry,
    ) -> Result<Self, AppendError> {
        let (manifest_store, mut manifest) = ManifestStore::open(dir)?;
        // Older builds retained per-append fragment placements even after the
        // complete segment was durable in origin and those fragments had been
        // deleted. Compact that legacy state before serving so the first new
        // descriptor does not republish an unbounded historical payload.
        let mut compacted = false;
        for segment in &mut manifest.segments {
            if segment.state == SegmentState::Flushed && !segment.appends.is_empty() {
                segment.appends.clear();
                compacted = true;
            }
        }
        if compacted {
            manifest_store.persist(&manifest)?;
        }
        let tail = AtomicU64::new(manifest.tail.0);
        let flushed = AtomicU64::new(manifest.flushed_through.0);
        let epoch = AtomicU64::new(manifest.epoch.0);
        Ok(Self {
            node_id,
            storage_binding_id: storage_binding_id.into(),
            store,
            bucket: bucket.into(),
            prefix: prefix.into(),
            transport,
            membership,
            geometry,
            manifest_store,
            state: Mutex::new(manifest),
            tail,
            flushed,
            epoch,
        })
    }

    /// The geometry this append uses: the degenerate single-node code for a
    /// one-node deployment, the configured geometry otherwise. Resolved per
    /// append so a membership change takes effect immediately.
    fn effective_geometry(&self) -> AppendGeometry {
        if self.membership.is_single_node() {
            AppendGeometry { k: 1, m: 0, w: 1 }
        } else {
            self.geometry
        }
    }

    /// Initializes an empty timeline at the base LSN created by the pageserver.
    /// Neon normally supplies this through its safekeeper management API before
    /// compute starts. Without it, walproposer sees `0/0`, attempts to fetch the
    /// initdb WAL prefix from an empty donor, and can never finish bootstrap.
    pub async fn initialize_timeline(&self, start_lsn: Lsn) -> Result<bool, AppendError> {
        if start_lsn == Lsn(0) {
            return Err(AppendError::Manifest(
                "timeline start LSN must not be 0/0".to_owned(),
            ));
        }
        let mut manifest = self.state.lock().await;
        if manifest.tail != Lsn(0) {
            // A compute wake restores the pageserver at its durable LSN and
            // repeats TIMELINE_CREATE before reconnecting walproposer. Treat
            // that as idempotent when the requested point is already covered
            // by this safekeeper. Never jump across WAL we do not have, and
            // never resurrect WAL below the retained range.
            if start_lsn.0 >= manifest.base.0 && start_lsn.0 <= manifest.tail.0 {
                return Ok(false);
            }
            return Err(AppendError::Manifest(format!(
                "timeline retains {}..{}, cannot initialize at {start_lsn}",
                manifest.base, manifest.tail,
            )));
        }
        manifest.base = start_lsn;
        manifest.tail = start_lsn;
        manifest.flushed_through = start_lsn;
        manifest.commit_lsn = start_lsn;
        manifest.truncate_lsn = start_lsn;
        manifest.epoch = Epoch(1);
        manifest.term_history = vec![(1, start_lsn)];
        manifest.revision = manifest.revision.saturating_add(1);
        self.replicate_state(&manifest).await?;
        self.manifest_store.persist(&manifest)?;
        self.tail.store(start_lsn.0, Ordering::Relaxed);
        self.flushed.store(start_lsn.0, Ordering::Relaxed);
        self.epoch.store(1, Ordering::Relaxed);
        Ok(true)
    }

    /// Stable object-id prefix for this timeline's replicated state records.
    fn state_prefix(&self) -> String {
        let mut digest = Sha256::new();
        digest.update(self.bucket.as_bytes());
        digest.update([0]);
        digest.update(self.prefix.trim_matches('/').as_bytes());
        digest.update([0]);
        digest.update(self.node_id.to_be_bytes());
        format!("sk/{:x}", digest.finalize())
    }

    /// Object id holding a full immutable copy of one manifest revision.
    fn state_object_id(&self, revision: u64) -> String {
        format!("{}/state/{revision:020}", self.state_prefix())
    }

    /// Stable object id whose payload is the latest committed revision number.
    fn head_object_id(&self) -> String {
        format!("{}/head", self.state_prefix())
    }

    /// Replicates an immutable manifest revision and then publishes its head.
    /// A revision is published only after its full descriptor reaches `w`
    /// distinct nodes.
    async fn replicate_state(&self, manifest: &Manifest) -> Result<(), AppendError> {
        let live = self.membership.live_nodes();
        let geometry = self.effective_geometry();
        let descriptor = serde_json::to_vec(manifest)
            .map(Bytes::from)
            .map_err(|error| AppendError::Manifest(format!("encode ring state: {error}")))?;
        let descriptor_key = FragmentKey {
            object_id: self.state_object_id(manifest.revision),
            index: STATE_DESCRIPTOR_INDEX,
        };
        let descriptor_record = FragmentRecord::new(descriptor_key, descriptor);
        let mut descriptor_nodes = Vec::new();
        for node in &live {
            match self.transport.place(node, descriptor_record.clone()).await {
                Ok(()) => descriptor_nodes.push(node.clone()),
                Err(error) => tracing::warn!(
                    node = node.as_str(),
                    revision = manifest.revision,
                    %error,
                    "failed to replicate safekeeper descriptor"
                ),
            }
        }
        if descriptor_nodes.len() < geometry.w {
            return Err(AppendError::QuorumUnavailable {
                needed: geometry.w,
                placed: descriptor_nodes.len(),
            });
        }

        let head_record = FragmentRecord::new(
            FragmentKey {
                object_id: self.head_object_id(),
                index: STATE_HEAD_INDEX,
            },
            Bytes::copy_from_slice(&manifest.revision.to_be_bytes()),
        );
        let mut heads = 0;
        for node in &descriptor_nodes {
            match self.transport.place(node, head_record.clone()).await {
                Ok(()) => heads += 1,
                Err(error) => tracing::warn!(
                    node = node.as_str(),
                    revision = manifest.revision,
                    %error,
                    "failed to publish safekeeper state head"
                ),
            }
        }
        if heads < geometry.w {
            return Err(AppendError::QuorumUnavailable {
                needed: geometry.w,
                placed: heads,
            });
        }
        Ok(())
    }

    /// Recovers the newest ring-committed state visible through live peers. A
    /// head is only a discovery pointer; one checksum-valid survivor of the
    /// descriptor is sufficient because publishing that head required the full
    /// descriptor to have reached the configured quorum first.
    pub async fn recover_from_ring(&self) -> Result<bool, AppendError> {
        let live = self.membership.live_nodes();
        let head_key = FragmentKey {
            object_id: self.head_object_id(),
            index: STATE_HEAD_INDEX,
        };
        let mut latest = None;
        for node in &live {
            if let Ok(Some(head)) = self.transport.load(node, &head_key).await
                && head.is_healthy()
                && head.bytes.len() == 8
            {
                let mut raw = [0_u8; 8];
                raw.copy_from_slice(&head.bytes);
                let revision = u64::from_be_bytes(raw);
                latest = Some(latest.map_or(revision, |seen: u64| seen.max(revision)));
            }
        }
        let Some(revision) = latest else {
            return Ok(false);
        };
        let descriptor_key = FragmentKey {
            object_id: self.state_object_id(revision),
            index: STATE_DESCRIPTOR_INDEX,
        };
        let mut candidates: std::collections::HashMap<Vec<u8>, usize> =
            std::collections::HashMap::new();
        for node in &live {
            if let Ok(Some(descriptor)) = self.transport.load(node, &descriptor_key).await
                && descriptor.is_healthy()
            {
                *candidates.entry(descriptor.bytes.to_vec()).or_default() += 1;
            }
        }
        let bytes = candidates
            .into_iter()
            .filter(|(_, count)| *count >= 1)
            .map(|(bytes, _)| bytes)
            .next()
            .ok_or_else(|| {
                AppendError::Manifest(format!(
                    "state revision {revision} is unavailable on every live peer"
                ))
            })?;
        let recovered: Manifest = serde_json::from_slice(&bytes)
            .map_err(|error| AppendError::Manifest(format!("decode ring state: {error}")))?;
        if recovered.revision != revision {
            return Err(AppendError::Manifest(format!(
                "state head {revision} points at revision {}",
                recovered.revision
            )));
        }
        let mut manifest = self.state.lock().await;
        if recovered.revision <= manifest.revision {
            return Ok(false);
        }
        self.manifest_store.persist(&recovered)?;
        self.tail.store(recovered.tail.0, Ordering::Relaxed);
        self.flushed
            .store(recovered.flushed_through.0, Ordering::Relaxed);
        self.epoch.store(recovered.epoch.0, Ordering::Relaxed);
        *manifest = recovered;
        Ok(true)
    }

    /// Returns the persisted Neon acceptor state for greeting, voting, and WAL
    /// responses.
    pub async fn safekeeper_state(&self) -> SafekeeperState {
        let manifest = self.state.lock().await;
        SafekeeperState {
            system_id: manifest.system_id,
            pg_version: manifest.pg_version,
            wal_segment_size: manifest.wal_segment_size,
            generation: manifest.generation,
            term: manifest.epoch.0,
            flush_lsn: manifest.tail,
            commit_lsn: manifest.commit_lsn,
            truncate_lsn: manifest.truncate_lsn,
            backup_lsn: manifest.flushed_through,
            remote_consistent_lsn: manifest.remote_consistent_lsn,
            local_start_lsn: manifest.base,
            term_history: manifest.term_history.clone(),
        }
    }

    /// Persists the timeline identity and PostgreSQL server properties carried
    /// by the walproposer greeting. Repeated identical greetings are free.
    pub async fn configure_timeline(
        &self,
        generation: u32,
        system_id: u64,
        pg_version: u32,
        wal_segment_size: u32,
    ) -> Result<(), AppendError> {
        let mut manifest = self.state.lock().await;
        // Neon uses zero while a compute is synchronizing from a safekeeper and
        // has not recovered PostgreSQL's control file yet. Treat that value as
        // unspecified once this timeline has a durable identity; overwriting or
        // rejecting the recovered identity makes every scale-to-zero wake fail.
        // A conflicting nonzero identity remains a hard fencing error.
        let effective_system_id = if system_id == 0 {
            manifest.system_id
        } else {
            system_id
        };
        if manifest.system_id != 0 && manifest.system_id != effective_system_id {
            return Err(AppendError::Manifest(format!(
                "timeline system id changed from {} to {system_id}",
                manifest.system_id
            )));
        }
        if generation < manifest.generation {
            return Err(AppendError::Manifest(format!(
                "stale membership generation {generation}; current is {}",
                manifest.generation
            )));
        }
        if manifest.generation == generation
            && manifest.system_id == effective_system_id
            && manifest.pg_version == pg_version
            && manifest.wal_segment_size == wal_segment_size
        {
            return Ok(());
        }
        manifest.generation = generation;
        manifest.system_id = effective_system_id;
        manifest.pg_version = pg_version;
        manifest.wal_segment_size = wal_segment_size;
        manifest.revision = manifest.revision.saturating_add(1);
        self.replicate_state(&manifest).await?;
        self.manifest_store.persist(&manifest)
    }

    /// Persists an election vote. A stale term or generation is refused without
    /// changing state; an equal vote is idempotent.
    pub async fn accept_vote(&self, generation: u32, term: u64) -> Result<bool, AppendError> {
        let mut manifest = self.state.lock().await;
        if generation < manifest.generation || term < manifest.epoch.0 {
            return Ok(false);
        }
        if generation == manifest.generation && term == manifest.epoch.0 {
            return Ok(true);
        }
        manifest.generation = generation;
        manifest.epoch = Epoch(term);
        manifest.revision = manifest.revision.saturating_add(1);
        self.replicate_state(&manifest).await?;
        self.manifest_store.persist(&manifest)?;
        self.epoch.store(term, Ordering::Relaxed);
        Ok(true)
    }

    /// Installs the elected proposer's term history and streaming boundary.
    pub async fn announce_elected(
        &self,
        generation: u32,
        term: u64,
        start_streaming_at: Lsn,
        term_history: Vec<(u64, Lsn)>,
    ) -> Result<(), AppendError> {
        let mut manifest = self.state.lock().await;
        if generation != manifest.generation || term != manifest.epoch.0 {
            return Err(AppendError::Fenced {
                current: manifest.epoch,
                presented: Epoch(term),
            });
        }
        if start_streaming_at.0 > manifest.tail.0 && manifest.tail != Lsn(0) {
            return Err(AppendError::WalGap {
                expected: manifest.tail,
                presented: start_streaming_at,
            });
        }
        manifest.term_history = term_history;
        manifest.revision = manifest.revision.saturating_add(1);
        self.replicate_state(&manifest).await?;
        self.manifest_store.persist(&manifest)
    }

    /// Advances Neon's commit and truncation watermarks after a durable append.
    pub async fn record_watermarks(
        &self,
        commit_lsn: Lsn,
        truncate_lsn: Lsn,
    ) -> Result<SafekeeperState, AppendError> {
        let mut manifest = self.state.lock().await;
        let commit_lsn = Lsn(commit_lsn.0.min(manifest.tail.0));
        let truncate_lsn = Lsn(truncate_lsn.0.min(manifest.tail.0));
        if commit_lsn.0 > manifest.commit_lsn.0 || truncate_lsn.0 > manifest.truncate_lsn.0 {
            manifest.commit_lsn = Lsn(manifest.commit_lsn.0.max(commit_lsn.0));
            manifest.truncate_lsn = Lsn(manifest.truncate_lsn.0.max(truncate_lsn.0));
            manifest.revision = manifest.revision.saturating_add(1);
            self.replicate_state(&manifest).await?;
            self.manifest_store.persist(&manifest)?;
        }
        Ok(SafekeeperState {
            system_id: manifest.system_id,
            pg_version: manifest.pg_version,
            wal_segment_size: manifest.wal_segment_size,
            generation: manifest.generation,
            term: manifest.epoch.0,
            flush_lsn: manifest.tail,
            commit_lsn: manifest.commit_lsn,
            truncate_lsn: manifest.truncate_lsn,
            backup_lsn: manifest.flushed_through,
            remote_consistent_lsn: manifest.remote_consistent_lsn,
            local_start_lsn: manifest.base,
            term_history: manifest.term_history.clone(),
        })
    }

    /// Records pageserver durability feedback. WAL deletion is never driven by
    /// walproposer's peer horizon alone: the pageserver must first confirm that
    /// the same prefix is durable in its remote storage.
    pub async fn record_remote_consistent_lsn(&self, lsn: Lsn) -> Result<(), AppendError> {
        if lsn == Lsn(0) {
            return Ok(());
        }
        let mut manifest = self.state.lock().await;
        let lsn = Lsn(lsn.0.min(manifest.tail.0));
        if lsn.0 <= manifest.remote_consistent_lsn.0 {
            return Ok(());
        }
        manifest.remote_consistent_lsn = lsn;
        manifest.revision = manifest.revision.saturating_add(1);
        self.replicate_state(&manifest).await?;
        self.manifest_store.persist(&manifest)
    }

    /// Encodes `records` and places its fragments on distinct live nodes,
    /// returning the placements once at least `w` are durable. Fewer than `w`
    /// is not a durable ack: the partial placements are cleaned up and the
    /// append fails, never a sub-quorum ack.
    async fn place(
        &self,
        object_id: &str,
        encoded: &Encoded,
        w: usize,
        live: &[NodeId],
    ) -> Result<Vec<Placement>, AppendError> {
        let probe = encoded
            .fragments
            .first()
            .map_or(0, |f| f.bytes.len() as u64);
        let ordered = placement_order(object_id, live);
        let mut nodes = Vec::with_capacity(ordered.len());
        for node in ordered {
            if self.transport.has_headroom(&node, probe).await {
                nodes.push(node);
            } else {
                tracing::warn!(
                    node = node.as_str(),
                    bytes = probe,
                    %object_id,
                    "safekeeper fragment holder has no reachable headroom"
                );
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
            match self.transport.place(node, record).await {
                Ok(()) => placements.push(Placement {
                    index,
                    node: node.as_str().to_owned(),
                }),
                Err(error) => tracing::warn!(
                    node = node.as_str(),
                    index,
                    %object_id,
                    %error,
                    "failed to place safekeeper WAL fragment"
                ),
            }
        }

        if placements.len() < w {
            self.drop_fragments(object_id, &placements).await;
            return Err(AppendError::QuorumUnavailable {
                needed: w,
                placed: placements.len(),
            });
        }
        Ok(placements)
    }

    /// Best-effort deletion of the fragments named by `placements` for
    /// `object_id`, for cleaning up a failed append or a flushed segment.
    async fn drop_fragments(&self, object_id: &str, placements: &[Placement]) {
        for placement in placements {
            let node = NodeId::new(placement.node.as_str());
            let _ = self
                .transport
                .delete(
                    &node,
                    &FragmentKey {
                        object_id: object_id.to_owned(),
                        index: placement.index,
                    },
                )
                .await;
        }
    }

    /// Reassembles one append's bytes from any `k` surviving, verified
    /// fragments. Tolerates the loss or corruption of up to the codec's erasure
    /// budget; fails loudly below `k`.
    async fn reassemble_append(&self, entry: &AppendEntry) -> Result<Bytes, AppendError> {
        let object_id = entry.object_id().to_owned();
        let geometry = Geometry {
            k: entry.k,
            m: entry.m,
            chunk: entry.chunk,
        };
        let mut fragments = Vec::new();
        for placement in &entry.placements {
            let node = NodeId::new(placement.node.as_str());
            let fkey = FragmentKey {
                object_id: object_id.clone(),
                index: placement.index,
            };
            if let Ok(Some(loaded)) = self.transport.load(&node, &fkey).await {
                let fragment = Fragment {
                    index: placement.index,
                    bytes: loaded.bytes,
                    checksum: loaded.checksum,
                };
                // Verify before use: a corrupt fragment is an erasure, not data.
                if fragment.verify() {
                    fragments.push(fragment);
                }
            }
            if fragments.len() >= entry.k {
                break;
            }
        }
        reassemble(geometry, entry.object_len, &fragments)
            .map_err(|e| AppendError::Reassembly(e.to_string()))
    }

    /// Reassembles a whole un-flushed segment: its appends concatenated in order.
    async fn reassemble_segment(&self, segment: &SegmentEntry) -> Result<Bytes, AppendError> {
        let mut out = Vec::new();
        for entry in &segment.appends {
            let bytes = self.reassemble_append(entry).await?;
            out.extend_from_slice(&bytes);
        }
        Ok(Bytes::from(out))
    }

    /// Reads a flushed segment's bytes back from its S3 object.
    async fn read_flushed_segment(&self, segment: &SegmentEntry) -> Result<Bytes, AppendError> {
        let s3_key = segment
            .s3_key
            .clone()
            .ok_or_else(|| AppendError::Origin("flushed segment has no S3 key".to_owned()))?;
        let key = CacheKey {
            storage_binding_id: self.storage_binding_id.clone(),
            bucket: self.bucket.clone(),
            key: s3_key,
        };
        let got = self
            .store
            .get(&key, ReadRange::Full)
            .await
            .map_err(|e| AppendError::Origin(e.to_string()))?;
        let mut buf = Vec::new();
        let mut body = got.body;
        while let Some(chunk) = body.next().await {
            buf.extend_from_slice(&chunk.map_err(|e| AppendError::Origin(e.to_string()))?);
        }
        Ok(Bytes::from(buf))
    }

    /// Reads a live range using one already-locked manifest snapshot. Keeping
    /// this separate lets append validate reconnect overlap while it owns the
    /// serialization lock, so no concurrent append can move the tail between
    /// validation and suffix placement.
    async fn read_manifest(
        &self,
        manifest: &Manifest,
        from: Lsn,
        to: Lsn,
    ) -> Result<Bytes, AppendError> {
        if to.0 < from.0 || from.0 < manifest.base.0 || to.0 > manifest.tail.0 {
            return Err(AppendError::OutOfRange { from, to });
        }
        if from == to {
            return Ok(Bytes::new());
        }
        let mut out = Vec::with_capacity((to.0 - from.0) as usize);
        for segment in &manifest.segments {
            if segment.end.0 <= from.0 || segment.start.0 >= to.0 {
                continue;
            }
            let bytes = match segment.state {
                SegmentState::Flushed => self.read_flushed_segment(segment).await?,
                SegmentState::Open => self.reassemble_segment(segment).await?,
            };
            let lo = from.0.max(segment.start.0);
            let hi = to.0.min(segment.end.0);
            let start = (lo - segment.start.0) as usize;
            let end = (hi - segment.start.0) as usize;
            out.extend_from_slice(&bytes[start..end]);
        }
        Ok(Bytes::from(out))
    }

    /// The deterministic S3 key a segment flushes to. Deterministic so a re-flush
    /// after a crash between the S3 put and the manifest persist overwrites the
    /// same object rather than orphaning one.
    fn segment_key(&self, segment: &SegmentEntry) -> String {
        format!(
            "{}/seg-{:020}-{}-{}.wal",
            self.prefix.trim_end_matches('/'),
            segment.id,
            segment.start.0,
            segment.end.0
        )
    }
}

/// Orders `live` nodes by descending rendezvous score over `seed`, a
/// deterministic distinct placement that spreads fragments across appends. Same
/// rendezvous machinery the cache ring and the write-back placement use.
fn placement_order(seed: &str, live: &[NodeId]) -> Vec<NodeId> {
    let key = CacheKey {
        storage_binding_id: "safekeeper-placement".to_owned(),
        bucket: "safekeeper".to_owned(),
        key: seed.to_owned(),
    };
    let mut scored: Vec<(u64, NodeId)> = live
        .iter()
        .map(|n| (rendezvous_hash(&key, n), n.clone()))
        .collect();
    scored.sort_by(|(sa, na), (sb, nb)| sb.cmp(sa).then_with(|| na.as_str().cmp(nb.as_str())));
    scored.into_iter().map(|(_, n)| n).collect()
}

/// Ensures the manifest has an open tail segment with room, creating a new one
/// when the last is flushed or has reached the target size. Returns its index.
fn ensure_tail_segment(manifest: &mut Manifest, start: Lsn) -> usize {
    let reuse = matches!(manifest.segments.last(), Some(seg)
        if seg.state == SegmentState::Open && (seg.end.0 - seg.start.0) < SEGMENT_TARGET);
    if !reuse {
        let id = manifest.next_segment_id;
        manifest.next_segment_id += 1;
        manifest.segments.push(SegmentEntry {
            id,
            start,
            end: start,
            state: SegmentState::Open,
            s3_key: None,
            appends: Vec::new(),
        });
    }
    manifest.segments.len() - 1
}

/// Wraps `bytes` as a one-chunk write body stream.
fn once_body(bytes: Bytes) -> WriteBodyStream {
    futures::stream::once(async move { Ok(bytes) }).boxed()
}

#[async_trait]
impl<S> AppendLog for EcAppendLog<S>
where
    S: ObjectRead + ObjectWrite,
{
    async fn append(
        &self,
        epoch: Epoch,
        begin_lsn: Lsn,
        records: Bytes,
    ) -> Result<Appended, AppendError> {
        let mut manifest = self.state.lock().await;
        if epoch != manifest.epoch {
            return Err(AppendError::Fenced {
                current: manifest.epoch,
                presented: epoch,
            });
        }
        let end_lsn =
            Lsn(begin_lsn
                .0
                .checked_add(records.len() as u64)
                .ok_or(AppendError::LsnOverflow {
                    begin: begin_lsn,
                    bytes: records.len(),
                })?);
        let fresh = manifest.segments.is_empty()
            && manifest.base == Lsn(0)
            && manifest.tail == Lsn(0)
            && manifest.flushed_through == Lsn(0);
        let durable_tail = if fresh { begin_lsn } else { manifest.tail };
        if begin_lsn.0 > durable_tail.0 {
            return Err(AppendError::WalGap {
                expected: durable_tail,
                presented: begin_lsn,
            });
        }
        if begin_lsn.0 < manifest.base.0 {
            return Err(AppendError::OutOfRange {
                from: begin_lsn,
                to: end_lsn,
            });
        }

        let overlap_end = Lsn(end_lsn.0.min(durable_tail.0));
        if overlap_end.0 > begin_lsn.0 {
            let existing = self
                .read_manifest(&manifest, begin_lsn, overlap_end)
                .await?;
            let overlap_len = existing.len();
            if let Some(offset) = existing
                .iter()
                .zip(records[..overlap_len].iter())
                .position(|(stored, proposed)| stored != proposed)
            {
                return Err(AppendError::ConflictingWal {
                    at: begin_lsn.advance(offset as u64),
                });
            }
        }
        if end_lsn.0 <= durable_tail.0 {
            return Ok(Appended {
                start: begin_lsn,
                end: end_lsn,
            });
        }

        let suffix_offset = (durable_tail.0 - begin_lsn.0) as usize;
        let suffix = records.slice(suffix_offset..);
        let start = durable_tail;
        let end = end_lsn;

        let previous = manifest.clone();
        let idx = ensure_tail_segment(&mut manifest, start);
        let (segment_id, seq) = {
            let seg = &manifest.segments[idx];
            (seg.id, seg.appends.len() as u64)
        };
        let object_id = format!("{}/w/{segment_id:016x}/{seq:016x}", self.state_prefix());

        let geometry = self.effective_geometry();
        let live = self.membership.live_nodes();
        let encoded = match encode(geometry.k, geometry.m, &suffix) {
            Ok(encoded) => encoded,
            Err(error) => {
                *manifest = previous.clone();
                return Err(AppendError::Codec(error.to_string()));
            }
        };
        let placements = match self.place(&object_id, &encoded, geometry.w, &live).await {
            Ok(placements) => placements,
            Err(error) => {
                *manifest = previous.clone();
                return Err(error);
            }
        };

        let entry = AppendEntry {
            object_id: object_id.clone(),
            seq,
            start,
            end,
            k: geometry.k,
            m: geometry.m,
            chunk: encoded.geometry.chunk,
            object_len: encoded.object_len,
            placements: placements.clone(),
        };
        {
            let seg = &mut manifest.segments[idx];
            seg.appends.push(entry);
            seg.end = end;
        }
        if fresh {
            manifest.base = begin_lsn;
            manifest.flushed_through = begin_lsn;
        }
        manifest.tail = end;
        manifest.revision = manifest.revision.saturating_add(1);

        if let Err(error) = self.replicate_state(&manifest).await {
            *manifest = previous;
            self.drop_fragments(&object_id, &placements).await;
            return Err(error);
        }

        // The ring descriptor is the coordinator-replacement authority. Keep a
        // local fsynced copy as the fast same-node restart path.
        if let Err(error) = self.manifest_store.persist(&manifest) {
            // The fragments are placed but the record is not durable — roll the
            // append back so the tail never reflects an un-fsynced ack, and drop
            // the orphaned fragments.
            *manifest = previous;
            self.drop_fragments(&object_id, &placements).await;
            return Err(error);
        }
        self.tail.store(end.0, Ordering::Relaxed);
        self.flushed
            .store(manifest.flushed_through.0, Ordering::Relaxed);

        Ok(Appended {
            start: begin_lsn,
            end,
        })
    }

    async fn read(&self, from: Lsn, to: Lsn) -> Result<Bytes, AppendError> {
        let manifest = self.state.lock().await;
        self.read_manifest(&manifest, from, to).await
    }

    async fn flush(&self) -> Result<Lsn, AppendError> {
        let mut manifest = self.state.lock().await;
        let mut i = 0;
        while i < manifest.segments.len() {
            if manifest.segments[i].state != SegmentState::Open {
                i += 1;
                continue;
            }
            let segment = manifest.segments[i].clone();
            // Reassemble the segment and write it to S3. Only after S3 confirms
            // it durable do we mark it flushed and drop the local fragments —
            // S3 first, then drop, so the buffer is never the sole copy of bytes
            // it has forgotten.
            let bytes = self.reassemble_segment(&segment).await?;
            let s3_key = self.segment_key(&segment);
            let key = CacheKey {
                storage_binding_id: self.storage_binding_id.clone(),
                bucket: self.bucket.clone(),
                key: s3_key.clone(),
            };
            self.store
                .put(&key, WriteMetadata::default(), once_body(bytes))
                .await
                .map_err(|e| AppendError::Origin(e.to_string()))?;

            manifest.segments[i].state = SegmentState::Flushed;
            manifest.segments[i].s3_key = Some(s3_key);
            // Origin now owns the complete segment. Recovery and reads need
            // only its LSN range and object key; retaining every append's
            // fragment placements made the replicated descriptor grow without
            // bound even though those fragments are deleted below.
            manifest.segments[i].appends.clear();
            // Segments flush in LSN order with no gaps, so the whole prefix up to
            // this segment's end is now in S3.
            manifest.flushed_through = segment.end;
            manifest.revision = manifest.revision.saturating_add(1);
            self.replicate_state(&manifest).await?;
            self.manifest_store.persist(&manifest)?;
            self.flushed.store(segment.end.0, Ordering::Relaxed);

            // The flushed record is durable; free the fragments.
            for entry in &segment.appends {
                self.drop_fragments(entry.object_id(), &entry.placements)
                    .await;
            }
            i += 1;
        }
        Ok(manifest.flushed_through)
    }

    async fn truncate(&self, up_to: Lsn) -> Result<(), AppendError> {
        let mut manifest = self.state.lock().await;
        if up_to.0 > manifest.flushed_through.0 {
            return Err(AppendError::TruncateBeyondFlush {
                up_to,
                flushed_through: manifest.flushed_through,
            });
        }
        if up_to.0 <= manifest.base.0 {
            return Ok(());
        }
        // Drop every segment that lies entirely below `up_to`; delete its S3
        // object (it is flushed, being below the flush watermark). A segment that
        // straddles `up_to` is kept whole.
        let mut kept = Vec::with_capacity(manifest.segments.len());
        let mut deletes = Vec::new();
        for segment in std::mem::take(&mut manifest.segments) {
            if segment.end.0 <= up_to.0 {
                if let Some(k) = &segment.s3_key {
                    deletes.push(CacheKey {
                        storage_binding_id: self.storage_binding_id.clone(),
                        bucket: self.bucket.clone(),
                        key: k.clone(),
                    });
                }
            } else {
                kept.push(segment);
            }
        }
        manifest.segments = kept;
        manifest.base = up_to;
        manifest.revision = manifest.revision.saturating_add(1);
        self.replicate_state(&manifest).await?;
        self.manifest_store.persist(&manifest)?;
        for key in deletes {
            let _ = self.store.delete(&key).await;
        }
        Ok(())
    }

    async fn fence(&self, new_epoch: Epoch) -> Result<(), AppendError> {
        let mut manifest = self.state.lock().await;
        if new_epoch.0 <= manifest.epoch.0 {
            return Err(AppendError::StaleFence {
                current: manifest.epoch,
                presented: new_epoch,
            });
        }
        manifest.epoch = new_epoch;
        manifest.revision = manifest.revision.saturating_add(1);
        self.replicate_state(&manifest).await?;
        self.manifest_store.persist(&manifest)?;
        self.epoch.store(new_epoch.0, Ordering::Relaxed);
        Ok(())
    }

    fn tail(&self) -> Lsn {
        Lsn(self.tail.load(Ordering::Relaxed))
    }

    fn flushed_through(&self) -> Lsn {
        Lsn(self.flushed.load(Ordering::Relaxed))
    }

    fn epoch(&self) -> Epoch {
        Epoch(self.epoch.load(Ordering::Relaxed))
    }
}
