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
use tokio::sync::Mutex;
use verglas_cache::writeback_codec::{Encoded, Fragment, Geometry, encode, reassemble};
use verglas_cluster::fragments::{FragmentKey, FragmentRecord};
use verglas_core::CacheKey;
use verglas_core::node::NodeId;
use verglas_core::read::{ObjectRead, ReadRange};
use verglas_core::ring::rendezvous_hash;
use verglas_core::write::{ObjectWrite, WriteBodyStream, WriteMetadata};

use crate::contract::{AppendError, AppendGeometry, AppendLog, Appended, Epoch, Lsn};
use crate::manifest::{
    AppendEntry, Manifest, ManifestStore, Placement, SegmentEntry, SegmentState,
};

/// Seal a segment and start a new one once the open one reaches this many bytes.
/// A fixed flush-granularity constant, not a tuning knob: the whole tuning
/// surface is the erasure geometry (see the crate contract, §7).
const SEGMENT_TARGET: u64 = 16 * 1024 * 1024;

/// The erasure-coded quorum append log. `S` is the S3 origin, used for the flush
/// write and for reading already-flushed ranges back; it is never on the append
/// (commit) path.
pub struct EcAppendLog<S> {
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
    pub fn open(
        store: Arc<S>,
        bucket: impl Into<String>,
        prefix: impl Into<String>,
        transport: Arc<dyn crate::FragmentTransport>,
        membership: Arc<dyn crate::LiveMembership>,
        dir: impl AsRef<Path>,
        geometry: AppendGeometry,
    ) -> Result<Self, AppendError> {
        let (manifest_store, manifest) = ManifestStore::open(dir)?;
        let tail = AtomicU64::new(manifest.tail.0);
        let flushed = AtomicU64::new(manifest.flushed_through.0);
        let epoch = AtomicU64::new(manifest.epoch.0);
        Ok(Self {
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
                placements.push(Placement {
                    index,
                    node: node.as_str().to_owned(),
                });
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
    async fn reassemble_append(
        &self,
        segment_id: u64,
        entry: &AppendEntry,
    ) -> Result<Bytes, AppendError> {
        let object_id = entry.object_id(segment_id);
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
            let bytes = self.reassemble_append(segment.id, entry).await?;
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
        bucket: "appendlog".to_owned(),
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
    async fn append(&self, epoch: Epoch, records: Bytes) -> Result<Appended, AppendError> {
        let mut manifest = self.state.lock().await;
        if epoch != manifest.epoch {
            return Err(AppendError::Fenced {
                current: manifest.epoch,
                presented: epoch,
            });
        }
        let start = manifest.tail;
        if records.is_empty() {
            // An empty append is a no-op: the tail does not move.
            return Ok(Appended { start, end: start });
        }
        let end = start.advance(records.len() as u64);

        let geometry = self.effective_geometry();
        let live = self.membership.live_nodes();

        let idx = ensure_tail_segment(&mut manifest, start);
        let (segment_id, seq) = {
            let seg = &manifest.segments[idx];
            (seg.id, seg.appends.len() as u64)
        };
        let object_id = format!("seg{segment_id:020}/app{seq:020}");

        let encoded = encode(geometry.k, geometry.m, &records).map_err(|e| {
            // A partial (padding-only) failure leaves nothing placed; the tail is
            // untouched, so this is a clean failure.
            AppendError::Codec(e.to_string())
        })?;
        let placements = self.place(&object_id, &encoded, geometry.w, &live).await?;

        let entry = AppendEntry {
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
        manifest.tail = end;

        // Fsync the manifest before acking: the ack must not outrun durability.
        if let Err(error) = self.manifest_store.persist(&manifest) {
            // The fragments are placed but the record is not durable — roll the
            // append back so the tail never reflects an un-fsynced ack, and drop
            // the orphaned fragments.
            let seg = &mut manifest.segments[idx];
            seg.appends.pop();
            seg.end = start;
            manifest.tail = start;
            self.drop_fragments(&object_id, &placements).await;
            return Err(error);
        }
        self.tail.store(end.0, Ordering::Relaxed);

        Ok(Appended { start, end })
    }

    async fn read(&self, from: Lsn, to: Lsn) -> Result<Bytes, AppendError> {
        if to.0 < from.0 {
            return Err(AppendError::OutOfRange { from, to });
        }
        let manifest = self.state.lock().await;
        if from.0 < manifest.base.0 || to.0 > manifest.tail.0 {
            return Err(AppendError::OutOfRange { from, to });
        }
        if from == to {
            return Ok(Bytes::new());
        }
        let mut out = Vec::with_capacity((to.0 - from.0) as usize);
        for segment in &manifest.segments {
            // Skip segments that do not overlap [from, to).
            if segment.end.0 <= from.0 || segment.start.0 >= to.0 {
                continue;
            }
            let bytes = match segment.state {
                SegmentState::Flushed => self.read_flushed_segment(segment).await?,
                SegmentState::Open => self.reassemble_segment(segment).await?,
            };
            let lo = from.0.max(segment.start.0);
            let hi = to.0.min(segment.end.0);
            let s = (lo - segment.start.0) as usize;
            let e = (hi - segment.start.0) as usize;
            out.extend_from_slice(&bytes[s..e]);
        }
        Ok(Bytes::from(out))
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
                bucket: self.bucket.clone(),
                key: s3_key.clone(),
            };
            self.store
                .put(&key, WriteMetadata::default(), once_body(bytes))
                .await
                .map_err(|e| AppendError::Origin(e.to_string()))?;

            manifest.segments[i].state = SegmentState::Flushed;
            manifest.segments[i].s3_key = Some(s3_key);
            // Segments flush in LSN order with no gaps, so the whole prefix up to
            // this segment's end is now in S3.
            manifest.flushed_through = segment.end;
            self.manifest_store.persist(&manifest)?;
            self.flushed.store(segment.end.0, Ordering::Relaxed);

            // The flushed record is durable; free the fragments.
            for entry in &segment.appends {
                let object_id = entry.object_id(segment.id);
                self.drop_fragments(&object_id, &entry.placements).await;
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
