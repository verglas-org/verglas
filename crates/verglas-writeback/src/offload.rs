//! The object offload stream (#164 §4): size-triggered batched propagation
//! for the write-back object path, generalizing the WAL segment-archive model
//! (`SegmentArchiver`, `bins/cache-node/src/safekeeper.rs`) to whole objects.
//!
//! An acknowledged object smaller than the configured size limit is appended
//! here instead of propagating on its own. The stream flushes — packs every
//! accumulated object into one S3 object and commits an index mapping each
//! logical key to its `(pack key, offset, length)` through
//! [`crate::ConsensusCommitter::commit_pack`] — when the accumulated bytes
//! reach the limit, or when a caller invokes drain. An object at or above the
//! limit bypasses accumulation entirely (see
//! `WriteCoordinator::finish_stream_ack`) and streams to the origin alone, so
//! a large write never waits behind a buffer of unrelated small ones.
//!
//! This module holds the pure accumulation and read-resolution state only.
//! Reassembly, upload, consensus commit, and fragment release stay on
//! `WriteCoordinator`, which is where every other write-back durability step
//! already lives — mirroring how `ArchiveTimeline`/`ImmutableSegmentStore`
//! stay separate from `SegmentArchiver`'s pure flush-boundary logic. That
//! separation is deliberate: collapsing WAL and object offload onto one
//! engine (issue #164 §6) is future work, and keeping the accumulator generic
//! over "a batch of pending references" here is what makes that later step
//! small.

use std::collections::HashMap;
use std::sync::PoisonError;
use std::sync::RwLock;

use tokio::sync::Mutex as AsyncMutex;

use crate::meta::StoredMetadata;

/// One acknowledged, still-fragmented object waiting in a binding's offload
/// stream. Only the reference is held — the bytes stay on NVMe as fragments
/// until flush reassembles them, so the buffer never doubles as a second copy
/// of data already durable.
#[derive(Debug, Clone)]
pub struct PendingEntry {
    /// The write-back journal naming this object's fragments.
    pub object_id: String,
    /// Logical origin key this entry resolves to after packing.
    pub key: String,
    /// Logical object length (the packed slice length).
    pub len: u64,
}

/// The size-bounded accumulator for one storage binding's offload stream.
#[derive(Default)]
struct OffloadBuffer {
    pending: Vec<PendingEntry>,
    bytes: u64,
}

impl OffloadBuffer {
    /// Adds one object's reference, returning the accumulated byte total.
    fn push(&mut self, entry: PendingEntry) -> u64 {
        self.bytes += entry.len;
        self.pending.push(entry);
        self.bytes
    }

    /// Removes and returns every currently buffered entry, or `None` when the
    /// buffer is empty — the no-op case a second drain must hit.
    fn take_all(&mut self) -> Option<Vec<PendingEntry>> {
        if self.pending.is_empty() {
            return None;
        }
        self.bytes = 0;
        Some(std::mem::take(&mut self.pending))
    }
}

/// One managed binding's size-triggered offload stream: the accumulator plus
/// the size limit that decides when an append must trigger a flush.
///
/// Async-locked because a flush drains it from the same task that appends to
/// it (the ack continuation) and only ever holds the lock across cheap
/// bookkeeping — the reassemble/upload/commit work happens after the lock is
/// released, so a slow flush never blocks the next append.
pub struct OffloadStream {
    buffer: AsyncMutex<OffloadBuffer>,
    /// Bytes at which an append must trigger a flush
    /// (`cache.writeback.offload_size_limit_bytes`).
    size_limit: u64,
}

impl OffloadStream {
    /// Builds an empty stream with the configured flush threshold. `size_limit`
    /// must be at least 1 (config validation enforces this); a zero limit
    /// would make every append trigger a single-object flush, defeating the
    /// point of the stream.
    pub fn new(size_limit: u64) -> Self {
        Self {
            buffer: AsyncMutex::new(OffloadBuffer::default()),
            size_limit: size_limit.max(1),
        }
    }

    /// The configured flush threshold. The caller decides bypass
    /// (`object_len > size_limit`) before ever calling [`Self::append`].
    pub fn size_limit(&self) -> u64 {
        self.size_limit
    }

    /// Appends one object reference. Returns the batch to flush when the
    /// accumulated size reaches the configured limit, `None` otherwise.
    pub async fn append(&self, entry: PendingEntry) -> Option<Vec<PendingEntry>> {
        let mut guard = self.buffer.lock().await;
        let total = guard.push(entry);
        if total >= self.size_limit {
            guard.take_all()
        } else {
            None
        }
    }

    /// Removes and returns every buffered entry regardless of accumulated
    /// size. `None` when nothing is buffered — an explicit drain on an idle
    /// stream is a no-op, never a flush of zero objects.
    pub async fn drain(&self) -> Option<Vec<PendingEntry>> {
        self.buffer.lock().await.take_all()
    }
}

/// Where one logical key's bytes live after its offload stream packed it:
/// which pack object, and the byte range inside it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackIndexEntry {
    /// Origin key of the packed S3 object holding this entry's bytes.
    pub pack_key: String,
    /// Byte offset of this entry's slice inside the pack object.
    pub offset: u64,
    /// Byte length of this entry's slice inside the pack object.
    pub length: u64,
    /// Synthetic ETag reported for this logical object — independent of the
    /// pack object's own origin ETag, exactly as the dirty-journal window
    /// already reports a synthetic ETag ahead of the origin's.
    pub etag: String,
    /// Client PUT metadata, so a post-flush read reports what the client
    /// originally sent.
    pub metadata: StoredMetadata,
    /// Millisecond Unix time the object was originally acked.
    pub created_ms: u64,
}

/// The local, per-process materialization of the pack index a flush commits
/// through consensus — the same "committed elsewhere, cached here for cheap
/// resolution" shape the dirty journal already uses ahead of its own
/// consensus commit. Reads consult this after the dirty-journal check misses;
/// gate G8 requires the commit itself to be consensus state, not this cache.
#[derive(Default)]
pub struct PackIndex {
    entries: RwLock<HashMap<(String, String, String), PackIndexEntry>>,
}

impl PackIndex {
    /// Builds an empty index.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records one entry, keyed by `(storage binding, bucket, key)`, after its
    /// pack's consensus commit has applied.
    pub fn insert(&self, storage_binding_id: &str, bucket: &str, key: &str, entry: PackIndexEntry) {
        let mut guard = self.entries.write().unwrap_or_else(PoisonError::into_inner);
        guard.insert(
            (
                storage_binding_id.to_owned(),
                bucket.to_owned(),
                key.to_owned(),
            ),
            entry,
        );
    }

    /// Looks up where `key` lives after packing, or `None` if it was never
    /// packed (still dirty, propagated the old direct way, or never written).
    pub fn resolve(
        &self,
        storage_binding_id: &str,
        bucket: &str,
        key: &str,
    ) -> Option<PackIndexEntry> {
        self.entries
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&(
                storage_binding_id.to_owned(),
                bucket.to_owned(),
                key.to_owned(),
            ))
            .cloned()
    }

    /// Removes a resolved entry, e.g. on delete — the physical bytes stay in
    /// their pack object (repack policy is an open decision in #164, tracked
    /// separately), but the key must stop resolving.
    pub fn remove(&self, storage_binding_id: &str, bucket: &str, key: &str) {
        let mut guard = self.entries.write().unwrap_or_else(PoisonError::into_inner);
        guard.remove(&(
            storage_binding_id.to_owned(),
            bucket.to_owned(),
            key.to_owned(),
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(object_id: &str, key: &str, len: u64) -> PendingEntry {
        PendingEntry {
            object_id: object_id.to_owned(),
            key: key.to_owned(),
            len,
        }
    }

    /// Appending below the limit accumulates and never returns a batch.
    #[tokio::test]
    async fn append_below_limit_accumulates() {
        let stream = OffloadStream::new(100);
        assert!(stream.append(entry("o1", "k1", 40)).await.is_none());
        assert!(stream.append(entry("o2", "k2", 40)).await.is_none());
    }

    /// Crossing the limit returns every accumulated entry as one batch.
    #[tokio::test]
    async fn append_crossing_limit_flushes_the_whole_batch() {
        let stream = OffloadStream::new(100);
        assert!(stream.append(entry("o1", "k1", 40)).await.is_none());
        let batch = stream
            .append(entry("o2", "k2", 70))
            .await
            .expect("crossing 100 flushes");
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].object_id, "o1");
        assert_eq!(batch[1].object_id, "o2");
    }

    /// After a threshold flush, the buffer is empty again.
    #[tokio::test]
    async fn buffer_resets_after_a_threshold_flush() {
        let stream = OffloadStream::new(10);
        stream.append(entry("o1", "k1", 10)).await;
        assert!(stream.drain().await.is_none(), "buffer reset after flush");
    }

    /// Drain flushes a partial (under-threshold) buffer.
    #[tokio::test]
    async fn drain_flushes_a_partial_buffer() {
        let stream = OffloadStream::new(1_000_000);
        stream.append(entry("o1", "k1", 10)).await;
        let batch = stream
            .drain()
            .await
            .expect("drain flushes the partial buffer");
        assert_eq!(batch.len(), 1);
    }

    /// A second drain on an empty stream is a no-op.
    #[tokio::test]
    async fn second_drain_on_empty_stream_is_a_noop() {
        let stream = OffloadStream::new(1_000_000);
        stream.append(entry("o1", "k1", 10)).await;
        assert!(stream.drain().await.is_some(), "first drain flushes");
        assert!(stream.drain().await.is_none(), "second drain is a no-op");
    }

    /// A pack index resolves what it was told, and forgets what was removed.
    #[test]
    fn pack_index_resolves_and_removes() {
        let index = PackIndex::new();
        assert!(index.resolve("b", "bkt", "k").is_none());
        index.insert(
            "b",
            "bkt",
            "k",
            PackIndexEntry {
                pack_key: "_verglas/packs/b/p1".to_owned(),
                offset: 0,
                length: 10,
                etag: "\"e\"".to_owned(),
                metadata: StoredMetadata::default(),
                created_ms: 0,
            },
        );
        let found = index.resolve("b", "bkt", "k").expect("resolves");
        assert_eq!(found.pack_key, "_verglas/packs/b/p1");
        // Isolated by (binding, bucket, key): a different binding never
        // aliases another's packed object.
        assert!(index.resolve("other-binding", "bkt", "k").is_none());
        index.remove("b", "bkt", "k");
        assert!(index.resolve("b", "bkt", "k").is_none());
    }
}
