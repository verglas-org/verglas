//! Read-your-writes reader wrapper (#180).
//!
//! A GET/HEAD for an object that is acked but not yet propagated is served by
//! reassembling it from its fragments, so a client that just wrote it reads it
//! back even though the origin does not have it yet. When nothing is dirty
//! the check is one relaxed atomic load, so an enabled-but-idle tier adds
//! nothing to the read hot path.
//!
//! Once an object's offload stream flushes it (#164 §4), its bytes no longer
//! live at its own origin key — they live inside a packed S3 object at a
//! byte range the flush's consensus commit named. A GET/HEAD for a flushed
//! key is served by resolving that pack index and reading the exact slice
//! from the pack object, so read-your-writes holds across a flush exactly as
//! it already holds across the dirty window. Every other read delegates to
//! the ordinary read path.

use std::ops::Range;

use bytes::Bytes;
use futures::{StreamExt, stream};
use verglas_core::CacheKey;
use verglas_core::read::{
    AttributesRequest, BodyStream, Checksums, DirectGet, DirectMeta, DirectReadOptions,
    ObjectAttributes, ObjectGet, ObjectMeta, ObjectRead, ReadError, ReadRange, Revalidation,
    ServedTier, TierCell,
};
use verglas_core::write::ObjectWrite;

use crate::coordinator::WriteCoordinator;
use crate::journal::Journal;
use crate::offload::PackIndexEntry;
use std::sync::Arc;

/// Wraps an inner read path and serves dirty (unpropagated) or freshly packed
/// objects that the ordinary read path does not yet (or no longer) have at
/// their own origin key.
pub struct WritebackReader<R, W: ObjectWrite> {
    /// The ordinary read path for keys that are neither dirty nor packed.
    inner: R,
    /// The coordinator, for the dirty index, fragment reassembly, and the
    /// pack index.
    coordinator: Arc<WriteCoordinator<W>>,
}

impl<R, W: ObjectWrite> WritebackReader<R, W> {
    /// Builds the reader over `inner` and the shared `coordinator`.
    pub fn new(inner: R, coordinator: Arc<WriteCoordinator<W>>) -> Self {
        Self { inner, coordinator }
    }

    /// Looks up the dirty journal for `key`, or `None`. Cheap when idle.
    fn dirty_journal(&self, key: &CacheKey) -> Option<Journal> {
        let journals = self.coordinator.journals();
        if journals.is_idle() {
            return None;
        }
        let object_id = journals.find_dirty(&key.storage_binding_id, &key.bucket, &key.key)?;
        journals.read(&object_id).ok().flatten()
    }

    /// Looks up `key`'s pack index entry, or `None` if it was never packed.
    fn pack_entry(&self, key: &CacheKey) -> Option<PackIndexEntry> {
        self.coordinator
            .pack_index()
            .resolve(&key.storage_binding_id, &key.bucket, &key.key)
    }

    /// Reads `range` of a packed key by resolving it to the exact byte slice
    /// of its pack object and reading that range through the inner path.
    async fn get_from_pack(
        &self,
        key: &CacheKey,
        entry: &PackIndexEntry,
        range: ReadRange,
    ) -> Result<ObjectGet, ReadError>
    where
        R: ObjectRead,
    {
        let served = resolve_range(range, entry.length)?;
        let pack_key = CacheKey {
            storage_binding_id: key.storage_binding_id.clone(),
            bucket: key.bucket.clone(),
            key: entry.pack_key.clone(),
        };
        let pack_range = ReadRange::Bounded(
            entry.offset + served.start,
            entry.offset + served.end.saturating_sub(1),
        );
        let got = self.inner.get(&pack_key, pack_range).await?;
        let mut buf = Vec::new();
        let mut body = got.body;
        while let Some(chunk) = body.next().await {
            buf.extend_from_slice(&chunk?);
        }
        let bytes = Bytes::from(buf);
        if bytes.len() as u64 != served.end - served.start {
            return Err(ReadError::Backend(
                "packed read returned an unexpected length".to_owned(),
            ));
        }
        Ok(ObjectGet {
            meta: pack_meta_for(entry),
            range: served,
            body: once_body(bytes),
            served_from: got.served_from,
        })
    }
}

impl<R, W> ObjectRead for WritebackReader<R, W>
where
    R: ObjectRead,
    W: ObjectWrite,
{
    /// Serves a dirty object by reassembling the requested range from
    /// fragments, or a flushed object by resolving its pack entry; delegates
    /// otherwise.
    async fn get(&self, key: &CacheKey, range: ReadRange) -> Result<ObjectGet, ReadError> {
        let Some(journal) = self.dirty_journal(key) else {
            if let Some(entry) = self.pack_entry(key) {
                return self.get_from_pack(key, &entry, range).await;
            }
            return self.inner.get(key, range).await;
        };
        let bytes = self
            .coordinator
            .reassemble(&journal)
            .await
            .map_err(|e| ReadError::Backend(e.to_string()))?;
        let served = resolve_range(range, bytes.len() as u64)?;
        let start = usize::try_from(served.start)
            .map_err(|_| ReadError::Backend("range start overflow".to_owned()))?;
        let end = usize::try_from(served.end)
            .map_err(|_| ReadError::Backend("range end overflow".to_owned()))?;
        let served_from = TierCell::new();
        // A dirty write-back object is reassembled from DRAM-resident fragments —
        // a warm serve for the request-duration histogram (#46).
        served_from.set(ServedTier::Dram);
        Ok(ObjectGet {
            meta: meta_for(&journal, bytes.len() as u64),
            range: served,
            body: once_body(bytes.slice(start..end)),
            served_from,
        })
    }

    /// Reports dirty or packed metadata; delegates otherwise.
    async fn head(&self, key: &CacheKey) -> Result<ObjectMeta, ReadError> {
        if let Some(journal) = self.dirty_journal(key) {
            return Ok(meta_for(&journal, journal.object_len));
        }
        if let Some(entry) = self.pack_entry(key) {
            return Ok(pack_meta_for(&entry));
        }
        self.inner.head(key).await
    }

    /// Revalidates against the dirty journal's or pack entry's synthetic
    /// ETag; delegates otherwise.
    async fn revalidate(&self, key: &CacheKey, etag: &str) -> Result<Revalidation, ReadError> {
        if let Some(journal) = self.dirty_journal(key) {
            let meta = meta_for(&journal, journal.object_len);
            return Ok(if meta.e_tag.as_deref() == Some(etag) {
                Revalidation::Unchanged
            } else {
                Revalidation::Changed(Box::new(meta))
            });
        }
        if let Some(entry) = self.pack_entry(key) {
            let meta = pack_meta_for(&entry);
            return Ok(if meta.e_tag.as_deref() == Some(etag) {
                Revalidation::Unchanged
            } else {
                Revalidation::Changed(Box::new(meta))
            });
        }
        self.inner.revalidate(key, etag).await
    }

    /// Preserves read-your-writes for checksum-enabled GETs.
    ///
    /// A quorum-acked dirty object has no origin version or multipart identity
    /// yet, so those explicitly origin-scoped reads delegate. Checksum mode by
    /// itself may still read the current object from fragments; its checksum
    /// block is empty until origin propagation assigns one, matching the PUT
    /// acknowledgement returned by the write-back coordinator.
    async fn get_direct(
        &self,
        key: &CacheKey,
        range: ReadRange,
        options: DirectReadOptions,
    ) -> Result<DirectGet, ReadError> {
        let Some(journal) = self.dirty_journal(key) else {
            if options.version_id.is_none()
                && options.part_number.is_none()
                && let Some(entry) = self.pack_entry(key)
            {
                let got = self.get_from_pack(key, &entry, range).await?;
                return Ok(DirectGet {
                    meta: DirectMeta {
                        meta: got.meta,
                        version_id: None,
                        parts_count: None,
                        checksums: Checksums::default(),
                    },
                    range: got.range,
                    body: got.body,
                });
            }
            return self.inner.get_direct(key, range, options).await;
        };
        if options.version_id.is_some() || options.part_number.is_some() {
            return self.inner.get_direct(key, range, options).await;
        }
        let bytes = self
            .coordinator
            .reassemble(&journal)
            .await
            .map_err(|error| ReadError::Backend(error.to_string()))?;
        let served = resolve_range(range, bytes.len() as u64)?;
        let start = usize::try_from(served.start)
            .map_err(|_| ReadError::Backend("range start overflow".to_owned()))?;
        let end = usize::try_from(served.end)
            .map_err(|_| ReadError::Backend("range end overflow".to_owned()))?;
        Ok(DirectGet {
            meta: dirty_direct_meta(&journal),
            range: served,
            body: once_body(bytes.slice(start..end)),
        })
    }

    /// Preserves dirty- or packed-object metadata for checksum-enabled HEADs.
    async fn head_direct(
        &self,
        key: &CacheKey,
        options: DirectReadOptions,
    ) -> Result<DirectMeta, ReadError> {
        let Some(journal) = self.dirty_journal(key) else {
            if options.version_id.is_none()
                && options.part_number.is_none()
                && let Some(entry) = self.pack_entry(key)
            {
                return Ok(DirectMeta {
                    meta: pack_meta_for(&entry),
                    version_id: None,
                    parts_count: None,
                    checksums: Checksums::default(),
                });
            }
            return self.inner.head_direct(key, options).await;
        };
        if options.version_id.is_some() || options.part_number.is_some() {
            return self.inner.head_direct(key, options).await;
        }
        Ok(dirty_direct_meta(&journal))
    }

    /// Reports the currently acknowledged dirty or packed object instead of
    /// consulting an origin that may not contain it at its own key.
    async fn object_attributes(
        &self,
        key: &CacheKey,
        request: AttributesRequest,
    ) -> Result<ObjectAttributes, ReadError> {
        let Some(journal) = self.dirty_journal(key) else {
            if request.version_id.is_none()
                && let Some(entry) = self.pack_entry(key)
            {
                let meta = pack_meta_for(&entry);
                return Ok(ObjectAttributes {
                    e_tag: meta.e_tag,
                    object_size: Some(meta.size),
                    storage_class: None,
                    last_modified: meta.last_modified,
                    version_id: None,
                    checksums: Checksums::default(),
                    object_parts: None,
                });
            }
            return self.inner.object_attributes(key, request).await;
        };
        if request.version_id.is_some() {
            return self.inner.object_attributes(key, request).await;
        }
        let meta = meta_for(&journal, journal.object_len);
        Ok(ObjectAttributes {
            e_tag: meta.e_tag,
            object_size: Some(meta.size),
            storage_class: None,
            last_modified: meta.last_modified,
            version_id: None,
            checksums: Checksums::default(),
            object_parts: None,
        })
    }
}

/// Builds the direct-read envelope for a quorum-acked dirty object.
fn dirty_direct_meta(journal: &Journal) -> DirectMeta {
    DirectMeta {
        meta: meta_for(journal, journal.object_len),
        version_id: None,
        parts_count: None,
        checksums: Checksums::default(),
    }
}

/// Builds the object metadata a dirty read reports.
fn meta_for(journal: &Journal, size: u64) -> ObjectMeta {
    journal
        .metadata
        .to_object_meta(size, journal.etag.clone(), journal.created_ms)
}

/// Builds the object metadata a packed read reports: the logical entry's own
/// length, ETag, and client-sent metadata, independent of the pack object's
/// own origin metadata.
fn pack_meta_for(entry: &PackIndexEntry) -> ObjectMeta {
    entry
        .metadata
        .to_object_meta(entry.length, entry.etag.clone(), entry.created_ms)
}

/// Resolves an HTTP byte range against a known object size.
fn resolve_range(range: ReadRange, size: u64) -> Result<Range<u64>, ReadError> {
    match range {
        ReadRange::Full => Ok(0..size),
        ReadRange::From(first) if first < size => Ok(first..size),
        ReadRange::From(_) => Err(ReadError::InvalidRange),
        ReadRange::Bounded(first, last) if first < size => {
            Ok(first..last.saturating_add(1).min(size))
        }
        ReadRange::Bounded(_, _) => Err(ReadError::InvalidRange),
        ReadRange::Suffix(length) => Ok(size.saturating_sub(length.min(size))..size),
    }
}

/// A one-chunk read body.
fn once_body(bytes: Bytes) -> BodyStream {
    use futures::StreamExt;
    stream::once(async move { Ok(bytes) }).boxed()
}
