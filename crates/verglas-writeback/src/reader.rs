//! Read-your-writes reader wrapper (#180).
//!
//! A GET/HEAD for an object that is acked but not yet propagated is served by
//! reassembling it from its fragments, so a client that just wrote it reads it
//! back even though the origin does not have it yet. Every other read delegates
//! to the ordinary read path. When nothing is dirty the check is one relaxed
//! atomic load, so an enabled-but-idle tier adds nothing to the read hot path.

use std::ops::Range;

use bytes::Bytes;
use futures::stream;
use verglas_core::CacheKey;
use verglas_core::read::{
    BodyStream, ObjectGet, ObjectMeta, ObjectRead, ReadError, ReadRange, Revalidation, ServedTier,
    TierCell,
};
use verglas_core::write::ObjectWrite;

use crate::coordinator::WriteCoordinator;
use crate::journal::Journal;
use std::sync::Arc;

/// Wraps an inner read path and serves dirty (unpropagated) objects from
/// fragments.
pub struct WritebackReader<R, W: ObjectWrite> {
    /// The ordinary read path for non-dirty keys.
    inner: R,
    /// The coordinator, for the dirty index and fragment reassembly.
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
        let object_id = journals.find_dirty(&key.bucket, &key.key)?;
        journals.read(&object_id).ok().flatten()
    }
}

impl<R, W> ObjectRead for WritebackReader<R, W>
where
    R: ObjectRead,
    W: ObjectWrite,
{
    /// Serves a dirty object by reassembling the requested range from
    /// fragments; delegates otherwise.
    async fn get(&self, key: &CacheKey, range: ReadRange) -> Result<ObjectGet, ReadError> {
        let Some(journal) = self.dirty_journal(key) else {
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

    /// Reports dirty metadata before propagation; delegates otherwise.
    async fn head(&self, key: &CacheKey) -> Result<ObjectMeta, ReadError> {
        match self.dirty_journal(key) {
            Some(journal) => Ok(meta_for(&journal, journal.object_len)),
            None => self.inner.head(key).await,
        }
    }

    /// Revalidates against the dirty journal's synthetic ETag; delegates
    /// otherwise.
    async fn revalidate(&self, key: &CacheKey, etag: &str) -> Result<Revalidation, ReadError> {
        match self.dirty_journal(key) {
            Some(journal) => {
                let meta = meta_for(&journal, journal.object_len);
                if meta.e_tag.as_deref() == Some(etag) {
                    Ok(Revalidation::Unchanged)
                } else {
                    Ok(Revalidation::Changed(Box::new(meta)))
                }
            }
            None => self.inner.revalidate(key, etag).await,
        }
    }
}

/// Builds the object metadata a dirty read reports.
fn meta_for(journal: &Journal, size: u64) -> ObjectMeta {
    journal
        .metadata
        .to_object_meta(size, journal.etag.clone(), journal.created_ms)
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
