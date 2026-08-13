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
    AttributesRequest, BodyStream, Checksums, DirectGet, DirectMeta, DirectReadOptions,
    ObjectAttributes, ObjectGet, ObjectMeta, ObjectRead, ReadError, ReadRange, Revalidation,
    ServedTier, TierCell,
};
use verglas_core::write::ObjectWrite;

use crate::coordinator::WriteCoordinator;
use crate::state::TransactionRecord;
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

    /// Looks up the dirty state for `key`, or `None`. Cheap when idle.
    fn dirty_state(&self, key: &CacheKey) -> Option<TransactionRecord> {
        let states = self.coordinator.states();
        if states.is_idle() {
            return None;
        }
        let object_id = states.find_dirty(&key.storage_binding_id, &key.bucket, &key.key)?;
        states.read(&object_id)
    }

    fn tombstoned(&self, key: &CacheKey) -> bool {
        self.coordinator
            .states()
            .is_tombstoned(&key.storage_binding_id, &key.bucket, &key.key)
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
        if self.tombstoned(key) {
            return Err(ReadError::NoSuchKey);
        }
        let Some(state) = self.dirty_state(key) else {
            return self.inner.get(key, range).await;
        };
        let bytes = self
            .coordinator
            .reassemble(&state)
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
            meta: meta_for(&state, bytes.len() as u64),
            range: served,
            body: once_body(bytes.slice(start..end)),
            served_from,
        })
    }

    /// Reports dirty metadata before propagation; delegates otherwise.
    async fn head(&self, key: &CacheKey) -> Result<ObjectMeta, ReadError> {
        if self.tombstoned(key) {
            return Err(ReadError::NoSuchKey);
        }
        match self.dirty_state(key) {
            Some(state) => Ok(meta_for(&state, state.object_len)),
            None => self.inner.head(key).await,
        }
    }

    /// Revalidates against the dirty state's synthetic ETag; delegates
    /// otherwise.
    async fn revalidate(&self, key: &CacheKey, etag: &str) -> Result<Revalidation, ReadError> {
        if self.tombstoned(key) {
            return Ok(Revalidation::Vanished);
        }
        match self.dirty_state(key) {
            Some(state) => {
                let meta = meta_for(&state, state.object_len);
                if meta.e_tag.as_deref() == Some(etag) {
                    Ok(Revalidation::Unchanged)
                } else {
                    Ok(Revalidation::Changed(Box::new(meta)))
                }
            }
            None => self.inner.revalidate(key, etag).await,
        }
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
        if self.tombstoned(key) {
            return Err(ReadError::NoSuchKey);
        }
        let Some(state) = self.dirty_state(key) else {
            return self.inner.get_direct(key, range, options).await;
        };
        if options.version_id.is_some() || options.part_number.is_some() {
            return self.inner.get_direct(key, range, options).await;
        }
        let bytes = self
            .coordinator
            .reassemble(&state)
            .await
            .map_err(|error| ReadError::Backend(error.to_string()))?;
        let served = resolve_range(range, bytes.len() as u64)?;
        let start = usize::try_from(served.start)
            .map_err(|_| ReadError::Backend("range start overflow".to_owned()))?;
        let end = usize::try_from(served.end)
            .map_err(|_| ReadError::Backend("range end overflow".to_owned()))?;
        Ok(DirectGet {
            meta: dirty_direct_meta(&state),
            range: served,
            body: once_body(bytes.slice(start..end)),
        })
    }

    /// Preserves dirty-object metadata for checksum-enabled HEADs.
    async fn head_direct(
        &self,
        key: &CacheKey,
        options: DirectReadOptions,
    ) -> Result<DirectMeta, ReadError> {
        if self.tombstoned(key) {
            return Err(ReadError::NoSuchKey);
        }
        let Some(state) = self.dirty_state(key) else {
            return self.inner.head_direct(key, options).await;
        };
        if options.version_id.is_some() || options.part_number.is_some() {
            return self.inner.head_direct(key, options).await;
        }
        Ok(dirty_direct_meta(&state))
    }

    /// Reports the currently acknowledged dirty object instead of consulting
    /// an origin that may not contain it yet.
    async fn object_attributes(
        &self,
        key: &CacheKey,
        request: AttributesRequest,
    ) -> Result<ObjectAttributes, ReadError> {
        if self.tombstoned(key) {
            return Err(ReadError::NoSuchKey);
        }
        let Some(state) = self.dirty_state(key) else {
            return self.inner.object_attributes(key, request).await;
        };
        if request.version_id.is_some() {
            return self.inner.object_attributes(key, request).await;
        }
        let meta = meta_for(&state, state.object_len);
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
fn dirty_direct_meta(state: &TransactionRecord) -> DirectMeta {
    DirectMeta {
        meta: meta_for(state, state.object_len),
        version_id: None,
        parts_count: None,
        checksums: Checksums::default(),
    }
}

/// Builds the object metadata a dirty read reports.
fn meta_for(state: &TransactionRecord, size: u64) -> ObjectMeta {
    state
        .metadata
        .to_object_meta(size, state.etag.clone(), state.created_ms)
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
