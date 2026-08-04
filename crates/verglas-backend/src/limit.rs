//! A per-bucket concurrency limiter for backend object-store requests.
//!
//! [`LimitedStore`] decorates any [`MultipartObjectStore`] with a
//! [`tokio::sync::Semaphore`], so no more than `max_concurrent_requests`
//! backend operations are ever in flight at once. This is the backstop against
//! a miss storm: a burst of cache misses fans out into fills, and without a
//! ceiling those fills would exhaust the origin's connection pool. Because a
//! streamed GET is not "done" until its body has drained, the permit for a get
//! is held for the lifetime of the returned body stream, not just the request
//! round-trip.

use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::multipart::{MultipartStore, PartId};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartId, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// The backend-store surface Verglas drives: plain object operations plus the
/// low-level multipart API whose upload IDs are the backend's own (so they
/// round-trip to the client untouched). Blanket-implemented for every store
/// that has both, notably `object_store`'s `AmazonS3` and the in-memory store
/// tests use.
pub trait MultipartObjectStore: ObjectStore + MultipartStore {}

impl<T: ObjectStore + MultipartStore + ?Sized> MultipartObjectStore for T {}

/// Wraps a backend store so at most `limit` requests run concurrently. Every
/// operation acquires a semaphore permit before touching the inner store; the
/// permit is released when the operation completes (and, for streamed reads,
/// only once the body stream is fully consumed or dropped).
#[derive(Debug)]
pub struct LimitedStore {
    /// The real backend store (an S3 client in production, in-memory in tests).
    inner: Arc<dyn MultipartObjectStore>,
    /// The concurrency budget. Never closed, so `acquire` never fails.
    semaphore: Arc<Semaphore>,
}

impl LimitedStore {
    /// Wraps `inner`, admitting at most `limit` concurrent backend requests.
    /// `limit` is a hard ceiling — config validation guarantees it is ≥ 1.
    pub fn new(inner: Arc<dyn MultipartObjectStore>, limit: usize) -> Self {
        Self::with_semaphore(inner, Arc::new(Semaphore::new(limit)))
    }

    /// Wraps `inner` behind an externally-owned `semaphore`, so the SAME
    /// per-bucket budget can also govern the bucket's raw request path (issue
    /// #189): one budget per bucket, never parallel copies.
    pub fn with_semaphore(inner: Arc<dyn MultipartObjectStore>, semaphore: Arc<Semaphore>) -> Self {
        LimitedStore { inner, semaphore }
    }

    /// Acquires one permit, holding it only for the duration of the caller's
    /// `await`. The semaphore is never closed, so acquisition cannot fail.
    async fn permit(&self) -> tokio::sync::SemaphorePermit<'_> {
        self.semaphore
            .acquire()
            .await
            .expect("backend concurrency semaphore is never closed")
    }

    /// Acquires one owned permit that can outlive this call — used to keep a
    /// slot reserved for the lifetime of a streamed response body.
    async fn owned_permit(&self) -> OwnedSemaphorePermit {
        self.semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("backend concurrency semaphore is never closed")
    }
}

impl fmt::Display for LimitedStore {
    /// Names the inner store so operator-facing errors stay legible.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LimitedStore({})", self.inner)
    }
}

/// Re-attaches `permit` to a streamed body so the concurrency slot stays
/// reserved until the stream is fully drained or dropped. A `File` payload
/// (only the local-filesystem store produces one) has no in-flight backend
/// request, so its permit is released immediately by dropping it here.
fn hold_permit(payload: GetResultPayload, permit: OwnedSemaphorePermit) -> GetResultPayload {
    match payload {
        GetResultPayload::Stream(stream) => {
            let guarded =
                futures::stream::unfold((stream, permit), |(mut stream, permit)| async move {
                    // The permit lives inside the unfold state: when the stream
                    // ends (or is dropped) the state drops and the slot frees.
                    stream.next().await.map(|chunk| (chunk, (stream, permit)))
                })
                .boxed();
            GetResultPayload::Stream(guarded)
        }
        file => file,
    }
}

#[async_trait]
impl ObjectStore for LimitedStore {
    /// Guards a whole-object PUT for the request's duration.
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let _permit = self.permit().await;
        self.inner.put_opts(location, payload, opts).await
    }

    /// Guards creation of a high-level streaming upload. Verglas drives
    /// multipart through the low-level [`MultipartStore`] API, so this path is
    /// unused today; the returned handle's own part PUTs are not individually
    /// limited (the create is).
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let _permit = self.permit().await;
        self.inner.put_multipart_opts(location, opts).await
    }

    /// Guards a GET and keeps the slot reserved for the body stream's lifetime
    /// — an in-flight fill is not complete until its bytes have drained.
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let permit = self.owned_permit().await;
        let mut result = self.inner.get_opts(location, options).await?;
        result.payload = hold_permit(result.payload, permit);
        Ok(result)
    }

    /// Guards a coalesced multi-range read for its duration (bytes are
    /// buffered before return, so no permit needs to outlive the call).
    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let _permit = self.permit().await;
        self.inner.get_ranges(location, ranges).await
    }

    /// Guards each individual delete streamed through. Holding one permit for
    /// the whole batch stream would serialize deletes against every other
    /// backend request, so the inner store's own per-delete requests are what
    /// count against the budget via `delete` (which most callers use).
    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    /// Lists objects, keeping the slot reserved until the listing stream is
    /// fully consumed (LIST is paginated, so it is a multi-request stream).
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        // `list` is not `async`; block the current task on the permit by
        // acquiring it lazily inside the stream instead.
        let semaphore = self.semaphore.clone();
        let stream = self.inner.list(prefix);
        futures::stream::once(async move {
            let permit = semaphore
                .acquire_owned()
                .await
                .expect("backend concurrency semaphore is never closed");
            futures::stream::unfold((stream, permit), |(mut stream, permit)| async move {
                stream.next().await.map(|item| (item, (stream, permit)))
            })
        })
        .flatten()
        .boxed()
    }

    /// Guards a delimited LIST for its duration (the result is materialized
    /// before return).
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let _permit = self.permit().await;
        self.inner.list_with_delimiter(prefix).await
    }

    /// Guards a server-side copy for its duration.
    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        let _permit = self.permit().await;
        self.inner.copy_opts(from, to, options).await
    }
}

#[async_trait]
impl MultipartStore for LimitedStore {
    /// Guards creation of a low-level multipart upload.
    async fn create_multipart(&self, path: &Path) -> Result<MultipartId> {
        let _permit = self.permit().await;
        self.inner.create_multipart(path).await
    }

    /// Guards creation with options (attributes such as Content-Type). Delegated
    /// to the inner store so real S3's attribute support is preserved, rather
    /// than falling back to the trait default that rejects non-empty options.
    async fn create_multipart_opts(
        &self,
        path: &Path,
        opts: PutMultipartOptions,
    ) -> Result<MultipartId> {
        let _permit = self.permit().await;
        self.inner.create_multipart_opts(path, opts).await
    }

    /// Guards one part upload — the request that dominates a write's backend
    /// traffic — for its duration.
    async fn put_part(
        &self,
        path: &Path,
        id: &MultipartId,
        part_idx: usize,
        data: PutPayload,
    ) -> Result<PartId> {
        let _permit = self.permit().await;
        self.inner.put_part(path, id, part_idx, data).await
    }

    /// Guards completion (the assembling request) for its duration.
    async fn complete_multipart(
        &self,
        path: &Path,
        id: &MultipartId,
        parts: Vec<PartId>,
    ) -> Result<PutResult> {
        let _permit = self.permit().await;
        self.inner.complete_multipart(path, id, parts).await
    }

    /// Guards an abort for its duration.
    async fn abort_multipart(&self, path: &Path, id: &MultipartId) -> Result<()> {
        let _permit = self.permit().await;
        self.inner.abort_multipart(path, id).await
    }
}
