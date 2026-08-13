//! `ResilientStore` (issue #20): the retry + circuit-breaker decorator that
//! wraps a bucket's concurrency-[limited](crate::LimitedStore) store.
//!
//! Ordering matters. From the outside in a request passes through
//! **breaker → retry → limiter → origin**:
//!
//! * the **breaker** is consulted first, so a shed request fails fast without
//!   taking a permit or issuing any I/O;
//! * the **retry** loop sits above the limiter, so each attempt re-acquires a
//!   permit and the backoff sleep between attempts holds none — a throttling
//!   origin never parks the whole permit pool in backoff (see [`crate::retry`]);
//! * the **limiter** bounds in-flight concurrency per attempt;
//! * one outcome per logical request (after retries) is fed back to the
//!   breaker, so it trips on *sustained* trouble, not on a single blip a retry
//!   would have papered over.
//!
//! Streaming operations (`list`, `delete_stream`) pass straight to the inner
//! limiter: they are paginated multi-request streams, not the unary fills the
//! retry/breaker logic reasons about.

use std::fmt;
use std::future::Future;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::multipart::{MultipartStore, PartId};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartId, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};
use verglas_core::config::RetryPolicy;

use crate::breaker::{Admission, CircuitBreaker};
use crate::limit::MultipartObjectStore;
use crate::retry::{Backoff, Retryability, classify};

/// The error surfaced when a request is shed by an open breaker. It is wrapped
/// as the `source` of an `object_store::Error::Generic` so it flows through the
/// object-store surface unchanged; the front-end renders it as an S3 503-shaped
/// "slow down / try again" rather than a spurious client error.
#[derive(Debug, thiserror::Error)]
#[error("circuit breaker open for bucket `{bucket}`: origin is shedding load")]
pub struct CircuitOpen {
    /// The bucket whose breaker is open.
    pub bucket: String,
}

/// Retry + breaker counters for a bucket (logging/metrics; #46 scrapes them).
#[derive(Debug, Default)]
pub(crate) struct RetryMetrics {
    /// Total retry attempts issued across all requests to this bucket.
    pub(crate) retries: AtomicU64,
}

/// Wraps a bucket's concurrency-limited store with retry/backoff and its
/// per-bucket circuit breaker.
pub struct ResilientStore {
    /// The concurrency-limited store (a [`crate::LimitedStore`]).
    inner: Arc<dyn MultipartObjectStore>,
    /// The bucket, for the shed-load error message.
    bucket: String,
    /// Retry/backoff tuning.
    retry: RetryPolicy,
    /// This bucket's circuit breaker (shared with the registry's integration point).
    breaker: Arc<CircuitBreaker>,
    /// Shared retry counter (aggregated at the registry).
    metrics: Arc<RetryMetrics>,
}

impl fmt::Debug for ResilientStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResilientStore")
            .field("bucket", &self.bucket)
            .field("inner", &self.inner)
            .finish()
    }
}

impl fmt::Display for ResilientStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ResilientStore({}, {})", self.bucket, self.inner)
    }
}

impl ResilientStore {
    /// Wraps `inner` (already limited) with `retry` policy and `breaker`.
    pub(crate) fn new(
        inner: Arc<dyn MultipartObjectStore>,
        bucket: String,
        retry: RetryPolicy,
        breaker: Arc<CircuitBreaker>,
        metrics: Arc<RetryMetrics>,
    ) -> Self {
        ResilientStore {
            inner,
            bucket,
            retry,
            breaker,
            metrics,
        }
    }

    /// The load-shed error for this bucket.
    fn circuit_open(&self) -> object_store::Error {
        object_store::Error::Generic {
            store: "verglas-backend",
            source: Box::new(CircuitOpen {
                bucket: self.bucket.clone(),
            }),
        }
    }

    /// Runs `op` under the breaker and the retry loop: admit (or shed), retry
    /// transient failures with backoff, then feed one outcome to the breaker.
    /// `op` is re-invoked per attempt, so it must re-issue against the inner
    /// store from cloned arguments.
    async fn guard<T, F, Fut>(&self, op: F) -> Result<T>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        guard_generic(
            &self.breaker,
            &self.retry,
            &self.metrics,
            classify,
            || self.circuit_open(),
            op,
        )
        .await
    }
}

/// Runs `op` under `breaker` and the retry loop: admit (or shed with `shed`'s
/// error), retry transient failures per `classify_error`, then feed one
/// outcome to the breaker. Generic over the error type so the typed
/// (`object_store::Error`) and raw ([`crate::RawError`]) surfaces share ONE
/// breaker/retry implementation — never parallel copies (issue #189).
async fn guard_generic<T, E, F, Fut>(
    breaker: &CircuitBreaker,
    retry: &RetryPolicy,
    metrics: &RetryMetrics,
    classify_error: fn(&E) -> Retryability,
    shed: impl FnOnce() -> E,
    op: F,
) -> std::result::Result<T, E>
where
    F: Fn() -> Fut,
    Fut: Future<Output = std::result::Result<T, E>>,
{
    let ticket = match breaker.admit() {
        Admission::Proceed(ticket) => ticket,
        Admission::Reject => return Err(shed()),
    };
    let result = retry_generic(retry, metrics, classify_error, op).await;
    // A terminal error (404/403) means the origin answered — healthy for
    // the breaker; only a transient error (or a shed) is unhealthy.
    let healthy = match &result {
        Ok(_) => true,
        Err(e) => matches!(classify_error(e), Retryability::No),
    };
    breaker.record(ticket, healthy);
    result
}

/// The retry loop: attempt `op`; on a transient error, back off (honouring a
/// `Retry-After` hint) and retry until `max_retries` or the wall-clock budget
/// is spent, then return the origin's own last error. The backoff sleep holds
/// no limiter permit — that is the whole point of sitting above the limiter.
async fn retry_generic<T, E, F, Fut>(
    retry: &RetryPolicy,
    metrics: &RetryMetrics,
    classify_error: fn(&E) -> Retryability,
    op: F,
) -> std::result::Result<T, E>
where
    F: Fn() -> Fut,
    Fut: Future<Output = std::result::Result<T, E>>,
{
    let start = Instant::now();
    let budget = Duration::from_millis(retry.budget_ms);
    let mut backoff = Backoff::new(retry.initial_backoff_ms, retry.max_backoff_ms);
    let mut retries_left = retry.max_retries;
    loop {
        match op().await {
            Ok(value) => return Ok(value),
            Err(err) => {
                let Retryability::Yes { retry_after } = classify_error(&err) else {
                    return Err(err);
                };
                if retries_left == 0 || start.elapsed() >= budget {
                    return Err(err);
                }
                let wait = retry_after.unwrap_or_else(|| backoff.next());
                // Never sleep past the budget.
                let remaining = budget.saturating_sub(start.elapsed());
                let wait = wait.min(remaining);
                if wait.is_zero() {
                    return Err(err);
                }
                tokio::time::sleep(wait).await;
                retries_left -= 1;
                metrics.retries.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

#[async_trait]
impl ObjectStore for ResilientStore {
    /// A guarded whole-object PUT.
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.guard(|| self.inner.put_opts(location, payload.clone(), opts.clone()))
            .await
    }

    /// A guarded high-level multipart create (retried; the returned handle's own
    /// part PUTs are the inner store's concern).
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.guard(|| self.inner.put_multipart_opts(location, opts.clone()))
            .await
    }

    /// A guarded GET. The retry re-issues the request on a transient failure of
    /// the *header* fetch; once the body stream is returned, mid-stream errors
    /// are the caller's to observe (the limiter still holds the permit for the
    /// stream's life). This is the documented boundary of trait-level retry.
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.guard(|| self.inner.get_opts(location, options.clone()))
            .await
    }

    /// A guarded coalesced multi-range read — the primary fill primitive.
    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        self.guard(|| self.inner.get_ranges(location, ranges)).await
    }

    /// LIST streams straight through the limiter: it is a paginated multi-request
    /// stream, not a unary op the retry/breaker logic models.
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    /// A guarded delimited LIST (materialized before return, so unary).
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.guard(|| self.inner.list_with_delimiter(prefix)).await
    }

    /// DELETE streams straight through the limiter (see [`list`](Self::list)).
    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    /// A guarded server-side copy.
    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.guard(|| self.inner.copy_opts(from, to, options.clone()))
            .await
    }
}

#[async_trait]
impl MultipartStore for ResilientStore {
    /// A guarded low-level multipart create.
    async fn create_multipart(&self, path: &Path) -> Result<MultipartId> {
        self.guard(|| self.inner.create_multipart(path)).await
    }

    /// A guarded multipart create with attributes.
    async fn create_multipart_opts(
        &self,
        path: &Path,
        opts: PutMultipartOptions,
    ) -> Result<MultipartId> {
        self.guard(|| self.inner.create_multipart_opts(path, opts.clone()))
            .await
    }

    /// A guarded part upload — the request that dominates a write's traffic.
    async fn put_part(
        &self,
        path: &Path,
        id: &MultipartId,
        part_idx: usize,
        data: PutPayload,
    ) -> Result<PartId> {
        self.guard(|| self.inner.put_part(path, id, part_idx, data.clone()))
            .await
    }

    /// A guarded multipart completion.
    async fn complete_multipart(
        &self,
        path: &Path,
        id: &MultipartId,
        parts: Vec<PartId>,
    ) -> Result<PutResult> {
        self.guard(|| self.inner.complete_multipart(path, id, parts.clone()))
            .await
    }

    /// A guarded multipart abort.
    async fn abort_multipart(&self, path: &Path, id: &MultipartId) -> Result<()> {
        self.guard(|| self.inner.abort_multipart(path, id)).await
    }
}

/// The raw request path behind the SAME per-bucket resilience stack as the
/// typed client (issue #189 review): every operation is admitted by the
/// bucket's shared [`CircuitBreaker`], retried under the shared
/// [`RetryPolicy`], and holds a permit from the bucket's shared concurrency
/// semaphore — the one [`crate::LimitedStore`] draws from, so raw and typed
/// requests count against ONE budget. A streamed GET's permit is held for the
/// body's life, matching the limiter's rule; the backoff sleep between
/// attempts holds none.
pub struct ResilientRawS3 {
    /// The byte-exact raw client.
    inner: Arc<crate::RawS3>,
    /// The bucket, for the shed-load error message.
    bucket: String,
    /// Retry/backoff tuning (the same policy the typed stack uses).
    retry: RetryPolicy,
    /// This bucket's circuit breaker — the SAME instance the typed stack
    /// records into, so both surfaces trip and recover together.
    breaker: Arc<CircuitBreaker>,
    /// This bucket's concurrency budget — the SAME semaphore the bucket's
    /// [`crate::LimitedStore`] acquires from.
    semaphore: Arc<tokio::sync::Semaphore>,
    /// Shared retry counter (aggregated at the registry).
    metrics: Arc<RetryMetrics>,
}

impl fmt::Debug for ResilientRawS3 {
    /// Names the bucket; the inner client holds credentials and stays opaque.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResilientRawS3")
            .field("bucket", &self.bucket)
            .finish_non_exhaustive()
    }
}

impl ResilientRawS3 {
    /// Wraps `inner` with the bucket's shared breaker, retry policy, and
    /// concurrency semaphore.
    pub(crate) fn new(
        inner: Arc<crate::RawS3>,
        bucket: String,
        retry: RetryPolicy,
        breaker: Arc<CircuitBreaker>,
        semaphore: Arc<tokio::sync::Semaphore>,
        metrics: Arc<RetryMetrics>,
    ) -> Self {
        ResilientRawS3 {
            inner,
            bucket,
            retry,
            breaker,
            semaphore,
            metrics,
        }
    }

    /// The load-shed error for this bucket (same message shape as the typed
    /// stack's [`CircuitOpen`], so operators see one vocabulary).
    fn shed(&self) -> crate::RawError {
        crate::RawError::Backend(format!(
            "circuit breaker open for bucket `{}`: origin is shedding load",
            self.bucket
        ))
    }

    /// Runs `op` under the shared breaker + retry loop. Each attempt must
    /// acquire its own permit inside `op`, so a backoff sleep never parks one.
    async fn guard<T, F, Fut>(&self, op: F) -> std::result::Result<T, crate::RawError>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = std::result::Result<T, crate::RawError>>,
    {
        guard_generic(
            &self.breaker,
            &self.retry,
            &self.metrics,
            crate::retry::classify_raw,
            || self.shed(),
            op,
        )
        .await
    }

    /// Acquires one owned permit from the bucket's shared budget. The
    /// semaphore is never closed, so acquisition cannot fail.
    async fn permit(&self) -> tokio::sync::OwnedSemaphorePermit {
        self.semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("backend concurrency semaphore is never closed")
    }

    /// A guarded, budgeted HEAD.
    pub async fn head(
        &self,
        key: &str,
    ) -> std::result::Result<crate::RawObjectMeta, crate::RawError> {
        self.guard(|| async {
            let _permit = self.permit().await;
            self.inner.head(key).await
        })
        .await
    }

    /// A guarded, budgeted conditional HEAD (`If-None-Match`), for #14
    /// revalidation. The origin's 304 is a terminal, healthy outcome.
    pub async fn head_if_none_match(
        &self,
        key: &str,
        etag: &str,
    ) -> std::result::Result<crate::RawObjectMeta, crate::RawError> {
        self.guard(|| async {
            let _permit = self.permit().await;
            self.inner.head_if_none_match(key, etag).await
        })
        .await
    }

    /// A guarded, budgeted GET whose permit stays held for the body stream's
    /// life — an in-flight read is not complete until its bytes have drained,
    /// exactly the limiter's rule for typed GETs.
    pub async fn get(
        &self,
        key: &str,
        range: crate::RawRange,
    ) -> std::result::Result<crate::RawGet, crate::RawError> {
        self.guard(|| async {
            let permit = self.permit().await;
            let got = self.inner.get(key, range).await?;
            Ok(hold_raw_permit(got, permit))
        })
        .await
    }

    /// A guarded, budgeted PUT of one materialized slice. `Bytes` chunks clone
    /// by reference count, so a retry re-issues the same bytes without
    /// copying.
    pub async fn put(
        &self,
        key: &str,
        headers: crate::RawWriteHeaders,
        chunks: Vec<bytes::Bytes>,
    ) -> std::result::Result<crate::RawPutOutcome, crate::RawError> {
        self.guard(|| async {
            let _permit = self.permit().await;
            self.inner.put(key, headers.clone(), chunks.clone()).await
        })
        .await
    }

    /// A guarded, budgeted DELETE.
    pub async fn delete(&self, key: &str) -> std::result::Result<(), crate::RawError> {
        self.guard(|| async {
            let _permit = self.permit().await;
            self.inner.delete(key).await
        })
        .await
    }

    /// A guarded, budgeted multipart create carrying the metadata (and any
    /// checksum selection) headers (issues #143/#208).
    pub async fn create_multipart(
        &self,
        key: &str,
        headers: crate::RawWriteHeaders,
    ) -> std::result::Result<crate::RawMultipartCreation, crate::RawError> {
        self.guard(|| async {
            let _permit = self.permit().await;
            self.inner.create_multipart(key, headers.clone()).await
        })
        .await
    }

    /// A guarded, budgeted part upload forwarding any per-part checksum (#208).
    pub async fn put_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: usize,
        checksum: crate::RawWriteChecksum,
        chunks: Vec<bytes::Bytes>,
    ) -> std::result::Result<crate::RawPartUpload, crate::RawError> {
        self.guard(|| async {
            let _permit = self.permit().await;
            self.inner
                .put_part(
                    key,
                    upload_id,
                    part_number,
                    checksum.clone(),
                    chunks.clone(),
                )
                .await
        })
        .await
    }

    /// A guarded, budgeted multipart completion carrying the per-part and
    /// object-level checksums the client asserted (issue #208).
    pub async fn complete_multipart(
        &self,
        key: &str,
        upload_id: &str,
        parts: Vec<crate::RawCompletedPart>,
        object_checksum: crate::RawWriteChecksum,
    ) -> std::result::Result<crate::RawPutOutcome, crate::RawError> {
        self.guard(|| async {
            let _permit = self.permit().await;
            // RawCompletedPart is not Clone (holds owned strings); rebuild the
            // manifest for each guarded attempt from borrowed data.
            let parts = parts
                .iter()
                .map(|p| crate::RawCompletedPart {
                    e_tag: p.e_tag.clone(),
                    checksums: p.checksums.clone(),
                })
                .collect();
            self.inner
                .complete_multipart(key, upload_id, parts, object_checksum.clone())
                .await
        })
        .await
    }

    /// A guarded, budgeted multipart abort.
    pub async fn abort_multipart(
        &self,
        key: &str,
        upload_id: &str,
    ) -> std::result::Result<(), crate::RawError> {
        self.guard(|| async {
            let _permit = self.permit().await;
            self.inner.abort_multipart(key, upload_id).await
        })
        .await
    }

    /// A guarded, budgeted verbatim bucket-level forward for the unmodeled-
    /// operation passthrough (issue #152). Buffered before return, so the permit
    /// is held only for the request's duration. A forwarded 4xx/5xx is the
    /// origin's own answer returned as `Ok`, so it does not trip the breaker.
    pub async fn forward_bucket(
        &self,
        method: &str,
        query: &str,
    ) -> std::result::Result<crate::RawForwardResponse, crate::RawError> {
        self.guard(|| async {
            let _permit = self.permit().await;
            self.inner.forward_bucket(method, query).await
        })
        .await
    }

    /// A guarded, budgeted ListObjectsV2 page (materialized before return, so
    /// the permit is held only for the request's duration).
    pub async fn list_v2(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        continuation: Option<&str>,
        max_keys: usize,
    ) -> std::result::Result<crate::RawListPage, crate::RawError> {
        self.guard(|| async {
            let _permit = self.permit().await;
            self.inner
                .list_v2(prefix, start_after, continuation, max_keys)
                .await
        })
        .await
    }

    /// A guarded, budgeted GET carrying the extra read params (version ID, part
    /// number, checksum mode). The permit stays held for the body stream's life
    /// exactly like [`ResilientRawS3::get`] (issues #153, #154, #156).
    pub async fn get_ext(
        &self,
        key: &str,
        range: crate::RawRange,
        extras: &crate::RawReadExtras,
    ) -> std::result::Result<crate::RawGet, crate::RawError> {
        self.guard(|| async {
            let permit = self.permit().await;
            let got = self.inner.get_ext(key, range, extras).await?;
            Ok(hold_raw_permit(got, permit))
        })
        .await
    }

    /// A guarded, budgeted HEAD carrying the extra read params (issues #153,
    /// #154, #156).
    pub async fn head_ext(
        &self,
        key: &str,
        extras: &crate::RawReadExtras,
    ) -> std::result::Result<crate::RawObjectMeta, crate::RawError> {
        self.guard(|| async {
            let _permit = self.permit().await;
            self.inner.head_ext(key, extras).await
        })
        .await
    }

    /// A guarded, budgeted UploadPartCopy (issue #153).
    pub async fn upload_part_copy(
        &self,
        dest_key: &str,
        upload_id: &str,
        part_number: u16,
        copy_source: &str,
        copy_range: Option<&str>,
    ) -> std::result::Result<(Option<String>, Option<std::time::SystemTime>), crate::RawError> {
        self.guard(|| async {
            let _permit = self.permit().await;
            self.inner
                .upload_part_copy(dest_key, upload_id, part_number, copy_source, copy_range)
                .await
        })
        .await
    }

    /// A guarded, budgeted ListMultipartUploads page (issue #153).
    pub async fn list_multipart_uploads(
        &self,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
        max_uploads: usize,
    ) -> std::result::Result<crate::RawUploadsPage, crate::RawError> {
        self.guard(|| async {
            let _permit = self.permit().await;
            self.inner
                .list_multipart_uploads(
                    prefix,
                    delimiter,
                    key_marker,
                    upload_id_marker,
                    max_uploads,
                )
                .await
        })
        .await
    }

    /// A guarded, budgeted GetObjectAttributes request (issue #154).
    pub async fn get_object_attributes(
        &self,
        key: &str,
        object_attributes: &[String],
        max_parts: Option<i32>,
        part_number_marker: Option<i32>,
        version_id: Option<&str>,
    ) -> std::result::Result<crate::RawAttributes, crate::RawError> {
        self.guard(|| async {
            let _permit = self.permit().await;
            self.inner
                .get_object_attributes(
                    key,
                    object_attributes,
                    max_parts,
                    part_number_marker,
                    version_id,
                )
                .await
        })
        .await
    }
}

/// Re-attaches `permit` to a raw GET's body stream so the shared concurrency
/// slot stays reserved until the stream is fully drained or dropped (the raw
/// twin of the limiter's `hold_permit`).
fn hold_raw_permit(got: crate::RawGet, permit: tokio::sync::OwnedSemaphorePermit) -> crate::RawGet {
    use futures::StreamExt;
    let body = futures::stream::unfold((got.body, permit), |(mut stream, permit)| async move {
        // The permit lives inside the unfold state: when the stream ends (or
        // is dropped) the state drops and the slot frees.
        stream.next().await.map(|chunk| (chunk, (stream, permit)))
    })
    .boxed();
    crate::RawGet {
        meta: got.meta,
        range: got.range,
        body,
    }
}
