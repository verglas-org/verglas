//! Retry/backoff + breaker behaviour end-to-end through the registry (issue
//! #20). A scripted fake origin fails a configured number of times (throttle,
//! not-found) then succeeds, so we can pin: transient errors are retried with
//! backoff; a Retry-After hint is honoured; non-retryable 4xx is returned at
//! once; the retry budget yields a clean error; the breaker trips and misses
//! fail fast; and a request's backoff sleep does NOT hold a limiter permit.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::multipart::{MultipartStore, PartId};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};

use verglas_backend::{BackendStore, BackendStores, MultipartObjectStore, ThrottleHint};
use verglas_core::config::{BreakerPolicy, RetryPolicy};

/// One scripted response from the fake origin for a `get_ranges` call.
#[derive(Clone)]
enum Step {
    /// Succeed, returning one 3-byte blob per requested range.
    Ok,
    /// A transient throttle, optionally carrying a Retry-After hint.
    Throttle(Option<Duration>),
    /// A hard 404 — the non-retryable case.
    NotFound,
}

/// Per-key script: a queue of steps, then `repeat` once the queue drains.
struct KeyScript {
    steps: VecDeque<Step>,
    repeat: Step,
}

/// A fake origin whose `get_ranges` follows a per-key script, counting total
/// calls so a test can assert how many attempts the retry layer made.
struct FakeOrigin {
    scripts: std::sync::Mutex<HashMap<String, KeyScript>>,
    calls: Arc<AtomicUsize>,
}

impl fmt::Debug for FakeOrigin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FakeOrigin")
    }
}

impl fmt::Display for FakeOrigin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FakeOrigin")
    }
}

impl FakeOrigin {
    /// Builds a fake with the given per-key scripts and a shared call counter.
    fn new(scripts: HashMap<String, KeyScript>, calls: Arc<AtomicUsize>) -> Arc<Self> {
        Arc::new(FakeOrigin {
            scripts: std::sync::Mutex::new(scripts),
            calls,
        })
    }

    /// Pops the next step for `key`, or its repeat step once the queue drains.
    fn next_step(&self, key: &str) -> Step {
        let mut scripts = self.scripts.lock().expect("scripts lock");
        match scripts.get_mut(key) {
            Some(script) => script
                .steps
                .pop_front()
                .unwrap_or_else(|| script.repeat.clone()),
            None => Step::Ok,
        }
    }
}

/// The `NotImplemented` error for methods a test never drives.
fn not_impl(op: &'static str) -> object_store::Error {
    object_store::Error::NotImplemented {
        operation: op.to_string(),
        implementer: "FakeOrigin".to_string(),
    }
}

#[async_trait]
impl ObjectStore for FakeOrigin {
    async fn put_opts(&self, _: &Path, _: PutPayload, _: PutOptions) -> Result<PutResult> {
        Err(not_impl("put_opts"))
    }

    async fn put_multipart_opts(
        &self,
        _: &Path,
        _: PutMultipartOptions,
    ) -> Result<Box<dyn object_store::MultipartUpload>> {
        Err(not_impl("put_multipart_opts"))
    }

    async fn get_opts(&self, _: &Path, _: GetOptions) -> Result<GetResult> {
        Err(not_impl("get_opts"))
    }

    /// The scripted fill primitive: each call consumes one step for the key.
    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        self.calls.fetch_add(1, SeqCst);
        match self.next_step(location.as_ref()) {
            Step::Ok => Ok(ranges.iter().map(|_| Bytes::from_static(b"xyz")).collect()),
            Step::Throttle(retry_after) => Err(object_store::Error::Generic {
                store: "fake",
                source: Box::new(ThrottleHint { retry_after }),
            }),
            Step::NotFound => Err(object_store::Error::NotFound {
                path: location.to_string(),
                source: Box::new(ThrottleHint { retry_after: None }),
            }),
        }
    }

    fn delete_stream(
        &self,
        _: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        futures::stream::once(async { Err(not_impl("delete_stream")) }).boxed()
    }

    fn list(&self, _: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        futures::stream::once(async { Err(not_impl("list")) }).boxed()
    }

    async fn list_with_delimiter(&self, _: Option<&Path>) -> Result<ListResult> {
        Err(not_impl("list_with_delimiter"))
    }

    async fn copy_opts(&self, _: &Path, _: &Path, _: CopyOptions) -> Result<()> {
        Err(not_impl("copy_opts"))
    }
}

#[async_trait]
impl MultipartStore for FakeOrigin {
    async fn create_multipart(&self, _: &Path) -> Result<MultipartId> {
        Err(not_impl("create_multipart"))
    }

    async fn put_part(&self, _: &Path, _: &MultipartId, _: usize, _: PutPayload) -> Result<PartId> {
        Err(not_impl("put_part"))
    }

    async fn complete_multipart(
        &self,
        _: &Path,
        _: &MultipartId,
        _: Vec<PartId>,
    ) -> Result<PutResult> {
        Err(not_impl("complete_multipart"))
    }

    async fn abort_multipart(&self, _: &Path, _: &MultipartId) -> Result<()> {
        Err(not_impl("abort_multipart"))
    }
}

/// A retry policy with negligible backoff so tests run fast, plus the caller's
/// `max_retries`/`budget_ms`.
fn fast_retry(max_retries: usize, budget_ms: u64) -> RetryPolicy {
    RetryPolicy {
        max_retries,
        initial_backoff_ms: 1,
        max_backoff_ms: 2,
        budget_ms,
    }
}

/// A breaker that never trips (huge sample floor), so retry tests see retry
/// behaviour in isolation.
fn inert_breaker() -> BreakerPolicy {
    BreakerPolicy {
        failure_rate: 1.0,
        min_samples: 1_000_000,
        cooldown_ms: 1_000,
        half_open_max_probes: 1,
    }
}

/// Builds a single-bucket store (budget-1 limiter unless overridden) for bucket
/// `bucket`, served by `fake`.
fn registry_with(
    fake: Arc<FakeOrigin>,
    concurrency: usize,
    retry: RetryPolicy,
    breaker: BreakerPolicy,
) -> Arc<BackendStore> {
    BackendStore::with_factory(
        "default",
        "bucket".to_owned(),
        concurrency,
        retry,
        breaker,
        move |_bucket| Ok(fake.clone() as Arc<dyn MultipartObjectStore>),
    )
    .expect("store builds")
}

/// A single-range request (a 3-byte read) built without a range-literal array,
/// which clippy flags as a likely typo.
fn one_range() -> Vec<Range<u64>> {
    std::iter::once(0..3).collect()
}

/// One key's script.
fn script(steps: Vec<Step>, repeat: Step) -> KeyScript {
    KeyScript {
        steps: steps.into(),
        repeat,
    }
}

#[tokio::test]
async fn retries_a_transient_error_then_succeeds() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut m = HashMap::new();
    m.insert(
        "k".to_string(),
        script(vec![Step::Throttle(None), Step::Throttle(None)], Step::Ok),
    );
    let fake = FakeOrigin::new(m, calls.clone());
    let registry = registry_with(fake, 4, fast_retry(3, 10_000), inert_breaker());

    let store = registry.store_for("default", "bucket").expect("store");
    let out = store.get_ranges(&Path::from("k"), &one_range()).await;
    assert!(
        out.is_ok(),
        "two throttles then success must succeed: {out:?}"
    );
    assert_eq!(
        calls.load(SeqCst),
        3,
        "two failures + one success = 3 attempts"
    );
}

#[tokio::test]
async fn a_non_retryable_error_is_returned_immediately() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut m = HashMap::new();
    m.insert("k".to_string(), script(vec![Step::NotFound], Step::Ok));
    let fake = FakeOrigin::new(m, calls.clone());
    let registry = registry_with(fake, 4, fast_retry(5, 10_000), inert_breaker());

    let store = registry.store_for("default", "bucket").expect("store");
    let err = store
        .get_ranges(&Path::from("k"), &one_range())
        .await
        .expect_err("404 must not be retried");
    assert!(
        matches!(err, object_store::Error::NotFound { .. }),
        "got {err:?}"
    );
    assert_eq!(calls.load(SeqCst), 1, "a 404 is returned on the first try");
}

#[tokio::test]
async fn a_retry_after_hint_is_honoured() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut m = HashMap::new();
    // One throttle that asks for a 60 ms wait, then success.
    m.insert(
        "k".to_string(),
        script(
            vec![Step::Throttle(Some(Duration::from_millis(60)))],
            Step::Ok,
        ),
    );
    let fake = FakeOrigin::new(m, calls.clone());
    // initial_backoff is 1 ms; if the hint is ignored the wait would be ~1 ms.
    let registry = registry_with(fake, 4, fast_retry(3, 10_000), inert_breaker());

    let store = registry.store_for("default", "bucket").expect("store");
    let started = Instant::now();
    store
        .get_ranges(&Path::from("k"), &one_range())
        .await
        .expect("succeeds after wait");
    let waited = started.elapsed();
    assert!(
        waited >= Duration::from_millis(50),
        "Retry-After must dominate the tiny backoff, waited {waited:?}"
    );
}

#[tokio::test]
async fn exhausting_the_retry_budget_returns_a_clean_error() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut m = HashMap::new();
    m.insert("k".to_string(), script(vec![], Step::Throttle(None)));
    let fake = FakeOrigin::new(m, calls.clone());
    let registry = registry_with(fake, 4, fast_retry(2, 10_000), inert_breaker());

    let store = registry.store_for("default", "bucket").expect("store");
    let err = store
        .get_ranges(&Path::from("k"), &one_range())
        .await
        .expect_err("a persistently throttling origin exhausts the budget");
    // The surfaced error is the origin's own, not a panic or a hang.
    assert!(
        matches!(err, object_store::Error::Generic { .. }),
        "got {err:?}"
    );
    assert_eq!(calls.load(SeqCst), 3, "first try + 2 retries = 3 attempts");
}

#[tokio::test]
async fn the_time_budget_bounds_the_retries() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut m = HashMap::new();
    m.insert("k".to_string(), script(vec![], Step::Throttle(None)));
    let fake = FakeOrigin::new(m, calls.clone());
    // Many retries allowed, but a 30 ms wall-clock budget with 10 ms backoffs.
    let retry = RetryPolicy {
        max_retries: 1_000,
        initial_backoff_ms: 10,
        max_backoff_ms: 10,
        budget_ms: 30,
    };
    let registry = registry_with(fake, 4, retry, inert_breaker());

    let store = registry.store_for("default", "bucket").expect("store");
    let started = Instant::now();
    let err = store.get_ranges(&Path::from("k"), &one_range()).await;
    assert!(err.is_err(), "budget exhausted → error");
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "the budget bounds the wait"
    );
    assert!(
        calls.load(SeqCst) < 100,
        "the time budget stopped retrying well before max_retries, got {}",
        calls.load(SeqCst)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_backoff_sleep_does_not_hold_a_limiter_permit() {
    // Budget of ONE permit for the bucket. Key "a" throttles once then succeeds
    // after a long (300 ms) backoff; key "b" always succeeds. If the retry held
    // its permit across the backoff sleep, "b" would block ~300 ms; because the
    // retry sits ABOVE the limiter, "a" releases its permit while sleeping and
    // "b" sails through.
    let calls = Arc::new(AtomicUsize::new(0));
    let mut m = HashMap::new();
    m.insert(
        "a".to_string(),
        script(vec![Step::Throttle(None)], Step::Ok),
    );
    m.insert("b".to_string(), script(vec![], Step::Ok));
    let fake = FakeOrigin::new(m, calls.clone());
    let retry = RetryPolicy {
        max_retries: 3,
        initial_backoff_ms: 300,
        max_backoff_ms: 300,
        budget_ms: 10_000,
    };
    let registry = registry_with(fake, 1, retry, inert_breaker());
    let store = registry.store_for("default", "bucket").expect("store");

    // Fire "a": it will fail once, then sleep ~300 ms in backoff (no permit).
    let a_store = store.clone();
    let a = tokio::spawn(async move {
        a_store
            .get_ranges(&Path::from("a"), &one_range())
            .await
            .expect("a eventually succeeds")
    });

    // Give "a" a moment to hit its first failure and enter backoff.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // "b" must not be starved by "a"'s backoff.
    let started = Instant::now();
    store
        .get_ranges(&Path::from("b"), &one_range())
        .await
        .expect("b succeeds");
    let b_latency = started.elapsed();
    assert!(
        b_latency < Duration::from_millis(150),
        "b blocked {b_latency:?} — the backoff sleep is holding the permit"
    );

    a.await.expect("join a");
}

#[tokio::test]
async fn an_open_breaker_fails_misses_fast() {
    // A breaker that trips quickly: 4 failures at 100% opens it. Once open, the
    // store short-circuits misses without touching the origin.
    let calls = Arc::new(AtomicUsize::new(0));
    let mut m = HashMap::new();
    m.insert("k".to_string(), script(vec![], Step::Throttle(None)));
    let fake = FakeOrigin::new(m, calls.clone());
    let breaker = BreakerPolicy {
        failure_rate: 0.5,
        min_samples: 4,
        cooldown_ms: 60_000,
        half_open_max_probes: 1,
    };
    // No retries, so each request is exactly one origin attempt → one outcome.
    let registry = registry_with(fake, 4, fast_retry(0, 10_000), breaker);
    let store = registry.store_for("default", "bucket").expect("store");

    // Drive four failing misses to trip the breaker.
    for _ in 0..4 {
        let _ = store.get_ranges(&Path::from("k"), &one_range()).await;
    }
    let calls_after_trip = calls.load(SeqCst);
    assert_eq!(
        calls_after_trip, 4,
        "four attempts reached the origin before tripping"
    );

    // The next miss is shed: it fails fast with a clear error and never touches
    // the origin (the call counter does not move).
    let err = store
        .get_ranges(&Path::from("k"), &one_range())
        .await
        .expect_err("an open breaker sheds the miss");
    assert!(
        err.to_string().contains("circuit breaker open"),
        "the error must name the open breaker, got: {err}"
    );
    assert_eq!(
        calls.load(SeqCst),
        calls_after_trip,
        "a shed miss must not reach the origin"
    );

    // The breaker integration point is visible for other subsystems (e.g. #51 prefetch).
    let seam = registry
        .breaker_for("default", "bucket")
        .expect("binding")
        .expect("breaker exposed");
    assert_eq!(seam.state(), verglas_backend::BreakerState::Open);
}
