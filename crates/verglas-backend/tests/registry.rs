//! The backend store (#235): the store serves a configured SET of buckets — a
//! single bucket and/or a set of glob patterns — building each bucket's client
//! lazily on first request and memoizing it, enforcing that bucket's own
//! concurrency budget, and rejecting any bucket outside the set with
//! `NoSuchBucket`. Per-bucket budgets are isolated: a burst against one served
//! bucket must not exhaust another's permits.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::multipart::{MultipartStore, PartId};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};
use tokio::sync::Semaphore;

use verglas_backend::{BackendError, BackendStore, BackendStores, MultipartObjectStore};
use verglas_core::config::{BreakerPolicy, RetryPolicy};

/// Default retry policy — these tests exercise construction/limiter behaviour,
/// not retry (covered in `resilience.rs`).
fn retry() -> RetryPolicy {
    RetryPolicy::default()
}

/// Default breaker policy — its high sample floor means it never trips here.
fn breaker() -> BreakerPolicy {
    BreakerPolicy::default()
}

/// The `NotImplemented` error the fake returns for operations a test does not
/// exercise.
fn not_impl(op: &'static str) -> object_store::Error {
    object_store::Error::NotImplemented {
        operation: op.to_string(),
        implementer: "GatedStore".to_string(),
    }
}

/// A backend store whose `copy` parks until its `gate` opens, tracking how many
/// copies are parked at once — used to observe the bucket's concurrency budget.
#[derive(Debug)]
struct GatedStore {
    /// Copies currently parked inside `copy_opts`.
    in_flight: Arc<AtomicUsize>,
    /// Released by the test to let parked copies finish; a closed gate (0
    /// permits) keeps them parked, holding their limiter permit.
    gate: Arc<Semaphore>,
}

impl fmt::Display for GatedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GatedStore")
    }
}

#[async_trait]
impl ObjectStore for GatedStore {
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

    /// Parks the caller — holding its limiter permit — until the gate opens.
    async fn copy_opts(&self, _: &Path, _: &Path, _: CopyOptions) -> Result<()> {
        self.in_flight.fetch_add(1, SeqCst);
        let permit = self.gate.acquire().await.expect("gate never closes");
        permit.forget();
        self.in_flight.fetch_sub(1, SeqCst);
        Ok(())
    }
}

#[async_trait]
impl MultipartStore for GatedStore {
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

/// Spins until `counter` reaches `target`, or panics after ~2s so a broken
/// limiter fails the test instead of hanging it.
async fn wait_for(counter: &Arc<AtomicUsize>, target: usize) {
    for _ in 0..2000 {
        if counter.load(SeqCst) >= target {
            return;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    panic!(
        "counter stalled at {} waiting for {target}",
        counter.load(SeqCst)
    );
}

#[tokio::test]
async fn serves_the_configured_bucket_and_rejects_others() {
    // The store is built for one bucket; that bucket resolves, every other
    // bucket is NoSuchBucket. The per-bucket client is memoized (one shared
    // handle across calls).
    let store = BackendStore::single(
        "default",
        "alpha",
        Arc::new(object_store::memory::InMemory::new()),
    )
    .clone();
    let s1 = store
        .store_for("default", "alpha")
        .expect("configured bucket serves");
    let s2 = store
        .store_for("default", "alpha")
        .expect("same store handed back");
    assert!(
        Arc::ptr_eq(&s1, &s2),
        "the one bucket's client is memoized to a single shared handle"
    );

    let err = store
        .store_for("default", "beta")
        .expect_err("an unserved bucket is rejected");
    assert!(
        matches!(err, BackendError::NoSuchBucket { .. }),
        "got: {err:?}"
    );
    // The raw path for an unserved bucket is NoSuchBucket too.
    let raw_err = store
        .raw_for("default", "beta")
        .expect_err("raw for an unserved bucket is rejected");
    assert!(
        matches!(raw_err, BackendError::NoSuchBucket { .. }),
        "got: {raw_err:?}"
    );
    // A breaker exists for the configured bucket, and none for any other.
    assert!(
        store
            .breaker_for("default", "alpha")
            .expect("binding")
            .is_some()
    );
    assert!(
        store
            .breaker_for("default", "beta")
            .expect("binding")
            .is_none()
    );
}

#[tokio::test]
async fn glob_set_serves_matches_lazily_and_rejects_non_matches() {
    // A store configured with a glob (`*--table-s3`) serves any matching bucket,
    // building its client lazily on first request and memoizing it, and rejects
    // a non-matching bucket without ever building a client for it. Builds are
    // counted to prove laziness and memoization.
    let builds = Arc::new(AtomicUsize::new(0));
    let store = {
        let b = builds.clone();
        BackendStore::with_glob_factory(
            "default",
            None,
            vec!["*--table-s3".to_owned()],
            4,
            retry(),
            breaker(),
            move |_bucket| {
                b.fetch_add(1, SeqCst);
                Ok(Arc::new(object_store::memory::InMemory::new())
                    as Arc<dyn MultipartObjectStore>)
            },
        )
    };

    // No bucket touched yet: nothing built.
    assert_eq!(
        builds.load(SeqCst),
        0,
        "no client built before first request"
    );

    // A non-matching bucket is rejected and builds nothing.
    let err = store
        .store_for("default", "plain-bucket")
        .expect_err("a bucket outside the glob set is rejected");
    assert!(
        matches!(err, BackendError::NoSuchBucket { .. }),
        "got: {err:?}"
    );
    assert_eq!(
        builds.load(SeqCst),
        0,
        "a rejected bucket must not build a client"
    );

    // A matching bucket builds once, then is memoized.
    let first = store
        .store_for("default", "abc--table-s3")
        .expect("a glob-matching bucket serves");
    let again = store
        .store_for("default", "abc--table-s3")
        .expect("memoized on the second call");
    assert!(
        Arc::ptr_eq(&first, &again),
        "the matching bucket's client is memoized"
    );
    assert_eq!(builds.load(SeqCst), 1, "built exactly once, then memoized");

    // A second, different matching bucket builds its own client.
    store
        .store_for("default", "def--table-s3")
        .expect("a second matching bucket serves");
    assert_eq!(builds.load(SeqCst), 2, "each distinct bucket builds once");

    // Breakers are per-bucket: each served bucket has one, non-matches have none.
    assert!(
        store
            .breaker_for("default", "abc--table-s3")
            .expect("binding")
            .is_some()
    );
    assert!(
        store
            .breaker_for("default", "def--table-s3")
            .expect("binding")
            .is_some()
    );
    assert!(
        store
            .breaker_for("default", "plain-bucket")
            .expect("binding")
            .is_none()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn per_bucket_budgets_are_isolated_across_matching_buckets() {
    // Two buckets match the same glob. Each carries its own single-permit
    // budget: saturating one bucket must not block a request on the other. A
    // regression that shared one budget across the set would deadlock the second
    // bucket's request behind the first bucket's parked copy.
    let in_flight = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(Semaphore::new(0)); // closed: parks the first copy
    let store = {
        let (if_, g) = (in_flight.clone(), gate.clone());
        BackendStore::with_glob_factory(
            "default",
            None,
            vec!["*--table-s3".to_owned()],
            1, // one permit PER bucket
            retry(),
            breaker(),
            move |_bucket| {
                Ok(Arc::new(GatedStore {
                    in_flight: if_.clone(),
                    gate: g.clone(),
                }) as Arc<dyn MultipartObjectStore>)
            },
        )
    };

    // Saturate bucket A: one copy parks, consuming A's single permit.
    let a = store
        .store_for("default", "a--table-s3")
        .expect("bucket a serves");
    {
        let a = a.clone();
        tokio::spawn(async move {
            let _ = a.copy(&Path::from("x"), &Path::from("y")).await;
        });
    }
    wait_for(&in_flight, 1).await;

    // A request on bucket B must proceed on its own permit, not block on A's.
    // Its copy parks too (the shared gate is closed), which is fine — the point
    // is that B *starts* (in_flight reaches 2), proving B has its own budget.
    let b = store
        .store_for("default", "b--table-s3")
        .expect("bucket b serves");
    {
        let b = b.clone();
        tokio::spawn(async move {
            let _ = b.copy(&Path::from("p"), &Path::from("q")).await;
        });
    }
    // If B shared A's saturated permit, this would stall and time out.
    wait_for(&in_flight, 2).await;
    assert_eq!(
        in_flight.load(SeqCst),
        2,
        "both buckets started under their own per-bucket permit"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_bucket_concurrency_budget_is_enforced() {
    // Budget of 1: the first copy parks holding the single permit, so a second
    // request against the same bucket cannot proceed until the gate opens.
    let in_flight = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(Semaphore::new(0)); // closed: parks the first copy

    let store = {
        let (if_, g) = (in_flight.clone(), gate.clone());
        BackendStore::with_factory(
            "default",
            "alpha".to_owned(),
            1,
            retry(),
            breaker(),
            move |_bucket| {
                Ok(Arc::new(GatedStore {
                    in_flight: if_.clone(),
                    gate: g.clone(),
                }) as Arc<dyn MultipartObjectStore>)
            },
        )
        .expect("store builds")
    };

    // Saturate the bucket: one copy parks, consuming the single permit.
    let client = store
        .store_for("default", "alpha")
        .expect("configured bucket serves");
    {
        let client = client.clone();
        tokio::spawn(async move {
            client
                .copy(&Path::from("x"), &Path::from("y"))
                .await
                .expect("the parked copy completes once its gate opens");
        });
    }
    wait_for(&in_flight, 1).await;

    // A second copy cannot start while the one permit is held: it must time out
    // rather than run. A regression (no limiter) would let it proceed.
    let second = tokio::time::timeout(
        Duration::from_millis(200),
        client.copy(&Path::from("p"), &Path::from("q")),
    )
    .await;
    assert!(
        second.is_err(),
        "a second request must block on the saturated single-permit budget"
    );
    assert_eq!(in_flight.load(SeqCst), 1, "still just the one parked copy");
}

#[tokio::test]
async fn construction_error_propagates() {
    let err = BackendStore::with_factory(
        "default",
        "nope".to_owned(),
        4,
        retry(),
        breaker(),
        |bucket| {
            Err(BackendError::Build {
                bucket: bucket.to_owned(),
                source: object_store::Error::NotImplemented {
                    operation: "build".to_owned(),
                    implementer: "test".to_owned(),
                },
            })
        },
    )
    .expect_err("construction fails");
    assert!(matches!(err, BackendError::Build { .. }), "got: {err:?}");
}
