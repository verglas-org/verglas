//! The per-bucket concurrency limiter (issue #18): with a budget of N, at most
//! N backend requests may be in flight at once, and the (N+1)th call blocks
//! until an earlier one finishes. Driven through a fake store that parks inside
//! a guarded operation so concurrency is directly observable.

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

use verglas_backend::LimitedStore;

/// The `NotImplemented` error the fake returns for every operation the limiter
/// test does not exercise.
fn not_impl(op: &'static str) -> object_store::Error {
    object_store::Error::NotImplemented {
        operation: op.to_string(),
        implementer: "BlockingStore".to_string(),
    }
}

/// A backend store whose `copy` parks until the test opens `gate`, recording
/// how many copies are in flight at once. Every other method is unused and
/// returns `NotImplemented`.
#[derive(Debug)]
struct BlockingStore {
    /// Copies currently parked inside `copy_opts`.
    in_flight: Arc<AtomicUsize>,
    /// High-water mark of `in_flight` — the observed peak concurrency.
    max_seen: Arc<AtomicUsize>,
    /// Released by the test to let parked copies finish; starts with 0 permits.
    gate: Arc<Semaphore>,
}

impl fmt::Display for BlockingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BlockingStore")
    }
}

#[async_trait]
impl ObjectStore for BlockingStore {
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

    /// Parks the caller — holding its limiter permit — until the test opens the
    /// gate, tracking peak concurrency while parked.
    async fn copy_opts(&self, _: &Path, _: &Path, _: CopyOptions) -> Result<()> {
        let now = self.in_flight.fetch_add(1, SeqCst) + 1;
        self.max_seen.fetch_max(now, SeqCst);
        let permit = self.gate.acquire().await.expect("gate never closes");
        permit.forget();
        self.in_flight.fetch_sub(1, SeqCst);
        Ok(())
    }
}

/// The fake is never driven through the multipart API; these exist only so it
/// satisfies `MultipartObjectStore`, which the limiter wraps.
#[async_trait]
impl MultipartStore for BlockingStore {
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

/// Fans `count` concurrent copies through a limiter with the given budget,
/// returns (in_flight, max_seen, gate, task handles).
fn fan_out(
    budget: usize,
    count: usize,
) -> (
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
    Arc<Semaphore>,
    Vec<tokio::task::JoinHandle<()>>,
) {
    let in_flight = Arc::new(AtomicUsize::new(0));
    let max_seen = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(Semaphore::new(0));
    let fake = Arc::new(BlockingStore {
        in_flight: in_flight.clone(),
        max_seen: max_seen.clone(),
        gate: gate.clone(),
    });
    let store = Arc::new(LimitedStore::new(fake, budget));
    let handles = (0..count)
        .map(|i| {
            let store = store.clone();
            tokio::spawn(async move {
                store
                    .copy(&Path::from(format!("a{i}")), &Path::from(format!("b{i}")))
                    .await
                    .expect("copy succeeds");
            })
        })
        .collect();
    (in_flight, max_seen, gate, handles)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn limiter_bounds_in_flight_requests_to_the_budget() {
    // Budget 2, four callers: two get in, the other two must wait.
    let (in_flight, max_seen, gate, handles) = fan_out(2, 4);

    wait_for(&in_flight, 2).await;
    // Let any escapee slip past before asserting the ceiling holds.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        in_flight.load(SeqCst),
        2,
        "the 3rd and 4th requests must block outside the store"
    );

    // Open the gate; everything drains and peak concurrency never exceeded 2.
    gate.add_permits(4);
    for handle in handles {
        handle.await.expect("task joins");
    }
    assert_eq!(
        max_seen.load(SeqCst),
        2,
        "peak concurrency equals the budget"
    );
    assert_eq!(in_flight.load(SeqCst), 0, "everything drained");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn limiter_admits_up_to_the_budget_concurrently() {
    // A larger budget must actually let more run at once — proving the bound is
    // the semaphore value, not an incidental serialization.
    let (in_flight, max_seen, gate, handles) = fan_out(4, 4);

    wait_for(&in_flight, 4).await;
    assert_eq!(in_flight.load(SeqCst), 4, "budget of 4 admits all four");

    gate.add_permits(4);
    for handle in handles {
        handle.await.expect("task joins");
    }
    assert_eq!(max_seen.load(SeqCst), 4);
}
