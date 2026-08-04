//! The prefetch planner and the shared, budgeted, priority executor.
//!
//! On a compaction (`replace`) commit the planner reads each added file's footer
//! and scores its `(partition, column)` chunks against the heat ledger, so the
//! replacement files' *hot* chunks are pulled into cache before a query asks —
//! carrying warmth across the rewrite by logical identity. The executor drains
//! the plan under one byte budget (the [`TokenBucket`] it shares with the eager
//! warming pipeline, #168) and one organic-traffic yield rule, so aggressive
//! prefetch never steals politeness from live reads.
//!
//! # One executor, four producers
//!
//! A single [`PrefetchExecutor`] serves every speculative producer — organic
//! repair, compaction repair (this planner), onboarding warming (#168), and
//! prewarm manifests (#56) — through one priority queue
//! (`organic > compaction-repair > onboarding > prewarm`). One executor means
//! one budget and one place to reason about S3 politeness (it respects #20's
//! breaker at the reader beneath it). Migrating the warming footer loop onto
//! this executor's queue is a tracked follow-up; today warming already shares
//! the same [`TokenBucket`] instance, so the byte budget is already unified.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use verglas_core::CacheKey;
use verglas_core::read::ReadRange;

use crate::fetch::{MetadataFetch, ObjectPath};
use crate::iceberg::DataFileEntry;
use crate::lifecycle::diff::SnapshotDiff;
use crate::lifecycle::heat::{HeatKey, HeatLedger, PartitionHash};
use crate::mapper::{ColumnId, FileFormat, TableId};
use crate::parquet_meta;
use crate::warming::WarmSource;
use crate::warming::budget::TokenBucket;

/// The four producer priorities, ordered so the highest-value fills drain first.
/// Organic-triggered repair (a live miss demanding a sibling) beats
/// compaction-repair, which beats onboarding, which beats background prewarm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PrefetchPriority {
    /// Background manifest prewarm (#56) — lowest.
    Prewarm,
    /// Onboarding warming a newly watched table (#168).
    Onboarding,
    /// Repairing cache after a compaction commit (this planner, #51).
    CompactionRepair,
    /// A fill a live organic miss triggered — highest.
    Organic,
}

/// Tunables for prefetch planning and execution.
#[derive(Debug, Clone)]
pub struct PrefetchConfig {
    /// Whether prefetch is enabled. Off leaves the cache to re-earn its hit rate
    /// lazily (the feature-flag-off baseline the benchmark compares against).
    pub enabled: bool,
    /// Concurrent fill workers draining the queue.
    pub concurrency: usize,
    /// Speculative footer suffix window (bytes) for added Parquet files.
    pub footer_read_bytes: u64,
    /// Minimum heat score a `(partition, column)` must clear to be prefetched.
    /// `0.0` prefetches any column with positive heat (accessed pre-compaction).
    pub heat_threshold: f32,
    /// Organic in-flight fills tolerated before prefetch yields (the yield rule's
    /// `K`): prefetch waits for a slot the organic path holds.
    pub organic_yield_k: usize,
    /// Maximum queued fills before the executor drops the lowest-priority ones
    /// (a hard bound; losing a speculative fill costs warmth, not correctness).
    pub max_queue: usize,
}

impl Default for PrefetchConfig {
    /// Conservative, polite defaults: on, 32 workers, a 64 KiB footer window,
    /// prefetch any positively-hot column, yield at 32 organic in-flight, 64k
    /// queue depth.
    fn default() -> PrefetchConfig {
        PrefetchConfig {
            enabled: true,
            concurrency: 32,
            footer_read_bytes: 64 * 1024,
            heat_threshold: 0.0,
            organic_yield_k: 32,
            max_queue: 64 * 1024,
        }
    }
}

/// One fill the planner scheduled: a range of an object to pull through the
/// cache, with its estimated byte cost and (for chunks) its heat score.
#[derive(Debug, Clone, PartialEq)]
pub struct FillSpec {
    /// Bucket the object lives in.
    pub bucket: String,
    /// Object key to fill.
    pub key: String,
    /// The range to pull (a suffix for footers, a bounded range for chunks, full
    /// for a hot Avro file).
    pub range: ReadRange,
    /// Estimated bytes this fill pulls (charged against the byte budget).
    pub est_bytes: u64,
    /// The heat score that admitted this fill (`0.0` for a footer prefetch).
    pub score: f32,
}

/// A compaction-repair plan: footers of every added file first (tiny, always
/// worth it), then the added files' hot column chunks ordered by score, then
/// hot Avro whole-file prefetches in offset order.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PrefetchPlan {
    /// Footer suffix reads for added Parquet files (issued first).
    pub footers: Vec<FillSpec>,
    /// Hot column-chunk fills, ordered by descending heat score.
    pub chunks: Vec<FillSpec>,
    /// Hot Avro whole-file fills (no footer; sequential is the access shape).
    pub whole_files: Vec<FillSpec>,
}

impl PrefetchPlan {
    /// The fills in execution order: footers, then hot chunks, then hot Avro
    /// files. Flattened for submission to the executor.
    pub fn ordered(&self) -> Vec<FillSpec> {
        let mut out =
            Vec::with_capacity(self.footers.len() + self.chunks.len() + self.whole_files.len());
        out.extend(self.footers.iter().cloned());
        out.extend(self.chunks.iter().cloned());
        out.extend(self.whole_files.iter().cloned());
        out
    }

    /// Total fills across all phases.
    pub fn len(&self) -> usize {
        self.footers.len() + self.chunks.len() + self.whole_files.len()
    }

    /// Whether the plan schedules nothing.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Builds a compaction-repair plan for a `replace`-style diff on `table`.
///
/// For each added Parquet file: schedules a footer fetch, then reads the footer
/// to enumerate `(partition, column)` chunks and schedules the chunks of any
/// column whose heat clears the threshold, ordered by score. For each added Avro
/// file (which has no footer): scores the whole file by partition heat and
/// schedules a sequential full-file fill if it clears the threshold (the Avro
/// addendum). Returns an empty plan when the diff should not prefetch data
/// (e.g. `append`) or prefetch is disabled. Off the request hot path (does IO).
pub async fn plan_prefetch(
    fetch: &dyn MetadataFetch,
    ledger: &HeatLedger,
    table: TableId,
    bucket: &str,
    diff: &SnapshotDiff,
    config: &PrefetchConfig,
    epoch: u32,
) -> PrefetchPlan {
    let mut plan = PrefetchPlan::default();
    if !config.enabled || !diff.op.prefetches_data() {
        return plan;
    }
    let ctx = PlanCtx {
        fetch,
        ledger,
        table,
        bucket,
        config,
        epoch,
    };

    for file in &diff.added {
        match file.format {
            FileFormat::Parquet => plan_parquet(&ctx, file, &mut plan).await,
            FileFormat::Avro | FileFormat::Orc => plan_row_oriented(&ctx, file, &mut plan),
        }
    }

    // Hot chunks drain by descending score so the hottest data lands first.
    plan.chunks.sort_by(|a, b| b.score.total_cmp(&a.score));
    plan
}

/// The shared inputs for planning one file's fills, bundled so the per-file
/// planners take few arguments.
struct PlanCtx<'a> {
    /// Metadata reader (footer fetch runs through it — through-cache in server).
    fetch: &'a dyn MetadataFetch,
    /// The heat ledger scored against.
    ledger: &'a HeatLedger,
    /// The table being repaired.
    table: TableId,
    /// The bucket the files live in.
    bucket: &'a str,
    /// Planning tunables.
    config: &'a PrefetchConfig,
    /// The current heat epoch scores are read at.
    epoch: u32,
}

/// Plans one added Parquet file: a footer fetch plus its hot column chunks.
async fn plan_parquet(ctx: &PlanCtx<'_>, file: &DataFileEntry, plan: &mut PrefetchPlan) {
    let est = footer_estimate(ctx.config.footer_read_bytes, file.size);
    plan.footers.push(FillSpec {
        bucket: ctx.bucket.to_owned(),
        key: file.path.clone(),
        range: ReadRange::Suffix(ctx.config.footer_read_bytes),
        est_bytes: est,
        score: 0.0,
    });

    // Read the footer to learn the column-chunk layout. Best-effort: a footer
    // that will not parse just means no chunk prefetch for this file.
    let path = ObjectPath {
        bucket: ctx.bucket.to_owned(),
        key: file.path.clone(),
    };
    let spans = match parquet_meta::parse_footer(ctx.fetch, &path).await {
        Ok(spans) => spans,
        Err(error) => {
            tracing::debug!(key = %file.path, %error, "prefetch footer parse failed; skipping chunks");
            return;
        }
    };

    let partition = PartitionHash::of(&file.partition);
    // Group chunk byte ranges by column so each column is scored once.
    let mut columns: std::collections::BTreeMap<ColumnId, Vec<(u64, u64)>> =
        std::collections::BTreeMap::new();
    for span in &spans {
        columns
            .entry(span.column)
            .or_default()
            .push((span.start, span.end));
    }
    for (column, ranges) in columns {
        let score = ctx.ledger.score(
            &HeatKey {
                table: ctx.table,
                partition,
                column,
            },
            ctx.epoch,
        );
        if score <= ctx.config.heat_threshold {
            continue;
        }
        for (start, end) in ranges {
            if end <= start {
                continue;
            }
            plan.chunks.push(FillSpec {
                bucket: ctx.bucket.to_owned(),
                key: file.path.clone(),
                range: ReadRange::Bounded(start, end - 1),
                est_bytes: end - start,
                score,
            });
        }
    }
}

/// Plans one added row-oriented (Avro/ORC) file: a sequential whole-file fill if
/// its partition heat clears the threshold (there is no column dimension, so it
/// keys on [`ColumnId::WHOLE_FILE`] — the Avro addendum).
fn plan_row_oriented(ctx: &PlanCtx<'_>, file: &DataFileEntry, plan: &mut PrefetchPlan) {
    let partition = PartitionHash::of(&file.partition);
    let score = ctx.ledger.score(
        &HeatKey {
            table: ctx.table,
            partition,
            column: ColumnId::WHOLE_FILE,
        },
        ctx.epoch,
    );
    if score <= ctx.config.heat_threshold {
        return;
    }
    plan.whole_files.push(FillSpec {
        bucket: ctx.bucket.to_owned(),
        key: file.path.clone(),
        range: ReadRange::Full,
        est_bytes: file.size.max(1),
        score,
    });
}

/// The estimated footer footprint of a file: the smaller of the file size and
/// the speculative window, never zero.
fn footer_estimate(window: u64, size: u64) -> u64 {
    if size == 0 { window } else { size.min(window) }
}

/// Live executor counters (surfaced by metrics, #46). Bytes are attributed to
/// the producer that scheduled the fill.
#[derive(Debug, Default)]
pub struct PrefetchMetrics {
    /// Fills submitted to the queue.
    pub submitted: AtomicU64,
    /// Fills that completed (bytes pulled through the cache).
    pub completed: AtomicU64,
    /// Fills that errored at the reader (logged and dropped).
    pub errors: AtomicU64,
    /// Fills dropped because the queue was over its bound.
    pub dropped: AtomicU64,
    /// Bytes filled by compaction-repair (the #51 headline).
    pub bytes_compaction_repair: AtomicU64,
    /// Bytes filled by onboarding.
    pub bytes_onboarding: AtomicU64,
    /// Bytes filled by prewarm.
    pub bytes_prewarm: AtomicU64,
    /// Bytes filled by organic-triggered repair.
    pub bytes_organic: AtomicU64,
}

impl PrefetchMetrics {
    /// Attributes `bytes` filled to `priority`'s per-producer counter.
    fn add_bytes(&self, priority: PrefetchPriority, bytes: u64) {
        let counter = match priority {
            PrefetchPriority::Organic => &self.bytes_organic,
            PrefetchPriority::CompactionRepair => &self.bytes_compaction_repair,
            PrefetchPriority::Onboarding => &self.bytes_onboarding,
            PrefetchPriority::Prewarm => &self.bytes_prewarm,
        };
        counter.fetch_add(bytes, Ordering::Relaxed);
    }
}

/// The organic-traffic yield gate: a small semaphore the serving path occupies
/// while it has reads in flight, and prefetch waits on before running a fill.
/// Under organic load the permits are held by live reads, so prefetch starves —
/// exactly the intended politeness (the issue's "second, smaller semaphore that
/// organic traffic can starve").
#[derive(Clone)]
pub struct OrganicYield {
    /// The shared permit pool (`K` permits).
    slots: Arc<Semaphore>,
}

impl OrganicYield {
    /// A gate with `k` permits (at least one).
    pub fn new(k: usize) -> OrganicYield {
        OrganicYield {
            slots: Arc::new(Semaphore::new(k.max(1))),
        }
    }

    /// Occupies a slot for a live read, non-blocking. Returns `None` when the
    /// gate is saturated — organic traffic never blocks on prefetch, it just
    /// proceeds ungated (and its occupancy already starves prefetch).
    pub fn occupy(&self) -> Option<OwnedSemaphorePermit> {
        Arc::clone(&self.slots).try_acquire_owned().ok()
    }

    /// Reserves a slot for a prefetch fill, waiting until organic frees one.
    async fn reserve(&self) -> OwnedSemaphorePermit {
        // The semaphore is never closed, so acquire cannot fail.
        Arc::clone(&self.slots)
            .acquire_owned()
            .await
            .expect("prefetch yield semaphore is never closed")
    }
}

/// A queued fill with its priority and FIFO sequence (for stable ordering within
/// a priority).
struct Queued {
    /// Producer priority.
    priority: PrefetchPriority,
    /// Insertion order, so equal priorities drain FIFO.
    seq: u64,
    /// The fill to issue.
    fill: FillSpec,
}

impl PartialEq for Queued {
    /// Ordering identity is `(priority, seq)`.
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.seq == other.seq
    }
}
impl Eq for Queued {}
impl PartialOrd for Queued {
    /// Delegates to `Ord`.
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Queued {
    /// Highest priority first; within a priority, lowest sequence first (FIFO).
    /// The `BinaryHeap` is a max-heap, so `seq` is reversed.
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.priority
            .cmp(&other.priority)
            .then_with(|| Reverse(self.seq).cmp(&Reverse(other.seq)))
    }
}

/// Shared executor state behind an `Arc`.
struct ExecInner {
    /// The priority queue of pending fills.
    queue: Mutex<BinaryHeap<Queued>>,
    /// Wakes idle workers when a fill is submitted.
    notify: Notify,
    /// The cache-backed reader fills flow through (reads warm the cache).
    reader: Arc<dyn WarmSource>,
    /// The shared byte-rate budget (one instance with warming, #168).
    budget: Arc<TokenBucket>,
    /// The organic-traffic yield gate.
    yield_gate: OrganicYield,
    /// Live counters.
    metrics: PrefetchMetrics,
    /// Queue length bound.
    max_queue: usize,
    /// Monotonic sequence source for FIFO ordering.
    seq: AtomicU64,
}

/// The single shared prefetch executor: a priority queue drained by a worker
/// pool under one byte budget and the organic yield rule. Producers submit
/// fills; workers pull the highest-priority fill, yield to organic traffic,
/// charge the budget, and pull the bytes through the cache.
pub struct PrefetchExecutor {
    /// Shared state (cloned into each worker).
    inner: Arc<ExecInner>,
}

impl PrefetchExecutor {
    /// Builds an executor over `reader`, sharing `budget` with warming, and
    /// spawns `config.concurrency` worker tasks. The workers live for the
    /// process (the returned handle owns the `Arc`; dropping it does not stop
    /// them, matching the server's detached-task model).
    pub fn spawn(
        reader: Arc<dyn WarmSource>,
        budget: Arc<TokenBucket>,
        config: &PrefetchConfig,
    ) -> PrefetchExecutor {
        let inner = Arc::new(ExecInner {
            queue: Mutex::new(BinaryHeap::new()),
            notify: Notify::new(),
            reader,
            budget,
            yield_gate: OrganicYield::new(config.organic_yield_k),
            metrics: PrefetchMetrics::default(),
            max_queue: config.max_queue.max(1),
            seq: AtomicU64::new(0),
        });
        for _ in 0..config.concurrency.max(1) {
            let worker = Arc::clone(&inner);
            tokio::spawn(async move { run_worker(worker).await });
        }
        PrefetchExecutor { inner }
    }

    /// The organic yield gate, so the serving path can occupy a slot per live
    /// read (its occupancy is what makes prefetch yield).
    pub fn yield_gate(&self) -> OrganicYield {
        self.inner.yield_gate.clone()
    }

    /// The live executor counters.
    pub fn metrics(&self) -> &PrefetchMetrics {
        &self.inner.metrics
    }

    /// Submits one fill at `priority`. Drops the lowest-priority queued fill (or
    /// this one, if it is the lowest) when the queue is over its bound — a hard
    /// ceiling, since a lost speculative fill costs only warmth.
    pub fn submit(&self, priority: PrefetchPriority, fill: FillSpec) {
        let seq = self.inner.seq.fetch_add(1, Ordering::Relaxed);
        {
            let mut queue = self.inner.queue.lock().unwrap_or_else(|p| p.into_inner());
            if queue.len() >= self.inner.max_queue {
                // Over the bound: only admit if this fill outranks the current
                // minimum; otherwise drop it. (A precise min-eviction would cost
                // a heap rebuild; dropping the incoming lowest is close enough
                // and cheap.)
                if let Some(min) = queue.iter().map(|q| q.priority).min()
                    && priority <= min
                {
                    self.inner.metrics.dropped.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            }
            queue.push(Queued {
                priority,
                seq,
                fill,
            });
        }
        self.inner.metrics.submitted.fetch_add(1, Ordering::Relaxed);
        self.inner.notify.notify_one();
    }

    /// Submits an entire plan at `priority` in execution order (footers first).
    /// The plan's fills already carry their bucket (set by the planner).
    pub fn submit_plan(&self, priority: PrefetchPriority, plan: &PrefetchPlan) {
        for fill in plan.ordered() {
            self.submit(priority, fill);
        }
    }
}

/// One worker: pull the highest-priority fill, yield to organic traffic, charge
/// the byte budget, and pull the bytes through the cache. Loops forever.
async fn run_worker(inner: Arc<ExecInner>) {
    loop {
        let next = {
            let mut queue = inner.queue.lock().unwrap_or_else(|p| p.into_inner());
            queue.pop()
        };
        let Some(task) = next else {
            // Nothing to do: wait for a submit to wake us.
            inner.notify.notified().await;
            continue;
        };

        // Yield to organic traffic: wait for a slot the serving path holds.
        let _slot = inner.yield_gate.reserve().await;
        // Charge the shared byte budget before issuing the fill.
        inner.budget.acquire(task.fill.est_bytes).await;

        let ck = CacheKey {
            bucket: task.fill.bucket.clone(),
            key: task.fill.key.clone(),
        };
        match inner.reader.read(&ck, task.fill.range).await {
            Ok(bytes) => {
                inner.metrics.completed.fetch_add(1, Ordering::Relaxed);
                inner.metrics.add_bytes(task.priority, bytes.len() as u64);
            }
            Err(error) => {
                tracing::debug!(key = %task.fill.key, %error, "prefetch fill failed");
                inner.metrics.errors.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Priorities order as `organic > compaction-repair > onboarding > prewarm`.
    #[test]
    fn priority_ordering_is_correct() {
        assert!(PrefetchPriority::Organic > PrefetchPriority::CompactionRepair);
        assert!(PrefetchPriority::CompactionRepair > PrefetchPriority::Onboarding);
        assert!(PrefetchPriority::Onboarding > PrefetchPriority::Prewarm);
    }

    /// The queue drains highest priority first, FIFO within a priority.
    #[test]
    fn queue_orders_by_priority_then_fifo() {
        let mut heap = BinaryHeap::new();
        let fill = || FillSpec {
            bucket: "b".into(),
            key: "k".into(),
            range: ReadRange::Full,
            est_bytes: 1,
            score: 0.0,
        };
        heap.push(Queued {
            priority: PrefetchPriority::Onboarding,
            seq: 0,
            fill: fill(),
        });
        heap.push(Queued {
            priority: PrefetchPriority::CompactionRepair,
            seq: 1,
            fill: fill(),
        });
        heap.push(Queued {
            priority: PrefetchPriority::CompactionRepair,
            seq: 2,
            fill: fill(),
        });
        heap.push(Queued {
            priority: PrefetchPriority::Prewarm,
            seq: 3,
            fill: fill(),
        });
        // CompactionRepair (seq 1 then 2), then Onboarding, then Prewarm.
        assert_eq!(
            heap.pop().map(|q| (q.priority, q.seq)),
            Some((PrefetchPriority::CompactionRepair, 1))
        );
        assert_eq!(
            heap.pop().map(|q| (q.priority, q.seq)),
            Some((PrefetchPriority::CompactionRepair, 2))
        );
        assert_eq!(
            heap.pop().map(|q| q.priority),
            Some(PrefetchPriority::Onboarding)
        );
        assert_eq!(
            heap.pop().map(|q| q.priority),
            Some(PrefetchPriority::Prewarm)
        );
    }

    /// The organic gate lets prefetch reserve when idle and starves it while a
    /// live read holds the only slot.
    #[tokio::test]
    async fn organic_gate_starves_prefetch_under_load() {
        let gate = OrganicYield::new(1);
        // Idle: prefetch reserves immediately.
        let slot = gate.reserve().await;
        drop(slot);
        // Organic occupies the only slot; prefetch cannot reserve now.
        let organic = gate.occupy().expect("first occupy succeeds");
        assert!(gate.occupy().is_none(), "gate saturated at K=1");
        // A prefetch reserve must not complete while organic holds the slot.
        let reserve = gate.reserve();
        tokio::pin!(reserve);
        tokio::select! {
            _ = &mut reserve => panic!("prefetch reserved while organic held the slot"),
            _ = tokio::time::sleep(std::time::Duration::from_millis(20)) => {}
        }
        // Releasing organic lets prefetch through.
        drop(organic);
        let _ = reserve.await;
    }
}
