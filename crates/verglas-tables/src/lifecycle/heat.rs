//! The heat ledger: logical-identity access statistics that survive rewrites.
//!
//! Heat must attach to identities that outlive compaction, so the ledger keys on
//! `(table, partition, column)` — never a file path. Partition tuples and
//! Iceberg column ids are stable across a rewrite (that is *why* Iceberg uses
//! column ids, not names), so a column hot before compaction stays hot under the
//! same key after it, and the prefetch planner can score the replacement files'
//! chunks against heat earned by the files they replaced.
//!
//! # Feeding the ledger without touching the hot path
//!
//! The request path never locks or aggregates. [`HeatFeed`] wraps the serving
//! reader and, per served read, pushes a raw [`HeatSample`] onto a **bounded,
//! drop-on-full** channel ([`HeatChannel`]) with a non-blocking `try_send` —
//! overflow is discarded, because losing samples costs warmth, not correctness.
//! A background aggregator ([`run_aggregator`]) drains the channel, runs the
//! mapper's `classify()` and partition/column resolution *off* the hot path, and
//! folds the access into the sharded, bounded EWMA map.
//!
//! # EWMA over epochs
//!
//! Heat is an exponentially-weighted moving average of bytes touched per epoch
//! (default 5 minutes, `α = 0.3`) — the same freshness shape as TinyLFU's aging
//! (Einziger et al., ACM TOS 2017, §3.3). Recent activity dominates; a partition
//! that goes cold decays away and eventually ages out of the bounded map. The
//! ledger is approximate: losing it costs warmth, not correctness (its NVMe /
//! Puffin checkpoint is a follow-up, tracked separately — see the crate PR).

use std::ops::Range;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use rustc_hash::FxHashMap;
use tokio::sync::mpsc;
use verglas_core::CacheKey;
use verglas_core::read::{
    AttributesRequest, DirectGet, DirectMeta, DirectReadOptions, ObjectAttributes, ObjectGet,
    ObjectMeta, ObjectRead, ReadError, ReadRange, Revalidation, TierCell,
};
use verglas_core::telemetry::{AccessEvent, Op, Telemetry, Tier};
use xxhash_rust::xxh3::xxh3_64;

use crate::mapper::{Classify, ColumnId, Mapper, TableId};

/// The EWMA smoothing factor `α`: the weight a fresh epoch's bytes carry.
pub const DEFAULT_ALPHA: f32 = 0.3;
/// The epoch length in seconds: how long bytes accumulate before folding.
pub const DEFAULT_EPOCH_SECS: u64 = 300;
/// The per-table entry cap the bounded map enforces (the issue's 64k figure).
pub const DEFAULT_TABLE_CAP: usize = 64 * 1024;
/// Shard count: reduces write contention between the aggregator and planner
/// reads. A power of two so the low bits of the key hash pick the shard.
const SHARD_COUNT: usize = 64;
/// The default drop-on-full sample channel depth. Sized so a burst of misses
/// buffers without blocking, yet overflow (dropped samples) is bounded.
pub const DEFAULT_CHANNEL_CAPACITY: usize = 16 * 1024;

/// A stable hash of a partition tuple. Survives file renames by construction:
/// two files of the same logical partition hash identically, whatever their
/// object keys, so heat carries across a rewrite.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PartitionHash(pub u64);

impl PartitionHash {
    /// Hashes a partition tuple into a stable value. The pairs are sorted by
    /// field name first, so a writer's field ordering never changes the hash —
    /// the same logical partition always yields the same value on both the
    /// aggregator side (from the mapper's `FileMeta`) and the planner side (from
    /// a manifest `DataFileEntry`).
    pub fn of(partition: &[(String, Option<String>)]) -> PartitionHash {
        let mut pairs: Vec<&(String, Option<String>)> = partition.iter().collect();
        pairs.sort_by(|a, b| a.0.cmp(&b.0));
        // Feed a length-delimited encoding so `("a","bc")` and `("ab","c")`
        // cannot collide.
        let mut buf: Vec<u8> = Vec::new();
        for (field, value) in pairs {
            buf.extend_from_slice(&(field.len() as u64).to_le_bytes());
            buf.extend_from_slice(field.as_bytes());
            match value {
                Some(v) => {
                    buf.push(1);
                    buf.extend_from_slice(&(v.len() as u64).to_le_bytes());
                    buf.extend_from_slice(v.as_bytes());
                }
                None => buf.push(0),
            }
        }
        PartitionHash(xxh3_64(&buf))
    }
}

/// The logical identity heat attaches to: table, partition, column. Copy so the
/// sharded map keys on a cheap value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HeatKey {
    /// The owning table.
    pub table: TableId,
    /// The partition, by stable hash.
    pub partition: PartitionHash,
    /// The column, or [`ColumnId::WHOLE_FILE`] for a whole-file / Avro access.
    pub column: ColumnId,
}

/// One identity's access statistics: the folded EWMA plus the current epoch's
/// in-progress accumulator.
#[derive(Debug, Clone, Copy)]
struct Heat {
    /// The folded EWMA of per-epoch byte volume, as of `epoch`.
    ewma_bytes: f32,
    /// Bytes accumulated in the (not-yet-folded) current epoch.
    acc_bytes: f32,
    /// The epoch `ewma_bytes`/`acc_bytes` are current as of.
    epoch: u32,
}

impl Heat {
    /// A fresh record at `epoch` with no history.
    fn new(epoch: u32) -> Heat {
        Heat {
            ewma_bytes: 0.0,
            acc_bytes: 0.0,
            epoch,
        }
    }

    /// Folds the accumulator and any elapsed empty epochs into the EWMA so the
    /// record is current as of `epoch`. Idempotent within an epoch.
    fn advance_to(&mut self, epoch: u32, alpha: f32) {
        if epoch <= self.epoch {
            return;
        }
        // Fold the just-finished epoch's accumulated bytes in.
        self.ewma_bytes = alpha * self.acc_bytes + (1.0 - alpha) * self.ewma_bytes;
        self.acc_bytes = 0.0;
        // Decay for each fully-empty epoch since (the first was folded above).
        let empty = epoch - self.epoch - 1;
        if empty > 0 {
            self.ewma_bytes *= (1.0 - alpha).powi(empty.min(64) as i32);
        }
        self.epoch = epoch;
    }

    /// Records `bytes` of access at `epoch`, advancing the fold first.
    fn record(&mut self, bytes: u64, epoch: u32, alpha: f32) {
        self.advance_to(epoch, alpha);
        self.acc_bytes += bytes as f32;
    }

    /// The heat score as of `epoch`: the EWMA it would fold to, counting the
    /// in-progress epoch's bytes. Non-mutating — the planner reads it without
    /// disturbing the record.
    fn score_at(&self, epoch: u32, alpha: f32) -> f32 {
        if epoch <= self.epoch {
            // Same epoch: the accumulator is not yet folded, but it is real heat
            // the planner should see, so weight it like a fold.
            alpha * self.acc_bytes + (1.0 - alpha) * self.ewma_bytes
        } else {
            let mut copy = *self;
            copy.advance_to(epoch, alpha);
            alpha * copy.acc_bytes + (1.0 - alpha) * copy.ewma_bytes
        }
    }
}

/// Maps wall-clock time onto epoch numbers. Monotonic base so tests can drive
/// epochs deterministically and the daemon uses the process clock.
#[derive(Debug, Clone, Copy)]
pub struct EpochClock {
    /// Unix-seconds origin epoch 0 is measured from.
    base_secs: u64,
    /// Seconds per epoch.
    epoch_secs: u64,
}

impl EpochClock {
    /// A clock with `epoch_secs`-long epochs starting now.
    pub fn new(epoch_secs: u64) -> EpochClock {
        let base_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        EpochClock {
            base_secs,
            epoch_secs: epoch_secs.max(1),
        }
    }

    /// The current epoch number.
    pub fn now(&self) -> u32 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(self.base_secs);
        (now.saturating_sub(self.base_secs) / self.epoch_secs) as u32
    }
}

/// Live occupancy counters for the ledger (surfaced by metrics, #46).
#[derive(Debug, Default)]
pub struct HeatStats {
    /// Samples the aggregator folded into the ledger.
    pub samples_recorded: AtomicU64,
    /// Samples classified as non-data (skipped — no heat identity).
    pub samples_skipped: AtomicU64,
    /// Entries evicted to keep a table under its bounded cap.
    pub entries_evicted: AtomicU64,
}

/// The sharded, bounded EWMA heat map. Writes come from the single background
/// aggregator; reads from the prefetch planner. Sharded so the two rarely
/// contend, bounded so a table's key space cannot grow without limit.
pub struct HeatLedger {
    /// Per-shard maps, each behind its own mutex (never held across `.await`).
    shards: Vec<Mutex<FxHashMap<HeatKey, Heat>>>,
    /// EWMA smoothing factor.
    alpha: f32,
    /// Per-shard, per-table entry cap. The global per-table target
    /// (`table_cap`) divided across shards, so the whole table's key space is
    /// bounded by roughly `table_cap` while each shard is checked cheaply.
    per_shard_cap: usize,
    /// Live occupancy counters.
    stats: HeatStats,
}

impl HeatLedger {
    /// Builds a ledger with the given smoothing factor and per-table cap. The
    /// cap is the global per-table target; it is divided across the shards so no
    /// table's key space grows past roughly `table_cap` overall.
    pub fn new(alpha: f32, table_cap: usize) -> HeatLedger {
        let shards = (0..SHARD_COUNT)
            .map(|_| Mutex::new(FxHashMap::default()))
            .collect();
        HeatLedger {
            shards,
            alpha: alpha.clamp(f32::MIN_POSITIVE, 1.0),
            per_shard_cap: (table_cap / SHARD_COUNT).max(1),
            stats: HeatStats::default(),
        }
    }

    /// A ledger with the documented defaults (`α = 0.3`, 64k entries/table).
    pub fn with_defaults() -> HeatLedger {
        HeatLedger::new(DEFAULT_ALPHA, DEFAULT_TABLE_CAP)
    }

    /// The live occupancy counters.
    pub fn stats(&self) -> &HeatStats {
        &self.stats
    }

    /// Which shard a key lives in (low bits of the composite key hash).
    fn shard_index(&self, key: &HeatKey) -> usize {
        let mixed = key.partition.0.wrapping_mul(0x9E37_79B9_7F4A_7C15)
            ^ (key.table.0 as u64)
            ^ ((key.column.0 as u64) << 32);
        (mixed as usize) & (SHARD_COUNT - 1)
    }

    /// Records `bytes` of access to `key` at `epoch`. Off the hot path (the
    /// aggregator calls it). Enforces the per-table cap on insert.
    pub fn record(&self, key: HeatKey, bytes: u64, epoch: u32) {
        let idx = self.shard_index(&key);
        let mut shard = self.shards[idx].lock().unwrap_or_else(|p| p.into_inner());
        if !shard.contains_key(&key) && shard_table_len(&shard, key.table) >= self.per_shard_cap {
            // Bounded: evict this table's coldest entry in this shard before
            // admitting a new identity. Approximate (per-shard), which is fine —
            // the ledger is approximate by design.
            if let Some(victim) = coldest_for_table(&shard, key.table, epoch, self.alpha) {
                shard.remove(&victim);
                self.stats.entries_evicted.fetch_add(1, Ordering::Relaxed);
            }
        }
        shard
            .entry(key)
            .or_insert_with(|| Heat::new(epoch))
            .record(bytes, epoch, self.alpha);
        self.stats.samples_recorded.fetch_add(1, Ordering::Relaxed);
    }

    /// The heat score for `key` as of `epoch`, or `0.0` if unseen. Non-mutating.
    pub fn score(&self, key: &HeatKey, epoch: u32) -> f32 {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].lock().unwrap_or_else(|p| p.into_inner());
        shard
            .get(key)
            .map(|h| h.score_at(epoch, self.alpha))
            .unwrap_or(0.0)
    }

    /// Total entries across all shards (for metrics/tests).
    pub fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.lock().unwrap_or_else(|p| p.into_inner()).len())
            .sum()
    }

    /// Whether the ledger holds no entries.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Counts a shard's entries belonging to `table`.
fn shard_table_len(shard: &FxHashMap<HeatKey, Heat>, table: TableId) -> usize {
    shard.keys().filter(|k| k.table == table).count()
}

/// Finds `table`'s coldest key in a shard (lowest score as of `epoch`).
fn coldest_for_table(
    shard: &FxHashMap<HeatKey, Heat>,
    table: TableId,
    epoch: u32,
    alpha: f32,
) -> Option<HeatKey> {
    shard
        .iter()
        .filter(|(k, _)| k.table == table)
        .min_by(|a, b| {
            a.1.score_at(epoch, alpha)
                .total_cmp(&b.1.score_at(epoch, alpha))
        })
        .map(|(k, _)| *k)
}

/// One raw access sample, pushed from the request path and resolved to a
/// [`HeatKey`] off the hot path by the aggregator.
#[derive(Debug, Clone)]
pub struct HeatSample {
    /// The origin bucket.
    pub bucket: String,
    /// The object key.
    pub key: String,
    /// The absolute byte range served (its length is the heat weight).
    pub range: Range<u64>,
}

/// The drop-on-full sample channel between the request path and the aggregator.
pub struct HeatChannel {
    /// Non-blocking producer end handed to [`HeatFeed`].
    tx: mpsc::Sender<HeatSample>,
    /// Consumer end handed to [`run_aggregator`] (taken once).
    rx: Option<mpsc::Receiver<HeatSample>>,
}

impl HeatChannel {
    /// Builds a channel with the given bounded depth.
    pub fn new(capacity: usize) -> HeatChannel {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        HeatChannel { tx, rx: Some(rx) }
    }

    /// A cloneable, non-blocking sender for the feed decorator.
    pub fn sender(&self) -> HeatSender {
        HeatSender {
            tx: self.tx.clone(),
        }
    }

    /// Takes the receiver for the aggregator (callable once).
    pub fn take_receiver(&mut self) -> Option<mpsc::Receiver<HeatSample>> {
        self.rx.take()
    }
}

/// The non-blocking producer end. Cloneable and cheap; a full channel drops the
/// sample rather than ever blocking the request path.
#[derive(Clone)]
pub struct HeatSender {
    /// The bounded channel sender.
    tx: mpsc::Sender<HeatSample>,
}

impl HeatSender {
    /// Pushes a sample, dropping it silently if the channel is full. Never
    /// blocks and never allocates beyond the sample itself — safe on the hot
    /// path.
    pub fn record(&self, sample: HeatSample) {
        let _ = self.tx.try_send(sample);
    }
}

/// A serving-reader decorator that feeds the heat ledger and marks organic
/// in-flight occupancy. Wraps the real [`ObjectRead`] (the cache engine) and,
/// per served read, pushes a [`HeatSample`] onto the drop-on-full channel and
/// (when configured) occupies an [`OrganicYield`](crate::lifecycle::prefetch::OrganicYield)
/// slot for the lifetime of the body stream, so aggressive prefetch yields to
/// live reads. The `classify()` that turns a sample into a [`HeatKey`] happens
/// in the aggregator, never here. Both hooks are optional, so a passthrough
/// (prefetch off) adds no measurable overhead.
pub struct HeatFeed<R> {
    /// The wrapped serving reader.
    inner: R,
    /// The non-blocking sample sender, absent when heat feeding is off.
    sender: Option<HeatSender>,
    /// The organic-traffic yield gate to occupy per read, absent when the yield
    /// rule is off.
    yield_gate: Option<crate::lifecycle::prefetch::OrganicYield>,
    /// Per-table telemetry hub (#60). Absent when telemetry is off; when present
    /// each served GET records one fixed-size event to its per-core tape.
    telemetry: Option<Arc<Telemetry>>,
    /// The live logical-key map used to resolve a served read's `TableId` for
    /// the telemetry event. Shared with the heat aggregator and coordinator.
    mapper: Option<Arc<Mapper>>,
}

impl<R: ObjectRead> HeatFeed<R> {
    /// Wraps `inner`, feeding samples through `sender` (no yield gate, no
    /// telemetry). For tests that only exercise the feed path.
    pub fn new(inner: R, sender: HeatSender) -> HeatFeed<R> {
        HeatFeed {
            inner,
            sender: Some(sender),
            yield_gate: None,
            telemetry: None,
            mapper: None,
        }
    }

    /// Wraps `inner` with optional heat feeding, an optional organic-yield gate,
    /// and optional per-table telemetry — the daemon's constructor.
    ///
    /// When `telemetry` is present it is paired with the live `mapper`: each
    /// served GET classifies its key once (the mapper's lock-free, allocation-
    /// free `classify()` — the same call the heat aggregator makes off the hot
    /// path) to get the interned `TableId`, then records one 16-byte event to a
    /// per-core tape when the body drains (so the serving tier is known). This is
    /// the single classify on the serve path; unmapped keys record under id `0`.
    pub fn with_options(
        inner: R,
        sender: Option<HeatSender>,
        yield_gate: Option<crate::lifecycle::prefetch::OrganicYield>,
        telemetry: Option<Arc<Telemetry>>,
        mapper: Option<Arc<Mapper>>,
    ) -> HeatFeed<R> {
        HeatFeed {
            inner,
            sender,
            yield_gate,
            telemetry,
            mapper,
        }
    }

    /// The interned `TableId` a served read belongs to, or `0` (`_unmapped`)
    /// when no mapper is wired or the key is under no watched table. Runs the
    /// mapper's lock-free `classify()`; metadata and data reads both attribute
    /// to their owning table.
    fn resolve_table_id(&self, key: &CacheKey, range: &Range<u64>) -> u32 {
        let Some(mapper) = &self.mapper else {
            return verglas_core::telemetry::UNMAPPED;
        };
        match mapper.classify(&key.bucket, &key.key, Some(range.clone())) {
            // Encode the mapper's 0-based id so telemetry can reserve 0 for
            // `_unmapped` (the mapper itself hands out ids from 0).
            Classify::TableData { table, .. } | Classify::TableMetadata { table, .. } => {
                verglas_core::telemetry::encode_table_id(table.0)
            }
            Classify::Unknown => verglas_core::telemetry::UNMAPPED,
        }
    }
}

impl<R: ObjectRead> ObjectRead for HeatFeed<R> {
    /// Serves the read through the inner reader, records a heat sample for the
    /// resolved range, and (when a yield gate is configured) holds an occupancy
    /// slot for the whole body stream so prefetch fills starve while live reads
    /// are in flight. The sample push is non-blocking; the body is unchanged.
    async fn get(&self, key: &CacheKey, range: ReadRange) -> Result<ObjectGet, ReadError> {
        // Occupy an organic slot before the read; hold it across the body.
        let permit = self.yield_gate.as_ref().and_then(|g| g.occupy());
        let started = Instant::now();
        let mut got = self.inner.get(key, range).await?;
        if let Some(sender) = &self.sender {
            sender.record(HeatSample {
                bucket: key.bucket.clone(),
                key: key.key.clone(),
                range: got.range.clone(),
            });
        }
        // Per-table telemetry (#60): resolve the TableId once here (allocation-
        // free classify), then let the body wrapper record the event when the
        // stream drains, so the serving tier is known. The record itself is one
        // thread-local tape write — never a lock or a shared per-table atomic.
        if let Some(telemetry) = &self.telemetry {
            let table = self.resolve_table_id(key, &got.range);
            let bytes = (got.range.end - got.range.start).min(u32::MAX as u64) as u32;
            got.body = Box::pin(TelemetryBody {
                inner: got.body,
                record: Some(TelemetryRecord {
                    telemetry: telemetry.clone(),
                    served_from: got.served_from.clone(),
                    table,
                    bytes,
                    started,
                }),
            });
        }
        if let Some(permit) = permit {
            got.body = Box::pin(GuardedBody {
                inner: got.body,
                _permit: permit,
            });
        }
        Ok(got)
    }

    /// Metadata reads carry no data heat — delegate untouched.
    fn head(
        &self,
        key: &CacheKey,
    ) -> impl std::future::Future<Output = Result<ObjectMeta, ReadError>> + Send {
        self.inner.head(key)
    }

    /// Delegates revalidation untouched.
    fn revalidate(
        &self,
        key: &CacheKey,
        etag: &str,
    ) -> impl std::future::Future<Output = Result<Revalidation, ReadError>> + Send {
        self.inner.revalidate(key, etag)
    }

    /// Delegates version-/part-/checksum-scoped reads untouched (issues
    /// #153/#154/#156): these are uncached passthroughs the heat feed does not
    /// sample.
    fn get_direct(
        &self,
        key: &CacheKey,
        range: ReadRange,
        options: DirectReadOptions,
    ) -> impl std::future::Future<Output = Result<DirectGet, ReadError>> + Send {
        self.inner.get_direct(key, range, options)
    }

    /// Delegates the metadata-only escape-hatch read untouched.
    fn head_direct(
        &self,
        key: &CacheKey,
        options: DirectReadOptions,
    ) -> impl std::future::Future<Output = Result<DirectMeta, ReadError>> + Send {
        self.inner.head_direct(key, options)
    }

    /// Delegates GetObjectAttributes untouched (issue #154).
    fn object_attributes(
        &self,
        key: &CacheKey,
        request: AttributesRequest,
    ) -> impl std::future::Future<Output = Result<ObjectAttributes, ReadError>> + Send {
        self.inner.object_attributes(key, request)
    }
}

/// A body stream that holds an organic-yield permit until the stream is drained
/// or dropped, so a live read counts as in-flight (starving prefetch) for its
/// whole duration, not just its header resolution.
struct GuardedBody {
    /// The wrapped body.
    inner: verglas_core::read::BodyStream,
    /// The occupancy permit; released on drop.
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl futures::Stream for GuardedBody {
    type Item = Result<bytes::Bytes, ReadError>;

    /// Delegates to the wrapped body; the permit rides along and drops with it.
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

/// What a [`TelemetryBody`] records when its stream ends: the resolved table,
/// the tier cell the engine writes as the body streams, the bytes served, and
/// the start instant for serve latency.
struct TelemetryRecord {
    telemetry: Arc<Telemetry>,
    served_from: TierCell,
    table: u32,
    bytes: u32,
    started: Instant,
}

/// A body wrapper that records one per-table telemetry event when the stream is
/// fully drained or dropped — the point at which the serving tier (`served_from`)
/// and the serve latency are both known (#60). The record is a single
/// thread-local tape write; it never blocks the stream.
struct TelemetryBody {
    inner: verglas_core::read::BodyStream,
    record: Option<TelemetryRecord>,
}

impl TelemetryBody {
    /// Emits the event exactly once (on end-of-stream or drop).
    fn record_once(&mut self) {
        if let Some(r) = self.record.take() {
            let tier = Tier::from_served(r.served_from.get());
            let latency_us = r.started.elapsed().as_micros().min(u32::MAX as u128) as u32;
            r.telemetry.record(AccessEvent {
                table: r.table,
                tier: tier.code(),
                op: Op::Get.code(),
                _pad: 0,
                bytes: r.bytes,
                latency_us,
            });
        }
    }
}

impl futures::Stream for TelemetryBody {
    type Item = Result<bytes::Bytes, ReadError>;

    /// Delegates to the wrapped body; records the telemetry event once the
    /// stream reports end-of-stream.
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let polled = self.inner.as_mut().poll_next(cx);
        if let std::task::Poll::Ready(None) = polled {
            self.record_once();
        }
        polled
    }
}

impl Drop for TelemetryBody {
    /// Records the event if the stream was dropped before draining (a client
    /// hang-up mid-transfer still counts the served bytes).
    fn drop(&mut self) {
        self.record_once();
    }
}

/// Resolves one sample to a [`HeatKey`] against the live map, or `None` if the
/// sample is not a data read of a known table. Runs the mapper's lock-free
/// `classify()` and reads the file's partition tuple — all off the hot path.
///
/// An access that resolves to a Parquet column chunk keys on that column; a
/// whole-file read, an Avro/ORC file, or an unresolved range keys on
/// [`ColumnId::WHOLE_FILE`] so heat still attaches to `(table, partition)` and
/// carries across compaction (the Avro addendum).
pub fn resolve_key(mapper: &Mapper, sample: &HeatSample) -> Option<HeatKey> {
    let range = Some(sample.range.clone());
    match mapper.classify(&sample.bucket, &sample.key, range) {
        Classify::TableData { table, chunk, .. } => {
            let state = mapper.state();
            let index = state.table(table)?;
            let file = index.file_by_path(&sample.key)?;
            let partition = PartitionHash::of(&file.partition);
            let column = chunk.map(|c| c.column).unwrap_or(ColumnId::WHOLE_FILE);
            Some(HeatKey {
                table,
                partition,
                column,
            })
        }
        // Table metadata and unknown objects carry no data heat.
        _ => None,
    }
}

/// Drains the sample channel forever, folding each data access into the ledger.
/// The single writer to the ledger: it resolves each sample against the mapper
/// and records the byte weight at the current epoch. Ends when every
/// [`HeatSender`] is dropped.
pub async fn run_aggregator(
    mut rx: mpsc::Receiver<HeatSample>,
    mapper: std::sync::Arc<Mapper>,
    ledger: std::sync::Arc<HeatLedger>,
    clock: EpochClock,
) {
    while let Some(sample) = rx.recv().await {
        let weight = sample.range.end.saturating_sub(sample.range.start);
        match resolve_key(&mapper, &sample) {
            Some(key) => ledger.record(key, weight, clock.now()),
            None => {
                ledger.stats.samples_skipped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The same logical partition hashes identically regardless of field
    /// ordering, and different partitions differ.
    #[test]
    fn partition_hash_is_order_stable_and_distinguishing() {
        let a = PartitionHash::of(&[
            ("day".into(), Some("3".into())),
            ("region".into(), Some("eu".into())),
        ]);
        let b = PartitionHash::of(&[
            ("region".into(), Some("eu".into())),
            ("day".into(), Some("3".into())),
        ]);
        assert_eq!(a, b, "field order must not change the hash");
        let c = PartitionHash::of(&[
            ("day".into(), Some("4".into())),
            ("region".into(), Some("eu".into())),
        ]);
        assert_ne!(a, c, "a different value must change the hash");
        // Length-delimited encoding: ("a","bc") must not collide with ("ab","c").
        let x = PartitionHash::of(&[("a".into(), Some("bc".into()))]);
        let y = PartitionHash::of(&[("ab".into(), Some("c".into()))]);
        assert_ne!(x, y);
    }

    /// Heat rises with access and a hotter key outscores a colder one.
    #[test]
    fn heat_rises_with_access_and_orders_keys() {
        let ledger = HeatLedger::with_defaults();
        let hot = HeatKey {
            table: TableId(0),
            partition: PartitionHash(1),
            column: ColumnId(0),
        };
        let cold = HeatKey {
            table: TableId(0),
            partition: PartitionHash(2),
            column: ColumnId(0),
        };
        for _ in 0..10 {
            ledger.record(hot, 1_000, 0);
        }
        ledger.record(cold, 1_000, 0);
        assert!(ledger.score(&hot, 0) > ledger.score(&cold, 0));
        assert_eq!(
            ledger.score(
                &HeatKey {
                    column: ColumnId(9),
                    ..hot
                },
                0
            ),
            0.0
        );
    }

    /// Heat decays across empty epochs — a key not touched for many epochs
    /// scores lower than one touched recently.
    #[test]
    fn heat_decays_across_epochs() {
        let ledger = HeatLedger::new(DEFAULT_ALPHA, DEFAULT_TABLE_CAP);
        let key = HeatKey {
            table: TableId(0),
            partition: PartitionHash(1),
            column: ColumnId(0),
        };
        ledger.record(key, 10_000, 0);
        let fresh = ledger.score(&key, 1);
        let stale = ledger.score(&key, 40);
        assert!(stale < fresh, "heat must decay: {stale} !< {fresh}");
    }

    /// The bounded map holds a table's key space to roughly the cap: inserting
    /// far more distinct identities than the cap triggers eviction and leaves
    /// the ledger bounded (never unbounded growth).
    #[test]
    fn bounded_map_caps_a_tables_key_space() {
        // A global cap of 2*SHARD_COUNT means ~2 entries per shard; inserting
        // thousands of distinct partitions must evict and stay bounded.
        let cap = 2 * SHARD_COUNT;
        let ledger = HeatLedger::new(DEFAULT_ALPHA, cap);
        let table = TableId(7);
        for p in 0..5_000u64 {
            ledger.record(
                HeatKey {
                    table,
                    partition: PartitionHash(p.wrapping_mul(0x9E37_79B9)),
                    column: ColumnId::WHOLE_FILE,
                },
                1_000,
                0,
            );
        }
        assert!(
            ledger.stats().entries_evicted.load(Ordering::Relaxed) >= 1,
            "no eviction fired despite far exceeding the cap"
        );
        // Bounded: never more than the per-shard cap across all shards.
        assert!(
            ledger.len() <= cap,
            "ledger grew past the cap: {} > {cap}",
            ledger.len()
        );
    }
}
