//! Eager metadata warming (#168): walk a watched table's live snapshot and
//! pull its planning metadata — `metadata.json`, the manifest list, the
//! manifests, and every Parquet footer — through the cache *before* an engine
//! asks, so a cold planning walk collapses toward a warm one (the server
//! already walked the metadata).
//!
//! The job reads through the same cache [`ObjectRead`] a client would use, so
//! the reads land in #50's pinned metadata store as a side effect: block
//! metadata is read as whole-object `Full` reads (pinning block entries), and
//! footers as speculative suffix reads (pinning suffix-granular entries). It is
//! polite by construction — a [`Semaphore`](tokio::sync::Semaphore) caps
//! in-flight fills and a [`TokenBucket`](budget::TokenBucket) caps the byte
//! rate (the same bucket #51's prefetch executor will share). A table whose
//! footer working set would blow the metadata budget alerts and caps instead
//! of thrashing.
//!
//! # NVMe long tail and restart (resumability)
//!
//! Warming holds no cursor. Every read flows through the cache, so a re-run
//! over an already-warm snapshot is all cache hits and issues no backend GETs —
//! resumability falls out of idempotence. The metadata store is a hybrid
//! DRAM+NVMe cache (#50): the hot planning set sits in DRAM and the long tail
//! spills to NVMe, so a warmed working set that outgrows DRAM survives on disk
//! and a restart re-serves it without re-fetching from the origin (the parse
//! memos rebuild lazily from the still-warm raw bytes). A pathological table
//! whose footers would blow even that budget alerts and caps rather than
//! thrashing (see [`Warmer::warm_footers`]).
//!
//! # Extension point: deletion vectors
//!
//! Deletion-vector (Puffin) warming is the natural next producer on this job:
//! once the manifest reader surfaces delete-file entries (#14), warm them here
//! exactly like footers (they share the profile — small, high-fanout,
//! critical-path). The extension point is the data-file loop in
//! [`Warmer::warm_footers`].

pub mod budget;
mod coordinator;

pub use coordinator::WarmingCoordinator;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::{StreamExt, TryStreamExt, stream};
use tokio::sync::Semaphore;
use verglas_core::CacheKey;
use verglas_core::read::{ObjectRead, ReadError, ReadRange};

use crate::iceberg::{self, DataFileEntry};
use crate::mapper::{FileFormat, MapError};
use crate::parquet_meta::{self, FooterFromTail};
use budget::TokenBucket;

/// The default in-flight cap for warming fills (#50/#168).
pub const DEFAULT_WARM_CONCURRENCY: usize = 64;
/// The default speculative footer window: arrow-rs / DuckDB read the last
/// 64 KiB of a Parquet file to grab the footer in one round trip.
pub const DEFAULT_FOOTER_READ_BYTES: u64 = 64 * 1024;

/// One table to warm: which `metadata.json` and which snapshot within it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WarmTarget {
    /// Bucket the table lives in.
    pub bucket: String,
    /// The `metadata.json` object key to start the walk from.
    pub metadata_key: String,
    /// The snapshot to resolve (the current one for onboarding; a new one on a
    /// watcher-driven refresh).
    pub snapshot_id: i64,
}

/// Tunables for a warming run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WarmConfig {
    /// Maximum footer fills in flight at once (the semaphore's permit count).
    pub concurrency: usize,
    /// The speculative footer suffix window in bytes.
    pub footer_read_bytes: u64,
    /// Ceiling on the estimated footer working set. A table whose footers would
    /// exceed this alerts and caps rather than thrashing the metadata store.
    pub meta_budget_bytes: u64,
}

impl Default for WarmConfig {
    /// Concurrency 64, a 64 KiB footer window, and an effectively unbounded
    /// budget (the server narrows it to the metadata store's real capacity).
    fn default() -> WarmConfig {
        WarmConfig {
            concurrency: DEFAULT_WARM_CONCURRENCY,
            footer_read_bytes: DEFAULT_FOOTER_READ_BYTES,
            meta_budget_bytes: u64::MAX,
        }
    }
}

/// Live, shareable warming counters — the source for admin-API progress
/// (`firn status`) and metrics (#46). All fields are monotonic.
#[derive(Debug, Default)]
pub struct WarmProgress {
    /// Tables a warming walk has begun.
    tables_started: AtomicU64,
    /// Tables a warming walk has finished.
    tables_completed: AtomicU64,
    /// Data files inspected (of any format).
    files_seen: AtomicU64,
    /// Data files that were Parquet (footer candidates).
    parquet_files: AtomicU64,
    /// Block-metadata objects warmed (metadata.json, manifest lists, manifests).
    block_objects_warmed: AtomicU64,
    /// Footers successfully pinned.
    footers_warmed: AtomicU64,
    /// Total footer bytes pinned.
    footer_bytes_warmed: AtomicU64,
    /// Backend GETs issued warming footers (speculative + refetches).
    footer_gets: AtomicU64,
    /// Footers that needed a second read (footer larger than the window).
    footer_refetches: AtomicU64,
    /// Non-Parquet data files skipped (never suffix-read — the Avro rule).
    skipped_non_parquet: AtomicU64,
    /// Footers skipped because the run hit its byte budget.
    skipped_over_budget: AtomicU64,
    /// Times a table's footprint exceeded the metadata budget.
    budget_alerts: AtomicU64,
}

impl WarmProgress {
    /// A point-in-time, serializable copy of every counter (for the admin API).
    pub fn snapshot(&self) -> WarmProgressSnapshot {
        WarmProgressSnapshot {
            tables_started: self.tables_started.load(Ordering::Relaxed),
            tables_completed: self.tables_completed.load(Ordering::Relaxed),
            files_seen: self.files_seen.load(Ordering::Relaxed),
            parquet_files: self.parquet_files.load(Ordering::Relaxed),
            block_objects_warmed: self.block_objects_warmed.load(Ordering::Relaxed),
            footers_warmed: self.footers_warmed.load(Ordering::Relaxed),
            footer_bytes_warmed: self.footer_bytes_warmed.load(Ordering::Relaxed),
            footer_gets: self.footer_gets.load(Ordering::Relaxed),
            footer_refetches: self.footer_refetches.load(Ordering::Relaxed),
            skipped_non_parquet: self.skipped_non_parquet.load(Ordering::Relaxed),
            skipped_over_budget: self.skipped_over_budget.load(Ordering::Relaxed),
            budget_alerts: self.budget_alerts.load(Ordering::Relaxed),
        }
    }
}

/// A serializable copy of [`WarmProgress`], surfaced by the admin API.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WarmProgressSnapshot {
    /// See [`WarmProgress::tables_started`](WarmProgress).
    pub tables_started: u64,
    /// Tables a warming walk has finished.
    pub tables_completed: u64,
    /// Data files inspected (of any format).
    pub files_seen: u64,
    /// Data files that were Parquet.
    pub parquet_files: u64,
    /// Block-metadata objects warmed.
    pub block_objects_warmed: u64,
    /// Footers successfully pinned.
    pub footers_warmed: u64,
    /// Total footer bytes pinned.
    pub footer_bytes_warmed: u64,
    /// Backend GETs issued warming footers.
    pub footer_gets: u64,
    /// Footers that needed a second read.
    pub footer_refetches: u64,
    /// Non-Parquet data files skipped.
    pub skipped_non_parquet: u64,
    /// Footers skipped because the run hit its byte budget.
    pub skipped_over_budget: u64,
    /// Times a table's footprint exceeded the metadata budget.
    pub budget_alerts: u64,
}

/// The outcome of warming one table (or one footer batch).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct WarmSummary {
    /// Block-metadata objects warmed.
    pub block_objects_warmed: u64,
    /// Footers pinned.
    pub footers_warmed: u64,
    /// Footers that needed a second read.
    pub footer_refetches: u64,
    /// Non-Parquet data files skipped.
    pub skipped_non_parquet: u64,
    /// Footers skipped because the run hit its byte budget.
    pub skipped_over_budget: u64,
    /// Footers that errored (read or parse); warming logs and moves on.
    pub errors: u64,
    /// Whether this run's footer footprint exceeded the budget.
    pub budget_alerted: bool,
}

impl WarmSummary {
    /// Folds another summary into this one.
    fn merge(&mut self, other: WarmSummary) {
        self.block_objects_warmed += other.block_objects_warmed;
        self.footers_warmed += other.footers_warmed;
        self.footer_refetches += other.footer_refetches;
        self.skipped_non_parquet += other.skipped_non_parquet;
        self.skipped_over_budget += other.skipped_over_budget;
        self.errors += other.errors;
        self.budget_alerted |= other.budget_alerted;
    }
}

/// Errors that abort a *table* warm (a footer error is recorded and skipped,
/// never fatal — warming is best-effort background work).
#[derive(Debug, thiserror::Error)]
pub enum WarmError {
    /// A block-metadata read failed.
    #[error("warming read failed for {key}: {source}")]
    Read {
        /// The object key that failed.
        key: String,
        /// The underlying read error.
        source: ReadError,
    },
    /// A metadata parse failed.
    #[error(transparent)]
    Parse(#[from] MapError),
    /// The metadata.json does not name the requested snapshot.
    #[error("metadata.json does not name snapshot {0}")]
    UnknownSnapshot(i64),
}

/// The cache-side read a warm issues: `range` of `key`, body collected.
///
/// Boxed (`async_trait`) rather than generic over [`ObjectRead`] on purpose:
/// the server spawns warming on a background task, and an *opaque* RPITIT
/// future trips the compiler's "Send not general enough" check across a
/// `tokio::spawn` for a generic reader. Boxing every read future — the same
/// trick the crate's other IO traits use — keeps the whole warming job `Send`.
/// The blanket impl below adapts any [`ObjectRead`] (the cache engine) into one.
#[async_trait]
pub trait WarmSource: Send + Sync {
    /// Reads `range` of `key` through the cache, returning the collected body.
    /// Only ever used for metadata objects (small); never a data body.
    async fn read(&self, key: &CacheKey, range: ReadRange) -> Result<Bytes, ReadError>;
}

#[async_trait]
impl<R: ObjectRead> WarmSource for R {
    /// Reads through the cache engine and drains the (metadata-sized) body.
    async fn read(&self, key: &CacheKey, range: ReadRange) -> Result<Bytes, ReadError> {
        let got = self.get(key, range).await?;
        let mut buf = BytesMut::new();
        let mut body = got.body;
        while let Some(chunk) = body.try_next().await? {
            buf.extend_from_slice(&chunk);
        }
        Ok(buf.freeze())
    }
}

/// The eager warming job. Holds the cache reader plus the shared politeness
/// controls (semaphore + byte budget) so the server can point many table warms
/// at one budget.
pub struct Warmer {
    /// The cache-backed reader warming fills flow through.
    reader: Arc<dyn WarmSource>,
    /// Run tunables.
    config: WarmConfig,
    /// Byte-rate budget, shared with #51's prefetch executor.
    budget: Arc<TokenBucket>,
    /// In-flight fill cap.
    semaphore: Arc<Semaphore>,
    /// Live progress counters.
    progress: Arc<WarmProgress>,
}

impl Warmer {
    /// Builds a warmer with its own semaphore (`config.concurrency` permits)
    /// and a fresh progress ledger, charging fills against `budget`. Pass the
    /// cache engine (an `Arc<Engine>`) as the reader.
    pub fn new(
        reader: Arc<dyn WarmSource>,
        config: WarmConfig,
        budget: Arc<TokenBucket>,
    ) -> Warmer {
        let semaphore = Arc::new(Semaphore::new(config.concurrency.max(1)));
        Warmer {
            reader,
            config,
            budget,
            semaphore,
            progress: Arc::new(WarmProgress::default()),
        }
    }

    /// The shared progress ledger, cloneable for the admin API.
    pub fn progress(&self) -> Arc<WarmProgress> {
        Arc::clone(&self.progress)
    }

    /// [`warm_table`](Warmer::warm_table) for background tasks: the returned
    /// future owns its `Arc<Self>`, so it is `Send` for a generic reader and can
    /// be driven inside `tokio::spawn` (a borrowed `&self` future is not, due to
    /// a compiler limitation around generic `&Self` across `.await`).
    pub async fn warm_table_owned(
        self: Arc<Self>,
        target: WarmTarget,
    ) -> Result<WarmSummary, WarmError> {
        self.warm_table(&target).await
    }

    /// Walks `target`'s snapshot through the cache — warming metadata.json, the
    /// manifest list, and every manifest as whole-object reads (pinning block
    /// entries a planning engine hits), then warms every Parquet footer.
    ///
    /// Idempotent and resumable: re-running against an already-warm snapshot
    /// re-reads through the cache and issues no backend GETs, and NVMe-resident
    /// bytes survive a restart, so a cursor is unnecessary.
    pub async fn warm_table(&self, target: &WarmTarget) -> Result<WarmSummary, WarmError> {
        self.progress.tables_started.fetch_add(1, Ordering::Relaxed);
        let mut summary = WarmSummary::default();

        // metadata.json — the walk root. Full read pins the block a client's
        // planning GET will hit.
        let md = self
            .read_object(&target.bucket, &target.metadata_key, ReadRange::Full)
            .await?;
        self.note_block(&mut summary);
        let doc = iceberg::parse_metadata_json(&md, &target.bucket, &target.metadata_key)?;
        let list_key = doc
            .snapshots
            .iter()
            .find(|s| s.snapshot_id == target.snapshot_id)
            .map(|s| s.manifest_list_key.clone())
            .ok_or(WarmError::UnknownSnapshot(target.snapshot_id))?;

        // Manifest list.
        let list_bytes = self
            .read_object(&target.bucket, &list_key, ReadRange::Full)
            .await?;
        self.note_block(&mut summary);
        let manifest_keys = iceberg::parse_manifest_list(&list_bytes, &target.bucket, &list_key)?;

        // Manifests, collecting the live data-file list as we go.
        let mut data_files = Vec::new();
        for manifest_key in &manifest_keys {
            let bytes = self
                .read_object(&target.bucket, manifest_key, ReadRange::Full)
                .await?;
            self.note_block(&mut summary);
            iceberg::parse_manifest(&bytes, &target.bucket, manifest_key, &mut data_files)?;
        }

        summary.merge(self.warm_footers(&target.bucket, &data_files).await);
        self.progress
            .tables_completed
            .fetch_add(1, Ordering::Relaxed);
        Ok(summary)
    }

    /// Warms the Parquet footers among `files`, gated by the semaphore and the
    /// byte budget. Non-Parquet files are skipped without any read (the Avro
    /// addendum — never suffix-read a file with no footer). If the estimated
    /// footprint exceeds the budget, alerts once and caps the run.
    pub async fn warm_footers(&self, bucket: &str, files: &[DataFileEntry]) -> WarmSummary {
        let footprint: u64 = files
            .iter()
            .filter(|f| f.format == FileFormat::Parquet)
            .map(|f| footer_estimate(&self.config, f))
            .sum();
        let mut base = WarmSummary::default();
        let cap = if footprint > self.config.meta_budget_bytes {
            base.budget_alerted = true;
            self.progress.budget_alerts.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                footprint,
                budget = self.config.meta_budget_bytes,
                files = files.len(),
                "warming footprint exceeds metadata budget; capping to stay polite"
            );
            self.config.meta_budget_bytes
        } else {
            u64::MAX
        };

        // Each footer future owns its shared state (cheap `Arc` clones) so the
        // whole batch future is `Send` — a `&self`-borrowing future across a
        // `buffer_unordered` await is not, for `tokio::spawn`.
        let ctx = FooterCtx {
            reader: Arc::clone(&self.reader),
            progress: Arc::clone(&self.progress),
            budget: Arc::clone(&self.budget),
            semaphore: Arc::clone(&self.semaphore),
            used: Arc::new(AtomicU64::new(0)),
            config: self.config.clone(),
            bucket: Arc::from(bucket),
            cap,
        };
        // Poll a bounded fan-out; the semaphore inside each task is the real
        // in-flight gate (and the one #51 will share).
        let fanout = self.config.concurrency.saturating_mul(2).max(1);
        stream::iter(files.to_vec())
            .map(move |file| warm_one_footer(ctx.clone(), file))
            .buffer_unordered(fanout)
            .fold(base, |mut acc, part| async move {
                acc.merge(part);
                acc
            })
            .await
    }

    /// Bumps the block-metadata counters for one warmed block object.
    fn note_block(&self, summary: &mut WarmSummary) {
        self.progress
            .block_objects_warmed
            .fetch_add(1, Ordering::Relaxed);
        summary.block_objects_warmed += 1;
    }

    /// Reads `range` of `key` fully through the cache, collecting the (small,
    /// metadata-sized) body into one buffer. Only ever used for metadata
    /// objects — footers and manifests — never data bodies.
    async fn read_object(
        &self,
        bucket: &str,
        key: &str,
        range: ReadRange,
    ) -> Result<Bytes, WarmError> {
        let ck = CacheKey {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
        };
        self.reader
            .read(&ck, range)
            .await
            .map_err(|source| WarmError::Read {
                key: key.to_owned(),
                source,
            })
    }
}

/// Owned, cheaply-cloned state for one footer warm. Held by value in each
/// footer future so the batch future owns everything and stays `Send`.
#[derive(Clone)]
struct FooterCtx {
    /// The cache-backed reader.
    reader: Arc<dyn WarmSource>,
    /// Progress counters.
    progress: Arc<WarmProgress>,
    /// Byte-rate budget.
    budget: Arc<TokenBucket>,
    /// In-flight fill cap.
    semaphore: Arc<Semaphore>,
    /// Bytes reserved against `cap` so far, shared across the batch.
    used: Arc<AtomicU64>,
    /// Run tunables.
    config: WarmConfig,
    /// The bucket the files live in.
    bucket: Arc<str>,
    /// The per-run byte ceiling (`u64::MAX` when not budget-capped).
    cap: u64,
}

/// The estimated footer footprint of a file: the smaller of the file size and
/// the speculative window, but never zero (so a size-less entry still consumes
/// budget).
fn footer_estimate(config: &WarmConfig, file: &DataFileEntry) -> u64 {
    if file.size == 0 {
        config.footer_read_bytes
    } else {
        file.size.min(config.footer_read_bytes)
    }
}

/// Warms one file's footer if it is Parquet and within budget. Owns its
/// context so the future is `Send + 'static`. Records progress and returns a
/// one-file summary; a read/parse failure is logged and counted, never fatal.
async fn warm_one_footer(ctx: FooterCtx, file: DataFileEntry) -> WarmSummary {
    let mut s = WarmSummary::default();
    ctx.progress.files_seen.fetch_add(1, Ordering::Relaxed);
    if file.format != FileFormat::Parquet {
        ctx.progress
            .skipped_non_parquet
            .fetch_add(1, Ordering::Relaxed);
        s.skipped_non_parquet = 1;
        return s;
    }
    ctx.progress.parquet_files.fetch_add(1, Ordering::Relaxed);

    // Reserve against the budget cap before any read; skip if it would exceed
    // (the pathological-table guard, not a hard failure).
    let est = footer_estimate(&ctx.config, &file);
    if ctx
        .used
        .fetch_add(est, Ordering::Relaxed)
        .saturating_add(est)
        > ctx.cap
    {
        ctx.progress
            .skipped_over_budget
            .fetch_add(1, Ordering::Relaxed);
        s.skipped_over_budget = 1;
        return s;
    }

    // Concurrency gate held across both reads (semaphore is never closed → Ok).
    let Ok(_permit) = ctx.semaphore.acquire().await else {
        return s;
    };
    ctx.budget.acquire(est).await;

    let window = ctx.config.footer_read_bytes;
    let tail = match read_suffix(&ctx.reader, &ctx.bucket, &file.path, window).await {
        Ok(bytes) => bytes,
        Err(error) => {
            tracing::warn!(key = %file.path, %error, "footer speculative read failed");
            s.errors = 1;
            return s;
        }
    };
    ctx.progress.footer_gets.fetch_add(1, Ordering::Relaxed);

    match parquet_meta::parse_footer_from_tail(&file.path, &tail) {
        Ok(FooterFromTail::Complete(_)) => {
            record_warmed(&ctx.progress, tail.len() as u64);
            s.footers_warmed = 1;
        }
        Ok(FooterFromTail::NeedLarger { needed }) => {
            // The footer overran the window: one exact read (the ≤2-GET rule).
            // Charge only the extra bytes against the budget.
            ctx.budget.acquire(needed.saturating_sub(est)).await;
            match read_suffix(&ctx.reader, &ctx.bucket, &file.path, needed).await {
                Ok(exact) => {
                    ctx.progress.footer_gets.fetch_add(1, Ordering::Relaxed);
                    ctx.progress
                        .footer_refetches
                        .fetch_add(1, Ordering::Relaxed);
                    s.footer_refetches = 1;
                    match parquet_meta::parse_footer_from_tail(&file.path, &exact) {
                        Ok(FooterFromTail::Complete(_)) => {
                            record_warmed(&ctx.progress, exact.len() as u64);
                            s.footers_warmed = 1;
                        }
                        _ => {
                            tracing::warn!(key = %file.path, "footer still short after exact read");
                            s.errors = 1;
                        }
                    }
                }
                Err(error) => {
                    tracing::warn!(key = %file.path, %error, "footer exact read failed");
                    s.errors = 1;
                }
            }
        }
        Err(error) => {
            tracing::warn!(key = %file.path, %error, "footer parse failed");
            s.errors = 1;
        }
    }
    s
}

/// Reads the last `len` bytes of `key` through the cache (pins the footer tail).
async fn read_suffix(
    reader: &Arc<dyn WarmSource>,
    bucket: &str,
    key: &str,
    len: u64,
) -> Result<Bytes, ReadError> {
    let ck = CacheKey {
        bucket: bucket.to_owned(),
        key: key.to_owned(),
    };
    reader.read(&ck, ReadRange::Suffix(len)).await
}

/// Records a warmed footer of `bytes` bytes in the progress ledger.
fn record_warmed(progress: &WarmProgress, bytes: u64) {
    progress.footers_warmed.fetch_add(1, Ordering::Relaxed);
    progress
        .footer_bytes_warmed
        .fetch_add(bytes, Ordering::Relaxed);
}
