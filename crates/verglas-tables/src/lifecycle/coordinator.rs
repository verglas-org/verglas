//! Watcher-driven prefetch: bind the diff/plan/executor machinery to a
//! [`CatalogWatcher`] so a compaction commit repairs the cache automatically.
//!
//! On each catalog change the coordinator resolves the table's new snapshot,
//! replays every commit since its persisted watermark (#305 — commits made
//! while the server was down are diffed too, not just the latest), and — for a
//! content-preserving-or-shifting operation (`replace`/`overwrite`/`delete`) —
//! plans the rewritten files' hot chunks against the heat ledger and submits
//! them to the shared executor at compaction-repair priority. Every processed
//! commit's removed files are demoted with a grace window read from the
//! table's properties, enriched with the engine's demotion receipts so the
//! grace sweep can **physically remove** their blocks once time travel no
//! longer needs them. The schedule and watermarks persist across restarts.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use tokio::sync::broadcast::error::RecvError;
use tokio::task::JoinHandle;
use verglas_cache::{BlockDemoter, DemoteRequest, HardEviction};
use verglas_core::CacheKey;

use crate::catalog::{CatalogWatcher, TableChanged};
use crate::fetch::MetadataFetch;
use crate::iceberg::{SnapshotRef, parse_object_uri};
use crate::lifecycle::diff::{diff_from_snapshot_ref, read_object};
use crate::lifecycle::heat::{EpochClock, HeatLedger};
use crate::lifecycle::prefetch::{
    PrefetchConfig, PrefetchExecutor, PrefetchPriority, plan_prefetch,
};
use crate::lifecycle::retire::{
    Demotion, RetireState, RetirementScheduler, TableWatermark, demotions_for_commit,
    grace_ms_from_properties, parse_table_properties,
};
use crate::mapper::Mapper;

/// Drives snapshot-driven prefetch and retirement from a [`CatalogWatcher`].
pub struct PrefetchCoordinator<W, F> {
    /// Immutable storage binding for the watched database.
    storage_binding_id: Arc<str>,
    /// The catalog watcher whose commits trigger repairs.
    watcher: Arc<W>,
    /// The metadata reader (through the cache in the server) for diffs + footers.
    fetch: Arc<F>,
    /// The live logical-key map (for the table id and, via the aggregator, heat).
    mapper: Arc<Mapper>,
    /// The heat ledger the planner scores against.
    ledger: Arc<HeatLedger>,
    /// The shared budgeted prefetch executor.
    executor: Arc<PrefetchExecutor>,
    /// The retirement scheduler for demoted (orphaned) files.
    retire: Arc<RetirementScheduler>,
    /// The cache engine's evict-first demotion sink (#198). A commit demotes
    /// its removed files here (capturing the enumeration receipts, #305) and
    /// [`PrefetchCoordinator::sweep_expired`] physically hard-evicts them once
    /// the grace window closes. `None` when the coordinator runs without a
    /// cache (tests that only exercise scheduling).
    demoter: Option<Arc<dyn BlockDemoter>>,
    /// Where the retirement schedule and watermarks persist (#305). `None`
    /// disables persistence (tests).
    state_path: Option<PathBuf>,
    /// Per-table replay watermarks: last processed snapshot, current
    /// metadata.json key, and the retained manifest-list keys.
    watermarks: Mutex<HashMap<String, TableWatermark>>,
    /// Planning tunables.
    config: PrefetchConfig,
    /// The heat epoch clock scores are read at.
    clock: EpochClock,
}

impl<W: CatalogWatcher + 'static, F: MetadataFetch + 'static> PrefetchCoordinator<W, F> {
    /// Binds the coordinator to its dependencies.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        storage_binding_id: impl Into<Arc<str>>,
        watcher: Arc<W>,
        fetch: Arc<F>,
        mapper: Arc<Mapper>,
        ledger: Arc<HeatLedger>,
        executor: Arc<PrefetchExecutor>,
        retire: Arc<RetirementScheduler>,
        demoter: Option<Arc<dyn BlockDemoter>>,
        state_path: Option<PathBuf>,
        config: PrefetchConfig,
        clock: EpochClock,
    ) -> PrefetchCoordinator<W, F> {
        PrefetchCoordinator {
            storage_binding_id: storage_binding_id.into(),
            watcher,
            fetch,
            mapper,
            ledger,
            executor,
            retire,
            demoter,
            state_path,
            watermarks: Mutex::new(HashMap::new()),
            config,
            clock,
        }
    }

    /// The retirement scheduler (for the server's hard-eviction sweep and tests).
    pub fn retire(&self) -> &Arc<RetirementScheduler> {
        &self.retire
    }

    /// Restores the persisted retirement state (#305): re-records the
    /// schedule, re-demotes every grace-pending object in the engine (a
    /// restart must not amnesty dead bytes), reloads the watermarks, and
    /// immediately reclaims anything whose grace window closed while the
    /// server was down. Call once at startup, before [`Self::spawn`].
    pub fn restore(&self, now_ms: u64) {
        let Some(path) = &self.state_path else {
            return;
        };
        let state = RetireState::load(path);
        if !state.demotions.is_empty() {
            if let Some(demoter) = &self.demoter {
                let requests: Vec<DemoteRequest> = state
                    .demotions
                    .iter()
                    .map(|d| DemoteRequest {
                        key: CacheKey {
                            storage_binding_id: d.storage_binding_id.clone(),
                            bucket: d.bucket.clone(),
                            key: d.key.clone(),
                        },
                        size: d.size,
                    })
                    .collect();
                // Receipts are ignored on restore: the persisted enrichment
                // (etag captured while the mapping was warm) is strictly
                // better than a fresh lookup after a restart.
                let _ = demoter.demote(&requests);
            }
            self.retire.record(&state.demotions);
        }
        *self.watermarks.lock().unwrap_or_else(|p| p.into_inner()) = state.tables;
        let reclaimed = self.sweep_expired(now_ms);
        if reclaimed > 0 {
            tracing::info!(
                files = reclaimed,
                "reclaimed retirements that expired while the server was down"
            );
        }
    }

    /// The grace-window hard-eviction sweep (#198/#305): drain the schedule's
    /// demotions whose deadline is at or before `now_ms` and **physically
    /// remove** their blocks from the cache stores, so a retired file's bytes
    /// return to the shared budget once its snapshots have expired and time
    /// travel no longer needs them — no eviction pressure required. Returns
    /// how many files were swept. The server calls this on a timer; a test
    /// drives `now_ms` directly. Off the request hot path.
    pub fn sweep_expired(&self, now_ms: u64) -> usize {
        let expired = self.retire.drain_expired(now_ms);
        if expired.is_empty() {
            return 0;
        }
        if let Some(demoter) = &self.demoter {
            let evictions: Vec<HardEviction> = expired
                .iter()
                .map(|d| HardEviction {
                    key: CacheKey {
                        storage_binding_id: d.storage_binding_id.clone(),
                        bucket: d.bucket.clone(),
                        key: d.key.clone(),
                    },
                    etag: d.etag.clone(),
                    size: d.size,
                    generation: d.generation,
                })
                .collect();
            let unenumerable = evictions.iter().filter(|e| e.etag.is_none()).count();
            let reclaimed = demoter.hard_evict(&evictions);
            tracing::info!(
                files = expired.len(),
                reclaimed_bytes = reclaimed,
                unenumerable,
                "hard-evicted retired files past their grace window"
            );
        }
        self.save_state();
        expired.len()
    }

    /// Handles one catalog change: replay every commit since the table's
    /// watermark (diffing each), demote the removed files (enriched, #305),
    /// retire the superseded `metadata.json` and any expired snapshots'
    /// manifest lists, submit a prefetch plan for the newest commit, and
    /// persist the updated schedule + watermark. A dropped table or a table
    /// with no resolvable pointer / id is skipped. Best-effort: a diff error
    /// is logged, never fatal.
    pub async fn on_change(&self, change: &TableChanged) {
        let Some(new_snapshot) = change.new_snapshot else {
            return;
        };
        let Some(state) = self.watcher.table_state(&change.table) else {
            return;
        };
        let Some((bucket, metadata_key)) = parse_object_uri(&state.metadata_location) else {
            return;
        };
        // The table id is assigned by the map updater on the table's first build;
        // without it we cannot score heat, so skip (a brand-new table's first
        // commit is an append anyway).
        let Some(table) = self.mapper.table_id(&change.table) else {
            return;
        };

        // One metadata read serves the doc, the snapshot set, and the grace
        // property.
        let metadata_bytes = match read_object(
            self.fetch.as_ref(),
            &self.storage_binding_id,
            &bucket,
            &metadata_key,
        )
        .await
        {
            Ok(bytes) => bytes,
            Err(error) => {
                tracing::warn!(table = %change.table, %error, "metadata read failed; skipping");
                return;
            }
        };
        let doc = match crate::iceberg::parse_metadata_json(&metadata_bytes, &bucket, &metadata_key)
        {
            Ok(doc) => doc,
            Err(error) => {
                tracing::warn!(table = %change.table, %error, "metadata parse failed; skipping");
                return;
            }
        };
        let grace = grace_ms_from_properties(&parse_table_properties(&metadata_bytes));
        let now_ms = now_unix_ms();
        let table_key = change.table.to_string();
        let watermark = self
            .watermarks
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&table_key)
            .cloned();

        // Replay every commit since the watermark (#305). Without one — first
        // sight of the table, or a watermark that expired out of the metadata
        // — only the new commit is processed, exactly the pre-replay behavior.
        let to_process = snapshots_to_process(
            &doc.snapshots,
            watermark.as_ref().map(|w| w.last_snapshot_id),
            new_snapshot,
        );
        let mut recorded: Vec<Demotion> = Vec::new();
        let mut newest_diff = None;
        for snap in to_process {
            let diff = match diff_from_snapshot_ref(
                self.fetch.as_ref(),
                &self.storage_binding_id,
                &bucket,
                snap,
            )
            .await
            {
                Ok(diff) => diff,
                Err(error) => {
                    tracing::warn!(table = %change.table, snapshot = snap.snapshot_id, %error, "commit diff failed; skipping commit");
                    continue;
                }
            };
            if !diff.removed.is_empty() {
                let requests: Vec<DemoteRequest> = diff
                    .removed
                    .iter()
                    .map(|file| DemoteRequest {
                        key: CacheKey {
                            storage_binding_id: self.storage_binding_id.to_string(),
                            bucket: bucket.clone(),
                            key: file.path.clone(),
                        },
                        size: file.size,
                    })
                    .collect();
                let receipts = match &self.demoter {
                    Some(demoter) => demoter.demote(&requests),
                    None => Vec::new(),
                };
                recorded.extend(demotions_for_commit(
                    &self.storage_binding_id,
                    &bucket,
                    &diff.removed,
                    &receipts,
                    grace,
                    now_ms,
                ));
            }
            if snap.snapshot_id == new_snapshot {
                newest_diff = Some(diff);
            }
        }

        // The superseded metadata.json and any expired snapshots' manifest
        // lists are dead objects too (#305) — the metadata churn that filled
        // meta.data. Sizes come from the engine's mapping receipts (0 when
        // cold: unreachable garbage left to LRU). Manifest *data* files are
        // shared across snapshots and are deliberately not retired without a
        // refcount — see [`TableWatermark::manifest_lists`].
        let mut dead_metadata: Vec<CacheKey> = Vec::new();
        if let Some(w) = &watermark {
            if w.metadata_key != metadata_key && !w.metadata_key.is_empty() {
                dead_metadata.push(CacheKey {
                    storage_binding_id: self.storage_binding_id.to_string(),
                    bucket: bucket.clone(),
                    key: w.metadata_key.clone(),
                });
            }
            for (snap_id, list_key) in &w.manifest_lists {
                if !doc.snapshots.iter().any(|s| s.snapshot_id == *snap_id) {
                    dead_metadata.push(CacheKey {
                        storage_binding_id: self.storage_binding_id.to_string(),
                        bucket: bucket.clone(),
                        key: list_key.clone(),
                    });
                }
            }
        }
        if !dead_metadata.is_empty() {
            let requests: Vec<DemoteRequest> = dead_metadata
                .iter()
                .map(|key| DemoteRequest {
                    key: key.clone(),
                    size: 0,
                })
                .collect();
            let receipts = match &self.demoter {
                Some(demoter) => demoter.demote(&requests),
                None => Vec::new(),
            };
            let deadline = now_ms.saturating_add(grace);
            for key in &dead_metadata {
                let receipt = receipts.iter().find(|r| &r.key == key);
                recorded.push(Demotion {
                    storage_binding_id: key.storage_binding_id.clone(),
                    bucket: key.bucket.clone(),
                    key: key.key.clone(),
                    hard_evict_at_ms: deadline,
                    etag: receipt.and_then(|r| r.etag.clone()),
                    size: receipt.map(|r| r.size).unwrap_or(0),
                    generation: receipt.map(|r| r.generation).unwrap_or(0),
                });
            }
        }

        if !recorded.is_empty() {
            self.retire.record(&recorded);
            tracing::info!(
                table = %change.table,
                demoted = recorded.len(),
                grace_ms = grace,
                "retired orphaned objects (demoted, grace pending)"
            );
        }

        // Advance the watermark to this commit and persist.
        {
            let mut marks = self.watermarks.lock().unwrap_or_else(|p| p.into_inner());
            marks.insert(
                table_key,
                TableWatermark {
                    last_snapshot_id: new_snapshot,
                    metadata_key: metadata_key.clone(),
                    manifest_lists: doc
                        .snapshots
                        .iter()
                        .map(|s| (s.snapshot_id, s.manifest_list_key.clone()))
                        .collect(),
                },
            );
        }
        self.save_state();

        // Plan and submit the prefetch for the newest data-carrying commit
        // only — a replay of downtime commits must not storm the executor with
        // stale plans.
        let Some(diff) = newest_diff else {
            return;
        };
        let plan = plan_prefetch(
            self.fetch.as_ref(),
            &self.ledger,
            table,
            &self.storage_binding_id,
            &bucket,
            &diff,
            &self.config,
            self.clock.now(),
        )
        .await;
        if !plan.is_empty() {
            tracing::info!(
                table = %change.table,
                op = ?diff.op,
                footers = plan.footers.len(),
                chunks = plan.chunks.len(),
                whole_files = plan.whole_files.len(),
                "submitting compaction-repair prefetch"
            );
            self.executor
                .submit_plan(PrefetchPriority::CompactionRepair, &plan);
        }
    }

    /// Persists the current schedule and watermarks (#305). Best-effort: a
    /// failed save costs durability across a restart, never correctness.
    fn save_state(&self) {
        let Some(path) = &self.state_path else {
            return;
        };
        let state = RetireState {
            demotions: self.retire.all(),
            tables: self
                .watermarks
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone(),
        };
        if let Err(error) = state.save(path) {
            tracing::warn!(%error, path = %path.display(), "failed to persist retire state");
        }
    }

    /// Spawns the background loop: consume catalog events, repairing on each
    /// commit, and on a timer run the grace-window hard-eviction sweep (#198).
    /// A lagged subscription is ignored (prefetch is best-effort — the next
    /// commit re-triggers repair). Ends when the watcher closes.
    pub fn spawn(self) -> JoinHandle<()> {
        let mut events = self.watcher.subscribe();
        tokio::spawn(async move {
            let mut sweep = tokio::time::interval(SWEEP_INTERVAL);
            // The first tick fires immediately; skip it (restore already swept).
            sweep.tick().await;
            loop {
                tokio::select! {
                    event = events.recv() => match event {
                        Ok(change) if change.new_snapshot.is_some() => {
                            self.on_change(&change).await;
                        }
                        Ok(_) => {}
                        Err(RecvError::Lagged(_)) => {}
                        Err(RecvError::Closed) => break,
                    },
                    _ = sweep.tick() => {
                        let _ = self.sweep_expired(now_unix_ms());
                    }
                }
            }
        })
    }
}

/// Selects the commits `on_change` must process (#305): everything after the
/// table's watermark, in metadata order, up to and including the new snapshot.
/// Without a usable watermark (first sight, or the watermark expired out of
/// the metadata) only the new commit is processed — retroactive cleanup of a
/// pre-watermark backlog is not attempted, its dead bytes age out under LRU.
fn snapshots_to_process(
    snapshots: &[SnapshotRef],
    watermark: Option<i64>,
    new_snapshot: i64,
) -> Vec<&SnapshotRef> {
    let newest_only = || {
        snapshots
            .iter()
            .filter(|s| s.snapshot_id == new_snapshot)
            .collect::<Vec<_>>()
    };
    let Some(watermark) = watermark else {
        return newest_only();
    };
    let Some(start) = snapshots.iter().position(|s| s.snapshot_id == watermark) else {
        return newest_only();
    };
    let Some(end) = snapshots.iter().position(|s| s.snapshot_id == new_snapshot) else {
        return newest_only();
    };
    if end <= start {
        return Vec::new();
    }
    snapshots[start + 1..=end].iter().collect()
}

/// How often the spawned loop runs the grace-window hard-eviction sweep (#198).
/// The grace window is hours to days (`history.expire.*`), so a few-minute
/// cadence keeps the demotion set bounded without a config knob; the sweep is
/// idempotent and cheap (a deadline scan off the request hot path).
const SWEEP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(300);

/// The current wall-clock time in Unix milliseconds (for grace deadlines).
fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A snapshot ref with only the fields selection cares about.
    fn snap(id: i64) -> SnapshotRef {
        SnapshotRef {
            snapshot_id: id,
            manifest_list_key: format!("t/metadata/snap-{id}.avro"),
            operation: Some("replace".to_owned()),
            parent_snapshot_id: None,
        }
    }

    /// Replay selection (#305): with a watermark, every commit after it up to
    /// the new snapshot is processed; without one (or with one that expired
    /// out of the metadata) only the new commit is.
    #[test]
    fn replay_selection_walks_from_the_watermark() {
        let snaps: Vec<SnapshotRef> = [1, 2, 3, 4, 5].map(snap).to_vec();

        // Watermark at 2, new at 5: replay 3, 4, 5.
        let picked = snapshots_to_process(&snaps, Some(2), 5);
        assert_eq!(
            picked.iter().map(|s| s.snapshot_id).collect::<Vec<_>>(),
            vec![3, 4, 5]
        );

        // No watermark: the new commit only.
        let picked = snapshots_to_process(&snaps, None, 5);
        assert_eq!(
            picked.iter().map(|s| s.snapshot_id).collect::<Vec<_>>(),
            vec![5]
        );

        // Watermark expired out of the metadata: the new commit only.
        let picked = snapshots_to_process(&snaps, Some(999), 4);
        assert_eq!(
            picked.iter().map(|s| s.snapshot_id).collect::<Vec<_>>(),
            vec![4]
        );

        // Already at (or past) the new snapshot: nothing to do.
        assert!(snapshots_to_process(&snaps, Some(5), 5).is_empty());
        assert!(snapshots_to_process(&snaps, Some(5), 3).is_empty());
    }
}
