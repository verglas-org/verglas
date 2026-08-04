//! Snapshot-history expiry for Iceberg table maintenance.
//!
//! [`expire_snapshots`] removes old entries from a table metadata's snapshot
//! list. It never rewrites data; it only mutates metadata via
//! `TableUpdate::RemoveSnapshots`. `crate::compaction` calls it after each pass
//! so a table's `metadata.json` stops growing with every commit.
//!
//! Platform `_LOGS` day-partition pruning and run-event logging are not owned
//! here — catalog-side lakekeeping (e.g. Lakekeeper expire/orphan queues) owns
//! telemetry retention. This module is the compute-side snapshot-expiry helper
//! compaction needs when it commits through the engine.

use std::collections::BTreeSet;

use iceberg::spec::MAIN_BRANCH;
use iceberg::table::Table;
use iceberg::{Catalog, TableCommit, TableIdent, TableRequirement, TableUpdate};
use serde::Serialize;

use crate::error::{AgentError, Result};
use crate::ident::ident_to_dotted;

/// The default number of most-recent snapshots [`expire_snapshots`] keeps for a
/// compaction pass. A flat keep-last count rather than a max-age cutoff: the
/// motivating case (issue #281) is a table whose snapshot count exploded not
/// from age but from commit *rate* — thousands of per-contract backtest
/// gap-fill commits landing within a short span — so an age cutoff would not
/// bound `metadata.json` growth between passes the way a keep-last count does.
/// This is a fixed platform constant, not a tuning knob, per the operator's
/// no-unasked-knobs rule; only a test constructs a smaller value to make the
/// expiry deterministic without seeding hundreds of snapshots.
pub const DEFAULT_KEEP_LAST_SNAPSHOTS: usize = 10;

/// How many times a metadata-only expire commit is retried past a concurrent
/// writer before the call surfaces the conflict. Mirrors the append and
/// compaction paths so a persistently contended table fails loudly instead of
/// spinning.
const COMMIT_ATTEMPTS: u32 = 8;

/// The result of expiring snapshot history on one table.
#[derive(Debug, Clone, Serialize)]
pub struct ExpireReport {
    /// The dotted `namespace.name` of the table.
    pub table: String,
    /// How many snapshots remain after the expire.
    pub kept_snapshots: u64,
    /// Snapshot ids removed, sorted ascending. Empty when nothing was expired.
    pub removed_snapshots: Vec<i64>,
}

impl ExpireReport {
    /// True when at least one snapshot was removed.
    pub fn did_expire(&self) -> bool {
        !self.removed_snapshots.is_empty()
    }
}

/// Expires old snapshot history on any table, keeping only the `keep_last` most
/// recent snapshots by commit sequence number, plus whichever snapshot the
/// `main` ref currently points to. A table already at or under `keep_last`
/// snapshots commits nothing and returns a zero-removal report.
///
/// This never touches live data: it only prunes entries from the table
/// metadata's snapshot list, so `count(*)` and the current rows are untouched
/// — only time travel to an expired snapshot id stops working. Removing a
/// snapshot from the metadata does not delete the data files it referenced; a
/// file can still be reachable from a snapshot that survives the expiry.
/// Deleting the now-unreachable files is a separate, explicit step the caller
/// drives (see `crate::compaction::cleanup_orphans`) — this function only ever
/// mutates table metadata, never storage.
///
/// Commits through the forked [`TableCommit::from_parts`] escape hatch:
/// upstream Iceberg 0.9.1's public transaction API has no way to build a
/// metadata-only `RemoveSnapshots` commit (only fast-append and property
/// updates). Concurrent commits are tolerated via CAS retry against the `main`
/// ref, mirroring the compaction path.
pub async fn expire_snapshots(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    keep_last: usize,
) -> Result<ExpireReport> {
    let mut table = catalog.load_table(ident).await?;
    let mut attempt = 1u32;
    loop {
        match try_expire_once(catalog, &table, ident, keep_last).await {
            Ok(report) => return Ok(report),
            Err(AgentError::Iceberg(e))
                if e.kind() == iceberg::ErrorKind::CatalogCommitConflicts
                    && attempt < COMMIT_ATTEMPTS =>
            {
                // A concurrent commit landed first. Reload and recompute the
                // keep-last window against the new snapshot list, then retry.
                tokio::time::sleep(std::time::Duration::from_millis(250 * u64::from(attempt)))
                    .await;
                table = catalog.load_table(ident).await?;
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}

/// One expire attempt against `table` as currently loaded: pick the
/// `keep_last` most recent snapshots by sequence number (monotonic per
/// commit, unlike a wall-clock timestamp two commits can share), always
/// keeping the `main` ref's target, and remove every other snapshot id in one
/// metadata-only commit. Returns a `CatalogCommitConflicts` error the caller
/// retries on.
async fn try_expire_once(
    catalog: &dyn Catalog,
    table: &Table,
    ident: &TableIdent,
    keep_last: usize,
) -> Result<ExpireReport> {
    let metadata = table.metadata();
    let total = metadata.snapshots().len();
    if total <= keep_last {
        return Ok(ExpireReport {
            table: ident_to_dotted(ident),
            kept_snapshots: total as u64,
            removed_snapshots: Vec::new(),
        });
    }

    let mut by_recency: Vec<&iceberg::spec::SnapshotRef> = metadata.snapshots().collect();
    by_recency.sort_unstable_by_key(|s| std::cmp::Reverse(s.sequence_number()));

    let mut keep: BTreeSet<i64> = by_recency
        .iter()
        .take(keep_last)
        .map(|s| s.snapshot_id())
        .collect();
    // The `main` ref's target survives regardless of the keep-last window — a
    // compaction pass must never expire the snapshot every read currently
    // serves from. Only `main` is a live ref in this codebase today; a future
    // named branch or tag would need the same protection added here.
    if let Some(current) = metadata.snapshot_for_ref(MAIN_BRANCH) {
        keep.insert(current.snapshot_id());
    }

    let mut removed_snapshots: Vec<i64> = by_recency
        .iter()
        .map(|s| s.snapshot_id())
        .filter(|id| !keep.contains(id))
        .collect();
    if removed_snapshots.is_empty() {
        return Ok(ExpireReport {
            table: ident_to_dotted(ident),
            kept_snapshots: total as u64,
            removed_snapshots: Vec::new(),
        });
    }
    removed_snapshots.sort_unstable();

    let update = TableUpdate::RemoveSnapshots {
        snapshot_ids: removed_snapshots.clone(),
    };
    let requirements = vec![
        TableRequirement::UuidMatch {
            uuid: metadata.uuid(),
        },
        TableRequirement::RefSnapshotIdMatch {
            r#ref: MAIN_BRANCH.to_owned(),
            snapshot_id: metadata.current_snapshot_id(),
        },
    ];
    let commit = TableCommit::from_parts(ident.clone(), requirements, vec![update]);
    catalog.update_table(commit).await?;

    Ok(ExpireReport {
        table: ident_to_dotted(ident),
        kept_snapshots: keep.len() as u64,
        removed_snapshots,
    })
}
