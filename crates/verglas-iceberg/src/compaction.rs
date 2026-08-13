//! Data-file compaction: rewrite many small files into fewer target-sized ones
//! and commit the result as a REPLACE snapshot.
//!
//! This is the compute-side half of auto-compaction. The catalog service signals
//! which tables are over threshold (its `maintenance/candidates` endpoint); this
//! module does the rewriting on real compute — reading the small data files
//! through the engine (cache-pathed when a server is in the path), writing new
//! target-sized files to the table location, and committing a REPLACE that
//! removes the old files and adds the new ones. It never runs in the catalog's
//! Durable Object, whose CPU and memory cannot open Parquet.
//!
//! Data-preserving by construction. Compaction only reorganizes storage: after a
//! rewrite `count(*)` and full row content are identical to before, and every
//! retained snapshot still time-travels. The load-bearing invariant is that the
//! REPLACE reads the exact rows of the files it removes and writes them back
//! unchanged (optionally reordered when a sort order is defined).
//!
//! Planning follows Apache Iceberg's size policy. Files are grouped by partition
//! spec and value; files below 75% of the table's target size are bin-packed into
//! new files that roll at the target. When the table defines a sort order, merged
//! rows are sorted by it before they are written (sort-compaction); otherwise the
//! rows are bin-packed in read order. Files in Iceberg's healthy size band and
//! lone small files with nothing to merge are carried forward untouched. Reads
//! use Iceberg-planned tasks, so row-level deletes are applied before rewrite.
//!
//! Commit mechanism. Like [`crate::retention`], the REPLACE snapshot is produced
//! at the manifest layer — a new manifest carrying the rewritten files as `Added`
//! entries and the untouched files as `Existing` entries, a new manifest list,
//! and a `Replace` snapshot committed through `Catalog::update_table` with a
//! `RefSnapshotIdMatch` requirement. A concurrent append that moved `main` makes
//! the requirement stale, so the commit fails with a conflict and is retried
//! against the reloaded table rather than silently overwriting the append.
//!
//! Orphan cleanup. After a REPLACE the old small files are still referenced by the
//! pre-compaction snapshot (for time travel), so they are not deleted yet.
//! [`cleanup_orphans`] deletes a candidate file only once it is unreachable from
//! *every* retained snapshot — the conservative rule that never drops a file a
//! live snapshot still needs.
//!
//! Snapshot expiry. A REPLACE is itself a new snapshot, and old small-file
//! commits (including per-record backtest gap-fill appends) leave old snapshots
//! behind too — left unchecked, a table's `metadata.json` grows without bound
//! purely from history, independent of how well-packed its data files are. After
//! each pass, [`expire_and_cleanup`] calls [`crate::retention::expire_snapshots`]
//! to drop history down to [`crate::retention::DEFAULT_KEEP_LAST_SNAPSHOTS`], then
//! deletes the data files that expiry left unreachable from every remaining
//! snapshot — the same reachability rule [`cleanup_orphans`] already enforces,
//! so an expired snapshot's files are cleaned up rather than orphaned on
//! storage forever.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef as ArrowSchemaRef;
use chrono::Utc;
use datafusion::arrow::compute::{
    SortColumn, SortOptions, concat_batches, lexsort_to_indices, take_record_batch,
};
use futures::TryStreamExt;
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::io::FileIO;
use iceberg::scan::FileScanTask;
use iceberg::spec::{
    DataContentType, DataFile, DataFileFormat, FormatVersion, MAIN_BRANCH, ManifestContentType,
    ManifestListWriter, ManifestWriterBuilder, Operation, Schema as IcebergSchema, Snapshot,
    SnapshotReference, SnapshotRetention, SortOrder, Struct, Summary, TableProperties, Transform,
};
use iceberg::table::Table;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::partitioning::PartitioningWriter;
use iceberg::writer::partitioning::fanout_writer::FanoutWriter;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, TableCommit, TableIdent, TableRequirement, TableUpdate};
use parquet::file::properties::WriterProperties;
use serde::Serialize;
pub use verglas_api::report::{CompactReport, CompactionReport};

use crate::error::{AgentError, Result};
use crate::ident::ident_to_dotted;
use crate::retention;

/// The subdirectory under a table's location that holds manifests and manifest
/// lists — the Iceberg convention the write path also follows.
const METADATA_DIR: &str = "metadata";

/// How many times a REPLACE commit is retried past a concurrent writer before the
/// compaction surfaces the conflict. Mirrors the append and retention paths so a
/// persistently contended table fails loudly instead of spinning.
const COMMIT_ATTEMPTS: u32 = 8;

/// The Apache Iceberg and S3 Tables default target file size: 512 MiB.
pub const DEFAULT_TARGET_FILE_BYTES: u64 = 512 * 1024 * 1024;

/// Apache Iceberg's default lower bound for a healthy file is 75% of target.
/// Files at or above this bound are not rewritten merely because they are a few
/// bytes below the target; this prevents no-benefit rewrite churn.
const MIN_FILE_SIZE_RATIO_PERCENT: u64 = 75;

/// Apache Iceberg's default maximum input size for one rewrite file group.
/// Verglas also applies [`CompactionOptions::max_files_per_group`] so a group is
/// bounded by both bytes and file count.
const MAX_FILE_GROUP_SIZE_BYTES: u64 = 100 * 1024 * 1024 * 1024;

/// Returns the lower edge of Iceberg's healthy file-size band.
fn min_file_size_bytes(target_file_bytes: u64) -> u64 {
    target_file_bytes.saturating_mul(MIN_FILE_SIZE_RATIO_PERCENT) / 100
}

/// How many undersized (below 75% of target) data files a table must exceed before the
/// scheduled scan spends a compaction pass on it. Deliberately generous: a table
/// with a few dozen small files is not yet worth a rewrite, but one that has
/// accumulated thousands of single-record files (the shape the managed catalog
/// used to compact away) clears this bar immediately. [`run_compaction`] applies
/// it as the table-selection gate; [`compact_table`] itself, once called,
/// compacts any partition with mergeable small files regardless.
pub const DEFAULT_MIN_SMALL_FILES: usize = 64;

/// How many small data files one bin-pack group merges and commits at most. This
/// bounds the work behind a single REPLACE commit so progress ratchets: a pass
/// commits group by group, and a run killed mid-pass keeps every group already
/// committed. Without a file-count cap a table of thousands of tiny files would
/// pack into one target-sized output and commit only once at the very end — the
/// all-or-nothing shape that made zero progress under the fleet's ACK ceiling.
pub const DEFAULT_MAX_FILES_PER_GROUP: usize = 512;

/// The wall-clock budget for one whole compaction pass. Checked only between
/// group commits (never against an in-flight read, write, or commit — those
/// always run to completion or fail on their own). After the budget is spent the
/// pass finishes the current group and stops with a partial report; the hourly
/// schedule (or repeated manual runs) converges the backlog. Held below the
/// fleet host-agent's completion-ACK ceiling so the final group's commit lands
/// with margin to spare.
pub const DEFAULT_PASS_BUDGET: Duration = Duration::from_secs(600);

/// Compaction knobs, kept deliberately minimal. These are platform constants, not
/// a user-facing config surface — the scheduled job and the manual trigger always
/// use the defaults; only tests construct a custom value (a tiny budget or a small
/// group cap) to drive the ratcheting deterministically.
#[derive(Debug, Clone, Copy)]
pub struct CompactionOptions {
    /// Optional target data-file size override. `None` uses the table's native
    /// `write.target-file-size-bytes` property (512 MiB when absent).
    pub target_file_bytes: Option<u64>,
    /// The scan only spends a pass on a table with strictly more than this many
    /// undersized data files (see [`run_compaction`]).
    pub min_small_files: usize,
    /// The most small files one group merges and commits at once — the per-commit
    /// work bound that makes progress ratchet across groups.
    pub max_files_per_group: usize,
    /// The wall budget for a whole pass, checked only between group commits.
    pub pass_budget: Duration,
}

impl Default for CompactionOptions {
    /// The platform defaults: 512 MiB target, 64 small-file selection floor, 512
    /// files per group, a 10-minute pass budget.
    fn default() -> Self {
        Self {
            target_file_bytes: None,
            min_small_files: DEFAULT_MIN_SMALL_FILES,
            max_files_per_group: DEFAULT_MAX_FILES_PER_GROUP,
            pass_budget: DEFAULT_PASS_BUDGET,
        }
    }
}

/// Resolves the rewrite target from an explicit test/caller override or the
/// table's native Iceberg properties.
fn target_file_bytes(table: &Table, options: CompactionOptions) -> Result<u64> {
    if let Some(target) = options.target_file_bytes {
        if target == 0 {
            return Err(AgentError::Compaction(
                "target file size must be greater than zero".to_owned(),
            ));
        }
        return Ok(target);
    }
    let properties = TableProperties::try_from(table.metadata().properties()).map_err(|error| {
        AgentError::Compaction(format!("read Iceberg table write properties: {error}"))
    })?;
    Ok(properties.write_target_file_size_bytes as u64)
}

/// Builds the engine's empty per-table aggregate.
fn empty_compact_report(ident: &TableIdent) -> CompactReport {
    CompactReport {
        table: ident_to_dotted(ident),
        groups_committed: 0,
        input_data_files: 0,
        output_data_files: 0,
        input_records: 0,
        output_records: 0,
        removed_files: Vec::new(),
        added_files: Vec::new(),
        sorted: false,
        undersized_remaining: 0,
        budget_bounded: false,
        snapshot_id: None,
        snapshots_expired: 0,
        orphan_files_deleted: 0,
    }
}

/// Folds one committed group into the table aggregate.
fn absorb_compact_report(aggregate: &mut CompactReport, group: CompactReport) {
    aggregate.groups_committed += group.groups_committed;
    aggregate.input_data_files += group.input_data_files;
    aggregate.output_data_files += group.output_data_files;
    aggregate.input_records += group.input_records;
    aggregate.output_records += group.output_records;
    aggregate.removed_files.extend(group.removed_files);
    aggregate.added_files.extend(group.added_files);
    aggregate.sorted |= group.sorted;
    if group.snapshot_id.is_some() {
        aggregate.snapshot_id = group.snapshot_id;
    }
}

/// The result of an orphan-cleanup pass over one table.
#[derive(Debug, Clone, Serialize)]
pub struct OrphanReport {
    /// The dotted `namespace.name` of the table.
    pub table: String,
    /// Candidate files that were unreachable from every retained snapshot and
    /// were deleted.
    pub deleted: Vec<String>,
    /// Candidate files still referenced by a retained snapshot; kept so time
    /// travel to that snapshot keeps working.
    pub retained_for_time_travel: Vec<String>,
}

/// Scans every namespace and compacts each table that has accumulated more than
/// `min_small_files` undersized data files. A table at or below the threshold is
/// skipped without a rewrite — most tables never hold enough small files to be
/// worth the read-and-rewrite, and this is the gate that keeps the scheduled pass
/// cheap. A per-table failure is recorded and the pass continues; only a failure
/// to list namespaces or tables aborts.
///
/// Progress ratchets and the pass is time-bounded. Each table is compacted group
/// by group, one REPLACE commit per bin-pack group, so a run killed mid-pass keeps
/// everything committed so far and the next run continues from the new table state.
/// A single wall budget ([`CompactionOptions::pass_budget`]) bounds the whole pass;
/// it is checked only between group commits and between tables, never against an
/// in-flight read/write/commit. When the budget is spent the pass stops with a
/// partial report and the hourly schedule (or repeated manual runs) converges the
/// backlog.
///
/// The candidate decision reads the table's manifests for the exact per-file
/// sizes rather than trusting any snapshot summary (whose totals are per-commit,
/// not a table-wide count). A table that clears the threshold but whose files are
/// all already adequate compacts nothing and is dropped from the report.
pub async fn run_compaction(
    catalog: &dyn Catalog,
    options: CompactionOptions,
) -> Result<CompactionReport> {
    let deadline = Instant::now() + options.pass_budget;
    let namespaces = catalog.list_namespaces(None).await?;
    let mut idents: Vec<TableIdent> = Vec::new();
    for namespace in namespaces {
        idents.extend(catalog.list_tables(&namespace).await?);
    }
    let total = idents.len();
    let mut report = CompactionReport {
        tables_scanned: 0,
        compacted: Vec::new(),
        failures: Vec::new(),
        groups_committed: 0,
        undersized_remaining: 0,
        budget_bounded: false,
        snapshots_expired: 0,
        orphan_files_deleted: 0,
    };

    for (index, ident) in idents.into_iter().enumerate() {
        report.tables_scanned += 1;
        match compact_table_if_over_threshold(catalog, &ident, options, deadline).await {
            Ok(table_report) => {
                report.groups_committed += table_report.groups_committed;
                report.undersized_remaining += table_report.undersized_remaining;
                // A table stopped mid-way (mergeable work left) makes the pass
                // budget-bounded.
                report.budget_bounded |= table_report.budget_bounded;
                report.snapshots_expired += table_report.snapshots_expired;
                report.orphan_files_deleted += table_report.orphan_files_deleted;
                if table_report.did_anything() {
                    report.compacted.push(table_report);
                }
            }
            Err(e) => report
                .failures
                .push((ident_to_dotted(&ident), e.to_string())),
        }
        // Between tables: once the budget is spent, stop and leave the rest for the
        // next run. Only flag the pass budget-bounded when tables actually go
        // unexamined — a pass that finished its last table on the buzzer was not
        // cut short.
        if Instant::now() >= deadline && index + 1 < total {
            report.budget_bounded = true;
            break;
        }
    }
    Ok(report)
}

/// The scan's per-table gate: load the table, and only when it holds strictly more
/// than `min_small_files` undersized data files spend a bounded compaction pass on
/// it. A below-threshold table returns an empty report (no group, no remaining
/// backlog — it is deliberately left until it grows). An over-threshold table is
/// compacted group by group up to the shared `deadline`.
///
/// Selection is a separate decision from the rewrite: the bounded pass, once
/// entered, compacts any partition with mergeable small files, but a table is only
/// picked here once its small-file count is large enough to be worth a pass.
async fn compact_table_if_over_threshold(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    options: CompactionOptions,
    deadline: Instant,
) -> Result<CompactReport> {
    let table = catalog.load_table(ident).await?;
    let target = target_file_bytes(&table, options)?;
    let small = undersized_file_count(&table, target).await?;
    if (small as usize) <= options.min_small_files {
        // Below the floor: not this pass's backlog. The load above was only to
        // read the manifest file sizes for the gate.
        return Ok(empty_compact_report(ident));
    }
    compact_table_bounded(catalog, ident, options, deadline).await
}

/// Counts the undersized files that are still mergeable in `table`: files below
/// Iceberg's 75%-of-target lower bound in a partition that holds at least one
/// other candidate. A partition with one candidate has nothing to merge, so it
/// contributes nothing.
async fn mergeable_small_file_count(table: &Table, target: u64) -> Result<u64> {
    let by_partition = live_files_by_partition(table).await?;
    let min_file_size = min_file_size_bytes(target);
    let mut remaining = 0u64;
    for files in by_partition.values() {
        let small = files
            .iter()
            .filter(|f| f.data_file.file_size_in_bytes() < min_file_size)
            .count();
        if small >= 2 {
            remaining += small as u64;
        }
    }
    Ok(remaining)
}

/// Counts the table's live data files below Iceberg's 75%-of-target lower bound.
/// Reads per-file sizes from manifests rather than snapshot summary totals.
pub async fn undersized_file_count(table: &Table, target_file_bytes: u64) -> Result<u64> {
    let by_partition = live_files_by_partition(table).await?;
    let min_file_size = min_file_size_bytes(target_file_bytes);
    let count = by_partition
        .values()
        .flatten()
        .filter(|f| f.data_file.file_size_in_bytes() < min_file_size)
        .count();
    Ok(count as u64)
}

/// Compacts one table to completion (within the pass budget): bin-pack (or
/// sort-compact) its small data files into target-sized files, committing one
/// REPLACE per bin-pack group so progress ratchets. Files are grouped by
/// partition spec and value; within a partition undersized files are merged when
/// there are at least two, in groups bounded by
/// [`CompactionOptions::max_files_per_group`]. Healthy and lone small files are
/// carried forward. When the table
/// has a sort order the merged rows are sorted by it. A table with nothing
/// mergeable commits nothing. Concurrent appends are tolerated by per-group CAS
/// retry.
///
/// The pass budget is derived from `options` here; [`run_compaction`] shares one
/// deadline across tables instead.
pub async fn compact_table(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    options: CompactionOptions,
) -> Result<CompactReport> {
    let deadline = Instant::now() + options.pass_budget;
    compact_table_bounded(catalog, ident, options, deadline).await
}

/// Compacts one table group by group until it is fully drained or the shared
/// `deadline` is reached. Each group is a REPLACE commit of its own (with CAS
/// retry), so a run killed at any point keeps every group already committed. The
/// budget is checked only between groups: an in-flight group commit always runs to
/// completion. Returns the per-table aggregate, with `budget_bounded` set only
/// when the pass stopped with mergeable work still left.
async fn compact_table_bounded(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    options: CompactionOptions,
    deadline: Instant,
) -> Result<CompactReport> {
    let mut aggregate = empty_compact_report(ident);
    loop {
        match compact_next_group(catalog, ident, options).await? {
            Some(group) => absorb_compact_report(&mut aggregate, group),
            None => break, // fully drained: no partition has a mergeable pair left
        }
        // Between groups: stop if the budget is spent, but only flag the pass as
        // budget-bounded when mergeable work actually remains — a table that
        // happened to finish on the last group is not "cut short".
        if Instant::now() >= deadline {
            let table = catalog.load_table(ident).await?;
            let target = target_file_bytes(&table, options)?;
            let remaining = mergeable_small_file_count(&table, target).await?;
            aggregate.undersized_remaining = remaining;
            aggregate.budget_bounded = remaining > 0;
            let (expired, orphaned) =
                expire_and_cleanup(catalog, ident, &aggregate.removed_files).await?;
            aggregate.snapshots_expired = expired;
            aggregate.orphan_files_deleted = orphaned;
            return Ok(aggregate);
        }
    }
    // Drained fully: no mergeable backlog remains.
    let table = catalog.load_table(ident).await?;
    let target = target_file_bytes(&table, options)?;
    aggregate.undersized_remaining = mergeable_small_file_count(&table, target).await?;
    let (expired, orphaned) = expire_and_cleanup(catalog, ident, &aggregate.removed_files).await?;
    aggregate.snapshots_expired = expired;
    aggregate.orphan_files_deleted = orphaned;
    Ok(aggregate)
}

/// Plans and commits the next bin-pack group for `ident`, with per-group CAS
/// retry. Reloads fresh metadata each attempt (so a conflict re-plans against the
/// mover's snapshot), plans one bounded group, rewrites it, and commits a REPLACE
/// that removes only that group's files and carries everything else forward.
/// Returns the committed group's report, or `None` when no mergeable group
/// remains.
async fn compact_next_group(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    options: CompactionOptions,
) -> Result<Option<CompactReport>> {
    let mut attempt = 1u32;
    loop {
        let table = catalog.load_table(ident).await?;
        let Some(group) = plan_one_group(&table, options).await? else {
            return Ok(None);
        };
        match rewrite_and_commit_group(catalog, &table, group, options).await {
            Ok(report) => return Ok(Some(report)),
            Err(AgentError::Iceberg(e))
                if e.kind() == iceberg::ErrorKind::CatalogCommitConflicts
                    && attempt < COMMIT_ATTEMPTS =>
            {
                // A concurrent append moved `main`. Reload and re-plan the next
                // group against the new snapshot, then retry. The append is never
                // dropped: the reloaded table carries it, and the recomputed
                // REPLACE carries its files forward.
                tokio::time::sleep(Duration::from_millis(250 * u64::from(attempt))).await;
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}

/// One bin-pack group: the small files to merge into new output, and every other
/// live file to carry forward as `Existing` in the group's REPLACE.
struct Group {
    /// The small files this group rewrites (removed from the live set).
    to_merge: Vec<LiveFile>,
    /// Every other live file, carried forward untouched.
    kept: Vec<LiveFile>,
}

/// Plans the next bin-pack group from `table` as currently loaded, or `None` when
/// no partition holds a mergeable pair. Within each partition, best-fit
/// decreasing packs files below Iceberg's healthy-size floor into groups bounded
/// by Iceberg's 100 GiB default and `max_files_per_group`. The fullest mergeable
/// bin is rewritten first. The bounds keep each read and commit finite while
/// allowing one rewrite to produce several target-sized output files.
async fn plan_one_group(table: &Table, options: CompactionOptions) -> Result<Option<Group>> {
    if table.metadata().current_snapshot().is_none() {
        return Ok(None);
    }
    let by_partition = live_files_by_partition(table).await?;
    let target = target_file_bytes(table, options)?;
    let min_file_size = min_file_size_bytes(target);

    // Build bounded bins independently per partition. Best-fit decreasing is a
    // standard bin-packing heuristic: process larger candidates first and place
    // each in the fullest bin that still has byte and file-count capacity.
    let mut best: Option<(u64, Vec<LiveFile>)> = None;
    for files in by_partition.values() {
        let mut small: Vec<LiveFile> = files
            .iter()
            .filter(|f| f.data_file.file_size_in_bytes() < min_file_size)
            .cloned()
            .collect();
        if small.len() < 2 {
            continue;
        }
        small.sort_unstable_by(|left, right| {
            right
                .data_file
                .file_size_in_bytes()
                .cmp(&left.data_file.file_size_in_bytes())
                .then_with(|| left.data_file.file_path().cmp(right.data_file.file_path()))
        });
        let mut bins: Vec<(u64, Vec<LiveFile>)> = Vec::new();
        for file in small {
            let size = file.data_file.file_size_in_bytes();
            let destination = bins
                .iter()
                .enumerate()
                .filter(|(_, (bytes, members))| {
                    members.len() < options.max_files_per_group
                        && bytes.saturating_add(size) <= MAX_FILE_GROUP_SIZE_BYTES
                })
                .max_by_key(|(_, (bytes, _))| *bytes)
                .map(|(index, _)| index);
            if let Some(index) = destination {
                bins[index].0 += size;
                bins[index].1.push(file);
            } else if options.max_files_per_group > 0 && size <= MAX_FILE_GROUP_SIZE_BYTES {
                bins.push((size, vec![file]));
            }
        }
        for bin in bins.into_iter().filter(|(_, members)| members.len() >= 2) {
            let replace = best
                .as_ref()
                .is_none_or(|current| (bin.1.len(), bin.0) > (current.1.len(), current.0));
            if replace {
                best = Some(bin);
            }
        }
    }
    let Some((_, to_merge)) = best else {
        return Ok(None);
    };

    // Everything not in this group is carried forward.
    let group_paths: HashSet<String> = to_merge
        .iter()
        .map(|f| f.data_file.file_path().to_owned())
        .collect();
    let kept: Vec<LiveFile> = by_partition
        .into_values()
        .flatten()
        .filter(|f| !group_paths.contains(f.data_file.file_path()))
        .collect();

    Ok(Some(Group { to_merge, kept }))
}

/// Rewrites one group's small files and commits the REPLACE against `table`.
/// Reads the group's rows through the engine, sorts them when the table defines a
/// sort order, writes them back as target-sized files, verifies the row count is
/// preserved, and commits a REPLACE that removes the group's files and keeps the
/// rest. Returns the single group's report (`groups_committed` = 1).
///
/// A REPLACE built against a stale `table` (one whose `main` a concurrent append
/// moved) surfaces the conflict as an `Iceberg` error of kind
/// `CatalogCommitConflicts`; the append is never overwritten.
async fn rewrite_and_commit_group(
    catalog: &dyn Catalog,
    table: &Table,
    group: Group,
    options: CompactionOptions,
) -> Result<CompactReport> {
    let ident = table.identifier();
    let metadata = table.metadata();
    let schema = metadata.current_schema().clone();
    let sort_order = metadata.default_sort_order().as_ref().clone();
    let sorted = !sort_order.is_unsorted();

    let batches = read_data_files(table.file_io(), &group.to_merge).await?;
    // Planned scan tasks apply position/equality deletes before rows reach the
    // writer. The logical input count is therefore the rows actually read, not
    // the raw record counts stored on the data files.
    let input_records: u64 = batches.iter().map(|batch| batch.num_rows() as u64).sum();
    let batches = if sorted {
        sort_batches(batches, &schema, &sort_order)?
    } else {
        batches
    };
    let target = target_file_bytes(table, options)?;
    let new_files = write_compacted(table, &schema, batches, target).await?;

    let output_records: u64 = new_files.iter().map(|f| f.record_count()).sum();
    if output_records != input_records {
        // Belt-and-suspenders: a rewrite that lost or duplicated a row is a hard
        // failure, never a silent commit. The REPLACE below is only reached when
        // the row count is preserved.
        return Err(AgentError::Compaction(format!(
            "row count changed during compaction of `{}`: read {input_records}, wrote {output_records}",
            ident_to_dotted(ident)
        )));
    }

    let removed_files: Vec<String> = group
        .to_merge
        .iter()
        .map(|f| f.data_file.file_path().to_owned())
        .collect();
    let added_files: Vec<String> = new_files.iter().map(|f| f.file_path().to_owned()).collect();

    let snapshot_id =
        commit_replace(catalog, table, &group.to_merge, &group.kept, new_files).await?;

    Ok(CompactReport {
        table: ident_to_dotted(ident),
        groups_committed: 1,
        input_data_files: group.to_merge.len() as u64,
        output_data_files: added_files.len() as u64,
        input_records,
        output_records,
        removed_files,
        added_files,
        sorted,
        undersized_remaining: 0,
        budget_bounded: false,
        snapshot_id: Some(snapshot_id),
        snapshots_expired: 0,
        orphan_files_deleted: 0,
    })
}

/// One compaction attempt against `table` as currently loaded: plan a single
/// bin-pack group and commit it, without retry. Exposed so a caller (a test, or
/// the retry loop's inner step) can drive one attempt against a specific —
/// possibly stale — loaded table and observe a conflict directly. A table with
/// nothing mergeable returns an empty report.
///
/// A REPLACE built against a stale `table` (one whose `main` a concurrent append
/// moved) surfaces the conflict as an `Iceberg` error of kind
/// `CatalogCommitConflicts`; the append is never overwritten.
pub async fn compact_table_once(
    catalog: &dyn Catalog,
    table: &Table,
    options: CompactionOptions,
) -> Result<CompactReport> {
    match plan_one_group(table, options).await? {
        Some(group) => rewrite_and_commit_group(catalog, table, group, options).await,
        None => Ok(empty_compact_report(&table.identifier().clone())),
    }
}

/// A live data file plus the manifest-entry metadata an `Existing` entry must
/// carry forward if the file is kept.
#[derive(Clone)]
struct LiveFile {
    /// The data file itself.
    data_file: DataFile,
    /// The snapshot that originally added the file.
    snapshot_id: i64,
    /// The file's data sequence number.
    sequence_number: i64,
    /// The file's file sequence number, when the source entry recorded one.
    file_sequence_number: Option<i64>,
    /// The manifest containing this live entry in the planned snapshot.
    manifest_path: String,
    /// The Iceberg-planned scan task for this file, including every applicable
    /// position/equality delete file.
    scan_task: FileScanTask,
}

/// A partition is identified by both spec id and values. Equal-looking structs
/// from different evolved specs must never be grouped together.
#[derive(Clone, PartialEq, Eq, Hash)]
struct PartitionKey {
    /// The partition spec that produced `partition`.
    spec_id: i32,
    /// Values encoded under that spec.
    partition: Struct,
}

/// Walks the current snapshot's manifests and returns the live data files grouped
/// by partition value. Files can only be merged within one partition, so both the
/// planner and the candidate count work from this grouping. A table with no
/// current snapshot has no live files.
async fn live_files_by_partition(table: &Table) -> Result<HashMap<PartitionKey, Vec<LiveFile>>> {
    let metadata = table.metadata();
    let Some(current) = metadata.current_snapshot() else {
        return Ok(HashMap::new());
    };
    // Use Iceberg's planner rather than manufacturing FileScanTasks. The planner
    // attaches the position/equality deletes applicable to each data file.
    let scan = table.scan().build()?;
    let planned: Vec<FileScanTask> = scan.plan_files().await?.try_collect().await?;
    let mut tasks_by_path: HashMap<String, FileScanTask> = planned
        .into_iter()
        .map(|task| (task.data_file_path.clone(), task))
        .collect();
    let manifest_list = current
        .load_manifest_list(table.file_io(), &table.metadata_ref())
        .await?;
    let mut by_partition: HashMap<PartitionKey, Vec<LiveFile>> = HashMap::new();
    for manifest_file in manifest_list.entries() {
        if manifest_file.content == ManifestContentType::Deletes {
            continue;
        }
        let manifest = manifest_file.load_manifest(table.file_io()).await?;
        for entry in manifest.entries() {
            if !entry.is_alive() || entry.content_type() != DataContentType::Data {
                continue;
            }
            let data_file = entry.data_file();
            if data_file.file_format() != DataFileFormat::Parquet {
                return Err(AgentError::Compaction(format!(
                    "compaction currently supports Parquet data files; `{}` is {:?}",
                    data_file.file_path(),
                    data_file.file_format()
                )));
            }
            let scan_task = tasks_by_path.remove(data_file.file_path()).ok_or_else(|| {
                AgentError::Compaction(format!(
                    "Iceberg scan planning omitted live data file `{}` from manifest `{}`",
                    data_file.file_path(),
                    manifest_file.manifest_path
                ))
            })?;
            let live = LiveFile {
                data_file: data_file.clone(),
                snapshot_id: entry.snapshot_id().unwrap_or(current.snapshot_id()),
                sequence_number: entry.sequence_number().unwrap_or(0),
                file_sequence_number: entry.file_sequence_number,
                manifest_path: manifest_file.manifest_path.clone(),
                scan_task,
            };
            by_partition
                .entry(PartitionKey {
                    spec_id: manifest_file.partition_spec_id,
                    partition: data_file.partition().clone(),
                })
                .or_default()
                .push(live);
        }
    }
    Ok(by_partition)
}

/// Reads Iceberg-planned tasks into Arrow batches through the table's FileIO.
/// Those tasks carry applicable row deletes and schema projection; a server in
/// the path still serves the underlying blocks.
async fn read_data_files(file_io: &FileIO, files: &[LiveFile]) -> Result<Vec<RecordBatch>> {
    let tasks: Vec<std::result::Result<FileScanTask, iceberg::Error>> = files
        .iter()
        .map(|file| Ok(file.scan_task.clone()))
        .collect();
    let task_stream = Box::pin(futures::stream::iter(tasks));
    let reader = ArrowReaderBuilder::new(file_io.clone()).build();
    let batch_stream = reader.read(task_stream)?;
    let batches: Vec<RecordBatch> = batch_stream
        .try_collect()
        .await
        .map_err(AgentError::Iceberg)?;
    Ok(batches)
}

/// Sorts the merged batches by the table's sort order (sort-compaction). The
/// batches are concatenated, sorted by the sort fields in order (respecting each
/// field's direction and null order), and returned as a single sorted batch.
fn sort_batches(
    batches: Vec<RecordBatch>,
    schema: &Arc<IcebergSchema>,
    sort_order: &SortOrder,
) -> Result<Vec<RecordBatch>> {
    let arrow_schema: ArrowSchemaRef = Arc::new(schema_to_arrow_schema(schema)?);
    let combined = concat_batches(&arrow_schema, &batches)
        .map_err(|e| AgentError::Compaction(format!("concat batches for sort: {e}")))?;
    if combined.num_rows() == 0 {
        return Ok(Vec::new());
    }

    let mut sort_columns = Vec::with_capacity(sort_order.fields.len());
    for field in &sort_order.fields {
        if field.transform != Transform::Identity {
            return Err(AgentError::Compaction(format!(
                "sort compaction currently supports only identity sort transforms; source id {} uses {:?}",
                field.source_id, field.transform
            )));
        }
        let name = schema.name_by_field_id(field.source_id).ok_or_else(|| {
            AgentError::Compaction(format!(
                "sort field source id {} is not in the table schema",
                field.source_id
            ))
        })?;
        let index = combined.schema().index_of(name).map_err(|e| {
            AgentError::Compaction(format!("sort column `{name}` not found in batch: {e}"))
        })?;
        let descending = matches!(field.direction, iceberg::spec::SortDirection::Descending);
        let nulls_first = matches!(field.null_order, iceberg::spec::NullOrder::First);
        sort_columns.push(SortColumn {
            values: combined.column(index).clone(),
            options: Some(SortOptions {
                descending,
                nulls_first,
            }),
        });
    }
    let indices = lexsort_to_indices(&sort_columns, None)
        .map_err(|e| AgentError::Compaction(format!("sort indices: {e}")))?;
    let sorted = take_record_batch(&combined, &indices)
        .map_err(|e| AgentError::Compaction(format!("take sorted rows: {e}")))?;
    Ok(vec![sorted])
}

/// Writes `batches` back as new Parquet data files that roll at `target_file_bytes`,
/// through the table's FileIO. An unpartitioned table takes the plain data-file
/// writer; a partitioned one fans out by partition value so every rewritten file
/// lands in the right partition — the same write path a normal append uses.
async fn write_compacted(
    table: &Table,
    schema: &Arc<IcebergSchema>,
    batches: Vec<RecordBatch>,
    target_file_bytes: u64,
) -> Result<Vec<DataFile>> {
    let location_gen = DefaultLocationGenerator::new(table.metadata().clone())?;
    let file_name_gen = DefaultFileNameGenerator::new(
        format!("verglas-compact-{}", uuid::Uuid::new_v4()),
        None,
        DataFileFormat::Parquet,
    );
    let parquet_writer =
        ParquetWriterBuilder::new(WriterProperties::builder().build(), schema.clone());
    let rolling = RollingFileWriterBuilder::new(
        parquet_writer,
        target_file_bytes as usize,
        table.file_io().clone(),
        location_gen,
        file_name_gen,
    );
    let data_file_builder = DataFileWriterBuilder::new(rolling);

    let partition_spec = table.metadata().default_partition_spec();
    if partition_spec.fields().is_empty() {
        let mut writer = data_file_builder.build(None).await?;
        for batch in batches {
            writer.write(batch).await?;
        }
        return Ok(writer.close().await?);
    }

    let splitter = iceberg::arrow::RecordBatchPartitionSplitter::try_new_with_computed_values(
        schema.clone(),
        partition_spec.clone(),
    )?;
    let mut writer = FanoutWriter::new(data_file_builder);
    for batch in batches {
        for (partition_key, partition_batch) in splitter.split(&batch)? {
            writer.write(partition_key, partition_batch).await?;
        }
    }
    Ok(writer.close().await?)
}

/// Builds the REPLACE snapshot by reusing unaffected manifests, rewriting only
/// manifests that contain removed files, and adding one manifest for replacement
/// files. Commits through `Catalog::update_table` with a UUID match and main-ref
/// CAS, so a concurrent append forces a conflict instead of a clobber. Returns
/// the new snapshot id.
async fn commit_replace(
    catalog: &dyn Catalog,
    table: &Table,
    removed: &[LiveFile],
    kept: &[LiveFile],
    added: Vec<DataFile>,
) -> Result<i64> {
    let metadata = table.metadata();
    let file_io = table.file_io();
    let location = metadata.location();
    let format_version = metadata.format_version();
    let commit_uuid = uuid::Uuid::new_v4();
    let snapshot_id = unique_snapshot_id(table);
    let next_seq = metadata.next_sequence_number();
    let parent = metadata.current_snapshot_id();
    let current_manifests = metadata
        .current_snapshot()
        .ok_or_else(|| AgentError::Compaction("table has no current snapshot".to_owned()))?
        .load_manifest_list(file_io, &table.metadata_ref())
        .await?;
    let affected_manifests: HashSet<&str> = removed
        .iter()
        .map(|file| file.manifest_path.as_str())
        .collect();
    let mut manifests = Vec::new();
    let mut created_manifest_paths = Vec::new();

    // Carry delete manifests and unaffected data manifests forward byte-for-byte.
    // For an affected data manifest, write only its still-live entries. This is
    // the same manifest-level replacement shape as Iceberg RewriteFiles: metadata
    // work scales with touched manifests rather than the full table.
    for manifest_file in current_manifests.entries() {
        if manifest_file.content == ManifestContentType::Deletes
            || !affected_manifests.contains(manifest_file.manifest_path.as_str())
        {
            manifests.push(manifest_file.clone());
            continue;
        }
        let manifest_kept: Vec<&LiveFile> = kept
            .iter()
            .filter(|file| file.manifest_path == manifest_file.manifest_path)
            .collect();
        if manifest_kept.is_empty() {
            continue;
        }
        let spec = metadata
            .partition_spec_by_id(manifest_file.partition_spec_id)
            .ok_or_else(|| {
                AgentError::Compaction(format!(
                    "partition spec {} for manifest `{}` is absent from table metadata",
                    manifest_file.partition_spec_id, manifest_file.manifest_path
                ))
            })?;
        let path = format!(
            "{location}/{METADATA_DIR}/{commit_uuid}-existing-{}.avro",
            manifests.len()
        );
        let output = file_io
            .new_output(&path)
            .map_err(|e| AgentError::Compaction(format!("open manifest output: {e}")))?;
        let builder = ManifestWriterBuilder::new(
            output,
            Some(snapshot_id),
            None,
            metadata.current_schema().clone(),
            spec.as_ref().clone(),
        );
        let mut writer = match format_version {
            FormatVersion::V1 => builder.build_v1(),
            FormatVersion::V2 => builder.build_v2_data(),
            FormatVersion::V3 => builder.build_v3_data(),
        };
        for file in manifest_kept {
            writer
                .add_existing_file(
                    file.data_file.clone(),
                    file.snapshot_id,
                    file.sequence_number,
                    file.file_sequence_number,
                )
                .map_err(|e| AgentError::Compaction(format!("add kept file to manifest: {e}")))?;
        }
        manifests.push(
            writer
                .write_manifest_file()
                .await
                .map_err(|e| AgentError::Compaction(format!("write manifest: {e}")))?,
        );
        created_manifest_paths.push(path);
    }

    // New output uses the table's current partition spec. Old-spec files that
    // were not selected remain in their original or spec-matched manifest.
    let added_manifest_path = format!("{location}/{METADATA_DIR}/{commit_uuid}-added.avro");
    let output = file_io
        .new_output(&added_manifest_path)
        .map_err(|e| AgentError::Compaction(format!("open manifest output: {e}")))?;
    let builder = ManifestWriterBuilder::new(
        output,
        Some(snapshot_id),
        None,
        metadata.current_schema().clone(),
        metadata.default_partition_spec().as_ref().clone(),
    );
    let mut writer = match format_version {
        FormatVersion::V1 => builder.build_v1(),
        FormatVersion::V2 => builder.build_v2_data(),
        FormatVersion::V3 => builder.build_v3_data(),
    };
    let added_count = added.len() as u64;
    let added_paths: Vec<String> = added
        .iter()
        .map(|file| file.file_path().to_owned())
        .collect();
    let added_records: u64 = added.iter().map(|f| f.record_count()).sum();
    let added_bytes: u64 = added.iter().map(|f| f.file_size_in_bytes()).sum();
    for file in added {
        writer
            .add_file(file, next_seq)
            .map_err(|e| AgentError::Compaction(format!("add compacted file to manifest: {e}")))?;
    }
    let kept_records: u64 = kept.iter().map(|f| f.data_file.record_count()).sum();
    let kept_bytes: u64 = kept.iter().map(|f| f.data_file.file_size_in_bytes()).sum();
    manifests.push(
        writer
            .write_manifest_file()
            .await
            .map_err(|e| AgentError::Compaction(format!("write manifest: {e}")))?,
    );
    created_manifest_paths.push(added_manifest_path);

    // The new manifest list combines reused, affected-rewritten, replacement,
    // and delete manifests.
    let manifest_list_path =
        format!("{location}/{METADATA_DIR}/snap-{snapshot_id}-0-{commit_uuid}.avro");
    let list_output = file_io
        .new_output(&manifest_list_path)
        .map_err(|e| AgentError::Compaction(format!("open manifest list output: {e}")))?;
    let mut list_writer = match format_version {
        FormatVersion::V1 => ManifestListWriter::v1(list_output, snapshot_id, parent),
        FormatVersion::V2 => ManifestListWriter::v2(list_output, snapshot_id, parent, next_seq),
        FormatVersion::V3 => ManifestListWriter::v3(
            list_output,
            snapshot_id,
            parent,
            next_seq,
            Some(metadata.next_row_id()),
        ),
    };
    list_writer
        .add_manifests(manifests.into_iter())
        .map_err(|e| AgentError::Compaction(format!("add manifest to list: {e}")))?;
    let list_next_row_id = list_writer.next_row_id();
    list_writer
        .close()
        .await
        .map_err(|e| AgentError::Compaction(format!("close manifest list: {e}")))?;

    // The REPLACE snapshot: operation Replace, a truthful full-table summary (so
    // the catalog's post-commit signal sees the table as healthy), and the new
    // manifest list.
    let total_files = added_count + kept.len() as u64;
    let total_records = added_records + kept_records;
    let total_bytes = added_bytes + kept_bytes;
    let mut summary_props: HashMap<String, String> = HashMap::new();
    summary_props.insert("total-data-files".to_owned(), total_files.to_string());
    summary_props.insert("total-records".to_owned(), total_records.to_string());
    summary_props.insert("total-files-size".to_owned(), total_bytes.to_string());
    summary_props.insert("added-data-files".to_owned(), added_count.to_string());
    summary_props.insert(
        "verglas-compaction".to_owned(),
        "bin-pack-and-sort".to_owned(),
    );
    let summary = Summary {
        operation: Operation::Replace,
        additional_properties: summary_props,
    };

    let snapshot_builder = Snapshot::builder()
        .with_manifest_list(manifest_list_path.clone())
        .with_snapshot_id(snapshot_id)
        .with_parent_snapshot_id(parent)
        .with_sequence_number(next_seq)
        .with_summary(summary)
        .with_schema_id(metadata.current_schema_id())
        .with_timestamp_ms(Utc::now().timestamp_millis());
    let snapshot = match list_next_row_id {
        Some(next_row_id) => snapshot_builder
            .with_row_range(metadata.next_row_id(), next_row_id - metadata.next_row_id())
            .build(),
        None => snapshot_builder.build(),
    };

    let updates = vec![
        TableUpdate::AddSnapshot { snapshot },
        TableUpdate::SetSnapshotRef {
            ref_name: MAIN_BRANCH.to_owned(),
            reference: SnapshotReference::new(
                snapshot_id,
                SnapshotRetention::branch(None, None, None),
            ),
        },
    ];
    let requirements = vec![
        TableRequirement::UuidMatch {
            uuid: metadata.uuid(),
        },
        TableRequirement::RefSnapshotIdMatch {
            r#ref: MAIN_BRANCH.to_owned(),
            snapshot_id: parent,
        },
    ];
    let commit = TableCommit::from_parts(table.identifier().clone(), requirements, updates);
    if let Err(error) = catalog.update_table(commit).await {
        if error.kind() == iceberg::ErrorKind::CatalogCommitConflicts {
            // A failed CAS is known not to have committed. Remove every artifact
            // created for this attempt before the caller reloads and retries.
            let mut artifacts = added_paths;
            artifacts.extend(created_manifest_paths);
            artifacts.push(manifest_list_path);
            for path in artifacts {
                // Cleanup is best-effort: preserve the original CAS conflict so
                // the retry loop reloads immediately. A later orphan sweep can
                // reclaim an artifact whose delete failed transiently.
                let _ = file_io.delete(&path).await;
            }
        }
        return Err(error.into());
    }
    Ok(snapshot_id)
}

/// Expires old snapshot history for `ident` down to
/// [`retention::DEFAULT_KEEP_LAST_SNAPSHOTS`], then reclaims whichever of
/// `just_removed` (the small-file paths this pass's own REPLACE groups already
/// took out of the live set) expiry left unreachable. Returns
/// `(snapshots_expired, orphan_files_deleted)`.
///
/// Deliberately bounded, so this is safe to run at the tail of a wall-budgeted
/// pass: [`retention::expire_snapshots`] is a metadata-only commit with no
/// manifest I/O, and [`cleanup_orphans`]'s reachability check walks only the
/// snapshots still retained *after* expiry — capped at the keep-last floor, not
/// the table's total history. It never scans the full snapshot history for
/// orphans: a file that a much earlier pass's rewrite removed, whose owning
/// snapshot only now ages out of the keep-last window, is not swept here. That
/// is a full-storage GC sweep, not something safe to fold into a per-table
/// tail step.
async fn expire_and_cleanup(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    just_removed: &[String],
) -> Result<(u64, u64)> {
    let expired =
        retention::expire_snapshots(catalog, ident, retention::DEFAULT_KEEP_LAST_SNAPSHOTS).await?;
    if !expired.did_expire() || just_removed.is_empty() {
        return Ok((expired.removed_snapshots.len() as u64, 0));
    }
    let orphans = cleanup_orphans(catalog, ident, just_removed).await?;
    Ok((
        expired.removed_snapshots.len() as u64,
        orphans.deleted.len() as u64,
    ))
}

/// Deletes the `candidates` that no retained snapshot of the table references —
/// the conservative orphan-file rule. A candidate still reachable from any
/// retained snapshot (e.g. the pre-compaction snapshot that time travel targets)
/// is kept. Run this after a compaction, and again after snapshot expiry drops the
/// old snapshots — a second pass finds the now-unreferenced old files and removes
/// them.
///
/// Only files handed in as candidates are ever considered for deletion, so this
/// can never touch a file the executor did not itself unlink. Reachability is
/// computed over every snapshot the metadata retains, not just the current one.
pub async fn cleanup_orphans(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    candidates: &[String],
) -> Result<OrphanReport> {
    let table = catalog.load_table(ident).await?;
    let reachable = reachable_data_files(&table).await?;
    let file_io = table.file_io();

    let mut deleted = Vec::new();
    let mut retained = Vec::new();
    for path in candidates {
        if reachable.contains(path) {
            retained.push(path.clone());
            continue;
        }
        // Only remove what still exists; a candidate already gone is a no-op.
        if file_io
            .exists(path)
            .await
            .map_err(|e| AgentError::Compaction(format!("stat orphan `{path}`: {e}")))?
        {
            file_io
                .delete(path)
                .await
                .map_err(|e| AgentError::Compaction(format!("delete orphan `{path}`: {e}")))?;
        }
        deleted.push(path.clone());
    }

    Ok(OrphanReport {
        table: ident_to_dotted(ident),
        deleted,
        retained_for_time_travel: retained,
    })
}

/// The set of data-file paths reachable from *every* retained snapshot of the
/// table — the union of the alive data files of all snapshots, not just the
/// current one. A file in this set is still needed by some snapshot (time travel)
/// and must never be deleted.
async fn reachable_data_files(table: &Table) -> Result<HashSet<String>> {
    let metadata = table.metadata();
    let file_io = table.file_io();
    let mut reachable = HashSet::new();
    for snapshot in metadata.snapshots() {
        let manifest_list = snapshot
            .load_manifest_list(file_io, &table.metadata_ref())
            .await?;
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(file_io).await?;
            for entry in manifest.entries() {
                if entry.is_alive() {
                    reachable.insert(entry.data_file().file_path().to_owned());
                }
            }
        }
    }
    Ok(reachable)
}

/// A random i64 snapshot id that does not collide with any existing snapshot of
/// `table`, mirroring iceberg's own id generation.
fn unique_snapshot_id(table: &Table) -> i64 {
    let metadata = table.metadata();
    loop {
        let (lhs, rhs) = uuid::Uuid::new_v4().as_u64_pair();
        let candidate = (lhs ^ rhs) as i64;
        let candidate = if candidate < 0 {
            candidate.saturating_neg()
        } else {
            candidate
        };
        if !metadata.snapshots().any(|s| s.snapshot_id() == candidate) {
            return candidate;
        }
    }
}
