//! Hermetic integration tests for small-file compaction, derived from the
//! acceptance criteria: many small files are rewritten into fewer target-sized
//! files; `count(*)` and full row content are byte-for-byte identical before and
//! after (compaction only reorganizes storage); the new snapshot is a REPLACE;
//! time travel to the pre-compaction snapshot still works; a REPLACE that
//! conflicts with an interleaved append is rejected and retried, never silently
//! overwriting the append; orphan cleanup deletes only files no retained
//! snapshot references; and a compaction pass expires old snapshot history so
//! `metadata.json` stops growing without bound, while a snapshot young enough to
//! survive the keep-last window (and the data files it alone still needs) is
//! never touched.
//!
//! These run fully in-process against an Iceberg `MemoryCatalog` over a temp
//! warehouse — no server, no network. Compaction commits through
//! `Catalog::update_table` with a `RefSnapshotIdMatch` requirement, the same
//! trait method + CAS the REST catalog serves, so the server-backed path
//! exercises the same code.

use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use futures::TryStreamExt;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{
    ManifestContentType, NullOrder, SortDirection, SortField, SortOrder, Transform,
};
use iceberg::{Catalog, CatalogBuilder, TableCommit, TableRequirement, TableUpdate};
use verglas_iceberg::compaction::{
    CompactionOptions, DEFAULT_MIN_SMALL_FILES, DEFAULT_TARGET_FILE_BYTES, cleanup_orphans,
    compact_table, compact_table_once, run_compaction, undersized_file_count,
};
use verglas_iceberg::{
    DEFAULT_KEEP_LAST_SNAPSHOTS, TimeTravel, inspect, parse_table_ident, query, write,
};

/// Builds an in-process memory catalog over a fresh temp warehouse path.
async fn memory_catalog() -> Arc<dyn Catalog> {
    let warehouse = tempfile::tempdir().expect("warehouse tempdir");
    let catalog = MemoryCatalogBuilder::default()
        .load(
            "memory",
            HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.to_string(),
                warehouse.path().to_str().expect("utf8 path").to_string(),
            )]),
        )
        .await
        .expect("memory catalog");
    std::mem::forget(warehouse);
    Arc::new(catalog)
}

/// Writes `contents` to a temp file, returning its path and leaking the tempdir
/// (the test process is short-lived).
fn source_file(name: &str, contents: &str) -> PathBuf {
    let dir = tempfile::tempdir().expect("source tempdir");
    let path = dir.path().join(name);
    let mut file = std::fs::File::create(&path).expect("create source");
    file.write_all(contents.as_bytes()).expect("write source");
    file.flush().expect("flush source");
    std::mem::forget(dir);
    path
}

/// count(*) over a table, as an i64, via the embedded query engine.
async fn count(catalog: Arc<dyn Catalog>, dotted: &str) -> i64 {
    let sql = format!("SELECT count(*) AS n FROM {dotted}");
    let report = query::query(catalog, &sql, None)
        .await
        .expect("count query");
    report.rows[0]["n"].as_i64().expect("count is an integer")
}

/// Every row of a table as an ordered JSON string, for whole-content comparison.
/// Sorted by all columns so the comparison is independent of file/scan order.
async fn all_rows(
    catalog: Arc<dyn Catalog>,
    dotted: &str,
    order_by: &str,
) -> Vec<serde_json::Value> {
    let sql = format!("SELECT * FROM {dotted} ORDER BY {order_by}");
    let report = query::query(catalog, &sql, None).await.expect("rows query");
    report
        .rows
        .into_iter()
        .map(serde_json::Value::Object)
        .collect()
}

/// The live data-file count of a table's current snapshot, via `show`.
async fn file_count(catalog: &dyn Catalog, dotted: &str) -> u64 {
    let ident = parse_table_ident(dotted).expect("ident");
    inspect::show(catalog, &ident)
        .await
        .expect("show")
        .file_count
        .expect("a file count")
}

/// Creates an unpartitioned `k,v` table and appends `count` further single-row
/// files, so the table ends with `count + 1` small data files.
async fn table_with_many_small_files(catalog: &dyn Catalog, dotted: &str, count: usize) {
    let ident = parse_table_ident(dotted).expect("ident");
    write::create_table(
        catalog,
        &ident,
        &source_file("seed.csv", "k,v\n0,zero\n"),
        None,
    )
    .await
    .expect("create");
    for i in 1..=count {
        write::append(
            catalog,
            &ident,
            &source_file("more.csv", &format!("k,v\n{i},row{i}\n")),
        )
        .await
        .expect("append");
    }
}

/// Apache Iceberg and S3 Tables use a 512 MiB default target file size.
#[test]
fn default_target_matches_iceberg() {
    assert_eq!(DEFAULT_TARGET_FILE_BYTES, 512 * 1024 * 1024);
}

/// Files in Iceberg's healthy band (at least 75% of target) are not rewrite
/// candidates. Rewriting every file merely because it is one byte below target
/// causes churn and can repeatedly replace two healthy files with two healthy
/// files without reducing the file count.
#[tokio::test]
async fn files_in_the_healthy_size_band_are_not_rewritten() {
    let catalog = memory_catalog().await;
    let dotted = "rlean.healthy_sizes";
    let ident = parse_table_ident(dotted).expect("ident");
    table_with_many_small_files(catalog.as_ref(), dotted, 5).await;
    let table = catalog.load_table(&ident).await.expect("load");
    let tasks: Vec<_> = table
        .scan()
        .build()
        .expect("scan")
        .plan_files()
        .await
        .expect("plan")
        .try_collect()
        .await
        .expect("tasks");
    let smallest = tasks
        .iter()
        .map(|task| task.file_size_in_bytes)
        .min()
        .expect("files");
    let target = smallest.saturating_mul(4) / 3;
    assert!(
        target > smallest,
        "test target must exceed the smallest file"
    );
    let before = file_count(catalog.as_ref(), dotted).await;

    let report = run_compaction(
        catalog.as_ref(),
        CompactionOptions {
            target_file_bytes: Some(target),
            min_small_files: 0,
            ..CompactionOptions::default()
        },
    )
    .await
    .expect("compaction scan");

    assert!(
        report.compacted.is_empty(),
        "files at or above 75% of target are already well sized"
    );
    assert_eq!(file_count(catalog.as_ref(), dotted).await, before);
}

/// With no explicit override, compaction follows the table's native
/// `write.target-file-size-bytes` property, as Apache Iceberg actions do.
#[tokio::test]
async fn default_options_use_the_table_target_file_size() {
    let catalog = memory_catalog().await;
    let dotted = "rlean.table_target";
    let ident = parse_table_ident(dotted).expect("ident");
    table_with_many_small_files(catalog.as_ref(), dotted, 5).await;
    let table = catalog.load_table(&ident).await.expect("load");
    let tasks: Vec<_> = table
        .scan()
        .build()
        .expect("scan")
        .plan_files()
        .await
        .expect("plan")
        .try_collect()
        .await
        .expect("tasks");
    let smallest = tasks
        .iter()
        .map(|task| task.file_size_in_bytes)
        .min()
        .expect("files");
    let target = smallest.saturating_mul(4) / 3;
    catalog
        .update_table(TableCommit::from_parts(
            ident.clone(),
            vec![TableRequirement::UuidMatch {
                uuid: table.metadata().uuid(),
            }],
            vec![TableUpdate::SetProperties {
                updates: HashMap::from([(
                    "write.target-file-size-bytes".to_owned(),
                    target.to_string(),
                )]),
            }],
        ))
        .await
        .expect("set target property");
    let before = file_count(catalog.as_ref(), dotted).await;

    let report = run_compaction(
        catalog.as_ref(),
        CompactionOptions {
            min_small_files: 0,
            ..CompactionOptions::default()
        },
    )
    .await
    .expect("compaction scan");

    assert!(
        report.compacted.is_empty(),
        "the table property places these files in the healthy size band"
    );
    assert_eq!(file_count(catalog.as_ref(), dotted).await, before);
}

/// A data-file rewrite should preserve manifests that do not contain any input
/// file in the rewrite group. Apache Iceberg's rewrite action replaces only the
/// selected files; rebuilding the table's complete manifest set for every group
/// makes compaction metadata work grow with the whole table.
#[tokio::test]
async fn compaction_preserves_unaffected_data_manifests() {
    let catalog = memory_catalog().await;
    let dotted = "rlean.manifest_reuse";
    let ident = parse_table_ident(dotted).expect("ident");
    table_with_many_small_files(catalog.as_ref(), dotted, 5).await;
    let large_value = "x".repeat(256 * 1024);
    write::append(
        catalog.as_ref(),
        &ident,
        &source_file("large.csv", &format!("k,v\n99,{large_value}\n")),
    )
    .await
    .expect("large append");

    let table = catalog.load_table(&ident).await.expect("load");
    let current = table.metadata().current_snapshot().expect("snapshot");
    let manifest_list = table
        .manifest_list_reader(current)
        .load()
        .await
        .expect("manifest list");
    let mut largest = None;
    for manifest_file in manifest_list
        .entries()
        .iter()
        .filter(|manifest| manifest.content == ManifestContentType::Data)
    {
        let manifest = manifest_file
            .load_manifest(table.file_io())
            .await
            .expect("manifest");
        for entry in manifest.entries().iter().filter(|entry| entry.is_alive()) {
            let size = entry.data_file().file_size_in_bytes();
            if largest
                .as_ref()
                .is_none_or(|(largest_size, _)| size > *largest_size)
            {
                largest = Some((size, manifest_file.manifest_path.clone()));
            }
        }
    }
    let (large_size, unaffected_manifest) = largest.expect("largest data file");

    compact_table_once(
        catalog.as_ref(),
        &table,
        CompactionOptions {
            target_file_bytes: Some(large_size),
            max_files_per_group: 512,
            ..CompactionOptions::default()
        },
    )
    .await
    .expect("compact");

    let table = catalog.load_table(&ident).await.expect("reload");
    let current = table.metadata().current_snapshot().expect("snapshot");
    let manifest_list = table
        .manifest_list_reader(current)
        .load()
        .await
        .expect("manifest list");
    assert!(
        manifest_list
            .entries()
            .iter()
            .any(|manifest| manifest.manifest_path == unaffected_manifest),
        "the manifest containing only the untouched large file must be reused"
    );
}

/// Acceptance: many small files compact into fewer files; `count(*)` and full row
/// content are identical; the new snapshot is a REPLACE; time travel to the
/// pre-compaction snapshot still returns the original rows.
#[tokio::test]
async fn compaction_reduces_files_and_preserves_content() {
    let catalog = memory_catalog().await;
    let dotted = "rlean.events";
    let ident = parse_table_ident(dotted).expect("ident");
    // 12 small files total (seed + 11 appends).
    table_with_many_small_files(catalog.as_ref(), dotted, 11).await;

    let files_before = file_count(catalog.as_ref(), dotted).await;
    assert_eq!(files_before, 12, "12 small files before compaction");
    let count_before = count(catalog.clone(), dotted).await;
    let rows_before = all_rows(catalog.clone(), dotted, "k").await;
    let pre_snapshot = inspect::show(catalog.as_ref(), &ident)
        .await
        .expect("show")
        .current_snapshot_id
        .expect("a current snapshot");

    let report = compact_table(catalog.as_ref(), &ident, CompactionOptions::default())
        .await
        .expect("compact");

    // Files were rewritten: 12 in, fewer out, none kept (all were small).
    assert_eq!(report.input_data_files, 12);
    assert!(
        report.output_data_files < report.input_data_files,
        "compaction reduced the file count: {} -> {}",
        report.input_data_files,
        report.output_data_files
    );
    assert!(
        !report.sorted,
        "no sort order => bin-pack, not sort-compact"
    );
    assert_eq!(
        report.input_records, report.output_records,
        "compaction preserves the row count"
    );
    assert!(report.snapshot_id.is_some(), "a REPLACE was committed");

    // The live file count dropped.
    let files_after = file_count(catalog.as_ref(), dotted).await;
    assert!(files_after < files_before, "{files_after} < {files_before}");

    // count(*) and full row content are identical.
    assert_eq!(count(catalog.clone(), dotted).await, count_before);
    let rows_after = all_rows(catalog.clone(), dotted, "k").await;
    assert_eq!(rows_after, rows_before, "row content is byte-identical");

    // The new snapshot is a REPLACE.
    let history = inspect::history(catalog.as_ref(), &ident)
        .await
        .expect("history");
    assert_eq!(
        history.snapshots.last().expect("a snapshot").operation,
        "replace"
    );

    // Time travel to the pre-compaction snapshot still returns the original rows.
    let travelled = query::query(
        catalog.clone(),
        "SELECT * FROM events ORDER BY k",
        Some(TimeTravel {
            reference: pre_snapshot.to_string(),
            table: dotted.to_owned(),
        }),
    )
    .await
    .expect("time-travel query");
    let travelled_rows: Vec<serde_json::Value> = travelled
        .rows
        .into_iter()
        .map(serde_json::Value::Object)
        .collect();
    assert_eq!(
        travelled_rows, rows_before,
        "time travel to the pre-compaction snapshot still works"
    );
}

/// Acceptance: a second compaction right after the first finds nothing mergeable
/// (one file now) and commits no snapshot — idempotent.
#[tokio::test]
async fn compaction_is_idempotent() {
    let catalog = memory_catalog().await;
    let dotted = "rlean.idem";
    let ident = parse_table_ident(dotted).expect("ident");
    table_with_many_small_files(catalog.as_ref(), dotted, 5).await;

    let first = compact_table(catalog.as_ref(), &ident, CompactionOptions::default())
        .await
        .expect("first compaction");
    assert!(first.snapshot_id.is_some());

    let second = compact_table(catalog.as_ref(), &ident, CompactionOptions::default())
        .await
        .expect("second compaction");
    assert!(
        second.snapshot_id.is_none(),
        "already-compacted table has nothing to merge"
    );
    assert_eq!(second.input_data_files, 0);
}

/// Acceptance: a table with a sort order (writeOrder) is sort-compacted — the
/// rewrite reports `sorted`, and row content is still fully preserved.
#[tokio::test]
async fn sort_order_triggers_sort_compaction() {
    let catalog = memory_catalog().await;
    let dotted = "rlean.sorted";
    let ident = parse_table_ident(dotted).expect("ident");

    // Create the table, then give it a sort order on `k` via a metadata commit.
    write::create_table(
        catalog.as_ref(),
        &ident,
        &source_file("seed.csv", "k,v\n5,five\n"),
        None,
    )
    .await
    .expect("create");
    let table = catalog.load_table(&ident).await.expect("load");
    let schema = table.metadata().current_schema();
    let k_id = schema.field_id_by_name("k").expect("k field id");
    let sort_order = SortOrder::builder()
        .with_sort_field(
            SortField::builder()
                .source_id(k_id)
                .transform(Transform::Identity)
                .direction(SortDirection::Ascending)
                .null_order(NullOrder::First)
                .build(),
        )
        .build(schema)
        .expect("sort order");
    let commit = TableCommit::from_parts(
        ident.clone(),
        vec![TableRequirement::UuidMatch {
            uuid: table.metadata().uuid(),
        }],
        vec![
            TableUpdate::AddSortOrder { sort_order },
            TableUpdate::SetDefaultSortOrder { sort_order_id: -1 },
        ],
    );
    catalog.update_table(commit).await.expect("set sort order");

    // Append several small files with out-of-order keys.
    for (k, v) in [(9, "nine"), (1, "one"), (7, "seven"), (3, "three")] {
        write::append(
            catalog.as_ref(),
            &ident,
            &source_file("more.csv", &format!("k,v\n{k},{v}\n")),
        )
        .await
        .expect("append");
    }

    let rows_before = all_rows(catalog.clone(), dotted, "k").await;
    let report = compact_table(catalog.as_ref(), &ident, CompactionOptions::default())
        .await
        .expect("compact");

    assert!(report.sorted, "a table with a sort order is sort-compacted");
    assert!(report.snapshot_id.is_some());
    assert_eq!(report.input_records, report.output_records);
    // Content is fully preserved (the sort only reorders storage).
    let rows_after = all_rows(catalog.clone(), dotted, "k").await;
    assert_eq!(rows_after, rows_before);
    assert_eq!(count(catalog.clone(), dotted).await, 5);
}

/// Sort compaction must not pretend that ordering by a transformed sort key is
/// equivalent to ordering by its source column. Until transform evaluation is
/// implemented, the executor fails before writing output.
#[tokio::test]
async fn transformed_sort_order_is_rejected_instead_of_mis_sorted() {
    let catalog = memory_catalog().await;
    let dotted = "rlean.transformed_sort";
    let ident = parse_table_ident(dotted).expect("ident");
    table_with_many_small_files(catalog.as_ref(), dotted, 4).await;
    let table = catalog.load_table(&ident).await.expect("load");
    let k_id = table
        .metadata()
        .current_schema()
        .field_id_by_name("k")
        .expect("k field id");
    let sort_order = SortOrder::builder()
        .with_sort_field(
            SortField::builder()
                .source_id(k_id)
                .transform(Transform::Bucket(16))
                .direction(SortDirection::Ascending)
                .null_order(NullOrder::First)
                .build(),
        )
        .build(table.metadata().current_schema())
        .expect("sort order");
    catalog
        .update_table(TableCommit::from_parts(
            ident.clone(),
            vec![TableRequirement::UuidMatch {
                uuid: table.metadata().uuid(),
            }],
            vec![
                TableUpdate::AddSortOrder { sort_order },
                TableUpdate::SetDefaultSortOrder { sort_order_id: -1 },
            ],
        ))
        .await
        .expect("set sort order");

    let error = compact_table(catalog.as_ref(), &ident, CompactionOptions::default())
        .await
        .expect_err("transformed order must not be silently mis-sorted");
    assert!(
        error.to_string().contains("identity sort transforms"),
        "unexpected error: {error}"
    );
}

/// Acceptance (must-pass): a REPLACE built against a stale snapshot — one whose
/// `main` a concurrent append has since advanced — is rejected with a commit
/// conflict, never silently overwriting the append. The retrying `compact_table`
/// then succeeds against the reloaded table and preserves the appended rows.
#[tokio::test]
async fn replace_conflicting_with_interleaved_append_is_rejected_then_retried() {
    let catalog = memory_catalog().await;
    let dotted = "rlean.contended";
    let ident = parse_table_ident(dotted).expect("ident");
    // 4 small files (seed + 3 appends), rows k = 0..=3.
    table_with_many_small_files(catalog.as_ref(), dotted, 3).await;

    // Snapshot a stale handle of the table as it is now.
    let stale = catalog.load_table(&ident).await.expect("load stale");

    // A concurrent append lands, advancing `main`.
    write::append(
        catalog.as_ref(),
        &ident,
        &source_file("interleaved.csv", "k,v\n99,interleaved\n"),
    )
    .await
    .expect("interleaved append");
    assert_eq!(count(catalog.clone(), dotted).await, 5);

    // A single REPLACE attempt against the STALE table must be rejected: its
    // `assert-ref-snapshot-id` no longer matches the advanced `main`.
    let err = compact_table_once(catalog.as_ref(), &stale, CompactionOptions::default())
        .await
        .expect_err("stale REPLACE must conflict");
    match err {
        verglas_iceberg::AgentError::Iceberg(e) => assert_eq!(
            e.kind(),
            iceberg::ErrorKind::CatalogCommitConflicts,
            "stale REPLACE conflicts, not some other failure"
        ),
        other => panic!("expected a commit conflict, got {other}"),
    }

    // The interleaved append survived — the failed REPLACE did not clobber it.
    assert_eq!(count(catalog.clone(), dotted).await, 5);

    // The retrying entry point reloads and succeeds, still holding all 5 rows.
    let report = compact_table(catalog.as_ref(), &ident, CompactionOptions::default())
        .await
        .expect("retried compaction");
    assert!(report.snapshot_id.is_some());
    assert_eq!(count(catalog.clone(), dotted).await, 5);
    // The rewrite reduced the file count.
    assert!(file_count(catalog.as_ref(), dotted).await < 5);
}

/// Acceptance: orphan cleanup is conservative — the small files a compaction just
/// removed are still referenced by the pre-compaction snapshot (time travel), so
/// they are retained; a genuinely unreferenced file is deleted.
#[tokio::test]
async fn orphan_cleanup_only_deletes_unreferenced_files() {
    let catalog = memory_catalog().await;
    let dotted = "rlean.orphans";
    let ident = parse_table_ident(dotted).expect("ident");
    table_with_many_small_files(catalog.as_ref(), dotted, 5).await;

    let report = compact_table(catalog.as_ref(), &ident, CompactionOptions::default())
        .await
        .expect("compact");
    assert!(!report.removed_files.is_empty());

    // The removed small files are still reachable from the pre-compaction
    // snapshot, so a cleanup pass keeps every one — time travel stays intact.
    let cleaned = cleanup_orphans(catalog.as_ref(), &ident, &report.removed_files)
        .await
        .expect("cleanup after compaction");
    assert!(
        cleaned.deleted.is_empty(),
        "no file a retained snapshot references is deleted"
    );
    assert_eq!(
        cleaned.retained_for_time_travel.len(),
        report.removed_files.len()
    );
    // Time travel to the pre-compaction files still resolves.
    assert_eq!(count(catalog.clone(), dotted).await, 6);

    // A file no snapshot references — write a stray object into the table dir —
    // is deleted.
    let table = catalog.load_table(&ident).await.expect("load");
    let stray = format!("{}/data/stray-orphan.parquet", table.metadata().location());
    table
        .file_io()
        .new_output(&stray)
        .expect("output")
        .write(bytes::Bytes::from_static(b"not referenced"))
        .await
        .expect("write stray");
    assert!(
        table.file_io().exists(&stray).await.expect("exists"),
        "stray file present before cleanup"
    );
    let cleaned = cleanup_orphans(catalog.as_ref(), &ident, std::slice::from_ref(&stray))
        .await
        .expect("cleanup stray");
    assert_eq!(cleaned.deleted, vec![stray.clone()]);
    assert!(
        !table.file_io().exists(&stray).await.expect("exists after"),
        "the unreferenced stray file was deleted"
    );
}

/// Acceptance: `undersized_file_count` reads the per-file sizes from the
/// manifests and counts the files below the target — the scan's candidate signal.
/// Every single-record file a table accumulated is undersized, so the count
/// equals the file count here.
#[tokio::test]
async fn undersized_file_count_counts_small_files() {
    let catalog = memory_catalog().await;
    let dotted = "rlean.small";
    let ident = parse_table_ident(dotted).expect("ident");
    // 8 single-record files (seed + 7 appends), all far below the 128 MiB target.
    table_with_many_small_files(catalog.as_ref(), dotted, 7).await;

    let table = catalog.load_table(&ident).await.expect("load");
    let count = undersized_file_count(&table, DEFAULT_TARGET_FILE_BYTES)
        .await
        .expect("count");
    assert_eq!(count, 8, "every tiny file is undersized");
}

/// Acceptance: the scan only spends a pass on a table whose undersized-file count
/// is over the threshold. A table with more than the floor of small files is
/// compacted; one at or below it is skipped, scanning every table and never
/// rewriting the below-threshold one.
#[tokio::test]
async fn run_compaction_selects_only_over_threshold_tables() {
    let catalog = memory_catalog().await;

    // Over threshold: floor + 5 single-record files.
    let over = DEFAULT_MIN_SMALL_FILES + 5;
    table_with_many_small_files(catalog.as_ref(), "rlean.busy", over - 1).await;
    // Under threshold: a handful of small files, far below the floor.
    let under_count = 6;
    table_with_many_small_files(catalog.as_ref(), "rlean.calm", under_count - 1).await;

    let report = run_compaction(catalog.as_ref(), CompactionOptions::default())
        .await
        .expect("run compaction");

    assert_eq!(report.tables_scanned, 2, "both tables were scanned");
    assert_eq!(
        report.compacted.len(),
        1,
        "only the over-threshold table was compacted"
    );
    assert_eq!(report.compacted[0].table, "rlean.busy");
    assert!(report.failures.is_empty());
    assert!(report.files_removed() as usize >= over);

    // The below-threshold table was left completely untouched — still every one
    // of its small files, no REPLACE.
    assert_eq!(
        file_count(catalog.as_ref(), "rlean.calm").await,
        under_count as u64,
        "a below-threshold table is not rewritten"
    );
}

/// Test-only options that drive the ratcheting deterministically: a tiny group
/// cap so a small table needs several groups, a low selection floor so a small
/// table is selected, and a zero pass budget so the pass commits exactly one group
/// per call and then stops — a faithful stand-in for a run killed after the first
/// group's commit (no wall-clock sleeps involved).
fn ratchet_options() -> CompactionOptions {
    CompactionOptions {
        min_small_files: 2,
        max_files_per_group: 4,
        pass_budget: Duration::ZERO,
        ..CompactionOptions::default()
    }
}

/// Acceptance (progress ratchets): a pass bounded by its wall budget commits its
/// first bin-pack group, then stops with an honest partial report — some groups
/// committed, fewer files, and a positive count of undersized files still
/// mergeable. This is the shape a run killed mid-pass leaves behind: everything
/// committed so far is durable.
#[tokio::test]
async fn a_budget_bounded_pass_commits_a_group_then_reports_remaining() {
    let catalog = memory_catalog().await;
    let dotted = "rlean.backlog";
    // 12 small files, more than one group at a cap of 4.
    table_with_many_small_files(catalog.as_ref(), dotted, 11).await;
    let count_before = count(catalog.clone(), dotted).await;
    let files_before = file_count(catalog.as_ref(), dotted).await;
    assert_eq!(files_before, 12);

    let report = run_compaction(catalog.as_ref(), ratchet_options())
        .await
        .expect("bounded pass");
    assert!(
        report.failures.is_empty(),
        "failures: {:?}",
        report.failures
    );

    // Exactly one group was committed, and the pass stopped on its budget with
    // work still to do.
    assert_eq!(report.groups_committed, 1, "one group committed this pass");
    assert!(report.budget_bounded, "the pass stopped on its budget");
    assert!(
        report.undersized_remaining > 0,
        "mergeable files still remain: {}",
        report.undersized_remaining
    );
    assert_eq!(report.compacted.len(), 1);
    let table_report = &report.compacted[0];
    assert_eq!(table_report.groups_committed, 1);
    assert!(table_report.budget_bounded);

    // Fewer files, but not yet fully compacted — and no rows lost.
    let files_after = file_count(catalog.as_ref(), dotted).await;
    assert!(
        files_after < files_before && files_after > 1,
        "partial progress: {files_before} -> {files_after}"
    );
    assert_eq!(count(catalog.clone(), dotted).await, count_before);
}

/// Acceptance (re-runnable and convergent): running budget-bounded passes back to
/// back — each committing one group, exactly as repeated runs after a kill would —
/// converges the backlog. File count falls monotonically, the undersized-remaining
/// count reaches zero, the final pass is no longer budget-bounded, and row content
/// is byte-identical throughout.
#[tokio::test]
async fn repeated_bounded_passes_converge_and_preserve_content() {
    let catalog = memory_catalog().await;
    let dotted = "rlean.converge";
    table_with_many_small_files(catalog.as_ref(), dotted, 11).await;
    let rows_before = all_rows(catalog.clone(), dotted, "k").await;
    let count_before = count(catalog.clone(), dotted).await;

    let mut prev_files = file_count(catalog.as_ref(), dotted).await;
    let mut passes = 0;
    let mut total_groups = 0u64;
    loop {
        let report = run_compaction(catalog.as_ref(), ratchet_options())
            .await
            .expect("bounded pass");
        total_groups += report.groups_committed;
        if report.groups_committed == 0 {
            // Nothing mergeable is left: the backlog is drained.
            assert!(!report.budget_bounded, "a no-op pass is not budget-bounded");
            assert_eq!(report.undersized_remaining, 0);
            break;
        }
        // Every productive pass strictly reduces the live file count.
        let now_files = file_count(catalog.as_ref(), dotted).await;
        assert!(
            now_files < prev_files,
            "each pass ratchets progress: {prev_files} -> {now_files}"
        );
        prev_files = now_files;
        // Content is preserved at every step.
        assert_eq!(count(catalog.clone(), dotted).await, count_before);
        passes += 1;
        assert!(passes < 20, "convergence must not loop forever");
    }

    assert!(total_groups >= 2, "convergence took several groups");
    // Fully compacted, content byte-identical to the start.
    assert!(file_count(catalog.as_ref(), dotted).await < 12);
    assert_eq!(all_rows(catalog.clone(), dotted, "k").await, rows_before);
    assert_eq!(count(catalog.clone(), dotted).await, count_before);
}

/// Acceptance (conflict retry per group): a single group's REPLACE built against a
/// stale snapshot conflicts and is rejected, never overwriting the interleaved
/// append; the retrying entry point then drains the table group by group, keeping
/// every row including the appended one.
#[tokio::test]
async fn per_group_commit_conflict_is_rejected_then_retried() {
    let catalog = memory_catalog().await;
    let dotted = "rlean.contended_groups";
    let ident = parse_table_ident(dotted).expect("ident");
    // 8 small files — more than one group at a cap of 4.
    table_with_many_small_files(catalog.as_ref(), dotted, 7).await;

    // A stale handle, then a concurrent append advancing `main`.
    let stale = catalog.load_table(&ident).await.expect("load stale");
    write::append(
        catalog.as_ref(),
        &ident,
        &source_file("interleaved.csv", "k,v\n99,interleaved\n"),
    )
    .await
    .expect("interleaved append");
    assert_eq!(count(catalog.clone(), dotted).await, 9);

    // A single group commit against the STALE table conflicts on the stale ref.
    let options = CompactionOptions {
        max_files_per_group: 4,
        ..CompactionOptions::default()
    };
    let err = compact_table_once(catalog.as_ref(), &stale, options)
        .await
        .expect_err("stale group REPLACE must conflict");
    match err {
        verglas_iceberg::AgentError::Iceberg(e) => assert_eq!(
            e.kind(),
            iceberg::ErrorKind::CatalogCommitConflicts,
            "stale group REPLACE conflicts, not some other failure"
        ),
        other => panic!("expected a commit conflict, got {other}"),
    }
    // The interleaved append survived the rejected commit.
    assert_eq!(count(catalog.clone(), dotted).await, 9);

    // The retrying entry point reloads and drains the table across several groups,
    // keeping all 9 rows.
    let report = compact_table(catalog.as_ref(), &ident, options)
        .await
        .expect("retried compaction");
    assert!(
        report.groups_committed >= 2,
        "drained across several groups"
    );
    assert!(report.snapshot_id.is_some());
    assert_eq!(count(catalog.clone(), dotted).await, 9);
    assert!(file_count(catalog.as_ref(), dotted).await < 9);
}

/// Acceptance: the threshold is a strict floor. A table with exactly the floor
/// count of small files is not selected; one file more is.
#[tokio::test]
async fn threshold_is_a_strict_floor() {
    let catalog = memory_catalog().await;

    // Exactly the floor: not selected.
    table_with_many_small_files(
        catalog.as_ref(),
        "rlean.at_floor",
        DEFAULT_MIN_SMALL_FILES - 1,
    )
    .await;
    let report = run_compaction(catalog.as_ref(), CompactionOptions::default())
        .await
        .expect("run compaction");
    assert!(
        report.compacted.is_empty(),
        "a table at exactly the floor is not compacted"
    );
    assert_eq!(
        file_count(catalog.as_ref(), "rlean.at_floor").await,
        DEFAULT_MIN_SMALL_FILES as u64
    );

    // One more file clears the floor: now it is selected.
    write::append(
        catalog.as_ref(),
        &parse_table_ident("rlean.at_floor").expect("ident"),
        &source_file("more.csv", "k,v\n999,over\n"),
    )
    .await
    .expect("append past the floor");
    let report = run_compaction(catalog.as_ref(), CompactionOptions::default())
        .await
        .expect("run compaction");
    assert_eq!(report.compacted.len(), 1, "one over-floor table compacted");
    assert_eq!(report.compacted[0].table, "rlean.at_floor");
}

/// The snapshot count a table needs before expiry has anything to do: enough
/// past the keep-last floor that the pass is guaranteed to prune history, and
/// enough files for the plan to bin-pack every one of them into a single
/// group (so every original file's snapshot is a candidate for expiry).
const MANY_SNAPSHOTS: usize = 20;

/// Acceptance (issue #281): a compaction pass on a table with many small
/// snapshots reduces the snapshot count rather than growing it further. Before
/// this, `compact_table`/`compact_table_bounded` committed one REPLACE per
/// bin-pack group with no expiration, so repeated runs only ever added history
/// — exactly the shape that grew the reported table's `metadata.json` to ~7 MiB
/// at snapshot ~8400. A single pass here both compacts and expires, so history
/// ends bounded at the keep-last floor instead of growing by one more entry.
#[tokio::test]
async fn compaction_expires_old_snapshots_and_bounds_history() {
    let catalog = memory_catalog().await;
    let dotted = "rlean.snapshot_bloat";
    let ident = parse_table_ident(dotted).expect("ident");
    // seed + 20 single-row appends = 21 snapshots, comfortably past the
    // keep-last floor.
    table_with_many_small_files(catalog.as_ref(), dotted, MANY_SNAPSHOTS).await;
    let snapshots_before = inspect::history(catalog.as_ref(), &ident)
        .await
        .expect("history before")
        .snapshots
        .len();
    assert_eq!(snapshots_before, MANY_SNAPSHOTS + 1);

    let report = compact_table(catalog.as_ref(), &ident, CompactionOptions::default())
        .await
        .expect("compact");
    assert!(
        report.snapshots_expired > 0,
        "old history was expired this pass: {report:?}"
    );

    let snapshots_after = inspect::history(catalog.as_ref(), &ident)
        .await
        .expect("history after")
        .snapshots
        .len();
    assert!(
        snapshots_after <= DEFAULT_KEEP_LAST_SNAPSHOTS,
        "history is bounded to the keep-last floor: {snapshots_after} <= {DEFAULT_KEEP_LAST_SNAPSHOTS}"
    );
    // Not merely capped going forward — strictly fewer snapshots than before
    // the pass, even though the pass itself added one more (the REPLACE).
    assert!(
        snapshots_after < snapshots_before,
        "expiry shrank history: {snapshots_before} -> {snapshots_after}"
    );

    // Expiring history never touches live rows.
    assert_eq!(
        count(catalog.clone(), dotted).await,
        (MANY_SNAPSHOTS + 1) as i64
    );
}

/// Acceptance (issue #281): time travel to a snapshot the keep-last window
/// still retains keeps working after a pass that expired older history. The
/// most recent pre-compaction snapshot is always inside the keep-last window
/// (it is second-newest, right behind the REPLACE the same pass just
/// committed), so it must never be a casualty of expiry.
#[tokio::test]
async fn time_travel_to_a_retained_snapshot_still_works_after_expiry() {
    let catalog = memory_catalog().await;
    let dotted = "rlean.expiry_time_travel";
    let ident = parse_table_ident(dotted).expect("ident");
    table_with_many_small_files(catalog.as_ref(), dotted, MANY_SNAPSHOTS).await;
    let rows_before = all_rows(catalog.clone(), dotted, "k").await;
    let pre_compaction_snapshot = inspect::show(catalog.as_ref(), &ident)
        .await
        .expect("show")
        .current_snapshot_id
        .expect("a current snapshot");

    let report = compact_table(catalog.as_ref(), &ident, CompactionOptions::default())
        .await
        .expect("compact");
    assert!(
        report.snapshots_expired > 0,
        "this pass must actually exercise expiry for the test to prove anything: {report:?}"
    );

    // Confirm the pre-compaction snapshot id itself is still present in
    // history — not merely that the query below happens to succeed some other
    // way.
    let history = inspect::history(catalog.as_ref(), &ident)
        .await
        .expect("history");
    assert!(
        history
            .snapshots
            .iter()
            .any(|s| s.snapshot_id == pre_compaction_snapshot),
        "the most recent pre-compaction snapshot survives expiry"
    );

    let travelled = query::query(
        catalog.clone(),
        "SELECT * FROM expiry_time_travel ORDER BY k",
        Some(TimeTravel {
            reference: pre_compaction_snapshot.to_string(),
            table: dotted.to_owned(),
        }),
    )
    .await
    .expect("time travel to a retained snapshot");
    let travelled_rows: Vec<serde_json::Value> = travelled
        .rows
        .into_iter()
        .map(serde_json::Value::Object)
        .collect();
    assert_eq!(
        travelled_rows, rows_before,
        "time travel to a snapshot the keep-last window retains still resolves the exact pre-compaction rows"
    );
}

/// Acceptance (issue #281): once its snapshot is expired, a removed small
/// file's data is actually reclaimed — not left as a permanent orphan on
/// storage — while a removed file whose adding snapshot is still inside the
/// keep-last window is correctly left in place for that snapshot's time
/// travel. This is the same conservative reachability rule
/// [`cleanup_orphans`] already enforces, driven here by the snapshots
/// `retention::expire_snapshots` just dropped.
#[tokio::test]
async fn expiring_a_snapshot_reclaims_its_orphaned_data_files() {
    let catalog = memory_catalog().await;
    let dotted = "rlean.expiry_orphans";
    let ident = parse_table_ident(dotted).expect("ident");

    // seed + 23 single-row appends = 24 tiny pre-compaction snapshots. Each is
    // a fast-append, whose manifest list is cumulative back to genesis — so
    // while *any* pre-compaction append snapshot survives the keep-last
    // window, it alone keeps every merged-away file "reachable" (real
    // Iceberg's own behavior: a REPLACE forgets what it removed, but an
    // earlier append never does, and a REPLACE still carries forward whatever
    // it did not touch this round). A `max_files_per_group` of 2 forces this
    // one pass to merge the 24 files pairwise down to 1 across ~23 sequential
    // REPLACE commits — comfortably more than the keep-last floor — so by the
    // time this pass's own expiry runs, every pre-compaction append snapshot
    // has been pushed out of history by this pass's own REPLACE chain, and
    // the files only *those* fully-superseded snapshots referenced stop being
    // reachable through anything retained.
    table_with_many_small_files(catalog.as_ref(), dotted, 23).await;
    let options = CompactionOptions {
        max_files_per_group: 2,
        ..CompactionOptions::default()
    };

    let report = compact_table(catalog.as_ref(), &ident, options)
        .await
        .expect("compact");
    assert!(
        report.groups_committed as usize >= DEFAULT_KEEP_LAST_SNAPSHOTS,
        "pairwise merging needed many sequential REPLACE commits: {report:?}"
    );
    assert!(
        report.snapshots_expired > 0,
        "this pass must actually exercise expiry for the test to prove anything: {report:?}"
    );
    assert!(
        !report.removed_files.is_empty(),
        "the bin-pack groups merged the original small files"
    );

    // Walk storage directly rather than trusting the report's own count.
    let table = catalog
        .load_table(&ident)
        .await
        .expect("load after compact");
    let file_io = table.file_io();
    let mut still_present = 0usize;
    for path in &report.removed_files {
        if file_io.exists(path).await.expect("stat removed file") {
            still_present += 1;
        }
    }
    let actually_deleted = report.removed_files.len() - still_present;
    assert_eq!(
        actually_deleted as u64, report.orphan_files_deleted,
        "the report's deleted count matches what is actually gone from storage"
    );
    assert!(
        report.orphan_files_deleted > 0,
        "at least one merged-away file, whose whole lineage expired, was reclaimed rather than left orphaned"
    );
    assert!(
        still_present > 0,
        "a file a still-retained intermediate REPLACE's own point-in-time view still needs \
         (its time-travel target) is correctly kept, not deleted out from under it"
    );

    // Row content is exactly what all the small files held — reclaiming
    // orphaned files never touches live data.
    assert_eq!(count(catalog.clone(), dotted).await, 24);
}

/// Acceptance: a table whose history is already at or under the keep-last
/// floor is left alone by expiry — no spurious `RemoveSnapshots` commit, and
/// nothing reported expired or deleted.
#[tokio::test]
async fn expiry_is_a_noop_under_the_keep_last_floor() {
    let catalog = memory_catalog().await;
    let dotted = "rlean.small_history";
    let ident = parse_table_ident(dotted).expect("ident");
    // seed + 3 appends = 4 snapshots, well under the keep-last floor even
    // after the one REPLACE this pass commits.
    table_with_many_small_files(catalog.as_ref(), dotted, 3).await;
    let snapshots_before = inspect::history(catalog.as_ref(), &ident)
        .await
        .expect("history before")
        .snapshots
        .len();

    let report = compact_table(catalog.as_ref(), &ident, CompactionOptions::default())
        .await
        .expect("compact");
    assert_eq!(
        report.snapshots_expired, 0,
        "well under the keep-last floor: nothing to expire"
    );
    assert_eq!(report.orphan_files_deleted, 0);

    let snapshots_after = inspect::history(catalog.as_ref(), &ident)
        .await
        .expect("history after")
        .snapshots
        .len();
    // Exactly one snapshot longer: the REPLACE this pass committed, and
    // nothing pruned.
    assert_eq!(snapshots_after, snapshots_before + 1);
}
