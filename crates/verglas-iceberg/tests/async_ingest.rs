//! Hermetic tests for the async `mode=append` ack path (`async_ingest`).
//!
//! Exercised fully in-process against an Iceberg `MemoryCatalog`, matching
//! `tests/tables_api.rs`'s hermetic style: no server, no MinIO, no network.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::{Catalog, CatalogBuilder, TableIdent};
use verglas_iceberg::{AsyncIngestQueue, parse_table_ident, tables_api, write};

/// Builds an in-process memory catalog over a fresh temp warehouse path. The
/// guard is leaked because the test process is short-lived (mirrors
/// `tests/tables_api.rs`'s helper).
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

/// A two-column schema: a required id and a nullable name.
fn simple_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

/// Creates `dotted` with the simple schema and returns its ident.
async fn create(catalog: &dyn Catalog, dotted: &str) -> TableIdent {
    let ident = parse_table_ident(dotted).expect("ident");
    write::create_table_from_schema(catalog, &ident, &simple_schema(), None)
        .await
        .expect("create table");
    ident
}

/// A batch matching `simple_schema` for the given ids and names.
fn batch(ids: &[i64], names: &[Option<&str>]) -> RecordBatch {
    RecordBatch::try_new(
        simple_schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(names.to_vec())),
        ],
    )
    .expect("build batch")
}

/// Live snapshot count of `ident`, `None` when the table has never committed.
async fn snapshot_count(catalog: &dyn Catalog, ident: &TableIdent) -> usize {
    catalog
        .load_table(ident)
        .await
        .expect("load table")
        .metadata()
        .snapshots()
        .count()
}

/// Polls until `queue` reports idle for `ident`, or panics after a bounded
/// wait — a background-task test helper, not production code.
async fn wait_idle(queue: &AsyncIngestQueue, ident: &TableIdent) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !queue.is_idle(ident) {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("queue went idle");
}

/// Acceptance: an async ack returns immediately with the accepted row count
/// and `committed: false`, the rows are not yet visible, and once the
/// background commit runs (triggered by `spawn_commit_if_idle`) they become
/// visible through the normal read path.
#[tokio::test]
async fn async_ack_then_eventual_commit_becomes_visible() {
    let catalog = memory_catalog().await;
    let ident = create(catalog.as_ref(), "sdk.events").await;
    let dir = tempfile::tempdir().expect("wal dir");
    let queue = Arc::new(AsyncIngestQueue::open(dir.path()).expect("open queue"));

    let ack = queue
        .ack_append(
            catalog.as_ref(),
            &ident,
            vec![batch(&[1, 2], &[Some("alice"), Some("bob")])],
        )
        .await
        .expect("ack");
    assert_eq!(ack.successful_rows, 2);
    assert!(!ack.committed, "the async ack never reports committed");

    let snap = tables_api::snapshot(catalog.as_ref(), &ident)
        .await
        .expect("snapshot");
    assert_eq!(snap.record_count, 0, "not committed yet");

    queue.spawn_commit_if_idle(Arc::clone(&catalog), ident.clone());
    wait_idle(&queue, &ident).await;

    let snap = tables_api::snapshot(catalog.as_ref(), &ident)
        .await
        .expect("snapshot");
    assert_eq!(snap.record_count, 2, "the background commit ran");
}

/// Acceptance: several acks that queue up before the committer runs are
/// coalesced into exactly one Iceberg commit (one new snapshot), not one per
/// ack.
#[tokio::test]
async fn consecutive_appends_coalesce_into_one_commit() {
    let catalog = memory_catalog().await;
    let ident = create(catalog.as_ref(), "sdk.events").await;
    let dir = tempfile::tempdir().expect("wal dir");
    let queue = Arc::new(AsyncIngestQueue::open(dir.path()).expect("open queue"));

    let before = snapshot_count(catalog.as_ref(), &ident).await;

    for (id, name) in [(1i64, "a"), (2, "b"), (3, "c")] {
        queue
            .ack_append(catalog.as_ref(), &ident, vec![batch(&[id], &[Some(name)])])
            .await
            .expect("ack");
    }

    queue.spawn_commit_if_idle(Arc::clone(&catalog), ident.clone());
    wait_idle(&queue, &ident).await;

    let snap = tables_api::snapshot(catalog.as_ref(), &ident)
        .await
        .expect("snapshot");
    assert_eq!(snap.record_count, 3, "all three rows landed");

    let after = snapshot_count(catalog.as_ref(), &ident).await;
    assert_eq!(after, before + 1, "three acks produced exactly one commit");
}

/// Acceptance: a row that does not match the table schema is rejected by the
/// async ack synchronously — nothing is journaled to disk and nothing is
/// queued for commit.
#[tokio::test]
async fn schema_rejection_is_synchronous_and_nothing_is_journaled() {
    let catalog = memory_catalog().await;
    let ident = create(catalog.as_ref(), "sdk.events").await;
    let dir = tempfile::tempdir().expect("wal dir");
    let queue = AsyncIngestQueue::open(dir.path()).expect("open queue");

    let mismatched = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![1i64]))],
    )
    .expect("build mismatched batch");

    let err = queue
        .ack_append(catalog.as_ref(), &ident, vec![mismatched])
        .await
        .expect_err("missing required `name` column is rejected");
    assert!(
        err.to_string().contains("name"),
        "the error names the offending column: {err}"
    );

    assert!(queue.is_idle(&ident), "nothing was queued");
    let wal_entries = std::fs::read_dir(dir.path()).expect("read wal dir").count();
    assert_eq!(wal_entries, 0, "nothing was journaled to disk");

    let snap = tables_api::snapshot(catalog.as_ref(), &ident)
        .await
        .expect("snapshot");
    assert_eq!(snap.record_count, 0, "the rejected append wrote nothing");
}

/// Acceptance: the synchronous path (what the HTTP layer takes for
/// `wait=true`/`commit=sync`) commits immediately and reports the committed
/// snapshot id in the same call — no queue, no background task.
#[tokio::test]
async fn synchronous_append_commits_immediately_with_a_snapshot_id() {
    let catalog = memory_catalog().await;
    let ident = create(catalog.as_ref(), "sdk.events").await;

    let report = write::append_batches(
        catalog.as_ref(),
        &ident,
        vec![batch(&[1], &[Some("alice")])],
        HashMap::new(),
    )
    .await
    .expect("synchronous append");

    assert_eq!(report.records_added, 1);
    assert!(report.snapshot_id.is_some(), "the commit already happened");

    let snap = tables_api::snapshot(catalog.as_ref(), &ident)
        .await
        .expect("snapshot");
    assert_eq!(
        snap.record_count, 1,
        "visible immediately, no polling needed"
    );
}

/// Acceptance (crash safety): rows acked and durably journaled, but never
/// committed because the process "crashed" before a committer ran, are
/// replayed and committed by a fresh queue instance opened over the same WAL
/// directory — the model is the write-back journal's replay-on-start.
#[tokio::test]
async fn uncommitted_journaled_rows_replay_after_simulated_restart() {
    let catalog = memory_catalog().await;
    let ident = create(catalog.as_ref(), "sdk.events").await;
    let dir = tempfile::tempdir().expect("wal dir");

    {
        // The "before crash" process: ack durably journals two requests but
        // never triggers a commit (no `spawn_commit_if_idle` call), exactly
        // simulating a crash in the ack-then-async-commit window.
        let queue = AsyncIngestQueue::open(dir.path()).expect("open queue");
        queue
            .ack_append(catalog.as_ref(), &ident, vec![batch(&[1], &[Some("a")])])
            .await
            .expect("ack 1");
        queue
            .ack_append(catalog.as_ref(), &ident, vec![batch(&[2], &[Some("b")])])
            .await
            .expect("ack 2");
    }

    let snap = tables_api::snapshot(catalog.as_ref(), &ident)
        .await
        .expect("snapshot");
    assert_eq!(snap.record_count, 0, "nothing committed before the crash");
    let wal_entries_before = std::fs::read_dir(dir.path()).expect("read wal dir").count();
    assert_eq!(wal_entries_before, 2, "both acks were durably journaled");

    // The "after restart" process: a fresh queue instance, same directory.
    let queue = AsyncIngestQueue::open(dir.path()).expect("reopen queue");
    let replayed = queue.replay(catalog.as_ref()).await.expect("replay");
    assert_eq!(replayed, 2, "both journaled rows were replayed");

    let snap = tables_api::snapshot(catalog.as_ref(), &ident)
        .await
        .expect("snapshot");
    assert_eq!(snap.record_count, 2, "the replay committed them");

    let wal_entries_after = std::fs::read_dir(dir.path()).expect("read wal dir").count();
    assert_eq!(wal_entries_after, 0, "the WAL was cleaned up after replay");
}
