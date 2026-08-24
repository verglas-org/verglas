//! Acceptance tests for the transactional WorkerStorage engine adapter.

use std::sync::{Arc, Mutex};

use arrow_ipc::reader::StreamReader;
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use verglas_do_engine::{
    CommitAuthority, CommitReceipt, DoEngine, DoStorage, IsolationLevel, SqliteReplicaStore,
    TableId, TransactionEnvelope, WorkerStateView, ensure_worker_tables,
};
use verglas_do_wasm::WorkerStorage;
use verglas_runtime::EngineWorkerStorage;

/// Sequence-assigning authority used to make commit visibility deterministic.
#[derive(Default)]
struct CountingAuthority {
    /// Number of committed envelopes.
    calls: Mutex<u64>,
}

#[async_trait]
impl CommitAuthority for CountingAuthority {
    /// Grants one contiguous sequence to each envelope.
    async fn commit(
        &self,
        envelope: &TransactionEnvelope,
    ) -> verglas_do_engine::Result<CommitReceipt> {
        let mut calls = self.calls.lock().expect("authority lock");
        *calls += 1;
        Ok(CommitReceipt::new(*calls, envelope.transaction_id()))
    }
}

/// Returns the schema used by the SQL adapter acceptance tests.
fn items_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

/// Creates one engine, SQL table, reserved tables, and an open event transaction.
async fn event_storage() -> (Arc<DoEngine>, EngineWorkerStorage) {
    let engine = Arc::new(DoEngine::new(
        "runtime-worker",
        Arc::new(CountingAuthority::default()),
    ));
    engine
        .create_table(TableId::new("items"), items_schema())
        .await
        .expect("items table");
    ensure_worker_tables(engine.as_ref())
        .await
        .expect("worker tables");
    let transaction = engine
        .begin(IsolationLevel::Snapshot)
        .await
        .expect("event transaction");
    let storage = EngineWorkerStorage::new(Arc::clone(&engine), transaction);
    (engine, storage)
}

/// Worker KV reads see writes and tombstones before the transaction commits.
#[tokio::test]
async fn storage_reads_own_staged_writes_and_deletes() {
    let (_engine, storage) = event_storage().await;

    storage
        .put("user:a".to_owned(), b"one".to_vec())
        .await
        .expect("put");
    assert_eq!(
        storage.get("user:a".to_owned()).await.expect("get"),
        Some(b"one".to_vec())
    );
    assert_eq!(
        storage.list("user:".to_owned(), 10).await.expect("list"),
        vec!["user:a".to_owned()]
    );
    assert!(storage.delete("user:a".to_owned()).await.expect("delete"));
    assert_eq!(storage.get("user:a".to_owned()).await.expect("get"), None);
    assert!(
        storage
            .list("user:".to_owned(), 10)
            .await
            .expect("list")
            .is_empty()
    );
}

/// Staged state is invisible to the committed view and appears after commit.
#[tokio::test]
async fn storage_staged_writes_become_visible_only_after_commit() {
    let (engine, storage) = event_storage().await;
    let view = WorkerStateView::new(engine.as_ref());

    storage
        .put("pending".to_owned(), b"value".to_vec())
        .await
        .expect("put");
    assert_eq!(view.kv_get("pending").await.expect("view get"), None);

    storage.commit().await.expect("commit event transaction");
    assert_eq!(
        view.kv_get("pending").await.expect("view get"),
        Some(b"value".to_vec())
    );
}

/// Alarm operations read their overlay and persist only after commit.
#[tokio::test]
async fn storage_alarm_verbs_round_trip_through_commit() {
    let (engine, storage) = event_storage().await;
    let view = WorkerStateView::new(engine.as_ref());

    assert_eq!(storage.get_alarm().await.expect("initial alarm"), None);
    storage.set_alarm(4_242).await.expect("set alarm");
    assert_eq!(
        storage.get_alarm().await.expect("staged alarm"),
        Some(4_242)
    );
    assert_eq!(view.alarm().await.expect("committed alarm"), None);

    storage.commit().await.expect("commit alarm");
    assert_eq!(view.alarm().await.expect("committed alarm"), Some(4_242));

    let transaction = engine
        .begin(IsolationLevel::Snapshot)
        .await
        .expect("clear transaction");
    let clearing = EngineWorkerStorage::new(Arc::clone(&engine), transaction);
    clearing.delete_alarm().await.expect("clear alarm");
    assert_eq!(clearing.get_alarm().await.expect("staged clear"), None);
    clearing.commit().await.expect("commit clear");
    assert_eq!(view.alarm().await.expect("cleared alarm"), None);
}

/// SQL and JSON-row SQL share one event transaction and see staged inserts.
#[tokio::test]
async fn storage_sql_and_sql_rows_see_staged_rows() {
    let (_engine, storage) = event_storage().await;
    let ipc = storage
        .sql("INSERT INTO items VALUES (1, 'first')".to_owned())
        .await
        .expect("insert SQL");
    let mut reader = StreamReader::try_new(std::io::Cursor::new(ipc), None).expect("IPC reader");
    let insert_batch = reader
        .next()
        .expect("insert batch")
        .expect("valid insert batch");
    assert_eq!(insert_batch.num_rows(), 1);

    storage
        .sql("INSERT INTO items VALUES (2, NULL)".to_owned())
        .await
        .expect("nullable insert SQL");
    let rows = storage
        .sql_rows("SELECT id, name FROM items ORDER BY id".to_owned())
        .await
        .expect("JSON rows SQL");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&rows).expect("rows JSON"),
        serde_json::json!([
            { "id": 1, "name": "first" },
            { "id": 2, "name": null }
        ])
    );
}

/// SQL rows remain available after the SQLite replica is reopened.
#[tokio::test]
async fn storage_sql_rows_survive_engine_reopen() {
    let directory = tempfile::tempdir().expect("replica directory");
    let replica_path = directory.path().join("replica.sqlite");
    let authority = Arc::new(CountingAuthority::default());
    let replica =
        Arc::new(SqliteReplicaStore::open(&replica_path, "persistent-worker").expect("replica"));
    let engine = Arc::new(
        DoEngine::open_persistent("persistent-worker", authority, Arc::clone(&replica))
            .expect("persistent engine"),
    );
    engine
        .create_table(TableId::new("items"), items_schema())
        .await
        .expect("items table");
    ensure_worker_tables(engine.as_ref())
        .await
        .expect("worker tables");
    let transaction = engine
        .begin(IsolationLevel::Snapshot)
        .await
        .expect("event transaction");
    let storage = EngineWorkerStorage::new(Arc::clone(&engine), transaction);
    storage
        .sql("INSERT INTO items VALUES (7, 'persisted')".to_owned())
        .await
        .expect("insert SQL");
    storage.commit().await.expect("commit SQL");

    let reopened_replica = Arc::new(
        SqliteReplicaStore::open(&replica_path, "persistent-worker").expect("reopened replica"),
    );
    let reopened = Arc::new(
        DoEngine::open_persistent(
            "persistent-worker",
            Arc::new(CountingAuthority::default()),
            reopened_replica,
        )
        .expect("reopened engine"),
    );
    let reopened_transaction = reopened
        .begin(IsolationLevel::Snapshot)
        .await
        .expect("reopened event transaction");
    let reopened_storage = EngineWorkerStorage::new(Arc::clone(&reopened), reopened_transaction);
    let rows = reopened_storage
        .sql_rows("SELECT id, name FROM items".to_owned())
        .await
        .expect("reopened JSON rows SQL");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&rows).expect("reopened rows JSON"),
        serde_json::json!([{ "id": 7, "name": "persisted" }])
    );
}
