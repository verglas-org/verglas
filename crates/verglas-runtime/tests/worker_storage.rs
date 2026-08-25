//! Acceptance tests for Turso-backed WorkerStorage event transactions.
//!
//! The test-only Turso constructor uses a real embedded database and exercises
//! the same SQL, reserved tables, rollback, and reopen paths as production.

#![cfg(feature = "test-support")]

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use tempfile::TempDir;
use tokio::sync::Mutex;
use verglas_do_turso::{OutboxKey, OutboxRecord, StreamAppender};
use verglas_do_wasm::{HostError, Request, Response, WorkerBindings, WorkerStorage};
use verglas_runtime::{BindingStreamAppender, TursoWorkerStorage};

/// Test-only Stream appender that records the ACKed batch without changing storage.
#[derive(Clone, Default)]
struct RecordingAppender {
    /// Records delivered outbox rows in append order.
    records: Arc<Mutex<Vec<OutboxRecord>>>,
}

#[async_trait]
impl StreamAppender for RecordingAppender {
    /// Records one batch as the Stream ACK boundary for this test.
    async fn append(&self, records: Vec<OutboxRecord>) -> verglas_do_turso::Result<()> {
        self.records.lock().await.extend(records);
        Ok(())
    }
}

/// Binding double that records the internal Stream append request.
#[derive(Default)]
struct RecordingBindings {
    /// Requests sent through the injected binding channel.
    requests: Mutex<Vec<Request>>,
}

#[async_trait]
impl WorkerBindings for RecordingBindings {
    /// Records the request and returns a durable-looking 202 response.
    async fn do_fetch(
        &self,
        _binding: String,
        _object: String,
        request: Request,
    ) -> Result<Response, HostError> {
        self.requests.lock().await.push(request);
        Ok(Response {
            status: 202,
            headers: Vec::new(),
            body: Vec::new(),
            accept_ws: None,
        })
    }
}

/// Opens one explicit test-only Turso store and one event capability.
async fn event_storage(
    root: &TempDir,
) -> Result<(Arc<verglas_do_turso::TursoStore>, TursoWorkerStorage), Box<dyn std::error::Error>> {
    let store = Arc::new(
        verglas_do_turso::TursoStore::open_for_test(
            root.path().join("worker.db"),
            "runtime-worker",
        )
        .await?,
    );
    let storage = TursoWorkerStorage::begin(Arc::clone(&store)).await?;
    Ok((store, storage))
}

/// KV, list, alarm, and attachment operations read their own staged writes.
#[tokio::test]
async fn event_storage_reads_own_worker_state_writes() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let (_store, storage) = event_storage(&root).await?;

    storage.put("user:a".to_owned(), b"one".to_vec()).await?;
    assert_eq!(
        storage.get("user:a".to_owned()).await?,
        Some(b"one".to_vec())
    );
    assert_eq!(storage.list("user:".to_owned(), 10).await?, vec!["user:a"]);
    assert!(storage.delete("user:a".to_owned()).await?);
    assert_eq!(storage.get("user:a".to_owned()).await?, None);

    storage.set_alarm(4_242).await?;
    assert_eq!(storage.get_alarm().await?, Some(4_242));
    storage.set_attachment(9, b"attachment".to_vec()).await?;
    assert_eq!(
        storage.get_attachment(9).await?,
        Some(b"attachment".to_vec())
    );
    assert_eq!(storage.attached_sockets().await?, vec![9]);
    storage.rollback().await?;
    Ok(())
}

/// A Stream send is only a staged Turso mutation until the event commits.
#[tokio::test]
async fn stream_send_is_staged_until_event_commit() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let (store, storage) = event_storage(&root).await?;
    storage
        .stream_send(
            "STREAM".to_owned(),
            "stream-id".to_owned(),
            r#"[{"value":1},{"value":2}]"#.to_owned(),
        )
        .await?;
    storage.rollback().await?;
    assert!(store.pending_outbox(10).await?.is_empty());

    let committed = TursoWorkerStorage::begin(Arc::clone(&store)).await?;
    committed
        .stream_send(
            "STREAM".to_owned(),
            "stream-id".to_owned(),
            r#"[{"value":3}]"#.to_owned(),
        )
        .await?;
    committed
        .commit()
        .await
        .expect_err("no appender must hold effects");
    let pending = store.pending_outbox(10).await?;
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].event_id(), "runtime-worker:1:0");
    Ok(())
}

/// SQL DDL and DML execute inside the same Turso event as Worker state.
#[tokio::test]
async fn event_storage_sql_ddl_dml_and_json_rows() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let (_store, storage) = event_storage(&root).await?;

    assert_eq!(
        storage
            .sql_rows("CREATE TABLE items (id INTEGER, name TEXT)".to_owned())
            .await?,
        "[]"
    );
    storage
        .sql_rows("INSERT INTO items VALUES (1, 'first')".to_owned())
        .await?;
    storage
        .sql_rows("UPDATE items SET name = 'updated' WHERE id = 1".to_owned())
        .await?;
    assert_eq!(
        serde_json::from_str::<Value>(
            &storage
                .sql_rows("SELECT id, name FROM items ORDER BY id".to_owned())
                .await?,
        )?,
        serde_json::json!([{ "id": 1, "name": "updated" }])
    );
    storage
        .sql_rows("DELETE FROM items WHERE id = 1".to_owned())
        .await?;
    assert_eq!(
        storage.sql_rows("SELECT * FROM items".to_owned()).await?,
        "[]"
    );
    storage.rollback().await?;
    Ok(())
}

/// Handler errors roll back SQL and every reserved Worker mutation.
#[tokio::test]
async fn handler_error_rolls_back_event_transaction() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let (store, storage) = event_storage(&root).await?;
    storage
        .put("failed".to_owned(), b"must-not-commit".to_vec())
        .await?;
    storage.set_alarm(99).await?;
    storage
        .sql_rows("CREATE TABLE failed_table (value TEXT)".to_owned())
        .await?;
    storage.rollback().await?;

    let next = TursoWorkerStorage::begin(Arc::clone(&store)).await?;
    assert_eq!(next.get("failed".to_owned()).await?, None);
    assert_eq!(next.get_alarm().await?, None);
    assert!(
        next.sql_rows("SELECT * FROM failed_table".to_owned())
            .await
            .is_err()
    );
    next.rollback().await?;
    Ok(())
}

/// Committed SQL and Worker state survive reopening the same Turso local path.
#[tokio::test]
async fn commit_and_reopen_preserves_event_state() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("worker.db");
    let store =
        Arc::new(verglas_do_turso::TursoStore::open_for_test(&path, "persistent-worker").await?);
    let storage = TursoWorkerStorage::begin(Arc::clone(&store)).await?;
    storage
        .put("persisted".to_owned(), b"value".to_vec())
        .await?;
    storage.set_alarm(12).await?;
    storage.set_attachment(3, b"socket".to_vec()).await?;
    storage
        .sql_rows("CREATE TABLE items (id INTEGER, name TEXT)".to_owned())
        .await?;
    storage
        .sql_rows("INSERT INTO items VALUES (7, 'persisted')".to_owned())
        .await?;
    storage.commit().await?;
    drop(storage);
    drop(store);

    let reopened =
        Arc::new(verglas_do_turso::TursoStore::open_for_test(&path, "persistent-worker").await?);
    let storage = TursoWorkerStorage::begin(Arc::clone(&reopened)).await?;
    assert_eq!(
        storage.get("persisted".to_owned()).await?,
        Some(b"value".to_vec())
    );
    assert_eq!(storage.get_alarm().await?, Some(12));
    assert_eq!(storage.get_attachment(3).await?, Some(b"socket".to_vec()));
    assert_eq!(
        serde_json::from_str::<Value>(
            &storage
                .sql_rows("SELECT id, name FROM items".to_owned())
                .await?,
        )?,
        serde_json::json!([{ "id": 7, "name": "persisted" }])
    );
    storage.rollback().await?;
    Ok(())
}

/// The runtime appender emits the Stream route and producer identities to internal append.
#[tokio::test]
async fn binding_appender_waits_for_internal_stream_ack() -> Result<(), Box<dyn std::error::Error>>
{
    let bindings = Arc::new(RecordingBindings::default());
    let appender = BindingStreamAppender::new(bindings.clone());
    appender
        .append(vec![OutboxRecord::new(
            "STREAM",
            "stream-id",
            OutboxKey::new("runtime-worker", 7, 3),
            serde_json::json!({ "value": 1 }),
        )])
        .await?;
    let requests = bindings.requests.lock().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[0].uri, "https://verglas.internal/stream/append");
    assert_eq!(
        requests[0].headers[1],
        (
            "x-verglas-producer-event-id".to_owned(),
            "[\"runtime-worker:7:3\"]".to_owned()
        )
    );
    Ok(())
}

/// A successful commit drains the injected outbox only after local state commits.
#[tokio::test]
async fn commit_drains_enabled_outbox_after_state_commit() -> Result<(), Box<dyn std::error::Error>>
{
    let root = tempfile::tempdir()?;
    let (store, storage) = event_storage(&root).await?;
    let appender = RecordingAppender::default();
    store.set_stream_appender(Arc::new(appender.clone())).await;
    storage
        .put("selected".to_owned(), b"state".to_vec())
        .await?;
    storage
        .stream_send(
            "STREAM".to_owned(),
            "stream-id".to_owned(),
            r#"[{"value":1}]"#.to_owned(),
        )
        .await?;
    storage.commit().await?;
    let records = appender.records.lock().await.clone();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].event_id(), "runtime-worker:1:0");
    assert!(store.pending_outbox(10).await?.is_empty());
    let event = store.begin_event().await?;
    assert_eq!(event.get_kv("selected").await?, Some(b"state".to_vec()));
    event.rollback().await?;
    Ok(())
}
