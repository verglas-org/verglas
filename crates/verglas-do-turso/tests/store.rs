//! Acceptance tests for Turso Durable Object storage and publication seams.
//!
//! These tests are written before the store implementation. They cover SQL,
//! Worker reserved state, rollback/commit visibility, and crash-safe outbox
//! publication without depending on a remote Turso service.

#![cfg(feature = "test-support")]

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;
use tempfile::TempDir;
use tokio::sync::Mutex;
use verglas_do_turso::{OutboxKey, OutboxRecord, StreamAppender, TursoStore};

/// Test appender that records every ACKed batch and can fail before acknowledgement.
#[derive(Clone, Default)]
struct RecordingAppender {
    batches: Arc<Mutex<Vec<Vec<OutboxRecord>>>>,
    fail: bool,
}

#[async_trait]
impl StreamAppender for RecordingAppender {
    /// Records a batch as durable only when failure mode is disabled.
    async fn append(&self, records: Vec<OutboxRecord>) -> verglas_do_turso::Result<()> {
        if self.fail {
            return Err(verglas_do_turso::Error::OutboxUnavailable);
        }
        self.batches.lock().await.push(records);
        Ok(())
    }
}

/// Returns a fresh test store rooted in a complete temporary sidecar family.
async fn store(root: &TempDir) -> Result<TursoStore, verglas_do_turso::Error> {
    TursoStore::open_for_test(root.path().join("worker.db"), "worker-test").await
}

/// SQL DDL and every basic DML operation share one event transaction.
#[tokio::test]
async fn sql_create_insert_update_delete_select() -> Result<(), verglas_do_turso::Error> {
    let root = tempfile::tempdir()?;
    let store = store(&root).await?;
    let event = store.begin_event().await?;
    event
        .execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        .await?;
    event
        .execute("INSERT INTO items (id, name) VALUES (1, 'first')")
        .await?;
    event
        .execute("UPDATE items SET name = 'updated' WHERE id = 1")
        .await?;
    let rows = event
        .query_json("SELECT id, name FROM items ORDER BY id")
        .await?;
    assert_eq!(rows, json!([{ "id": 1, "name": "updated" }]));
    event.execute("DELETE FROM items WHERE id = 1").await?;
    assert_eq!(event.query_json("SELECT * FROM items").await?, json!([]));
    event.commit_and_push().await?;
    Ok(())
}

/// KV, alarm, and attachment writes are visible before commit and disappear on rollback.
#[tokio::test]
async fn worker_state_reads_own_writes_and_rolls_back() -> Result<(), verglas_do_turso::Error> {
    let root = tempfile::tempdir()?;
    let store = store(&root).await?;
    let event = store.begin_event().await?;
    event.put_kv("key", b"value".to_vec()).await?;
    event.set_alarm(42).await?;
    event.set_attachment(9, b"attachment".to_vec()).await?;
    assert_eq!(event.get_kv("key").await?, Some(b"value".to_vec()));
    assert_eq!(event.get_alarm().await?, Some(42));
    assert_eq!(event.get_attachment(9).await?, Some(b"attachment".to_vec()));
    event.rollback().await?;

    let reopened = store.begin_event().await?;
    assert_eq!(reopened.get_kv("key").await?, None);
    assert_eq!(reopened.get_alarm().await?, None);
    assert_eq!(reopened.get_attachment(9).await?, None);
    reopened.rollback().await?;
    Ok(())
}

/// Committed Worker state survives closing and reopening the local database.
#[tokio::test]
async fn worker_state_commit_reopen() -> Result<(), verglas_do_turso::Error> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("worker.db");
    let store = TursoStore::open_for_test(&path, "worker-test").await?;
    let event = store.begin_event().await?;
    event.put_kv("key", b"value".to_vec()).await?;
    event.set_alarm(42).await?;
    event.set_attachment(9, b"attachment".to_vec()).await?;
    event.commit_and_push().await?;
    drop(store);

    let reopened = TursoStore::open_for_test(&path, "worker-test").await?;
    let event = reopened.begin_event().await?;
    assert_eq!(event.get_kv("key").await?, Some(b"value".to_vec()));
    assert_eq!(event.get_alarm().await?, Some(42));
    assert_eq!(event.get_attachment(9).await?, Some(b"attachment".to_vec()));
    event.rollback().await?;
    Ok(())
}

/// A handler failure can explicitly roll back without publishing any state.
#[tokio::test]
async fn handler_error_rolls_back_everything() -> Result<(), verglas_do_turso::Error> {
    let root = tempfile::tempdir()?;
    let store = store(&root).await?;
    let event = store.begin_event().await?;
    event.put_kv("failed", b"must-not-commit".to_vec()).await?;
    event
        .execute("CREATE TABLE failed_table (value TEXT)")
        .await?;
    event.rollback().await?;
    let event = store.begin_event().await?;
    assert_eq!(event.get_kv("failed").await?, None);
    assert!(event.execute("SELECT * FROM failed_table").await.is_err());
    event.rollback().await?;
    Ok(())
}

/// Stream sends stage records with one event sequence and contiguous indexes.
#[tokio::test]
async fn stream_send_stages_records_inside_the_source_transaction()
-> Result<(), verglas_do_turso::Error> {
    let root = tempfile::tempdir()?;
    let store = store(&root).await?;
    let event = store.begin_event().await?;
    event.put_kv("selected", b"state".to_vec()).await?;
    let keys = event
        .append_stream_records(
            "STREAM",
            "stream-id",
            vec![json!({ "kind": "one" }), json!({ "kind": "two" })],
        )
        .await?;
    assert_eq!(keys[0].event_id(), "worker-test:1:0");
    assert_eq!(keys[1].event_id(), "worker-test:1:1");
    event.rollback().await?;
    assert!(store.pending_outbox(10).await?.is_empty());
    Ok(())
}

/// A source commit before Stream send is replayed from the durable outbox on activation.
#[tokio::test]
async fn source_commit_before_stream_send_replays_on_activation()
-> Result<(), verglas_do_turso::Error> {
    let root = tempfile::tempdir()?;
    let store = store(&root).await?;
    let event = store.begin_event().await?;
    event
        .append_stream_records("STREAM", "stream-id", vec![json!({ "value": 1 })])
        .await?;
    event.commit_and_push().await?;
    assert_eq!(store.pending_outbox(10).await?.len(), 1);

    let appender = RecordingAppender::default();
    store.set_stream_appender(Arc::new(appender.clone())).await;
    store.drain_outbox().await?;
    assert_eq!(appender.batches.lock().await.len(), 1);
    assert!(store.pending_outbox(10).await?.is_empty());
    Ok(())
}

/// Beginning the next source event drains a committed outbox before opening it.
#[tokio::test]
async fn begin_event_drains_outbox_before_next_source_event() -> Result<(), verglas_do_turso::Error>
{
    let root = tempfile::tempdir()?;
    let store = store(&root).await?;
    let event = store.begin_event().await?;
    event
        .append_stream_records("STREAM", "stream-id", vec![json!({ "value": 1 })])
        .await?;
    event.commit_and_push().await?;

    let appender = RecordingAppender::default();
    store.set_stream_appender(Arc::new(appender.clone())).await;
    let next = store.begin_event().await?;
    assert_eq!(appender.batches.lock().await.len(), 1);
    next.rollback().await?;
    assert!(store.pending_outbox(10).await?.is_empty());
    Ok(())
}

/// A committed outbox row survives before-send and replays after an expired claim.
#[tokio::test]
async fn outbox_crash_windows_are_replayable() -> Result<(), verglas_do_turso::Error> {
    let root = tempfile::tempdir()?;
    let store = store(&root).await?;
    let event = store.begin_event().await?;
    event.put_kv("selected", b"state".to_vec()).await?;
    let key = event
        .append_stream_records("STREAM", "stream-id", vec![json!({ "kind": "selected" })])
        .await?[0]
        .clone();
    event.commit_and_push().await?;

    let pending = store.pending_outbox(10).await?;
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].key, key);
    assert_eq!(pending[0].event_id(), "worker-test:1:0");

    store.mark_outbox_inflight(&key, "relay-a", 10).await?;
    assert!(store.pending_outbox(10).await?.is_empty());
    store.reclaim_expired_outbox(10).await?;
    assert_eq!(store.pending_outbox(10).await?.len(), 1);

    store.mark_outbox_inflight(&key, "relay-a", 20).await?;
    store.mark_outbox_delivered(&key, "relay-a").await?;
    assert!(store.pending_outbox(10).await?.is_empty());
    Ok(())
}

/// A Stream ACK followed by a source crash resends the same identity for Stream deduplication.
#[tokio::test]
async fn ack_before_delivered_mark_replays_the_same_identity() -> Result<(), verglas_do_turso::Error>
{
    let root = tempfile::tempdir()?;
    let store = store(&root).await?;
    let appender = RecordingAppender::default();
    store.set_stream_appender(Arc::new(appender.clone())).await;
    let event = store.begin_event().await?;
    event
        .append_stream_records("STREAM", "stream-id", vec![json!({ "value": 1 })])
        .await?;
    event.commit_and_push().await?;
    let pending = store.pending_outbox(10).await?;
    let key = pending[0].key.clone();
    store.mark_outbox_inflight(&key, "crashed-relay", 0).await?;
    appender.append(pending).await?;
    store.reclaim_expired_outbox(0).await?;
    store.drain_outbox().await?;
    let batches = appender.batches.lock().await;
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0][0].event_id(), batches[1][0].event_id());
    Ok(())
}

/// A delivered row stays suppressed when the source store reopens after the mark.
#[tokio::test]
async fn delivered_mark_survives_recovery_without_resend() -> Result<(), verglas_do_turso::Error> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("worker.db");
    let store = TursoStore::open_for_test(&path, "worker-test").await?;
    let appender = RecordingAppender::default();
    store.set_stream_appender(Arc::new(appender.clone())).await;
    let event = store.begin_event().await?;
    event
        .append_stream_records("STREAM", "stream-id", vec![json!({ "value": 1 })])
        .await?;
    event.commit_and_push().await?;
    store.drain_outbox().await?;
    assert_eq!(appender.batches.lock().await.len(), 1);
    drop(store);

    let reopened = TursoStore::open_for_test(&path, "worker-test").await?;
    let replay = RecordingAppender::default();
    reopened.set_stream_appender(Arc::new(replay.clone())).await;
    reopened.drain_outbox().await?;
    assert!(replay.batches.lock().await.is_empty());
    Ok(())
}

/// An appender failure keeps the relay lease and blocks the next serialized event.
#[tokio::test]
async fn appender_failure_holds_effects_and_next_event() -> Result<(), verglas_do_turso::Error> {
    let root = tempfile::tempdir()?;
    let store = store(&root).await?;
    store
        .set_stream_appender(Arc::new(RecordingAppender {
            batches: Arc::new(Mutex::new(Vec::new())),
            fail: true,
        }))
        .await;
    let event = store.begin_event().await?;
    event
        .append_stream_records("STREAM", "stream-id", vec![json!({ "value": 1 })])
        .await?;
    event.commit_and_push().await?;
    assert!(store.drain_outbox().await.is_err());
    assert!(matches!(
        store.begin_event().await,
        Err(verglas_do_turso::Error::OutboxInFlight)
    ));
    Ok(())
}

/// Reserved and Turso internal table names are rejected by tenant SQL.
#[tokio::test]
async fn sql_cannot_touch_reserved_or_internal_tables() -> Result<(), verglas_do_turso::Error> {
    let root = tempfile::tempdir()?;
    let store = store(&root).await?;
    let event = store.begin_event().await?;
    for statement in [
        "SELECT * FROM __worker_kv",
        "DELETE FROM __verglas_outbox",
        "SELECT * FROM sqlite_master",
        "SELECT * FROM __turso_changes",
        "SELECT * FROM turso_metadata",
    ] {
        assert!(event.execute(statement).await.is_err(), "{statement}");
    }
    event.rollback().await?;
    Ok(())
}

/// The public outbox key remains stable across relay retries.
#[test]
fn outbox_key_is_ordered_and_stable() {
    let key = OutboxKey::new("source", 7, 3);
    assert_eq!(key.event_id(), "source:7:3");
    let record = OutboxRecord::new("STREAM", "stream-id", key.clone(), json!({ "x": 1 }));
    assert_eq!(record.key, key);
    assert_eq!(record.stream_name, "stream-id");
}
