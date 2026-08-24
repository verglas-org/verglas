//! Acceptance tests for Turso Durable Object storage and publication seams.
//!
//! These tests are written before the store implementation. They cover SQL,
//! Worker reserved state, rollback/commit visibility, and crash-safe outbox
//! publication without depending on a remote Turso service.

#![cfg(feature = "test-support")]

use serde_json::json;
use tempfile::TempDir;
use verglas_do_turso::{OutboxKey, OutboxRecord, TursoStore};

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

/// The outbox identity is deterministic and written in the same transaction as state.
#[tokio::test]
async fn outbox_crash_windows_are_replayable() -> Result<(), verglas_do_turso::Error> {
    let root = tempfile::tempdir()?;
    let store = store(&root).await?;
    let event = store.begin_event().await?;
    event.put_kv("selected", b"state".to_vec()).await?;
    let key = event
        .append_outbox(0, json!({ "kind": "selected", "value": 1 }))
        .await?;
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
    let record = OutboxRecord::new(key.clone(), json!({ "x": 1 }));
    assert_eq!(record.key, key);
}
