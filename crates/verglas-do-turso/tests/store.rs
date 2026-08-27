//! Acceptance tests for Turso Durable Object storage and publication seams.
//!
//! These tests cover SQL, Worker reserved state, rollback/commit visibility,
//! embedded WAL checkpoint durability, and crash-safe outbox publication.

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

/// Returns a fresh embedded store rooted in a complete temporary sidecar family.
async fn store(root: &TempDir) -> Result<TursoStore, verglas_do_turso::Error> {
    TursoStore::open(root.path().join("worker.db"), "worker-test").await
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
    event.commit().await?;
    Ok(())
}

/// Embedded Turso checkpoints vector state atomically and exposes every required distance function.
#[tokio::test]
async fn native_vector_values_and_distance_are_available() -> Result<(), verglas_do_turso::Error> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("worker.db");
    let store = TursoStore::open(&path, "worker-test").await?;
    let event = store.begin_event().await?;
    event
        .execute(
            "CREATE TABLE vectors (id TEXT PRIMARY KEY, embedding F32_BLOB(3), metadata_json TEXT)",
        )
        .await?;
    event
        .execute("CREATE TABLE receipts (mutation_id TEXT PRIMARY KEY)")
        .await?;
    event
        .execute("CREATE TABLE metadata_indexes (property_name TEXT PRIMARY KEY)")
        .await?;
    event
        .execute("INSERT INTO vectors VALUES ('a', vector32('[1,0,0]'), '{\"kind\":\"doc\"}')")
        .await?;
    event
        .execute("INSERT INTO receipts VALUES ('mutation-a')")
        .await?;
    event
        .execute("INSERT INTO metadata_indexes VALUES ('kind')")
        .await?;
    event.commit().await?;
    drop(store);

    let reopened = TursoStore::open(&path, "worker-test").await?;
    let event = reopened.begin_event().await?;
    let rows = event
        .query_json(
            "SELECT id, vector_extract(embedding) AS embedding_json,
                    vector_distance_cos(embedding, vector32('[1,0,0]')) AS cosine,
                    vector_distance_l2(embedding, vector32('[1,0,0]')) AS euclidean,
                    vector_distance_dot(embedding, vector32('[1,0,0]')) AS dot_product,
                    json_extract(metadata_json, '$.\"kind\"') AS kind
             FROM vectors WHERE json_extract(metadata_json, '$.\"kind\"') = 'doc'",
        )
        .await?;
    assert_eq!(rows[0]["id"], json!("a"));
    assert_eq!(rows[0]["embedding_json"], json!("[1,0,0]"));
    assert!(
        rows[0]["cosine"]
            .as_f64()
            .is_some_and(|value| value.abs() < 0.000_001)
    );
    assert!(
        rows[0]["euclidean"]
            .as_f64()
            .is_some_and(|value| value.abs() < 0.000_001)
    );
    assert!(
        rows[0]["dot_product"]
            .as_f64()
            .is_some_and(|value| (value + 1.0).abs() < 0.000_001)
    );
    assert_eq!(rows[0]["kind"], json!("doc"));
    event.rollback().await?;

    let failed = reopened.begin_event().await?;
    failed
        .execute("INSERT INTO vectors VALUES ('b', vector32('[0,1,0]'), NULL)")
        .await?;
    failed
        .execute("INSERT INTO receipts VALUES ('mutation-b')")
        .await?;
    failed
        .execute("INSERT INTO metadata_indexes VALUES ('temporary')")
        .await?;
    failed.rollback().await?;
    drop(reopened);

    let reopened = TursoStore::open(&path, "worker-test").await?;
    let event = reopened.begin_event().await?;
    assert_eq!(
        event
            .query_json("SELECT id FROM vectors ORDER BY id")
            .await?,
        json!([{ "id": "a" }])
    );
    assert_eq!(
        event
            .query_json("SELECT mutation_id FROM receipts ORDER BY mutation_id")
            .await?,
        json!([{ "mutation_id": "mutation-a" }])
    );
    assert_eq!(
        event
            .query_json("SELECT property_name FROM metadata_indexes ORDER BY property_name")
            .await?,
        json!([{ "property_name": "kind" }])
    );
    event.rollback().await?;
    Ok(())
}

/// Embedded Turso persists graph state atomically and plans both adjacency directions by index.
#[tokio::test]
async fn graph_adjacency_indexes_survive_reopen_and_cover_both_directions()
-> Result<(), verglas_do_turso::Error> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("worker.db");
    let store = TursoStore::open(&path, "graph-test").await?;
    let event = store.begin_event().await?;
    event
        .execute(
            "CREATE TABLE graph_nodes (
                external_id TEXT PRIMARY KEY, kind TEXT NOT NULL,
                properties_json TEXT, mutation_id TEXT NOT NULL)",
        )
        .await?;
    event
        .execute(
            "CREATE TABLE graph_edges (
                external_id TEXT PRIMARY KEY, from_id TEXT NOT NULL,
                to_id TEXT NOT NULL, kind TEXT NOT NULL,
                properties_json TEXT, mutation_id TEXT NOT NULL)",
        )
        .await?;
    event
        .execute(
            "CREATE TABLE graph_mutations (
                mutation_id TEXT PRIMARY KEY, operation TEXT NOT NULL)",
        )
        .await?;
    event
        .execute(
            "CREATE TABLE graph_property_indexes (
                scope TEXT NOT NULL, property_name TEXT NOT NULL,
                index_type TEXT NOT NULL, PRIMARY KEY(scope, property_name))",
        )
        .await?;
    event
        .execute(
            "CREATE INDEX graph_edges_out
             ON graph_edges(from_id, kind, to_id, external_id)",
        )
        .await?;
    event
        .execute(
            "CREATE INDEX graph_edges_in
             ON graph_edges(to_id, kind, from_id, external_id)",
        )
        .await?;
    event
        .execute(
            "INSERT INTO graph_nodes VALUES
             ('a', 'person', '{\"rank\":1}', 'mutation-a'),
             ('b', 'person', '{\"rank\":2}', 'mutation-a')",
        )
        .await?;
    event
        .execute(
            "INSERT INTO graph_edges VALUES
             ('ab', 'a', 'b', 'knows', '{\"trust\":0.9}', 'mutation-a')",
        )
        .await?;
    event
        .execute("INSERT INTO graph_mutations VALUES ('mutation-a', 'upsert')")
        .await?;
    event
        .execute("INSERT INTO graph_property_indexes VALUES ('node', 'rank', 'number')")
        .await?;
    event.commit().await?;
    drop(store);

    let reopened = TursoStore::open(&path, "graph-test").await?;
    let event = reopened.begin_event().await?;
    let outbound = event
        .query_json(
            "EXPLAIN QUERY PLAN
             SELECT external_id FROM graph_edges
             WHERE from_id = 'a' AND kind = 'knows'
             ORDER BY from_id, kind, to_id, external_id",
        )
        .await?;
    let inbound = event
        .query_json(
            "EXPLAIN QUERY PLAN
             SELECT external_id FROM graph_edges
             WHERE to_id = 'b' AND kind = 'knows'
             ORDER BY to_id, kind, from_id, external_id",
        )
        .await?;
    assert!(plan_mentions(&outbound, "graph_edges_out"));
    assert!(plan_mentions(&inbound, "graph_edges_in"));
    assert_eq!(
        event
            .query_json(
                "SELECT external_id AS id FROM graph_edges
                 WHERE from_id = 'a' AND kind = 'knows'",
            )
            .await?,
        json!([{ "id": "ab" }])
    );
    event.rollback().await?;

    let failed = reopened.begin_event().await?;
    failed
        .execute("INSERT INTO graph_nodes VALUES ('c', 'person', NULL, 'mutation-b')")
        .await?;
    failed
        .execute("INSERT INTO graph_mutations VALUES ('mutation-b', 'upsert')")
        .await?;
    failed
        .execute("INSERT INTO graph_property_indexes VALUES ('edge', 'trust', 'number')")
        .await?;
    failed.rollback().await?;
    drop(reopened);

    let reopened = TursoStore::open(&path, "graph-test").await?;
    let event = reopened.begin_event().await?;
    assert_eq!(
        event
            .query_json("SELECT external_id AS id FROM graph_nodes ORDER BY external_id")
            .await?,
        json!([{ "id": "a" }, { "id": "b" }])
    );
    assert_eq!(
        event
            .query_json("SELECT mutation_id FROM graph_mutations ORDER BY mutation_id")
            .await?,
        json!([{ "mutation_id": "mutation-a" }])
    );
    assert_eq!(
        event
            .query_json(
                "SELECT scope, property_name FROM graph_property_indexes
                 ORDER BY scope, property_name",
            )
            .await?,
        json!([{ "scope": "node", "property_name": "rank" }])
    );
    event.rollback().await?;
    Ok(())
}

/// Returns whether one JSON query plan names the expected index.
fn plan_mentions(plan: &serde_json::Value, index: &str) -> bool {
    plan.as_array().is_some_and(|rows| {
        rows.iter().any(|row| {
            row.as_object().is_some_and(|columns| {
                columns
                    .values()
                    .filter_map(serde_json::Value::as_str)
                    .any(|value| value.contains(index))
            })
        })
    })
}

/// Embedded Turso atomically persists Query rows, receipts, watermarks, and endpoint indexes.
#[tokio::test]
async fn query_materialization_survives_reopen_and_uses_endpoint_index()
-> Result<(), verglas_do_turso::Error> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("worker.db");
    let store = TursoStore::open(&path, "query-test").await?;
    let event = store.begin_event().await?;
    event.execute("CREATE TABLE query_view_rows (view_name TEXT NOT NULL, group_key TEXT NOT NULL, dimensions_json TEXT NOT NULL, measures_json TEXT NOT NULL, PRIMARY KEY(view_name, group_key))").await?;
    event.execute("CREATE TABLE query_batch_receipts (batch_id TEXT PRIMARY KEY, payload_digest TEXT NOT NULL)").await?;
    event.execute("CREATE TABLE query_source_watermarks (source TEXT PRIMARY KEY, last_sequence INTEGER NOT NULL)").await?;
    event.execute("CREATE INDEX query_endpoint_sales_by_day ON query_view_rows(view_name, json_extract(dimensions_json, '$.region'), group_key)").await?;
    event.execute("INSERT INTO query_view_rows VALUES ('daily_sales', '[\"2026-08-26\",\"west\"]', '{\"day\":\"2026-08-26\",\"region\":\"west\"}', '{\"revenue\":25}')").await?;
    event
        .execute("INSERT INTO query_batch_receipts VALUES ('batch-1', 'digest-1')")
        .await?;
    event
        .execute("INSERT INTO query_source_watermarks VALUES ('orders', 3)")
        .await?;
    event.commit().await?;
    drop(store);

    let reopened = TursoStore::open(&path, "query-test").await?;
    let event = reopened.begin_event().await?;
    let plan = event.query_json("EXPLAIN QUERY PLAN SELECT measures_json FROM query_view_rows INDEXED BY query_endpoint_sales_by_day WHERE view_name = 'daily_sales' AND json_extract(dimensions_json, '$.region') = 'west' ORDER BY group_key").await?;
    assert!(plan_mentions(&plan, "query_endpoint_sales_by_day"));
    assert_eq!(
        event
            .query_json(
                "SELECT json_extract(measures_json, '$.revenue') AS revenue FROM query_view_rows"
            )
            .await?,
        json!([{ "revenue": 25 }])
    );
    assert_eq!(
        event
            .query_json("SELECT batch_id FROM query_batch_receipts")
            .await?,
        json!([{ "batch_id": "batch-1" }])
    );
    assert_eq!(
        event
            .query_json("SELECT source, last_sequence FROM query_source_watermarks")
            .await?,
        json!([{ "source": "orders", "last_sequence": 3 }])
    );
    event.rollback().await?;

    let failed = reopened.begin_event().await?;
    failed
        .execute("INSERT INTO query_batch_receipts VALUES ('batch-2', 'digest-2')")
        .await?;
    failed
        .execute("UPDATE query_source_watermarks SET last_sequence = 4 WHERE source = 'orders'")
        .await?;
    failed.rollback().await?;
    drop(reopened);

    let reopened = TursoStore::open(&path, "query-test").await?;
    let event = reopened.begin_event().await?;
    assert_eq!(
        event
            .query_json("SELECT batch_id FROM query_batch_receipts")
            .await?,
        json!([{ "batch_id": "batch-1" }])
    );
    assert_eq!(
        event
            .query_json("SELECT last_sequence FROM query_source_watermarks")
            .await?,
        json!([{ "last_sequence": 3 }])
    );
    event.rollback().await?;
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

/// Production `open` commits through the local WAL checkpoint and survives reopen.
#[tokio::test]
async fn production_open_commit_checkpoint_reopen() -> Result<(), verglas_do_turso::Error> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("worker.db");
    let store = TursoStore::open(&path, "worker-test").await?;
    let event = store.begin_event().await?;
    event.put_kv("key", b"value".to_vec()).await?;
    event.set_alarm(42).await?;
    event.set_attachment(9, b"attachment".to_vec()).await?;
    event.commit().await?;
    drop(store);

    let reopened = TursoStore::open(&path, "worker-test").await?;
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
    event.commit().await?;
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
    event.commit().await?;

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
    event.commit().await?;

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
    event.commit().await?;
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
    let store = TursoStore::open(&path, "worker-test").await?;
    let appender = RecordingAppender::default();
    store.set_stream_appender(Arc::new(appender.clone())).await;
    let event = store.begin_event().await?;
    event
        .append_stream_records("STREAM", "stream-id", vec![json!({ "value": 1 })])
        .await?;
    event.commit().await?;
    store.drain_outbox().await?;
    assert_eq!(appender.batches.lock().await.len(), 1);
    drop(store);

    let reopened = TursoStore::open(&path, "worker-test").await?;
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
    event.commit().await?;
    assert!(store.drain_outbox().await.is_err());
    assert!(matches!(
        store.begin_event().await,
        Err(verglas_do_turso::Error::OutboxInFlight)
    ));
    Ok(())
}

/// Shutdown rolls back an abandoned event and refuses committed outbox work.
#[tokio::test]
async fn shutdown_fence_requires_an_empty_outbox() -> Result<(), verglas_do_turso::Error> {
    let root = tempfile::tempdir()?;
    let store = store(&root).await?;
    let abandoned = store.begin_event().await?;
    abandoned
        .append_stream_records("STREAM", "stream-id", vec![json!({ "value": 1 })])
        .await?;
    drop(abandoned);
    store.shutdown_fence().await?;
    assert!(store.pending_outbox(1).await?.is_empty());

    let committed = store.begin_event().await?;
    committed
        .append_stream_records("STREAM", "stream-id", vec![json!({ "value": 2 })])
        .await?;
    committed.commit().await?;
    assert!(matches!(
        store.shutdown_fence().await,
        Err(verglas_do_turso::Error::ShutdownOutboxPending)
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
