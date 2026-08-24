//! Acceptance tests for the reserved Worker-state tables: KV, alarm, and
//! WebSocket attachments as ordinary committed relational DO state.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use verglas_do_engine::{
    CommitAuthority, CommitReceipt, DoEngine, Error, IsolationLevel, TransactionEnvelope,
    WorkerStateView, ensure_worker_tables, stage_alarm_clear, stage_alarm_set, stage_attachment,
    stage_kv_delete, stage_kv_put,
};

/// Sequence-assigning in-process authority for engine tests.
#[derive(Default)]
struct CountingAuthority {
    /// Number of commits granted so far; doubles as the next sequence.
    calls: Mutex<u64>,
}

#[async_trait]
impl CommitAuthority for CountingAuthority {
    /// Grants the next contiguous commit sequence.
    async fn commit(
        &self,
        envelope: &TransactionEnvelope,
    ) -> verglas_do_engine::Result<CommitReceipt> {
        let mut calls = self.calls.lock().expect("authority lock");
        *calls += 1;
        Ok(CommitReceipt::new(*calls, envelope.transaction_id()))
    }
}

/// Builds one engine with the reserved Worker tables created.
async fn worker_engine() -> DoEngine {
    let engine = DoEngine::new("worker-do", Arc::new(CountingAuthority::default()));
    ensure_worker_tables(&engine).await.expect("worker tables");
    engine
}

/// Stages mutations in one transaction and commits it through the engine.
macro_rules! commit_stages {
    ($engine:expr, $($stage:expr),+ $(,)?) => {{
        let mut txn = $engine
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("begin transaction");
        $( $stage(txn.as_mut()).expect("stage mutation"); )+
        $engine.commit(txn).await.expect("commit")
    }};
}

/// Table creation is idempotent and later engines can re-ensure them.
#[tokio::test]
async fn ensure_worker_tables_is_idempotent() {
    let engine = worker_engine().await;
    ensure_worker_tables(&engine)
        .await
        .expect("second ensure succeeds");
}

/// A committed put is readable, a second put wins, and delete tombstones.
#[tokio::test]
async fn kv_put_overwrite_and_delete_round_trip() {
    let engine = worker_engine().await;
    use verglas_do_engine::DoStorage;

    commit_stages!(engine, |t| stage_kv_put(t, "greeting", b"hello".to_vec()));
    let view = WorkerStateView::new(&engine);
    assert_eq!(
        view.kv_get("greeting").await.expect("get"),
        Some(b"hello".to_vec())
    );

    commit_stages!(engine, |t| stage_kv_put(t, "greeting", b"hi".to_vec()));
    assert_eq!(
        view.kv_get("greeting").await.expect("get"),
        Some(b"hi".to_vec())
    );

    commit_stages!(engine, |t| stage_kv_delete(t, "greeting"));
    assert_eq!(view.kv_get("greeting").await.expect("get"), None);
}

/// Prefix listing is sorted, bounded, and excludes tombstoned keys.
#[tokio::test]
async fn kv_list_respects_prefix_bound_and_tombstones() {
    let engine = worker_engine().await;
    use verglas_do_engine::DoStorage;

    commit_stages!(
        engine,
        |t| stage_kv_put(t, "user:a", b"1".to_vec()),
        |t| stage_kv_put(t, "user:b", b"2".to_vec()),
        |t| stage_kv_put(t, "user:c", b"3".to_vec()),
        |t| stage_kv_put(t, "other:z", b"4".to_vec()),
    );
    commit_stages!(engine, |t| stage_kv_delete(t, "user:b"));

    let view = WorkerStateView::new(&engine);
    let listed = view.kv_list("user:", 10).await.expect("list");
    assert_eq!(listed, vec!["user:a".to_owned(), "user:c".to_owned()]);

    let bounded = view.kv_list("user:", 1).await.expect("bounded list");
    assert_eq!(bounded, vec!["user:a".to_owned()]);
}

/// The single durable alarm sets, replaces, and clears as committed state.
#[tokio::test]
async fn alarm_set_replace_and_clear() {
    let engine = worker_engine().await;
    use verglas_do_engine::DoStorage;

    let view = WorkerStateView::new(&engine);
    assert_eq!(view.alarm().await.expect("alarm"), None);

    commit_stages!(engine, |t| stage_alarm_set(t, 1_111));
    assert_eq!(view.alarm().await.expect("alarm"), Some(1_111));

    commit_stages!(engine, |t| stage_alarm_set(t, 2_222));
    assert_eq!(view.alarm().await.expect("alarm"), Some(2_222));

    commit_stages!(engine, stage_alarm_clear);
    assert_eq!(view.alarm().await.expect("alarm"), None);
}

/// Attachments last-write-win per socket and detach with a tombstone.
#[tokio::test]
async fn attachments_round_trip_and_detach() {
    let engine = worker_engine().await;
    use verglas_do_engine::DoStorage;

    commit_stages!(
        engine,
        |t| stage_attachment(t, 7, Some(b"session-7".to_vec())),
        |t| stage_attachment(t, 9, Some(b"session-9".to_vec())),
    );

    let view = WorkerStateView::new(&engine);
    assert_eq!(
        view.attachment(7).await.expect("attachment"),
        Some(b"session-7".to_vec())
    );
    assert_eq!(view.attached_sockets().await.expect("attached"), vec![7, 9]);

    commit_stages!(engine, |t| stage_attachment(t, 7, None));
    assert_eq!(view.attachment(7).await.expect("attachment"), None);
    assert_eq!(view.attached_sockets().await.expect("attached"), vec![9]);
}

/// A staged-but-uncommitted mutation is invisible to committed-state readers.
#[tokio::test]
async fn uncommitted_stage_is_invisible_to_view() {
    let engine = worker_engine().await;
    use verglas_do_engine::DoStorage;

    let mut txn = engine
        .begin(IsolationLevel::Snapshot)
        .await
        .expect("begin transaction");
    stage_kv_put(txn.as_mut(), "pending", b"soon".to_vec()).expect("stage");

    let view = WorkerStateView::new(&engine);
    assert_eq!(view.kv_get("pending").await.expect("get"), None);
    drop(txn);
    assert_eq!(view.kv_get("pending").await.expect("get"), None);
}

/// A rejected authority leaves worker state unchanged.
#[tokio::test]
async fn rejected_commit_leaves_worker_state_unchanged() {
    /// Authority that refuses every envelope.
    struct RejectingAuthority;

    #[async_trait]
    impl CommitAuthority for RejectingAuthority {
        /// Rejects the commit with an authority error.
        async fn commit(
            &self,
            _envelope: &TransactionEnvelope,
        ) -> verglas_do_engine::Result<CommitReceipt> {
            Err(Error::Authority("quorum unavailable".to_owned()))
        }
    }

    let engine = DoEngine::new("worker-do", Arc::new(RejectingAuthority));
    ensure_worker_tables(&engine).await.expect("worker tables");
    use verglas_do_engine::DoStorage;

    let mut txn = engine
        .begin(IsolationLevel::Snapshot)
        .await
        .expect("begin transaction");
    stage_kv_put(txn.as_mut(), "doomed", b"never".to_vec()).expect("stage");
    engine
        .commit(txn)
        .await
        .expect_err("authority rejection must fail the commit");

    let view = WorkerStateView::new(&engine);
    assert_eq!(view.kv_get("doomed").await.expect("get"), None);
}
