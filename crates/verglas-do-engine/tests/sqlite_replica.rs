//! SQLite replica pager persistence and recovery tests.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use futures::TryStreamExt;
use uuid::Uuid;
use verglas_do_engine::{
    ApplyOutcome, CommitAuthority, CommitReceipt, DoEngine, DoStorage, Error, IsolationLevel,
    MutationDomain, Projection, SnapshotFence, SqliteReplicaStore, TableId, TransactionEnvelope,
};

struct Authority;

#[async_trait]
impl CommitAuthority for Authority {
    async fn commit(
        &self,
        envelope: &TransactionEnvelope,
    ) -> verglas_do_engine::Result<CommitReceipt> {
        Ok(CommitReceipt::new(1, envelope.transaction_id()))
    }
}

#[test]
fn replica_reopens_with_applied_and_archive_state() {
    let directory = tempfile::tempdir().expect("temporary replica directory");
    let path = directory.path().join("replica.sqlite");
    let transaction_id = Uuid::from_u128(17);
    {
        let store = SqliteReplicaStore::open(&path, "agent-17").expect("open replica");
        assert_eq!(
            store
                .apply_committed(1, transaction_id, b"canonical-envelope")
                .expect("apply command"),
            ApplyOutcome::Applied
        );
        assert_eq!(store.state().expect("state").applied_sequence(), 1);
        assert_eq!(store.pending_archive().expect("pending").len(), 1);
        store
            .mark_archived(1, "sha256:one")
            .expect("archive receipt");
        store
            .mark_checkpointed(1, "checkpoint-one")
            .expect("checkpoint receipt");
    }

    let reopened = SqliteReplicaStore::open(&path, "agent-17").expect("reopen replica");
    let state = reopened.state().expect("recovered state");
    assert_eq!(state.applied_sequence(), 1);
    assert_eq!(state.archive_sequence(), 1);
    assert_eq!(state.checkpoint_sequence(), 1);
    assert!(reopened.pending_archive().expect("pending").is_empty());
    let replay = reopened.replay().expect("replay committed log");
    assert_eq!(replay.len(), 1);
    assert_eq!(replay[0].commit_sequence(), 1);
    assert_eq!(replay[0].transaction_id(), transaction_id);
}

#[test]
fn exact_retry_is_idempotent_and_conflicting_retry_fails() {
    let directory = tempfile::tempdir().expect("temporary replica directory");
    let store = SqliteReplicaStore::open(directory.path().join("replica.sqlite"), "agent")
        .expect("open replica");
    let transaction_id = Uuid::from_u128(9);
    assert_eq!(
        store
            .apply_committed(1, transaction_id, b"same")
            .expect("first apply"),
        ApplyOutcome::Applied
    );
    assert_eq!(
        store
            .apply_committed(1, transaction_id, b"same")
            .expect("exact retry"),
        ApplyOutcome::Duplicate
    );
    assert!(matches!(
        store.apply_committed(1, transaction_id, b"different"),
        Err(Error::ReplicaConflict(_))
    ));
}

#[test]
fn archive_and_checkpoint_watermarks_cannot_skip_or_exceed_applied_state() {
    let directory = tempfile::tempdir().expect("temporary replica directory");
    let store = SqliteReplicaStore::open(directory.path().join("replica.sqlite"), "agent")
        .expect("open replica");
    store
        .apply_committed(1, Uuid::from_u128(1), b"one")
        .expect("apply one");
    store
        .apply_committed(2, Uuid::from_u128(2), b"two")
        .expect("apply two");

    assert!(matches!(
        store.mark_archived(2, "skip"),
        Err(Error::ReplicaSequence(_))
    ));
    assert!(matches!(
        store.mark_checkpointed(2, "ahead-of-archive"),
        Err(Error::ReplicaSequence(_))
    ));
    store.mark_archived(1, "one").expect("archive one");
    store.mark_archived(2, "two").expect("archive two");
    store
        .mark_checkpointed(2, "checkpoint-two")
        .expect("checkpoint through two");
}

#[test]
fn clean_requires_verified_archive_and_checkpoint_coverage() {
    let directory = tempfile::tempdir().expect("temporary replica directory");
    let store = SqliteReplicaStore::open(directory.path().join("replica.sqlite"), "agent")
        .expect("open replica");
    let lease = verglas_do_engine::LeaseIdentity::new("held-token", 3);
    let transaction_id = Uuid::from_u128(3);
    store
        .apply_replicated(&lease, 1, transaction_id, b"one")
        .expect("apply replicated transaction");

    assert!(matches!(
        store.clean_replicated(&lease, 1),
        Err(Error::ReplicaSequence(message)) if message.contains("archive")
    ));
    assert_eq!(
        store.replay().expect("replay after rejected clean").len(),
        1
    );

    store
        .mark_archived(1, "transaction-one")
        .expect("archive transaction");
    assert!(matches!(
        store.clean_replicated(&lease, 1),
        Err(Error::ReplicaSequence(message)) if message.contains("checkpoint")
    ));
    assert_eq!(
        store
            .replay()
            .expect("replay after checkpoint rejection")
            .len(),
        1
    );

    store
        .mark_checkpointed(1, "checkpoint-one")
        .expect("checkpoint transaction");
    store
        .clean_replicated(&lease, 1)
        .expect("clean covered transaction");
    assert!(
        store
            .replay()
            .expect("replay after covered clean")
            .is_empty()
    );
}

#[test]
fn replica_coverage_records_archive_checkpoint_and_fence() {
    let directory = tempfile::tempdir().expect("temporary replica directory");
    let store = SqliteReplicaStore::open(directory.path().join("replica.sqlite"), "agent")
        .expect("open replica");
    let lease = verglas_do_engine::LeaseIdentity::new("held-token", 4);
    store
        .apply_replicated(&lease, 1, Uuid::from_u128(41), b"one")
        .expect("apply one");
    store
        .apply_replicated(&lease, 2, Uuid::from_u128(42), b"two")
        .expect("apply two");
    store
        .mark_coverage(&lease, 2, 2, "checkpoint-two")
        .expect("record coverage");
    let state = store.state().expect("coverage state");
    assert_eq!(state.applied_sequence(), 2);
    assert_eq!(state.archive_sequence(), 2);
    assert_eq!(state.checkpoint_sequence(), 2);
    assert!(matches!(
        store.mark_coverage(
            &verglas_do_engine::LeaseIdentity::new("stale-token", 4),
            2,
            2,
            "checkpoint-two",
        ),
        Err(Error::ReplicaConflict(_))
    ));
}

#[tokio::test]
async fn persistent_engine_replays_rows_after_process_restart() {
    let directory = tempfile::tempdir().expect("temporary replica directory");
    let path = directory.path().join("replica.sqlite");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let table = TableId::new("events");
    {
        let replica = Arc::new(SqliteReplicaStore::open(&path, "agent").expect("open replica"));
        let engine = DoEngine::open_persistent("agent", Arc::new(Authority), replica)
            .expect("open persistent engine");
        engine
            .create_table(table.clone(), schema.clone())
            .await
            .expect("create table");
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![42]))])
                .expect("batch");
        let mut transaction = engine.begin(IsolationLevel::Snapshot).await.expect("begin");
        transaction
            .append(MutationDomain::Relational, table.clone(), batch)
            .expect("append");
        engine.commit(transaction).await.expect("commit");
    }

    let replica = Arc::new(SqliteReplicaStore::open(&path, "agent").expect("reopen replica"));
    let recovered =
        DoEngine::open_persistent("agent", Arc::new(Authority), replica).expect("recover engine");
    assert_eq!(recovered.applied_sequence(), 1);
    let rows = recovered
        .scan(table, SnapshotFence::at(1), Projection::all(), vec![])
        .await
        .expect("scan recovered table")
        .try_collect::<Vec<_>>()
        .await
        .expect("collect recovered rows");
    assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
}
