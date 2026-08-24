//! Durability offload compacts transactions at 10 seconds, 16 MiB, or drain.

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::TryStreamExt;
use object_store::ObjectStore;
use object_store::memory::InMemory;
use object_store::path::Path;
use uuid::Uuid;
use verglas_do_engine::{
    IsolationLevel, ObjectStoreOffloadBatchArchive, OffloadBatchArchive, OffloadBatchPolicy,
    OffloadBatcher, SqliteReplicaStore, TransactionEnvelope,
};

fn pending(count: u64) -> Vec<verglas_do_engine::ManagedTransactionArchive> {
    let directory = tempfile::tempdir().expect("directory");
    let store = SqliteReplicaStore::open(directory.path().join("replica.sqlite"), "agent-1")
        .expect("replica");
    for sequence in 1..=count {
        let transaction_id = Uuid::from_u128(u128::from(sequence));
        let envelope = TransactionEnvelope::new(
            "agent-1",
            transaction_id,
            sequence - 1,
            IsolationLevel::Snapshot,
        );
        store
            .apply_committed(
                sequence,
                transaction_id,
                &envelope.canonical_bytes().expect("canonical"),
            )
            .expect("apply");
    }
    store.pending_archive().expect("pending")
}

#[test]
fn production_policy_is_ten_seconds_and_sixteen_mibibytes() {
    let policy = OffloadBatchPolicy::production();
    assert_eq!(policy.max_delay(), Duration::from_secs(10));
    assert_eq!(policy.max_bytes(), 16 * 1024 * 1024);
}

#[test]
fn elapsed_time_flushes_compacted_contiguous_batch() {
    let now = Instant::now();
    let mut batcher = OffloadBatcher::new(OffloadBatchPolicy::production());
    for transaction in pending(2) {
        assert!(batcher.push(transaction, now).expect("push").is_none());
    }
    assert!(batcher.flush_due(now + Duration::from_secs(9)).is_none());
    let batch = batcher
        .flush_due(now + Duration::from_secs(10))
        .expect("time flush");
    assert_eq!(batch.from_sequence(), 1);
    assert_eq!(batch.through_sequence(), 2);
    assert_eq!(batch.transactions().len(), 2);
}

#[test]
fn byte_limit_and_explicit_drain_flush_without_waiting() {
    let now = Instant::now();
    let transactions = pending(2);
    let one_size = transactions[0].canonical_envelope().len();
    let policy =
        OffloadBatchPolicy::new(Duration::from_secs(10), one_size + 1).expect("test policy");
    let mut batcher = OffloadBatcher::new(policy);
    assert!(
        batcher
            .push(transactions[0].clone(), now)
            .expect("first push")
            .is_none()
    );
    let full = batcher
        .push(transactions[1].clone(), now)
        .expect("second push")
        .expect("byte flush");
    assert_eq!(full.transactions().len(), 2);

    let mut drain = OffloadBatcher::new(OffloadBatchPolicy::production());
    assert!(
        drain
            .push(transactions[0].clone(), now)
            .expect("drain push")
            .is_none()
    );
    assert_eq!(drain.drain().expect("drain").transactions().len(), 1);
    assert!(drain.drain().is_none());
}

#[tokio::test]
async fn one_create_only_object_contains_the_whole_compacted_range() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let archive = ObjectStoreOffloadBatchArchive::new(store.clone(), Path::from("archive"));
    let mut batcher = OffloadBatcher::new(OffloadBatchPolicy::production());
    for transaction in pending(2) {
        assert!(
            batcher
                .push(transaction, Instant::now())
                .expect("push")
                .is_none()
        );
    }
    let batch = batcher.drain().expect("batch");
    let receipt = archive.archive(&batch).await.expect("archive batch");
    assert_eq!(receipt.from_sequence(), 1);
    assert_eq!(receipt.through_sequence(), 2);
    assert_eq!(receipt.transactions(), 2);
    assert!(!receipt.etag().is_empty());
    archive.archive(&batch).await.expect("exact retry");

    let objects = store
        .list(Some(&Path::from("archive")))
        .try_collect::<Vec<_>>()
        .await
        .expect("list");
    assert_eq!(objects.len(), 1);
    assert!(
        objects[0]
            .location
            .as_ref()
            .ends_with("00000000000000000001-00000000000000000002.batch")
    );
}
