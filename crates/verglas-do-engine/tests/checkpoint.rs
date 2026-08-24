//! Verified SQLite checkpoints gate independent worker scale-to-zero.

use std::sync::Arc;

use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use uuid::Uuid;
use verglas_do_engine::{
    IsolationLevel, ObjectStoreCheckpointPublisher, SqliteReplicaStore, TransactionEnvelope,
};

#[tokio::test]
async fn checkpoint_is_uploaded_verified_then_advances_local_watermark() {
    let directory = tempfile::tempdir().expect("directory");
    let store = SqliteReplicaStore::open(directory.path().join("replica.sqlite"), "agent-1")
        .expect("replica");
    let transaction_id = Uuid::from_u128(61);
    let envelope = TransactionEnvelope::new("agent-1", transaction_id, 0, IsolationLevel::Snapshot);
    store
        .apply_committed(
            1,
            transaction_id,
            &envelope.canonical_bytes().expect("canonical"),
        )
        .expect("apply");
    store
        .mark_archived(1, "transaction-object")
        .expect("archive");
    let objects = Arc::new(InMemory::new());
    let publisher = ObjectStoreCheckpointPublisher::new(objects.clone(), "tenant-a");

    let receipt = publisher
        .publish(&store, directory.path().join("checkpoint.sqlite"))
        .await
        .expect("publish checkpoint");
    assert_eq!(receipt.through_sequence(), 1);
    assert_eq!(store.state().expect("state").checkpoint_sequence(), 1);
    let bytes = objects
        .get(&Path::from(receipt.object_path()))
        .await
        .expect("checkpoint object")
        .bytes()
        .await
        .expect("checkpoint bytes");
    assert!(bytes.starts_with(b"SQLite format 3\0"));

    let restored = publisher
        .restore(
            "agent-1",
            &receipt,
            directory.path().join("restored.sqlite"),
        )
        .await
        .expect("restore verified checkpoint");
    let restored_state = restored.state().expect("restored state");
    assert_eq!(restored_state.applied_sequence(), 1);
    assert_eq!(restored_state.archive_sequence(), 1);
    assert_eq!(restored.replay().expect("replay").len(), 1);
}
