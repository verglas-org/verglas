//! Asynchronous managed transaction archive tests.

use std::sync::{Arc, Mutex};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use object_store::memory::InMemory;
use object_store::path::Path;
use verglas_do_engine::{
    ArchiveReceipt, CommitAuthority, CommitReceipt, DoEngine, DoStorage, IsolationLevel,
    ManagedTransactionArchive, MutationDomain, ObjectStoreOffloadBatchArchive, OffloadBatchPolicy,
    TableId, TransactionArchive, TransactionEnvelope,
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

#[derive(Default)]
struct RecordingArchive {
    attempts: Mutex<Vec<u64>>,
    fail_once: Mutex<bool>,
}

#[async_trait]
impl TransactionArchive for RecordingArchive {
    async fn archive(
        &self,
        transaction: &ManagedTransactionArchive,
    ) -> verglas_do_engine::Result<ArchiveReceipt> {
        self.attempts
            .lock()
            .expect("attempt lock")
            .push(transaction.commit_sequence());
        let mut fail_once = self.fail_once.lock().expect("failure lock");
        if *fail_once {
            *fail_once = false;
            return Err(verglas_do_engine::Error::Archive(
                "temporary R2 failure".to_owned(),
            ));
        }
        Ok(ArchiveReceipt::new(
            transaction.commit_sequence(),
            "verified-etag",
        ))
    }
}

fn batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).expect("valid batch")
}

async fn committed_engine() -> Arc<DoEngine> {
    let engine = Arc::new(DoEngine::new("do", Arc::new(Authority)));
    let table = TableId::new("events");
    engine
        .create_table(table.clone(), batch().schema())
        .await
        .expect("create table");
    let mut transaction = engine.begin(IsolationLevel::Snapshot).await.expect("begin");
    transaction
        .append(MutationDomain::Relational, table, batch())
        .expect("append");
    engine.commit(transaction).await.expect("commit");
    engine
}

#[tokio::test]
async fn committed_transaction_waits_for_the_async_archive_consumer() {
    let engine = committed_engine().await;
    let archive = RecordingArchive::default();

    assert_eq!(engine.archive_watermark(), 0);
    assert!(archive.attempts.lock().expect("attempt lock").is_empty());

    let report = engine
        .offload_pending(&archive)
        .await
        .expect("offload committed range");
    assert_eq!(report.archived_transactions(), 1);
    assert_eq!(report.through(), 1);
    assert_eq!(engine.archive_watermark(), 1);
    assert_eq!(*archive.attempts.lock().expect("attempt lock"), vec![1]);
}

#[tokio::test]
async fn failed_archive_keeps_the_commit_pending_for_retry() {
    let engine = committed_engine().await;
    let archive = RecordingArchive {
        attempts: Mutex::new(Vec::new()),
        fail_once: Mutex::new(true),
    };

    assert!(engine.offload_pending(&archive).await.is_err());
    assert_eq!(engine.archive_watermark(), 0);

    let report = engine
        .offload_pending(&archive)
        .await
        .expect("retry succeeds");
    assert_eq!(report.through(), 1);
    assert_eq!(*archive.attempts.lock().expect("attempt lock"), vec![1, 1]);
}

#[tokio::test]
async fn explicit_drain_publishes_one_compacted_range_and_advances_watermark() {
    let engine = committed_engine().await;
    let archive = ObjectStoreOffloadBatchArchive::new(
        Arc::new(InMemory::new()),
        Path::from("managed-archive"),
    );

    let report = engine
        .drain_offload(&archive, OffloadBatchPolicy::production())
        .await
        .expect("drain compacted archive");
    assert_eq!(report.archived_transactions(), 1);
    assert_eq!(report.through(), 1);
    assert_eq!(engine.archive_watermark(), 1);
}
