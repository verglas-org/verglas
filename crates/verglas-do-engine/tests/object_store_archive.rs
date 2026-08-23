//! Object-store archive integration using the provider-neutral in-memory backend.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use futures::TryStreamExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use verglas_do_engine::{
    CommitAuthority, CommitReceipt, DoEngine, DoStorage, IsolationLevel, MutationDomain,
    ObjectStoreTransactionArchive, TableId, TransactionEnvelope,
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

#[tokio::test]
async fn verified_archive_writes_the_canonical_envelope_under_the_do_prefix() {
    let engine = DoEngine::new("agent-7", Arc::new(Authority));
    let table = TableId::new("events");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![9]))])
        .expect("valid batch");
    engine
        .create_table(table.clone(), schema)
        .await
        .expect("create table");
    let mut transaction = engine.begin(IsolationLevel::Snapshot).await.expect("begin");
    transaction
        .append(MutationDomain::Relational, table, batch)
        .expect("append");
    engine.commit(transaction).await.expect("commit");

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let archive = ObjectStoreTransactionArchive::new(store.clone(), Path::from("transactions"));
    let report = engine
        .offload_pending(&archive)
        .await
        .expect("archive transaction");
    assert_eq!(report.through(), 1);

    let objects = store
        .list(Some(&Path::from("transactions/agent-7")))
        .try_collect::<Vec<_>>()
        .await
        .expect("list archive");
    assert_eq!(objects.len(), 1);
    let bytes = store
        .get(&objects[0].location)
        .await
        .expect("get archive")
        .bytes()
        .await
        .expect("read archive");
    assert!(!bytes.is_empty());
}

#[tokio::test]
async fn conflicting_preexisting_transaction_object_fails_without_overwrite() {
    let engine = DoEngine::new("agent-8", Arc::new(Authority));
    let table = TableId::new("events");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    engine
        .create_table(table.clone(), schema.clone())
        .await
        .expect("create table");
    let mut transaction = engine.begin(IsolationLevel::Snapshot).await.expect("begin");
    let transaction_id = transaction.envelope().transaction_id();
    transaction
        .append(
            MutationDomain::Relational,
            table,
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![10]))])
                .expect("batch"),
        )
        .expect("append");
    engine.commit(transaction).await.expect("commit");

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = Path::from(format!(
        "transactions/agent-8/{:020}-{transaction_id}.arrow",
        1
    ));
    store
        .put(&path, bytes::Bytes::from_static(b"conflicting").into())
        .await
        .expect("precreate conflict");
    let archive = ObjectStoreTransactionArchive::new(store.clone(), Path::from("transactions"));
    assert!(engine.offload_pending(&archive).await.is_err());
    assert_eq!(
        store
            .get(&path)
            .await
            .expect("get conflict")
            .bytes()
            .await
            .expect("read conflict")
            .as_ref(),
        b"conflicting"
    );
}
