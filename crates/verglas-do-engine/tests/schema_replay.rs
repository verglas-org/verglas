//! Durable empty-table declaration replay tests.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use futures::TryStreamExt;
use verglas_do_engine::{
    CommitAuthority, CommitReceipt, DoEngine, DoStorage, IsolationLevel, Projection, SnapshotFence,
    SqliteReplicaStore, TableId, TransactionEnvelope,
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
async fn empty_table_schema_is_recovered_from_the_transaction_log() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("replica.sqlite");
    let table = TableId::new("empty_events");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    {
        let replica = Arc::new(SqliteReplicaStore::open(&path, "schema-do").expect("open replica"));
        let engine = DoEngine::open_persistent("schema-do", Arc::new(Authority), replica)
            .expect("open engine");
        engine
            .create_table(table.clone(), schema.clone())
            .await
            .expect("create table");
        let transaction = engine
            .begin(IsolationLevel::Serializable)
            .await
            .expect("begin declaration transaction");
        engine
            .commit(transaction)
            .await
            .expect("commit declaration");
    }

    let replica = Arc::new(SqliteReplicaStore::open(&path, "schema-do").expect("reopen replica"));
    let recovered = DoEngine::open_persistent("schema-do", Arc::new(Authority), replica)
        .expect("recover engine");
    assert_eq!(
        recovered.table_schema(&table).expect("recovered schema"),
        schema
    );
    let rows = recovered
        .scan(table, SnapshotFence::at(1), Projection::all(), vec![])
        .await
        .expect("scan recovered empty table")
        .try_collect::<Vec<_>>()
        .await
        .expect("collect recovered empty table");
    assert!(rows.is_empty());
}
