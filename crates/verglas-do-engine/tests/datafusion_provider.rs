//! DataFusion provider tests over an explicit Durable Object snapshot.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use verglas_do_engine::{
    CommitAuthority, CommitReceipt, DoEngine, DoStorage, DoTableProvider, IsolationLevel,
    MutationDomain, SnapshotFence, TableId, TransactionEnvelope,
};

struct SequenceAuthority;

#[async_trait]
impl CommitAuthority for SequenceAuthority {
    async fn commit(
        &self,
        envelope: &TransactionEnvelope,
    ) -> verglas_do_engine::Result<CommitReceipt> {
        Ok(CommitReceipt::new(1, envelope.transaction_id()))
    }
}

fn batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![2, 1])),
            Arc::new(StringArray::from(vec!["second", "first"])),
        ],
    )
    .expect("valid batch")
}

#[tokio::test]
async fn sql_reads_the_explicit_do_snapshot() {
    let engine = Arc::new(DoEngine::new("do", Arc::new(SequenceAuthority)));
    let table = TableId::new("events");
    engine
        .create_table(table.clone(), batch().schema())
        .await
        .expect("create table");
    let mut transaction = engine.begin(IsolationLevel::Snapshot).await.expect("begin");
    transaction
        .append(MutationDomain::Relational, table.clone(), batch())
        .expect("append");
    engine.commit(transaction).await.expect("commit");

    let provider =
        DoTableProvider::open(engine, table, SnapshotFence::at(1)).expect("open provider");
    let context = SessionContext::new();
    context
        .register_table("events", Arc::new(provider))
        .expect("register provider");
    let result = context
        .sql("SELECT id, name FROM events WHERE id = 1")
        .await
        .expect("plan query")
        .collect()
        .await
        .expect("execute query");

    assert_eq!(result.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    let ids = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id array");
    assert_eq!(ids.value(0), 1);
}
