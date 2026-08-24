//! Serializable transaction conflict tests for the Durable Object commit fence.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use verglas_do_engine::{
    CommitAuthority, CommitReceipt, DoEngine, DoStorage, Error, IsolationLevel, MutationDomain,
    TableId, TransactionEnvelope,
};

#[derive(Default)]
struct SequenceAuthority {
    sequence: AtomicU64,
}

#[async_trait]
impl CommitAuthority for SequenceAuthority {
    async fn commit(
        &self,
        envelope: &TransactionEnvelope,
    ) -> verglas_do_engine::Result<CommitReceipt> {
        let sequence = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(CommitReceipt::new(sequence, envelope.transaction_id()))
    }
}

fn row(id: i64, schema: &Arc<Schema>) -> RecordBatch {
    RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![id]))])
        .expect("valid row")
}

#[tokio::test]
async fn serializable_commit_rejects_a_stale_write_snapshot() {
    let authority = Arc::new(SequenceAuthority::default());
    let engine = DoEngine::new("occ-do", authority.clone());
    let table = TableId::new("events");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    engine
        .create_table(table.clone(), schema.clone())
        .await
        .expect("create table");

    let mut first = engine
        .begin_with_id(IsolationLevel::Serializable, uuid::Uuid::from_u128(1))
        .await
        .expect("begin first");
    let mut second = engine
        .begin_with_id(IsolationLevel::Serializable, uuid::Uuid::from_u128(2))
        .await
        .expect("begin second");
    first
        .append(MutationDomain::Relational, table.clone(), row(1, &schema))
        .expect("append first");
    second
        .append(MutationDomain::Relational, table, row(2, &schema))
        .expect("append second");

    engine.commit(first).await.expect("first commit");
    let error = engine
        .commit(second)
        .await
        .expect_err("stale serializable write must conflict");
    assert!(matches!(error, Error::TransactionConflict { .. }));
    assert_eq!(
        authority.sequence.load(Ordering::SeqCst),
        1,
        "conflicting commit must not reach authority"
    );
}
