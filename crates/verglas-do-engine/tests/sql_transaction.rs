//! SQL BEGIN/INSERT/read-your-writes/COMMIT acceptance test.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use verglas_do_engine::{
    CommitAuthority, CommitReceipt, DoEngine, DoSession, IsolationLevel, TableId,
    TransactionEnvelope,
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

#[tokio::test]
async fn insert_is_private_until_one_commit_and_visible_inside_transaction() {
    let engine = Arc::new(DoEngine::new("do", Arc::new(SequenceAuthority::default())));
    let table = TableId::new("events");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    engine
        .create_table(table.clone(), schema)
        .await
        .expect("create table");

    let session = DoSession::begin(engine.clone(), [table], IsolationLevel::Snapshot)
        .await
        .expect("begin SQL transaction");
    let count = session
        .execute("INSERT INTO events VALUES (1, 'first')")
        .await
        .expect("execute INSERT");
    assert_eq!(count.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);

    let private = session
        .execute("SELECT id FROM events")
        .await
        .expect("read private write");
    let ids = private[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id array");
    assert_eq!(ids.values(), &[1]);
    assert_eq!(engine.applied_sequence(), 0);

    let receipt = session.commit().await.expect("commit transaction");
    assert_eq!(receipt.commit_sequence(), 1);
    assert_eq!(engine.applied_sequence(), 1);
}
