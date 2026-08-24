//! DataFusion UPDATE and DELETE tests for canonical private mutations.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use verglas_do_engine::{
    CommitAuthority, CommitReceipt, DoEngine, DoSession, DoStorage, IsolationLevel, MutationDomain,
    MutationKind, TableId, TransactionEnvelope,
};

#[derive(Default)]
struct SequenceAuthority {
    sequence: AtomicU64,
    envelopes: Mutex<Vec<Vec<u8>>>,
}

#[async_trait]
impl CommitAuthority for SequenceAuthority {
    async fn commit(
        &self,
        envelope: &TransactionEnvelope,
    ) -> verglas_do_engine::Result<CommitReceipt> {
        self.envelopes
            .lock()
            .expect("envelope lock")
            .push(envelope.canonical_bytes()?);
        let sequence = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(CommitReceipt::new(sequence, envelope.transaction_id()))
    }
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]))
}

fn rows(schema: &Arc<Schema>) -> RecordBatch {
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["old", "keep"])),
        ],
    )
    .expect("valid rows")
}

#[tokio::test]
async fn update_and_delete_are_private_until_commit_and_change_visible_rows() {
    let authority = Arc::new(SequenceAuthority::default());
    let engine = Arc::new(DoEngine::new("dml-do", authority.clone()));
    let table = TableId::new("events");
    let table_schema = schema();
    engine
        .create_table(table.clone(), table_schema.clone())
        .await
        .expect("create table");
    let mut seed = engine
        .begin(IsolationLevel::Snapshot)
        .await
        .expect("begin seed");
    seed.append(
        MutationDomain::Relational,
        table.clone(),
        rows(&table_schema),
    )
    .expect("append seed");
    engine.commit(seed).await.expect("commit seed");

    let update = DoSession::begin(engine.clone(), [table.clone()], IsolationLevel::Snapshot)
        .await
        .expect("begin update");
    let result = update
        .execute("UPDATE events SET value = 'new' WHERE id = 1")
        .await
        .expect("execute update");
    assert_eq!(result.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    let private = update
        .execute("SELECT id, value FROM events ORDER BY id")
        .await
        .expect("read update");
    let values = private[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("value array");
    assert_eq!(values.value(0), "new");
    assert_eq!(values.value(1), "keep");
    update.commit().await.expect("commit update");
    let update_envelope = authority
        .envelopes
        .lock()
        .expect("envelope lock")
        .last()
        .cloned()
        .expect("update envelope");
    assert_eq!(
        TransactionEnvelope::from_canonical_bytes(&update_envelope)
            .expect("decode update envelope")
            .mutations()[0]
            .kind(),
        MutationKind::Replace
    );

    let delete = DoSession::begin(engine.clone(), [table], IsolationLevel::Snapshot)
        .await
        .expect("begin delete");
    let result = delete
        .execute("DELETE FROM events WHERE id = 2")
        .await
        .expect("execute delete");
    assert_eq!(result.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    let private = delete
        .execute("SELECT id FROM events")
        .await
        .expect("read delete");
    let ids = private[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id array");
    assert_eq!(ids.values(), &[1]);
    delete.commit().await.expect("commit delete");
    let delete_envelope = authority
        .envelopes
        .lock()
        .expect("envelope lock")
        .last()
        .cloned()
        .expect("delete envelope");
    assert_eq!(
        TransactionEnvelope::from_canonical_bytes(&delete_envelope)
            .expect("decode delete envelope")
            .mutations()[0]
            .kind(),
        MutationKind::Replace
    );
}
