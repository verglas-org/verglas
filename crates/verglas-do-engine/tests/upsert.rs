//! Canonical upsert mutation tests for the relational mutation domain.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use futures::TryStreamExt;
use verglas_do_engine::{
    CommitAuthority, CommitReceipt, DoEngine, DoStorage, IsolationLevel, MutationDomain,
    MutationKind, Projection, SnapshotFence, TableId, TransactionEnvelope,
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

fn batch(schema: &Arc<Schema>, ids: Vec<i64>, values: Vec<&str>) -> RecordBatch {
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(values)),
        ],
    )
    .expect("valid batch")
}

#[tokio::test]
async fn upsert_is_a_canonical_mutation_and_replaces_first_column_keys() {
    let engine = DoEngine::new("upsert-do", Arc::new(SequenceAuthority::default()));
    let table = TableId::new("events");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    engine
        .create_table(table.clone(), schema.clone())
        .await
        .expect("create table");

    let mut seed = engine
        .begin(IsolationLevel::Snapshot)
        .await
        .expect("begin seed");
    seed.append(
        MutationDomain::Relational,
        table.clone(),
        batch(&schema, vec![1, 2], vec!["old", "keep"]),
    )
    .expect("append seed");
    engine.commit(seed).await.expect("commit seed");

    let mut transaction = engine
        .begin(IsolationLevel::Snapshot)
        .await
        .expect("begin upsert");
    transaction
        .append_with_kind(
            MutationKind::Upsert,
            MutationDomain::Relational,
            table.clone(),
            batch(&schema, vec![1, 3], vec!["new", "added"]),
        )
        .expect("append upsert");
    let canonical = transaction
        .envelope()
        .canonical_bytes()
        .expect("canonical bytes");
    assert_eq!(
        TransactionEnvelope::from_canonical_bytes(&canonical)
            .expect("decode canonical")
            .mutations()[0]
            .kind(),
        MutationKind::Upsert
    );
    engine.commit(transaction).await.expect("commit upsert");

    let rows = engine
        .scan(table, SnapshotFence::at(2), Projection::all(), vec![])
        .await
        .expect("scan upsert")
        .try_collect::<Vec<_>>()
        .await
        .expect("collect upsert");
    assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    let values = rows[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("value column");
    assert_eq!(values.value(0), "new");
}
