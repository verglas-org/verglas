//! Acceptance tests for one atomic relational, vector, and graph commit.

use std::sync::{Arc, Mutex};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use futures::TryStreamExt;
use verglas_do_engine::{
    CommitAuthority, CommitReceipt, DoEngine, DoStorage, IsolationLevel, MutationDomain,
    Projection, SnapshotFence, TableId, TransactionEnvelope,
};

#[derive(Default)]
struct RecordingAuthority {
    envelopes: Mutex<Vec<Vec<u8>>>,
}

#[async_trait]
impl CommitAuthority for RecordingAuthority {
    async fn commit(
        &self,
        envelope: &TransactionEnvelope,
    ) -> verglas_do_engine::Result<CommitReceipt> {
        self.envelopes
            .lock()
            .expect("authority lock")
            .push(envelope.canonical_bytes()?);
        Ok(CommitReceipt::new(1, envelope.transaction_id()))
    }
}

fn batch(id: i64, value: &str) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![id])),
            Arc::new(StringArray::from(vec![value])),
        ],
    )
    .expect("valid batch")
}

async fn one_authority_commit_atomically_applies_all_domains_body() -> verglas_do_engine::Result<()>
{
    let authority = Arc::new(RecordingAuthority::default());
    let engine = DoEngine::new("memory-do", authority.clone());
    let table = TableId::new("documents");
    engine
        .create_table(table.clone(), batch(0, "schema").schema())
        .await?;

    let mut transaction = engine.begin(IsolationLevel::Snapshot).await?;
    transaction.append(
        MutationDomain::Relational,
        table.clone(),
        batch(1, "document"),
    )?;
    transaction.append(MutationDomain::Vector, table.clone(), batch(1, "embedding"))?;
    transaction.append(MutationDomain::Graph, table.clone(), batch(1, "edge"))?;

    let receipt = engine.commit(transaction).await?;
    assert_eq!(receipt.commit_sequence(), 1);
    assert_eq!(authority.envelopes.lock().expect("authority lock").len(), 1);

    let rows = engine
        .scan(
            table,
            SnapshotFence::at(receipt.commit_sequence()),
            Projection::all(),
            vec![],
        )
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    assert_eq!(engine.domain_watermark(MutationDomain::Relational), 1);
    assert_eq!(engine.domain_watermark(MutationDomain::Vector), 1);
    assert_eq!(engine.domain_watermark(MutationDomain::Graph), 1);

    Ok(())
}

#[tokio::test]
async fn one_authority_commit_atomically_applies_all_domains() {
    one_authority_commit_atomically_applies_all_domains_body()
        .await
        .expect("unified transaction commits");
}

async fn canonical_envelope_is_stable_for_an_exact_retry_body() -> verglas_do_engine::Result<()> {
    let authority = Arc::new(RecordingAuthority::default());
    let engine = DoEngine::new("memory-do", authority);
    let table = TableId::new("events");
    engine
        .create_table(table.clone(), batch(0, "schema").schema())
        .await?;

    let transaction_id = uuid::Uuid::from_u128(7);
    let mut first = engine
        .begin_with_id(IsolationLevel::Snapshot, transaction_id)
        .await?;
    first.append(MutationDomain::Relational, table.clone(), batch(7, "same"))?;
    let mut second = engine
        .begin_with_id(IsolationLevel::Snapshot, transaction_id)
        .await?;
    second.append(MutationDomain::Relational, table, batch(7, "same"))?;

    assert_eq!(
        first.envelope().canonical_bytes()?,
        second.envelope().canonical_bytes()?
    );
    Ok(())
}

#[tokio::test]
async fn canonical_envelope_is_stable_for_an_exact_retry() {
    canonical_envelope_is_stable_for_an_exact_retry_body()
        .await
        .expect("canonical envelopes match");
}

#[tokio::test]
async fn canonical_envelope_round_trips_for_replica_replay() {
    let authority = Arc::new(RecordingAuthority::default());
    let engine = DoEngine::new("memory-do", authority);
    let table = TableId::new("events");
    engine
        .create_table(table.clone(), batch(0, "schema").schema())
        .await
        .expect("create table");
    let mut transaction = engine
        .begin_with_id(IsolationLevel::Serializable, uuid::Uuid::from_u128(88))
        .await
        .expect("begin");
    transaction
        .append(MutationDomain::Relational, table, batch(8, "replay"))
        .expect("append");
    let canonical = transaction
        .envelope()
        .canonical_bytes()
        .expect("encode envelope");

    let decoded = TransactionEnvelope::from_canonical_bytes(&canonical).expect("decode envelope");
    assert_eq!(decoded.do_id(), "memory-do");
    assert_eq!(decoded.transaction_id(), uuid::Uuid::from_u128(88));
    assert_eq!(decoded.isolation(), IsolationLevel::Serializable);
    assert_eq!(decoded.mutations().len(), 1);
    assert_eq!(decoded.mutations()[0].batch().num_rows(), 1);
    assert_eq!(decoded.canonical_bytes().expect("re-encode"), canonical);
}
