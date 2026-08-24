//! MVCC safety tests for preflight, authority failure, and snapshot scans.

use std::sync::{Arc, Mutex};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use futures::TryStreamExt;
use verglas_do_engine::{
    CommitAuthority, CommitReceipt, DoEngine, DoStorage, Error, IsolationLevel, MutationDomain,
    Projection, SnapshotFence, TableId, TransactionEnvelope,
};

#[derive(Default)]
struct CountingAuthority {
    calls: Mutex<u64>,
}

#[async_trait]
impl CommitAuthority for CountingAuthority {
    async fn commit(
        &self,
        envelope: &TransactionEnvelope,
    ) -> verglas_do_engine::Result<CommitReceipt> {
        let mut calls = self.calls.lock().expect("authority lock");
        *calls += 1;
        Ok(CommitReceipt::new(*calls, envelope.transaction_id()))
    }
}

struct RejectingAuthority;

#[async_trait]
impl CommitAuthority for RejectingAuthority {
    async fn commit(
        &self,
        _envelope: &TransactionEnvelope,
    ) -> verglas_do_engine::Result<CommitReceipt> {
        Err(Error::Authority("quorum unavailable".to_owned()))
    }
}

fn batch(value: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![value]))])
        .expect("valid batch")
}

#[tokio::test]
async fn invalid_transaction_never_reaches_commit_authority() {
    let authority = Arc::new(CountingAuthority::default());
    let engine = DoEngine::new("do", authority.clone());
    let mut transaction = engine.begin(IsolationLevel::Snapshot).await.expect("begin");
    transaction
        .append(
            MutationDomain::Relational,
            TableId::new("missing"),
            batch(1),
        )
        .expect("private append");

    let error = engine.commit(transaction).await.expect_err("unknown table");
    assert!(matches!(error, Error::UnknownTable(_)));
    assert_eq!(*authority.calls.lock().expect("authority lock"), 0);
}

#[tokio::test]
async fn failed_authority_ack_never_changes_visibility() {
    let engine = DoEngine::new("do", Arc::new(RejectingAuthority));
    let table = TableId::new("events");
    engine
        .create_table(table.clone(), batch(0).schema())
        .await
        .expect("create table");
    let mut transaction = engine.begin(IsolationLevel::Snapshot).await.expect("begin");
    transaction
        .append(MutationDomain::Relational, table.clone(), batch(1))
        .expect("private append");

    assert!(matches!(
        engine.commit(transaction).await,
        Err(Error::Authority(_))
    ));
    let rows = engine
        .scan(table, SnapshotFence::at(0), Projection::all(), vec![])
        .await
        .expect("scan remains available")
        .try_collect::<Vec<_>>()
        .await
        .expect("empty stream");
    assert!(rows.is_empty());
    assert_eq!(engine.domain_watermark(MutationDomain::Relational), 0);
}

#[tokio::test]
async fn invalid_projection_returns_an_error_instead_of_panicking() {
    let authority = Arc::new(CountingAuthority::default());
    let engine = DoEngine::new("do", authority);
    let table = TableId::new("events");
    engine
        .create_table(table.clone(), batch(0).schema())
        .await
        .expect("create table");

    let result = engine
        .scan(
            table,
            SnapshotFence::at(0),
            Projection::Columns(vec![99]),
            vec![],
        )
        .await;
    assert!(matches!(result, Err(Error::InvalidProjection { .. })));
}
