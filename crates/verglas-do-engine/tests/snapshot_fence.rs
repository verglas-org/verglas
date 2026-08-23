//! Snapshot fence tests for fixed DataFusion visibility and read-your-writes.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use futures::TryStreamExt;
use verglas_do_engine::{
    CommitAuthority, CommitReceipt, DoEngine, DoSession, DoStorage, IsolationLevel, MutationDomain,
    Projection, SnapshotFence, TableId, TransactionEnvelope,
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

fn row(schema: &Arc<Schema>, id: i64) -> RecordBatch {
    RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![id]))])
        .expect("valid row")
}

#[tokio::test]
async fn a_fenced_query_excludes_later_commits_but_merges_private_writes() {
    let engine = Arc::new(DoEngine::new(
        "fence-do",
        Arc::new(SequenceAuthority::default()),
    ));
    let table = TableId::new("events");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    engine
        .create_table(table.clone(), schema.clone())
        .await
        .expect("create table");

    let mut first = engine
        .begin(IsolationLevel::Snapshot)
        .await
        .expect("begin first");
    first
        .append(MutationDomain::Relational, table.clone(), row(&schema, 1))
        .expect("append first");
    engine.commit(first).await.expect("commit first");

    let mut later = engine
        .begin(IsolationLevel::Snapshot)
        .await
        .expect("begin later");
    later
        .append(MutationDomain::Relational, table.clone(), row(&schema, 2))
        .expect("append later");
    engine.commit(later).await.expect("commit later");

    let session = DoSession::begin_at(
        engine.clone(),
        [table.clone()],
        IsolationLevel::Snapshot,
        SnapshotFence::at(1),
    )
    .await
    .expect("begin fenced session");

    let fenced = engine
        .scan(
            table.clone(),
            SnapshotFence::at(1),
            Projection::all(),
            vec![],
        )
        .await
        .expect("fenced scan")
        .try_collect::<Vec<_>>()
        .await
        .expect("collect fenced scan");
    assert_eq!(fenced.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);

    session
        .execute("INSERT INTO events VALUES (3)")
        .await
        .expect("append private row");
    let rows = session
        .execute("SELECT id FROM events ORDER BY id")
        .await
        .expect("read fenced private rows");
    let ids = rows[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id array");
    assert_eq!(ids.values(), &[1, 3]);
}
