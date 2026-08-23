//! Live graph adjacency projection over canonical graph mutation batches.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::{Float64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use verglas_do_engine::{
    ArtifactKind, CommitAuthority, CommitReceipt, DoEngine, DoStorage, GraphIndexConfig,
    IsolationLevel, MutationDomain, SqliteReplicaStore, TableId, TransactionEnvelope,
};
use verglas_graph::Direction;

#[derive(Default)]
struct Authority {
    sequence: AtomicU64,
}

#[async_trait]
impl CommitAuthority for Authority {
    async fn commit(
        &self,
        envelope: &TransactionEnvelope,
    ) -> verglas_do_engine::Result<CommitReceipt> {
        Ok(CommitReceipt::new(
            self.sequence.fetch_add(1, Ordering::SeqCst) + 1,
            envelope.transaction_id(),
        ))
    }
}

fn edges() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("edge_id", DataType::Utf8, false),
        Field::new("src_id", DataType::Utf8, false),
        Field::new("predicate", DataType::Utf8, false),
        Field::new("dst_id", DataType::Utf8, false),
        Field::new("confidence", DataType::Float64, false),
        Field::new("provenance", DataType::Utf8, false),
        Field::new("supersedes", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["e1", "e2"])),
            Arc::new(StringArray::from(vec!["alice", "bob"])),
            Arc::new(StringArray::from(vec!["knows", "knows"])),
            Arc::new(StringArray::from(vec!["bob", "carol"])),
            Arc::new(Float64Array::from(vec![0.9, 0.8])),
            Arc::new(StringArray::from(vec!["memory-1", "memory-2"])),
            Arc::new(StringArray::from(vec![None::<&str>, None])),
        ],
    )
    .expect("edge batch")
}

fn revision(schema: Arc<Schema>) -> RecordBatch {
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["e3"])),
            Arc::new(StringArray::from(vec!["alice"])),
            Arc::new(StringArray::from(vec!["avoids"])),
            Arc::new(StringArray::from(vec!["bob"])),
            Arc::new(Float64Array::from(vec![0.7])),
            Arc::new(StringArray::from(vec!["memory-3"])),
            Arc::new(StringArray::from(vec![Some("e1")])),
        ],
    )
    .expect("revision batch")
}

#[tokio::test]
async fn committed_graph_mutations_update_adjacency_at_the_same_sequence() {
    let engine = DoEngine::new("graph", Arc::new(Authority::default()));
    let table = TableId::new("edges");
    engine
        .create_table(table.clone(), edges().schema())
        .await
        .expect("create table");
    engine
        .register_graph_index(GraphIndexConfig::new(table.clone()))
        .await
        .expect("register graph");
    let mut transaction = engine.begin(IsolationLevel::Snapshot).await.expect("begin");
    transaction
        .append(MutationDomain::Graph, table.clone(), edges())
        .expect("append graph mutation");

    engine.commit(transaction).await.expect("commit");
    let neighbors = engine
        .graph_neighbors(&table, "alice", Direction::Out, Some("knows"))
        .expect("adjacency lookup");
    assert_eq!(neighbors.len(), 1);
    assert_eq!(neighbors[0].node_id, "bob");
    assert_eq!(neighbors[0].edge_id, "e1");
    assert_eq!(engine.graph_index_through(&table), Some(1));

    let mut revision_transaction = engine.begin(IsolationLevel::Snapshot).await.expect("begin");
    revision_transaction
        .append(
            MutationDomain::Graph,
            table.clone(),
            revision(edges().schema()),
        )
        .expect("append belief revision");
    engine
        .commit(revision_transaction)
        .await
        .expect("commit revision");
    assert!(
        engine
            .graph_neighbors(&table, "alice", Direction::Out, Some("knows"))
            .expect("old adjacency")
            .is_empty()
    );
    let revised = engine
        .graph_neighbors(&table, "alice", Direction::Out, Some("avoids"))
        .expect("revised adjacency");
    assert_eq!(revised[0].edge_id, "e3");
    assert_eq!(engine.graph_index_through(&table), Some(2));
    let artifact = engine
        .graph_puffin_artifact(&table)
        .await
        .expect("materialize graph Puffin");
    assert_eq!(artifact.kind(), ArtifactKind::GraphPuffin);
    assert_eq!(artifact.coverage().through(), 2);
    let decoded = verglas_graph::puffin::from_puffin_bytes(artifact.bytes())
        .await
        .expect("decode graph Puffin");
    assert_eq!(decoded.snapshot_id(), 2);
}

#[tokio::test]
async fn malformed_graph_batch_never_reaches_commit_authority() {
    let authority = Arc::new(Authority::default());
    let engine = DoEngine::new("invalid-graph", authority.clone());
    let table = TableId::new("edges");
    let malformed_schema = Arc::new(Schema::new(vec![
        Field::new("edge_id", DataType::Utf8, false),
        Field::new("src_id", DataType::Utf8, false),
        Field::new("predicate", DataType::Utf8, false),
        Field::new("dst_id", DataType::Utf8, false),
        Field::new("confidence", DataType::Utf8, false),
        Field::new("provenance", DataType::Utf8, false),
        Field::new("supersedes", DataType::Utf8, true),
    ]));
    let malformed = RecordBatch::try_new(
        malformed_schema,
        vec![
            Arc::new(StringArray::from(vec!["e1"])),
            Arc::new(StringArray::from(vec!["alice"])),
            Arc::new(StringArray::from(vec!["knows"])),
            Arc::new(StringArray::from(vec!["bob"])),
            Arc::new(StringArray::from(vec!["not-a-number"])),
            Arc::new(StringArray::from(vec!["memory-1"])),
            Arc::new(StringArray::from(vec![None::<&str>])),
        ],
    )
    .expect("malformed batch shape");
    engine
        .create_table(table.clone(), malformed.schema())
        .await
        .expect("create table");
    engine
        .register_graph_index(GraphIndexConfig::new(table.clone()))
        .await
        .expect("register graph");
    let mut transaction = engine.begin(IsolationLevel::Snapshot).await.expect("begin");
    transaction
        .append(MutationDomain::Graph, table, malformed)
        .expect("append mutation");

    let error = engine.commit(transaction).await.expect_err("reject schema");
    assert!(error.to_string().contains("confidence must be Float64"));
    assert_eq!(authority.sequence.load(Ordering::SeqCst), 0);
    assert_eq!(engine.applied_sequence(), 0);
}

#[tokio::test]
async fn restart_rebuilds_adjacency_from_the_persisted_graph_log() {
    let directory = tempfile::tempdir().expect("replica directory");
    let replica = Arc::new(
        SqliteReplicaStore::open(directory.path().join("replica.sqlite"), "graph")
            .expect("open pager"),
    );
    let table = TableId::new("edges");
    {
        let engine =
            DoEngine::open_persistent("graph", Arc::new(Authority::default()), replica.clone())
                .expect("open engine");
        engine
            .create_table(table.clone(), edges().schema())
            .await
            .expect("create table");
        engine
            .register_graph_index(GraphIndexConfig::new(table.clone()))
            .await
            .expect("register graph");
        let mut transaction = engine.begin(IsolationLevel::Snapshot).await.expect("begin");
        transaction
            .append(MutationDomain::Graph, table.clone(), edges())
            .expect("append graph mutation");
        engine.commit(transaction).await.expect("commit");
    }

    let recovered = DoEngine::open_persistent("graph", Arc::new(Authority::default()), replica)
        .expect("recover engine");
    recovered
        .register_graph_index(GraphIndexConfig::new(table.clone()))
        .await
        .expect("rebuild graph");
    let neighbors = recovered
        .graph_neighbors(&table, "carol", Direction::In, Some("knows"))
        .expect("reverse adjacency");
    assert_eq!(neighbors[0].node_id, "bob");
    assert_eq!(recovered.graph_index_through(&table), Some(1));
}
