//! Live Vamana projection and exact-tail vector semantics.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow_array::types::Float32Type;
use arrow_array::{Array, FixedSizeListArray, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use verglas_do_engine::{
    ArtifactKind, CommitAuthority, CommitReceipt, DoEngine, DoStorage, IsolationLevel,
    MutationDomain, SqliteReplicaStore, TableId, TransactionEnvelope, VectorIndexConfig,
};
use verglas_vector::Metric;

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

struct RecordingFailureAuthority(Arc<AtomicBool>);

#[async_trait]
impl CommitAuthority for RecordingFailureAuthority {
    async fn commit(
        &self,
        _envelope: &TransactionEnvelope,
    ) -> verglas_do_engine::Result<CommitReceipt> {
        self.0.store(true, Ordering::SeqCst);
        Err(verglas_do_engine::Error::Authority(
            "must not be called".to_owned(),
        ))
    }
}

fn vectors() -> RecordBatch {
    let embeddings = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
        vec![
            Some(vec![Some(1.0), Some(0.0)]),
            Some(vec![Some(0.0), Some(1.0)]),
        ],
        2,
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("embedding", embeddings.data_type().clone(), false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![1, 2])), Arc::new(embeddings)],
    )
    .expect("vector batch")
}

#[tokio::test]
async fn committed_vector_mutations_update_vamana_at_the_same_sequence() {
    let engine = DoEngine::new("vectors", Arc::new(Authority));
    let table = TableId::new("memories");
    engine
        .create_table(table.clone(), vectors().schema())
        .await
        .expect("create table");
    engine
        .register_vector_index(VectorIndexConfig::new(
            table.clone(),
            "id",
            "embedding",
            2,
            Metric::Cosine,
        ))
        .await
        .expect("register index");
    let mut transaction = engine.begin(IsolationLevel::Snapshot).await.expect("begin");
    transaction
        .append(MutationDomain::Vector, table.clone(), vectors())
        .expect("append vector mutation");

    engine.commit(transaction).await.expect("commit");
    let result = engine
        .vector_search(&table, &[1.0, 0.0], 1)
        .expect("search vector projection");
    assert_eq!(result[0].id, 1);
    assert_eq!(engine.vector_index_through(&table), Some(1));
    let artifact = engine
        .vector_puffin_artifact(&table)
        .await
        .expect("materialize Vamana Puffin");
    assert_eq!(artifact.kind(), ArtifactKind::VamanaPuffin);
    assert_eq!(artifact.coverage().through(), 1);
    let decoded = verglas_vector::puffin::from_puffin_bytes(artifact.bytes())
        .await
        .expect("decode Vamana Puffin");
    assert_eq!(decoded.reflected_snapshot(), 1);
}

#[tokio::test]
async fn invalid_vector_dimension_never_reaches_commit_authority() {
    let called = Arc::new(AtomicBool::new(false));
    let engine = DoEngine::new(
        "invalid-vectors",
        Arc::new(RecordingFailureAuthority(called.clone())),
    );
    let table = TableId::new("memories");
    let embeddings = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
        vec![Some(vec![Some(1.0), Some(0.0), Some(0.5)])],
        3,
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("embedding", embeddings.data_type().clone(), false),
    ]));
    let malformed = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![1])), Arc::new(embeddings)],
    )
    .expect("three-dimensional vector");
    engine
        .create_table(table.clone(), malformed.schema())
        .await
        .expect("create table");
    engine
        .register_vector_index(VectorIndexConfig::new(
            table.clone(),
            "id",
            "embedding",
            2,
            Metric::Cosine,
        ))
        .await
        .expect("register index");
    let mut transaction = engine.begin(IsolationLevel::Snapshot).await.expect("begin");
    transaction
        .append(MutationDomain::Vector, table, malformed)
        .expect("append mutation");

    let error = engine
        .commit(transaction)
        .await
        .expect_err("reject dimension");
    assert!(error.to_string().contains("dimension mismatch"));
    assert!(!called.load(Ordering::SeqCst));
    assert_eq!(engine.applied_sequence(), 0);
}

#[tokio::test]
async fn restart_rebuilds_vamana_from_the_persisted_canonical_log() {
    let directory = tempfile::tempdir().expect("replica directory");
    let replica = Arc::new(
        SqliteReplicaStore::open(directory.path().join("replica.sqlite"), "vectors")
            .expect("open pager"),
    );
    let table = TableId::new("memories");
    {
        let engine = DoEngine::open_persistent("vectors", Arc::new(Authority), replica.clone())
            .expect("open engine");
        engine
            .create_table(table.clone(), vectors().schema())
            .await
            .expect("create table");
        engine
            .register_vector_index(VectorIndexConfig::new(
                table.clone(),
                "id",
                "embedding",
                2,
                Metric::Cosine,
            ))
            .await
            .expect("register index");
        let mut transaction = engine.begin(IsolationLevel::Snapshot).await.expect("begin");
        transaction
            .append(MutationDomain::Vector, table.clone(), vectors())
            .expect("append vector mutation");
        engine.commit(transaction).await.expect("commit");
    }

    let recovered =
        DoEngine::open_persistent("vectors", Arc::new(Authority), replica).expect("recover engine");
    recovered
        .register_vector_index(VectorIndexConfig::new(
            table.clone(),
            "id",
            "embedding",
            2,
            Metric::Cosine,
        ))
        .await
        .expect("rebuild index");
    assert_eq!(recovered.vector_index_through(&table), Some(1));
    assert_eq!(
        recovered
            .vector_search(&table, &[0.0, 1.0], 1)
            .expect("search rebuilt projection")[0]
            .id,
        2
    );
}
