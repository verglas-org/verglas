//! Real Iceberg snapshot commits consume the shared offload batch.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use futures::TryStreamExt;
use iceberg::arrow::arrow_schema_to_schema_auto_assign_ids;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{FormatVersion, ManifestListWriter, Transform, UnboundPartitionSpec};
use iceberg::table::Table;
use iceberg::{
    Catalog, CatalogBuilder, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent,
};
use object_store::memory::InMemory;
use object_store::path::Path;
use uuid::Uuid;
use verglas_do_engine::{
    CommitAuthority, CommitReceipt, DoEngine, DoStorage, IcebergCommitter, IsolationLevel,
    LakehouseObject, MutationDomain, MutationKind, ObjectStoreDerivedArtifactPublisher,
    ObjectStoreOffloadBatchArchive, OffloadBatch, OffloadBatchPolicy, OffloadBatcher,
    PublicationAuthorization, SqliteReplicaStore, StorageBinding, TableId, TransactionEnvelope,
    VerifiedIcebergArchive,
};

struct TestAuthority {
    next: AtomicU64,
}

#[async_trait]
impl CommitAuthority for TestAuthority {
    async fn commit(
        &self,
        envelope: &verglas_do_engine::TransactionEnvelope,
    ) -> verglas_do_engine::Result<CommitReceipt> {
        let sequence = self.next.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(CommitReceipt::new(sequence, envelope.transaction_id()))
    }
}

#[derive(Debug)]
struct BarrierCatalog {
    inner: Arc<dyn Catalog>,
    barrier: Arc<tokio::sync::Barrier>,
    updates: AtomicUsize,
    corrupt_manifest_after_update: AtomicBool,
}

impl BarrierCatalog {
    fn new(inner: Arc<dyn Catalog>, parties: usize) -> Self {
        Self {
            inner,
            barrier: Arc::new(tokio::sync::Barrier::new(parties)),
            updates: AtomicUsize::new(0),
            corrupt_manifest_after_update: AtomicBool::new(false),
        }
    }

    fn corrupt_manifest_after_update(inner: Arc<dyn Catalog>) -> Self {
        Self {
            inner,
            barrier: Arc::new(tokio::sync::Barrier::new(1)),
            updates: AtomicUsize::new(2),
            corrupt_manifest_after_update: AtomicBool::new(true),
        }
    }

    async fn remove_first_manifest(&self, table: &Table) -> iceberg::Result<Table> {
        let Some(snapshot) = table.metadata().current_snapshot() else {
            return Ok(table.clone());
        };
        let entries = table
            .manifest_list_reader(snapshot)
            .load()
            .await?
            .entries()
            .to_vec();
        if entries.len() < 2 {
            return Ok(table.clone());
        }
        let output = table
            .file_io()
            .new_output(snapshot.manifest_list())?
            .writer()
            .await?;
        let mut writer = ManifestListWriter::v2(
            output,
            snapshot.snapshot_id(),
            snapshot.parent_snapshot_id(),
            snapshot.sequence_number(),
        );
        writer.add_manifests(entries.into_iter().skip(1))?;
        writer.close().await?;
        self.inner.load_table(table.identifier()).await
    }
}

#[async_trait]
impl Catalog for BarrierCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(parent).await
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<Namespace> {
        self.inner.create_namespace(namespace, properties).await
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<Namespace> {
        self.inner.get_namespace(namespace).await
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> iceberg::Result<bool> {
        self.inner.namespace_exists(namespace).await
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        self.inner.update_namespace(namespace, properties).await
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<()> {
        self.inner.drop_namespace(namespace).await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        self.inner.list_tables(namespace).await
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> iceberg::Result<Table> {
        self.inner.create_table(namespace, creation).await
    }

    async fn load_table(&self, table: &TableIdent) -> iceberg::Result<Table> {
        self.inner.load_table(table).await
    }

    async fn drop_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        self.inner.drop_table(table).await
    }

    async fn purge_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        self.inner.purge_table(table).await
    }

    async fn table_exists(&self, table: &TableIdent) -> iceberg::Result<bool> {
        self.inner.table_exists(table).await
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> iceberg::Result<()> {
        self.inner.rename_table(src, dest).await
    }

    async fn register_table(
        &self,
        table: &TableIdent,
        metadata_location: String,
    ) -> iceberg::Result<Table> {
        self.inner.register_table(table, metadata_location).await
    }

    async fn update_table(&self, commit: TableCommit) -> iceberg::Result<Table> {
        if self.updates.fetch_add(1, Ordering::SeqCst) < 2 {
            self.barrier.wait().await;
        }
        let updated = self.inner.update_table(commit).await?;
        if self
            .corrupt_manifest_after_update
            .swap(false, Ordering::SeqCst)
        {
            self.remove_first_manifest(&updated).await
        } else {
            Ok(updated)
        }
    }
}

async fn catalog_with_table() -> (Arc<dyn Catalog>, TableIdent) {
    catalog_with_table_format(FormatVersion::V2).await
}

async fn catalog_with_table_format(
    format_version: FormatVersion,
) -> (Arc<dyn Catalog>, TableIdent) {
    let catalog = Arc::new(
        MemoryCatalogBuilder::default()
            .load(
                "test",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_owned(),
                    "memory://warehouse".to_owned(),
                )]),
            )
            .await
            .expect("memory catalog"),
    );
    let namespace = NamespaceIdent::new("managed".to_owned());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("namespace");
    let arrow_schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
    let schema = arrow_schema_to_schema_auto_assign_ids(&arrow_schema).expect("iceberg schema");
    let table = TableIdent::new(namespace, "events".to_owned());
    catalog
        .create_table(
            table.namespace(),
            TableCreation::builder()
                .name(table.name().to_owned())
                .location("memory://warehouse/managed/events".to_owned())
                .schema(schema)
                .format_version(format_version)
                .build(),
        )
        .await
        .expect("table");
    (catalog, table)
}

async fn catalog_with_partitioned_table() -> (Arc<dyn Catalog>, TableIdent) {
    let catalog = Arc::new(
        MemoryCatalogBuilder::default()
            .load(
                "test",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_owned(),
                    "memory://warehouse".to_owned(),
                )]),
            )
            .await
            .expect("memory catalog"),
    );
    let namespace = NamespaceIdent::new("managed".to_owned());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("namespace");
    let arrow_schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
    let schema = arrow_schema_to_schema_auto_assign_ids(&arrow_schema).expect("iceberg schema");
    let partition_spec = UnboundPartitionSpec::builder()
        .add_partition_field(1, "value", Transform::Identity)
        .expect("partition field")
        .build();
    let table = TableIdent::new(namespace, "partitioned_events".to_owned());
    catalog
        .create_table(
            table.namespace(),
            TableCreation::builder()
                .name(table.name().to_owned())
                .location("memory://warehouse/managed/partitioned_events".to_owned())
                .schema(schema)
                .partition_spec(partition_spec)
                .build(),
        )
        .await
        .expect("partitioned table");
    (catalog, table)
}

async fn catalog_with_two_column_table() -> (Arc<dyn Catalog>, TableIdent) {
    let catalog = Arc::new(
        MemoryCatalogBuilder::default()
            .load(
                "test",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_owned(),
                    "memory://warehouse".to_owned(),
                )]),
            )
            .await
            .expect("memory catalog"),
    );
    let namespace = NamespaceIdent::new("managed".to_owned());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("namespace");
    let arrow_schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]);
    let schema = arrow_schema_to_schema_auto_assign_ids(&arrow_schema).expect("iceberg schema");
    let table = TableIdent::new(namespace, "two_column_events".to_owned());
    catalog
        .create_table(
            table.namespace(),
            TableCreation::builder()
                .name(table.name().to_owned())
                .location("memory://warehouse/managed/two_column_events".to_owned())
                .schema(schema)
                .build(),
        )
        .await
        .expect("two-column table");
    (catalog, table)
}

fn pending_two_column_batch() -> OffloadBatch {
    let directory = tempfile::tempdir().expect("directory");
    let replica = SqliteReplicaStore::open(directory.path().join("replica.sqlite"), "lake-1")
        .expect("replica");
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Utf8, false),
        Field::new("id", DataType::Int64, false),
    ]));
    let rows = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["seven", "eleven"])),
            Arc::new(Int64Array::from(vec![7, 11])),
        ],
    )
    .expect("rows");
    let transaction_id = Uuid::from_u128(31);
    let mut envelope =
        TransactionEnvelope::new("lake-1", transaction_id, 0, IsolationLevel::Snapshot);
    envelope.append(
        MutationDomain::Relational,
        TableId::new("two_column_events"),
        rows,
    );
    let canonical = envelope.canonical_bytes().expect("canonical");
    replica
        .apply_committed(1, transaction_id, &canonical)
        .expect("apply");
    let mut batcher = OffloadBatcher::new(OffloadBatchPolicy::production());
    let transaction = replica
        .pending_archive()
        .expect("pending")
        .into_iter()
        .next()
        .expect("transaction");
    batcher.push(transaction, Instant::now()).expect("batch");
    batcher.drain().expect("drain")
}

async fn committed_engine() -> DoEngine {
    let engine = DoEngine::new(
        "lake-1",
        Arc::new(TestAuthority {
            next: AtomicU64::new(0),
        }),
    );
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    engine
        .create_table(TableId::new("events"), schema.clone())
        .await
        .expect("engine table");
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![7, 11]))]).expect("rows");
    let mut transaction = engine.begin(IsolationLevel::Snapshot).await.expect("begin");
    transaction
        .append(MutationDomain::Relational, TableId::new("events"), batch)
        .expect("append");
    engine.commit(transaction).await.expect("commit");
    engine
}

fn pending_batch() -> OffloadBatch {
    pending_batch_with(Uuid::from_u128(1), 1, vec![7, 11])
}

fn pending_batch_with(transaction_id: Uuid, sequence: u64, values: Vec<i64>) -> OffloadBatch {
    pending_batch_for("events", transaction_id, sequence, values)
}

fn pending_batch_for(
    table: &str,
    transaction_id: Uuid,
    sequence: u64,
    values: Vec<i64>,
) -> OffloadBatch {
    let directory = tempfile::tempdir().expect("directory");
    let replica = SqliteReplicaStore::open(directory.path().join("replica.sqlite"), "lake-1")
        .expect("replica");
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let rows =
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).expect("rows");
    for prior_sequence in 1..sequence {
        let prior_id = Uuid::from_u128(1_000 + u128::from(prior_sequence));
        let prior = TransactionEnvelope::new(
            "lake-1",
            prior_id,
            prior_sequence.saturating_sub(1),
            IsolationLevel::Snapshot,
        );
        let prior_bytes = prior.canonical_bytes().expect("prior canonical");
        replica
            .apply_committed(prior_sequence, prior_id, &prior_bytes)
            .expect("prior apply");
    }
    let mut envelope = TransactionEnvelope::new(
        "lake-1",
        transaction_id,
        sequence.saturating_sub(1),
        IsolationLevel::Snapshot,
    );
    envelope.append(MutationDomain::Relational, TableId::new(table), rows);
    let canonical = envelope.canonical_bytes().expect("canonical");
    replica
        .apply_committed(sequence, transaction_id, &canonical)
        .expect("apply");
    let mut batcher = OffloadBatcher::new(OffloadBatchPolicy::production());
    let transaction = replica
        .pending_archive()
        .expect("pending")
        .into_iter()
        .find(|transaction| transaction.commit_sequence() == sequence)
        .expect("transaction");
    batcher.push(transaction, Instant::now()).expect("batch");
    batcher.drain().expect("drain")
}

fn pending_batch_with_kind(
    table: &str,
    transaction_id: Uuid,
    kind: MutationKind,
    domain: MutationDomain,
) -> OffloadBatch {
    let directory = tempfile::tempdir().expect("directory");
    let replica = SqliteReplicaStore::open(directory.path().join("replica.sqlite"), "lake-1")
        .expect("replica");
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let rows =
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![7, 11]))]).expect("rows");
    let mut envelope =
        TransactionEnvelope::new("lake-1", transaction_id, 0, IsolationLevel::Snapshot);
    envelope.append_with_kind(kind, domain, TableId::new(table), rows);
    let canonical = envelope.canonical_bytes().expect("canonical");
    replica
        .apply_committed(1, transaction_id, &canonical)
        .expect("apply");
    let mut batcher = OffloadBatcher::new(OffloadBatchPolicy::production());
    let transaction = replica
        .pending_archive()
        .expect("pending")
        .into_iter()
        .next()
        .expect("transaction");
    batcher.push(transaction, Instant::now()).expect("batch");
    batcher.drain().expect("drain")
}

fn managed_lakehouse() -> LakehouseObject {
    LakehouseObject::new(
        "lake-1",
        StorageBinding::Managed,
        Arc::new(ObjectStoreDerivedArtifactPublisher::new(
            Arc::new(InMemory::new()),
            "managed",
        )),
    )
}

async fn authorized_commit(
    committer: &IcebergCommitter,
    batch: &OffloadBatch,
) -> verglas_do_engine::Result<verglas_do_engine::IcebergCommitReceipt> {
    managed_lakehouse()
        .commit_batch(batch, committer, PublicationAuthorization::Autonomous)
        .await
}

async fn authorized_commit_with_coverage(
    committer: &IcebergCommitter,
    batch: &OffloadBatch,
    coverage: verglas_do_engine::IcebergIndexCoverage,
) -> verglas_do_engine::Result<verglas_do_engine::IcebergCommitReceipt> {
    managed_lakehouse()
        .commit_batch_with_coverage(
            batch,
            committer,
            PublicationAuthorization::Autonomous,
            coverage,
        )
        .await
}

fn managed_sink(committer: Arc<IcebergCommitter>, do_id: &str) -> VerifiedIcebergArchive {
    let archive = Arc::new(ObjectStoreOffloadBatchArchive::new(
        Arc::new(InMemory::new()),
        Path::from("archive"),
    ));
    VerifiedIcebergArchive::new(
        archive,
        committer,
        StorageBinding::Managed,
        PublicationAuthorization::Autonomous,
        do_id.to_owned(),
    )
}

#[tokio::test]
async fn unsupported_mutations_are_rejected_before_lake_materialization() {
    let (catalog, table) = catalog_with_table().await;
    let committer = IcebergCommitter::new(catalog, table, "lake-1");
    for kind in [MutationKind::Replace, MutationKind::Upsert] {
        let error = authorized_commit(
            &committer,
            &pending_batch_with_kind(
                "events",
                Uuid::from_u128(40 + kind as u128),
                kind,
                MutationDomain::Relational,
            ),
        )
        .await
        .expect_err("non-INSERT mutation");
        assert!(
            error
                .to_string()
                .contains("only relational INSERT mutations")
        );
    }
    let error = authorized_commit(
        &committer,
        &pending_batch_with_kind(
            "events",
            Uuid::from_u128(42),
            MutationKind::Insert,
            MutationDomain::Vector,
        ),
    )
    .await
    .expect_err("vector mutation");
    assert!(error.to_string().contains("domain Vector"));
}

#[tokio::test]
async fn shared_batch_commits_verified_snapshot_and_standard_reader_rows() {
    let (catalog, table) = catalog_with_table().await;
    let committer = Arc::new(IcebergCommitter::new(catalog.clone(), table, "lake-1"));
    let sink = managed_sink(committer.clone(), "lake-1");
    let engine = committed_engine().await;

    let report = engine
        .drain_offload(&sink, OffloadBatchPolicy::production())
        .await
        .expect("materialize");
    assert_eq!(report.through(), 1);
    assert_eq!(engine.archive_watermark(), 1);

    let committed = catalog
        .load_table(committer.table_identifier())
        .await
        .expect("committed table");
    let snapshot = committed.metadata().current_snapshot().expect("snapshot");
    let summary = snapshot.summary();
    assert_eq!(
        summary.additional_properties["verglas.commit-range-start"],
        "1"
    );
    assert_eq!(
        summary.additional_properties["verglas.commit-range-end"],
        "1"
    );
    assert_eq!(
        summary.additional_properties["verglas.commit-sequence"],
        "1"
    );
    assert_eq!(summary.additional_properties["verglas.vamana-through"], "0");
    assert_eq!(summary.additional_properties["verglas.graph-through"], "0");

    let rows = committed
        .scan()
        .build()
        .expect("scan")
        .to_arrow()
        .await
        .expect("reader")
        .try_collect::<Vec<_>>()
        .await
        .expect("rows");
    let values = rows[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("value column");
    assert_eq!(values.values(), &[7, 11]);
}

#[tokio::test]
async fn schema_reordering_is_rejected_before_lake_materialization() {
    let (catalog, table) = catalog_with_two_column_table().await;
    let committer = IcebergCommitter::new(catalog.clone(), table.clone(), "lake-1");
    let error = authorized_commit(&committer, &pending_two_column_batch())
        .await
        .expect_err("schema reorder");
    assert!(error.to_string().contains("exactly match"));
    let committed = catalog.load_table(&table).await.expect("two-column table");
    assert_eq!(committed.metadata().snapshots().count(), 0);
}

#[tokio::test]
async fn unsupported_format_versions_are_rejected_before_lake_materialization() {
    for format in [FormatVersion::V1, FormatVersion::V3] {
        let (catalog, table) = catalog_with_table_format(format).await;
        let committer = IcebergCommitter::new(catalog.clone(), table.clone(), "lake-1");
        let error = authorized_commit(&committer, &pending_batch())
            .await
            .expect_err("unsupported format version");
        assert!(error.to_string().contains("format V2 only"));
        let committed = catalog.load_table(&table).await.expect("versioned table");
        assert_eq!(committed.metadata().snapshots().count(), 0);
    }
}

#[tokio::test]
async fn partitioned_tables_are_rejected_before_lake_materialization() {
    let (catalog, table) = catalog_with_partitioned_table().await;
    let committer = IcebergCommitter::new(catalog.clone(), table.clone(), "lake-1");
    let batch = pending_batch_for("partitioned_events", Uuid::from_u128(11), 1, vec![2, 1]);
    let error = authorized_commit(&committer, &batch)
        .await
        .expect_err("partitioned table");
    assert!(error.to_string().contains("rejects partitioned tables"));
    let committed = catalog.load_table(&table).await.expect("partitioned table");
    assert_eq!(committed.metadata().snapshots().count(), 0);
}

#[tokio::test]
async fn exact_batch_retry_does_not_create_a_second_snapshot() {
    let (catalog, table) = catalog_with_table().await;
    let committer = IcebergCommitter::new(catalog.clone(), table, "lake-1");
    let batch = pending_batch();

    let first = authorized_commit(&committer, &batch)
        .await
        .expect("first materialization");
    let first_table = catalog
        .load_table(committer.table_identifier())
        .await
        .expect("first table");
    let first_count = first_table.metadata().snapshots().count();

    let retry = authorized_commit(&committer, &batch)
        .await
        .expect("retry materialization");
    let retried = catalog
        .load_table(committer.table_identifier())
        .await
        .expect("retried table");
    assert_eq!(retried.metadata().snapshots().count(), first_count);
    assert_eq!(retry.snapshot_id(), first.snapshot_id());
    assert_eq!(retry.materialization_id(), first.materialization_id());
}

#[tokio::test]
async fn overlapping_materialization_range_is_rejected_without_duplicate_rows() {
    let (catalog, table) = catalog_with_table().await;
    let committer = IcebergCommitter::new(catalog.clone(), table.clone(), "lake-1");
    authorized_commit(
        &committer,
        &pending_batch_with(Uuid::from_u128(21), 1, vec![7]),
    )
    .await
    .expect("first range");
    let error = authorized_commit(
        &committer,
        &pending_batch_with(Uuid::from_u128(22), 1, vec![7, 11]),
    )
    .await
    .expect_err("overlapping range");
    assert!(error.to_string().contains("overlaps"));
    let committed = catalog.load_table(&table).await.expect("table");
    assert_eq!(committed.metadata().snapshots().count(), 1);
}

#[tokio::test]
async fn direct_committer_requires_lakehouse_authorization() {
    let (catalog, table) = catalog_with_table().await;
    let committer = IcebergCommitter::new(catalog, table, "lake-1");
    let error = committer
        .commit_batch(&pending_batch())
        .await
        .expect_err("direct commit must be fenced");
    assert!(error.to_string().contains("LakehouseObject authorization"));
}

#[tokio::test]
async fn customer_binding_needs_explicit_iceberg_invocation() {
    let (catalog, table) = catalog_with_table().await;
    let committer = Arc::new(IcebergCommitter::new(catalog, table, "lake-1"));
    let object_archive = Arc::new(ObjectStoreOffloadBatchArchive::new(
        Arc::new(InMemory::new()),
        Path::from("archive"),
    ));
    let lakehouse = LakehouseObject::new(
        "lake-1",
        StorageBinding::Customer,
        Arc::new(ObjectStoreDerivedArtifactPublisher::new(
            Arc::new(InMemory::new()),
            "customer",
        )),
    );
    let autonomous = lakehouse.verified_offload_archive(
        object_archive.clone(),
        committer.clone(),
        PublicationAuthorization::Autonomous,
    );
    let engine = committed_engine().await;
    assert!(
        engine
            .drain_offload(&autonomous, OffloadBatchPolicy::production())
            .await
            .is_err()
    );
    assert_eq!(engine.archive_watermark(), 0);

    let explicit = lakehouse.verified_offload_archive(
        object_archive,
        committer,
        PublicationAuthorization::Explicit,
    );
    engine
        .drain_offload(&explicit, OffloadBatchPolicy::production())
        .await
        .expect("explicit materialization");
    assert_eq!(engine.archive_watermark(), 1);
}

#[tokio::test]
async fn failed_commit_does_not_advance_archive_watermark() {
    let (catalog, table) = catalog_with_table().await;
    let committer = Arc::new(IcebergCommitter::new(catalog, table, "different-do"));
    let sink = managed_sink(committer, "different-do");
    let engine = committed_engine().await;

    let error = engine
        .drain_offload(&sink, OffloadBatchPolicy::production())
        .await
        .expect_err("wrong DO must fail");
    assert!(error.to_string().contains("different-do"));
    assert_eq!(engine.archive_watermark(), 0);
}

#[tokio::test]
async fn concurrent_exact_retries_publish_one_snapshot() {
    let (catalog, table) = catalog_with_table().await;
    let first = IcebergCommitter::new(catalog.clone(), table.clone(), "lake-1");
    let second = first.clone();
    let batch = pending_batch();

    let (left, right) = tokio::join!(
        authorized_commit(&first, &batch),
        authorized_commit(&second, &batch)
    );
    let left = left.expect("first concurrent commit");
    let right = right.expect("second concurrent commit");
    assert_eq!(left.snapshot_id(), right.snapshot_id());
    let committed = catalog
        .load_table(&table)
        .await
        .expect("concurrently committed table");
    assert_eq!(committed.metadata().snapshots().count(), 1);
}

#[tokio::test]
async fn concurrent_distinct_batches_retry_after_cas_winner() {
    let (catalog, table) = catalog_with_table().await;
    let catalog = Arc::new(BarrierCatalog::new(catalog, 2));
    let first = IcebergCommitter::new(catalog.clone(), table.clone(), "lake-1");
    let second = first.clone();
    let left_batch = pending_batch_with(Uuid::from_u128(2), 1, vec![13]);
    let right_batch = pending_batch_with(Uuid::from_u128(3), 2, vec![17]);

    let (left, right) = tokio::join!(
        authorized_commit(&first, &left_batch),
        authorized_commit(&second, &right_batch)
    );
    left.expect("first distinct commit");
    right.expect("second distinct commit should retry");
    let committed = catalog
        .load_table(&table)
        .await
        .expect("concurrently committed table");
    assert_eq!(committed.metadata().snapshots().count(), 2);
    let rows = committed
        .scan()
        .build()
        .expect("scan")
        .to_arrow()
        .await
        .expect("reader")
        .try_collect::<Vec<_>>()
        .await
        .expect("rows");
    let mut actual = rows
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("value column")
                .values()
                .iter()
                .copied()
        })
        .collect::<Vec<_>>();
    actual.sort_unstable();
    assert_eq!(actual, [13, 17]);
}

#[tokio::test]
async fn verification_rejects_a_partial_manifest_list() {
    let (catalog, table) = catalog_with_table().await;
    let first = IcebergCommitter::new(catalog.clone(), table.clone(), "lake-1");
    authorized_commit(&first, &pending_batch_with(Uuid::from_u128(50), 1, vec![5]))
        .await
        .expect("first snapshot");
    let corrupting = Arc::new(BarrierCatalog::corrupt_manifest_after_update(catalog));
    let second = IcebergCommitter::new(corrupting, table, "lake-1");
    let error = authorized_commit(
        &second,
        &pending_batch_with(Uuid::from_u128(51), 2, vec![6]),
    )
    .await
    .expect_err("partial manifest list");
    assert!(error.to_string().contains("planned snapshot"));
}

#[tokio::test]
async fn index_coverage_is_recorded_in_snapshot_summary() {
    let (catalog, table) = catalog_with_table().await;
    let committer = IcebergCommitter::new(catalog.clone(), table, "lake-1");
    let batch = pending_batch();

    authorized_commit_with_coverage(
        &committer,
        &batch,
        verglas_do_engine::IcebergIndexCoverage::new(1, 1),
    )
    .await
    .expect("indexed materialization");
    let committed = catalog
        .load_table(committer.table_identifier())
        .await
        .expect("indexed table");
    let summary = committed
        .metadata()
        .current_snapshot()
        .expect("indexed snapshot")
        .summary();
    assert_eq!(summary.additional_properties["verglas.vamana-through"], "1");
    assert_eq!(summary.additional_properties["verglas.graph-through"], "1");
}
