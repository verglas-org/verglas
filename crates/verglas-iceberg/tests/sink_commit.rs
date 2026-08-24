//! Hermetic acceptance tests for the Sink-owned Iceberg commit engine.
//!
//! The fixture uses the existing in-process MemoryCatalog and a temporary
//! warehouse. These tests prove ownership, schema inference, deterministic
//! Parquet output, snapshot identity, replay, changed-payload rejection, and
//! the file-before-metadata crash seam without a server or object store.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use futures::TryStreamExt;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::table::Table;
use iceberg::{
    Catalog, CatalogBuilder, Error, ErrorKind, Namespace, NamespaceIdent, TableCommit,
    TableCreation, TableIdent,
};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::basic::Compression;
use serde_json::{Value, json};
use verglas_iceberg::parse_table_ident;
use verglas_iceberg::tables_api::{self, SinkBatchConfig, SinkBatchRequest, SinkCompression};
use verglas_iceberg::write::{self, TableCache};

/// Builds a MemoryCatalog over a leaked temporary warehouse for one test.
async fn memory_catalog() -> Arc<dyn Catalog> {
    let warehouse = tempfile::tempdir().expect("warehouse tempdir");
    let catalog = MemoryCatalogBuilder::default()
        .load(
            "memory",
            HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.to_string(),
                warehouse.path().to_str().expect("utf8 path").to_string(),
            )]),
        )
        .await
        .expect("memory catalog");
    std::mem::forget(warehouse);
    Arc::new(catalog)
}

/// Builds the two-column schema used to create an unowned table in rejection tests.
fn existing_schema() -> Arc<arrow_schema::Schema> {
    Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
        arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, true),
    ]))
}

/// Creates one typed sink configuration.
fn config(sink_id: &str, compression: SinkCompression) -> SinkBatchConfig {
    SinkBatchConfig {
        sink_id: sink_id.to_owned(),
        compression,
    }
}

/// Builds a request with the deterministic file identity expected by the Sink.
fn request(
    batch_id: &str,
    payload_digest: &str,
    sink_id: &str,
    records: Vec<Value>,
) -> SinkBatchRequest {
    SinkBatchRequest {
        batch_id: batch_id.to_owned(),
        payload_digest: payload_digest.to_owned(),
        file_id: tables_api::deterministic_sink_file_id(sink_id, batch_id),
        records,
    }
}

/// Returns the first live data file in a table.
async fn first_data_file(table: &Table) -> iceberg::scan::FileScanTask {
    let mut tasks = table
        .scan()
        .build()
        .expect("scan")
        .plan_files()
        .await
        .expect("plan files");
    tasks.try_next().await.expect("collect task").expect("file")
}

/// Reads Parquet metadata from one Iceberg data file.
async fn parquet_metadata(table: &Table, path: &str) -> ArrowReaderMetadata {
    let bytes = table
        .file_io()
        .new_input(path)
        .expect("input")
        .read()
        .await
        .expect("read parquet");
    ArrowReaderMetadata::load(&bytes, ArrowReaderOptions::default()).expect("parquet metadata")
}

/// Acceptance: a missing table is created for the sink, JSON rows are inferred
/// and committed, and the snapshot and file carry the complete deterministic
/// Sink identity.
#[tokio::test]
async fn sink_creates_owned_table_and_writes_deterministic_parquet() {
    let catalog = memory_catalog().await;
    let cache = TableCache::new();
    let ident = parse_table_ident("analytics.events").expect("ident");
    let sink = config("primary", SinkCompression::Zstd);
    let batch = request(
        "[\"orders\",\"sql\",1,2,\"primary\"]",
        "payload-1",
        "primary",
        vec![
            json!({"id": 1, "name": "alice"}),
            json!({"id": 2, "name": null}),
        ],
    );
    assert_eq!(
        batch.file_id,
        "verglas/primary/batch-6155eed0061e2e02e55e7e18ea782c134c2287bbe7757db4138c67c4f577656a.parquet"
    );

    let receipt =
        tables_api::commit_sink_batch(catalog.as_ref(), &cache, &ident, &sink, batch.clone())
            .await
            .expect("sink commit");

    assert_eq!(receipt.batch_id, batch.batch_id);
    assert_eq!(receipt.file_id, batch.file_id);
    assert_eq!(receipt.rows_committed, 2);
    assert_eq!(receipt.accepted, 2);
    assert!(!receipt.snapshot_id.is_empty());

    let table = catalog.load_table(&ident).await.expect("load table");
    let inferred = table.metadata().current_schema();
    assert!(matches!(
        inferred
            .field_by_name("id")
            .expect("id")
            .field_type
            .as_ref(),
        iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long)
    ));
    assert!(inferred.field_by_name("id").expect("id").required);
    assert!(!inferred.field_by_name("name").expect("name").required);
    assert_eq!(
        table
            .metadata()
            .properties()
            .get(tables_api::SINK_OWNER_PROPERTY),
        Some(&"primary".to_owned())
    );
    assert_eq!(
        table
            .metadata()
            .properties()
            .get(tables_api::SINK_COMPRESSION_PROPERTY),
        Some(&"zstd".to_owned())
    );

    let snapshot_id: i64 = receipt.snapshot_id.parse().expect("snapshot id");
    let snapshot = table
        .metadata()
        .snapshot_by_id(snapshot_id)
        .expect("snapshot");
    let summary = &snapshot.summary().additional_properties;
    assert_eq!(
        summary.get(tables_api::SINK_BATCH_ID_PROPERTY),
        Some(&batch.batch_id)
    );
    assert_eq!(
        summary.get(tables_api::SINK_PAYLOAD_DIGEST_PROPERTY),
        Some(&batch.payload_digest)
    );
    assert_eq!(
        summary.get(tables_api::SINK_FILE_ID_PROPERTY),
        Some(&batch.file_id)
    );
    assert_eq!(
        summary.get(tables_api::SINK_ROW_COUNT_PROPERTY),
        Some(&"2".to_owned())
    );
    assert_eq!(
        summary.get(tables_api::SINK_OWNER_PROPERTY),
        Some(&"primary".to_owned())
    );

    let file = first_data_file(&table).await;
    assert!(
        file.data_file_path
            .ends_with(&format!("/data/{}", batch.file_id))
    );
    let metadata = parquet_metadata(&table, &file.data_file_path).await;
    assert_eq!(metadata.metadata().file_metadata().num_rows(), 2);
    assert_eq!(
        metadata.metadata().row_group(0).column(0).compression(),
        Compression::ZSTD(Default::default())
    );
}

/// Acceptance: every protocol compression spelling produces the corresponding
/// Parquet codec, while the Sink API does not expose any unvalidated codec.
#[tokio::test]
async fn sink_validates_and_writes_each_compression() {
    let codecs = [
        (SinkCompression::Zstd, "zstd"),
        (SinkCompression::Snappy, "snappy"),
        (SinkCompression::Gzip, "gzip"),
        (SinkCompression::Lz4, "lz4"),
        (SinkCompression::Uncompressed, "uncompressed"),
    ];
    for (index, (compression, spelling)) in codecs.into_iter().enumerate() {
        assert_eq!(
            spelling.parse::<SinkCompression>().expect("codec"),
            compression
        );
        let catalog = memory_catalog().await;
        let cache = TableCache::new();
        let ident = parse_table_ident(&format!("analytics.codec_{index}")).expect("ident");
        let sink_id = format!("codec-{index}");
        let sink = config(&sink_id, compression);
        let batch = request(
            &format!("codec-batch-{index}"),
            &format!("codec-payload-{index}"),
            &sink_id,
            vec![json!({"id": index as i64})],
        );
        let receipt = tables_api::commit_sink_batch(catalog.as_ref(), &cache, &ident, &sink, batch)
            .await
            .expect("codec commit");
        let table = catalog.load_table(&ident).await.expect("table");
        let file = first_data_file(&table).await;
        let metadata = parquet_metadata(&table, &file.data_file_path).await;
        let actual = metadata.metadata().row_group(0).column(0).compression();
        match compression {
            SinkCompression::Zstd => assert!(matches!(actual, Compression::ZSTD(_))),
            SinkCompression::Snappy => assert_eq!(actual, Compression::SNAPPY),
            SinkCompression::Gzip => assert!(matches!(actual, Compression::GZIP(_))),
            SinkCompression::Lz4 => assert_eq!(actual, Compression::LZ4_RAW),
            SinkCompression::Uncompressed => assert_eq!(actual, Compression::UNCOMPRESSED),
        }
        assert_eq!(receipt.rows_committed, 1);
    }
    assert!("brotli".parse::<SinkCompression>().is_err());
}

/// Acceptance: a replay of the same batch and digest returns the original
/// receipt and snapshot without adding a second file, snapshot, or row.
#[tokio::test]
async fn sink_replay_is_exactly_once() {
    let catalog = memory_catalog().await;
    let cache = TableCache::new();
    let ident = parse_table_ident("analytics.events").expect("ident");
    let sink = config("primary", SinkCompression::Snappy);
    let batch = request(
        "batch-1",
        "payload-1",
        "primary",
        vec![json!({"id": 1, "name": "alice"})],
    );

    let first =
        tables_api::commit_sink_batch(catalog.as_ref(), &cache, &ident, &sink, batch.clone())
            .await
            .expect("first commit");
    let before = catalog.load_table(&ident).await.expect("load before");
    let snapshots_before = before.metadata().snapshots().len();
    let files_before = before
        .scan()
        .build()
        .expect("scan")
        .plan_files()
        .await
        .expect("plan")
        .try_collect::<Vec<_>>()
        .await
        .expect("collect")
        .len();

    let second = tables_api::commit_sink_batch(catalog.as_ref(), &cache, &ident, &sink, batch)
        .await
        .expect("replay");
    assert_eq!(second, first);

    let after = catalog.load_table(&ident).await.expect("load after");
    assert_eq!(after.metadata().snapshots().len(), snapshots_before);
    let files_after = after
        .scan()
        .build()
        .expect("scan")
        .plan_files()
        .await
        .expect("plan")
        .try_collect::<Vec<_>>()
        .await
        .expect("collect")
        .len();
    assert_eq!(files_after, files_before);
    let rows = tables_api::rows(catalog.as_ref(), &ident, None, None)
        .await
        .expect("rows");
    assert_eq!(rows.rows.len(), 1);
}

/// Acceptance: the batch key cannot be reused with a changed digest, file, or
/// compression configuration.
#[tokio::test]
async fn sink_rejects_changed_batch_identity_and_configuration() {
    let catalog = memory_catalog().await;
    let cache = TableCache::new();
    let ident = parse_table_ident("analytics.events").expect("ident");
    let sink = config("primary", SinkCompression::Gzip);
    let batch = request(
        "batch-1",
        "payload-1",
        "primary",
        vec![json!({"id": 1, "name": "alice"})],
    );
    tables_api::commit_sink_batch(catalog.as_ref(), &cache, &ident, &sink, batch.clone())
        .await
        .expect("first commit");

    let changed_payload = request(
        &batch.batch_id,
        "payload-2",
        "primary",
        vec![json!({"id": 1, "name": "changed"})],
    );
    let payload_error =
        tables_api::commit_sink_batch(catalog.as_ref(), &cache, &ident, &sink, changed_payload)
            .await
            .expect_err("changed payload must fail");
    assert!(payload_error.to_string().contains("payload"));

    let changed_file = SinkBatchRequest {
        file_id: "verglas/primary/other.parquet".to_owned(),
        ..batch.clone()
    };
    let file_error =
        tables_api::commit_sink_batch(catalog.as_ref(), &cache, &ident, &sink, changed_file)
            .await
            .expect_err("changed file must fail");
    assert!(file_error.to_string().contains("file"));

    let changed_config = config("primary", SinkCompression::Lz4);
    let config_error =
        tables_api::commit_sink_batch(catalog.as_ref(), &cache, &ident, &changed_config, batch)
            .await
            .expect_err("changed compression must fail");
    assert!(config_error.to_string().contains("compression"));
}

/// Acceptance: a pre-existing table that does not carry this sink's owner is
/// rejected instead of being silently claimed or appended to.
#[tokio::test]
async fn sink_rejects_an_existing_table_not_owned_by_sink() {
    let catalog = memory_catalog().await;
    let ident = parse_table_ident("analytics.events").expect("ident");
    write::create_table_from_schema(catalog.as_ref(), &ident, &existing_schema(), None)
        .await
        .expect("create unowned table");

    let cache = TableCache::new();
    let sink = config("primary", SinkCompression::Uncompressed);
    let batch = request(
        "batch-1",
        "payload-1",
        "primary",
        vec![json!({"id": 1, "name": "alice"})],
    );
    let error = tables_api::commit_sink_batch(catalog.as_ref(), &cache, &ident, &sink, batch)
        .await
        .expect_err("unowned table must fail");
    assert!(error.to_string().contains("owned") || error.to_string().contains("owner"));
}

/// A catalog wrapper fails one metadata update after the data file is closed.
/// Retrying the same request must reuse the deterministic path and commit one
/// file; changing its payload while metadata is absent must fail safely.
#[tokio::test]
async fn sink_retries_file_written_before_catalog_metadata() {
    let inner = memory_catalog().await;
    let failing = FailOnceCatalog::new(inner.clone());
    let cache = TableCache::new();
    let ident = parse_table_ident("analytics.events").expect("ident");
    let sink = config("primary", SinkCompression::Zstd);
    let batch = request(
        "batch-crash",
        "payload-crash",
        "primary",
        vec![json!({"id": 7, "name": "before crash"})],
    );

    let first_error = tables_api::commit_sink_batch(&failing, &cache, &ident, &sink, batch.clone())
        .await
        .expect_err("injected catalog failure");
    assert!(first_error.to_string().contains("injected"));

    let table = inner.load_table(&ident).await.expect("table was created");
    let expected_path = format!("{}/data/{}", table.metadata().location(), batch.file_id);
    assert!(
        table
            .file_io()
            .new_input(&expected_path)
            .expect("input")
            .exists()
            .await
            .expect("exists")
    );
    assert!(table.metadata().current_snapshot_id().is_none());

    let changed = request(
        &batch.batch_id,
        "different-payload",
        "primary",
        vec![json!({"id": 7, "name": "changed"})],
    );
    let changed_error =
        tables_api::commit_sink_batch(inner.as_ref(), &cache, &ident, &sink, changed)
            .await
            .expect_err("changed payload cannot reuse orphan file");
    assert!(changed_error.to_string().contains("payload"));

    let receipt = tables_api::commit_sink_batch(inner.as_ref(), &cache, &ident, &sink, batch)
        .await
        .expect("retry");
    assert_eq!(receipt.rows_committed, 1);
    let committed = inner.load_table(&ident).await.expect("committed table");
    assert_eq!(
        committed
            .scan()
            .build()
            .expect("scan")
            .plan_files()
            .await
            .expect("plan")
            .try_collect::<Vec<_>>()
            .await
            .expect("collect")
            .len(),
        1
    );
}

/// A catalog that fails one update after the file writer has closed its output.
#[derive(Debug)]
struct FailOnceCatalog {
    inner: Arc<dyn Catalog>,
    fail_next: AtomicBool,
}

impl FailOnceCatalog {
    /// Creates a wrapper that fails the first metadata update only.
    fn new(inner: Arc<dyn Catalog>) -> Self {
        Self {
            inner,
            fail_next: AtomicBool::new(true),
        }
    }
}

#[async_trait]
impl Catalog for FailOnceCatalog {
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
        if self.fail_next.swap(false, Ordering::SeqCst) {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "injected catalog failure",
            ));
        }
        self.inner.update_table(commit).await
    }
}
