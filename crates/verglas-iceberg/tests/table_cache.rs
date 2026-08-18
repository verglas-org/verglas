//! Hermetic tests for `write::TableCache` (the warm-append metadata cache,
//! ingest-perf-pipeline).
//!
//! The profiled symptom: a single-row append against an already-open table
//! pays a `catalog.load_table` round trip before it ever writes a byte, even
//! though the serving process just committed that same table moments ago and
//! already holds its metadata in memory. `TableCache` lets a repeat append
//! against the same identifier start from the last committed `Table` instead
//! of an unconditional `catalog.load_table`.
//!
//! This does not remove every `load_table` call: the pinned `iceberg-rust`
//! fork's `Transaction::do_commit` unconditionally refreshes the table from
//! the catalog at the start of every commit attempt regardless of what base
//! table it was handed (`crates/iceberg/src/transaction/mod.rs`, `do_commit`),
//! so one `load_table` per commit is a fixed cost of the vendored commit path,
//! outside this repo. What the cache removes is the *second*, redundant one —
//! the explicit lookup this crate's own code used to make before ever
//! starting the transaction. A cold (first) append against an identifier
//! costs 2 `load_table` calls either way; every warm append after it costs 2
//! uncached, 1 cached.
//!
//! The CAS commit (`update_table`) still runs every time and is still the
//! sole correctness authority; a cached table that lost a race is caught by
//! the existing `CatalogCommitConflicts` retry in `write::commit_data_files`,
//! which reloads through the catalog and refreshes the cache.
//!
//! Hermetic: a delegating catalog wrapper counts `load_table` calls. No
//! server, no timing assertions.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::table::Table;
use iceberg::{
    Catalog, CatalogBuilder, Namespace, NamespaceIdent, Result as IcebergResult, TableCommit,
    TableCreation, TableIdent,
};
use verglas_iceberg::{parse_table_ident, write};

/// Builds an in-process memory catalog over a fresh temp warehouse path.
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
    // Keep the warehouse dir alive for the catalog's lifetime by leaking the
    // guard — the process is a short-lived test.
    std::mem::forget(warehouse);
    Arc::new(catalog)
}

/// A one-column schema: a required int64 id.
fn id_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

/// One single-row batch matching [`id_schema`].
fn one_row(i: i64) -> RecordBatch {
    RecordBatch::try_new(id_schema(), vec![Arc::new(Int64Array::from(vec![i]))]).expect("batch")
}

/// Delegates every `Catalog` call to `inner`, counting `load_table` calls —
/// the exact catalog round trip a warm append should skip when the identifier
/// is already cached.
#[derive(Debug)]
struct CountingCatalog {
    inner: Arc<dyn Catalog>,
    load_table_calls: AtomicUsize,
}

impl CountingCatalog {
    fn new(inner: Arc<dyn Catalog>) -> Self {
        Self {
            inner,
            load_table_calls: AtomicUsize::new(0),
        }
    }

    fn load_table_calls(&self) -> usize {
        self.load_table_calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl Catalog for CountingCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> IcebergResult<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(parent).await
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> IcebergResult<Namespace> {
        self.inner.create_namespace(namespace, properties).await
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> IcebergResult<Namespace> {
        self.inner.get_namespace(namespace).await
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> IcebergResult<bool> {
        self.inner.namespace_exists(namespace).await
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> IcebergResult<()> {
        self.inner.update_namespace(namespace, properties).await
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> IcebergResult<()> {
        self.inner.drop_namespace(namespace).await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> IcebergResult<Vec<TableIdent>> {
        self.inner.list_tables(namespace).await
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> IcebergResult<Table> {
        self.inner.create_table(namespace, creation).await
    }

    async fn load_table(&self, table: &TableIdent) -> IcebergResult<Table> {
        self.load_table_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.load_table(table).await
    }

    async fn drop_table(&self, table: &TableIdent) -> IcebergResult<()> {
        self.inner.drop_table(table).await
    }

    async fn purge_table(&self, table: &TableIdent) -> IcebergResult<()> {
        self.inner.purge_table(table).await
    }

    async fn table_exists(&self, table: &TableIdent) -> IcebergResult<bool> {
        self.inner.table_exists(table).await
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> IcebergResult<()> {
        self.inner.rename_table(src, dest).await
    }

    async fn register_table(
        &self,
        table: &TableIdent,
        metadata_location: String,
    ) -> IcebergResult<Table> {
        self.inner.register_table(table, metadata_location).await
    }

    async fn update_table(&self, commit: TableCommit) -> IcebergResult<Table> {
        self.inner.update_table(commit).await
    }
}

/// Acceptance (ingest-perf-pipeline): a warm append (the table already in
/// `TableCache`) issues exactly one `catalog.load_table` call — from the
/// vendored `Transaction::do_commit`'s own unconditional refresh — instead of
/// two. The cold (first) append against an identifier still pays both: the
/// cache-miss load plus `do_commit`'s refresh.
#[tokio::test]
async fn cached_append_skips_the_redundant_load_table_on_warm_appends() {
    let inner = memory_catalog().await;
    let ident = parse_table_ident("sdk.warm").expect("ident");
    write::create_table_from_schema(inner.as_ref(), &ident, &id_schema(), None)
        .await
        .expect("create");

    let catalog = CountingCatalog::new(inner);
    let cache = write::TableCache::new();

    write::append_batches_cached(&catalog, &cache, &ident, vec![one_row(0)], HashMap::new())
        .await
        .expect("cold cached append");
    assert_eq!(
        catalog.load_table_calls(),
        2,
        "cold append: one cache-miss load plus do_commit's own refresh"
    );

    write::append_batches_cached(&catalog, &cache, &ident, vec![one_row(1)], HashMap::new())
        .await
        .expect("warm cached append");
    assert_eq!(
        catalog.load_table_calls(),
        3,
        "warm append: cache hit skips the explicit load, only do_commit's refresh remains"
    );

    write::append_batches_cached(&catalog, &cache, &ident, vec![one_row(2)], HashMap::new())
        .await
        .expect("second warm cached append");
    assert_eq!(
        catalog.load_table_calls(),
        4,
        "every further warm append costs exactly one load_table call, not two"
    );
}

/// Acceptance: the same three appends without `TableCache` cost twice as many
/// `load_table` calls — the explicit lookup this crate's own code makes,
/// which the cache exists to remove, plus `do_commit`'s unconditional
/// refresh, on every single append.
#[tokio::test]
async fn uncached_append_pays_two_load_table_calls_every_time() {
    let inner = memory_catalog().await;
    let ident = parse_table_ident("sdk.cold").expect("ident");
    write::create_table_from_schema(inner.as_ref(), &ident, &id_schema(), None)
        .await
        .expect("create");

    let catalog = CountingCatalog::new(inner);

    for i in 0..3 {
        write::append_batches(&catalog, &ident, vec![one_row(i)], HashMap::new())
            .await
            .expect("uncached append");
    }

    assert_eq!(
        catalog.load_table_calls(),
        6,
        "no cache: every append pays its own explicit load plus do_commit's refresh"
    );
}

/// Acceptance: a cache entry that fell behind — because another writer
/// committed through the same catalog without going through this process's
/// cache — still produces a correct append. The stale-base commit hits
/// Iceberg's own `RefSnapshotIdMatch` requirement (a real `CatalogCommitConflicts`,
/// not injected), the existing retry in `commit_data_files` reloads the fresh
/// table through the catalog, and the cache is refreshed with the result. No
/// row is lost or duplicated.
#[tokio::test]
async fn cached_append_recovers_from_a_stale_cache_entry() {
    let catalog = memory_catalog().await;
    let ident = parse_table_ident("sdk.warm").expect("ident");
    write::create_table_from_schema(catalog.as_ref(), &ident, &id_schema(), None)
        .await
        .expect("create");

    let cache = write::TableCache::new();

    // Populates the cache at the post-create snapshot.
    write::append_batches_cached(
        catalog.as_ref(),
        &cache,
        &ident,
        vec![one_row(0)],
        HashMap::new(),
    )
    .await
    .expect("first cached append");

    // A second writer commits directly through the catalog, bypassing the
    // cache entirely — the cache now names a stale base snapshot.
    write::append_batches(catalog.as_ref(), &ident, vec![one_row(1)], HashMap::new())
        .await
        .expect("bypassing append");

    // The cached append's optimistic base is stale; it must still succeed.
    let report = write::append_batches_cached(
        catalog.as_ref(),
        &cache,
        &ident,
        vec![one_row(2)],
        HashMap::new(),
    )
    .await
    .expect("cached append recovers from a stale base");
    assert_eq!(report.records_added, 1);

    // All three rows are present exactly once.
    let rows = verglas_iceberg::tables_api::rows(catalog.as_ref(), &ident, None, None)
        .await
        .expect("rows");
    let mut ids: Vec<i64> = rows
        .rows
        .iter()
        .map(|r| r["id"].as_i64().expect("id"))
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![0, 1, 2], "no row lost or duplicated");
}
