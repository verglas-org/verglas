//! Concurrent-writer commit behavior for the agent write path (issue #306).
//!
//! Another writer landing a snapshot between our table load and our commit
//! makes the catalog answer `CatalogCommitConflicts`. The write path must
//! reload the table and re-attach the already-written data files (bounded
//! attempts), so an append against a busy table succeeds instead of failing on
//! the first conflict; a persistent conflict must still surface after the
//! bounded attempts run out.
//!
//! Hermetic: a delegating catalog wrapper injects conflicts on `update_table`,
//! exactly the seam a concurrent writer races on.

use std::collections::HashMap;
use std::fmt::Debug;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::table::Table;
use iceberg::{
    Catalog, CatalogBuilder, Error, ErrorKind, Namespace, NamespaceIdent, Result as IcebergResult,
    TableCommit, TableCreation, TableIdent,
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

/// Writes `contents` to a temp file with the given name, returning its path and
/// keeping the tempdir alive by leaking it (test process is short-lived).
fn source_file(name: &str, contents: &str) -> PathBuf {
    let dir = tempfile::tempdir().expect("source tempdir");
    let path = dir.path().join(name);
    let mut file = std::fs::File::create(&path).expect("create source");
    file.write_all(contents.as_bytes()).expect("write source");
    file.flush().expect("flush source");
    std::mem::forget(dir);
    path
}

/// A catalog that fails the next `conflicts_left` `update_table` calls with
/// `CatalogCommitConflicts` — the exact answer a REST catalog gives when a
/// concurrent writer landed first — and delegates everything else.
#[derive(Debug)]
struct ConflictingCatalog {
    inner: Arc<dyn Catalog>,
    conflicts_left: AtomicUsize,
    update_calls: AtomicUsize,
}

impl ConflictingCatalog {
    fn new(inner: Arc<dyn Catalog>, conflicts: usize) -> Self {
        Self {
            inner,
            conflicts_left: AtomicUsize::new(conflicts),
            update_calls: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl Catalog for ConflictingCatalog {
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
        self.update_calls.fetch_add(1, Ordering::SeqCst);
        if self
            .conflicts_left
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(Error::new(
                ErrorKind::CatalogCommitConflicts,
                "injected: a concurrent writer landed first",
            ));
        }
        self.inner.update_table(commit).await
    }
}

/// Acceptance (#306): an append that hits transient commit conflicts succeeds
/// once a retry lands, and the rows are really there.
#[tokio::test]
async fn append_retries_past_transient_commit_conflicts() {
    let inner = memory_catalog().await;
    let ident = parse_table_ident("sys.sources").expect("ident");

    let seed = source_file("seed.csv", "name,revision\na,1\n");
    write::create_table(inner.as_ref(), &ident, &seed, None)
        .await
        .expect("create");

    // Two injected conflicts: the first two commit attempts race and lose.
    let catalog = ConflictingCatalog::new(inner, 2);
    let more = source_file("more.csv", "name,revision\nb,2\nc,3\n");
    let report = write::append(&catalog, &ident, &more)
        .await
        .expect("append should retry past transient conflicts");
    assert_eq!(report.records_added, 2);
    assert!(
        catalog.update_calls.load(Ordering::SeqCst) >= 3,
        "two conflicted attempts plus the one that landed"
    );
}

/// Acceptance (#306): a persistent conflict still surfaces after the bounded
/// attempts run out — the retry is not an infinite loop.
#[tokio::test]
async fn append_surfaces_persistent_conflicts() {
    let inner = memory_catalog().await;
    let ident = parse_table_ident("sys.sources").expect("ident");

    let seed = source_file("seed.csv", "name,revision\na,1\n");
    write::create_table(inner.as_ref(), &ident, &seed, None)
        .await
        .expect("create");

    // Every commit attempt conflicts.
    let catalog = ConflictingCatalog::new(inner, usize::MAX);
    let more = source_file("more.csv", "name,revision\nb,2\n");
    let err = write::append(&catalog, &ident, &more)
        .await
        .expect_err("a persistent conflict must surface");
    let msg = format!("{err}");
    assert!(
        msg.contains("CatalogCommitConflicts") || msg.contains("concurrent writer"),
        "error should be the commit conflict, got: {msg}"
    );
}
