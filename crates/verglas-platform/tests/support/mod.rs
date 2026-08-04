//! A hermetic Iceberg catalog for the verglas-platform registry tests: a SQLite-backed SQL
//! catalog over a temp file with a local-filesystem warehouse. No docker, no
//! network. The `TempDir` guards live in [`TestCatalog`] so the files survive
//! the test's lifetime and are removed on drop.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::io::LocalFsStorageFactory;
use iceberg::{Catalog, CatalogBuilder};
use iceberg_catalog_sql::{
    SQL_CATALOG_PROP_BIND_STYLE, SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlBindStyle,
    SqlCatalogBuilder,
};
use tempfile::TempDir;

/// A hermetic catalog plus the temp dirs backing it.
pub struct TestCatalog {
    /// The catalog under test, as a trait object the store accepts.
    pub catalog: Arc<dyn Catalog>,
    _warehouse: TempDir,
    _db_dir: TempDir,
}

impl TestCatalog {
    /// Builds a fresh hermetic catalog with its own temp warehouse and empty
    /// sqlite database.
    pub async fn new() -> TestCatalog {
        let warehouse = TempDir::new().expect("warehouse temp dir");
        let db_dir = TempDir::new().expect("sqlite temp dir");

        let db_path = db_dir.path().join("catalog.db");
        let uri = format!("sqlite:{}?mode=rwc", db_path.display());
        let warehouse_uri = format!("file://{}", warehouse.path().display());

        let catalog = SqlCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "platform-test",
                HashMap::from_iter([
                    (SQL_CATALOG_PROP_URI.to_string(), uri),
                    (SQL_CATALOG_PROP_WAREHOUSE.to_string(), warehouse_uri),
                    (
                        SQL_CATALOG_PROP_BIND_STYLE.to_string(),
                        SqlBindStyle::QMark.to_string(),
                    ),
                ]),
            )
            .await
            .expect("build hermetic sqlite catalog");

        TestCatalog {
            catalog: Arc::new(catalog),
            _warehouse: warehouse,
            _db_dir: db_dir,
        }
    }
}
