//! Acceptance for the unified deployment record (#329): a locally-registered
//! `verglas_sys` worker row projects to the canonical [`Deployment`] shape.

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::io::LocalFsStorageFactory;
use iceberg::{Catalog, CatalogBuilder};
use iceberg_catalog_sql::{
    SQL_CATALOG_PROP_BIND_STYLE, SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlBindStyle,
    SqlCatalogBuilder,
};
use tempfile::TempDir;
use verglas_platform::{Deployment, SystemCatalog, WorkerSpec};

/// A hermetic SQLite catalog with a local-filesystem warehouse.
async fn hermetic_catalog() -> (Arc<dyn Catalog>, TempDir, TempDir) {
    let warehouse = TempDir::new().expect("warehouse temp dir");
    let db_dir = TempDir::new().expect("sqlite temp dir");
    let db_path = db_dir.path().join("catalog.db");
    let uri = format!("sqlite:{}?mode=rwc", db_path.display());
    let warehouse_uri = format!("file://{}", warehouse.path().display());
    let catalog = SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "parity-test",
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
    (Arc::new(catalog), warehouse, db_dir)
}

/// A registered worker row projects to the unified deployment record.
#[tokio::test]
async fn local_worker_row_projects_to_deployment_record() {
    let (catalog, _wh, _db) = hermetic_catalog().await;
    let sys = SystemCatalog::new(catalog);

    let local_row = sys
        .register_worker(WorkerSpec {
            name: "flow_alerts".to_owned(),
            code: "bun ingest/flow-alerts.ts".to_owned(),
            triggers: r#"[{"type":"cron","schedule":"*/5 * * * *"}]"#.to_owned(),
            output: Some("acme.flow_alerts".to_owned()),
            config: r#"{"symbol":"SPY"}"#.to_owned(),
            created_by: "operator".to_owned(),
        })
        .await
        .expect("register worker");
    let local = Deployment::from_worker(&local_row);

    assert_eq!(local.kind, "worker");
    assert_eq!(local.name, "flow_alerts");
    assert_eq!(local.trigger, "cron");
    assert_eq!(local.placement, "local");
    assert_eq!(local.code, "bun ingest/flow-alerts.ts");
    assert_eq!(local.schedule, None);
    assert_eq!(local.target_tables, vec!["acme.flow_alerts".to_owned()]);
    assert_eq!(local.status, "running");
    assert_eq!(local.config, r#"{"symbol":"SPY"}"#);
    assert_eq!(local.created_at, local_row.created_at);
}
