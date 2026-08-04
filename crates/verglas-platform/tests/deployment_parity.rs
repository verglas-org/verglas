//! Acceptance for the unified deployment record (#329): a locally-registered
//! `verglas_sys` worker row and a cloud D1 `deployments` row project to the
//! same [`Deployment`] shape.

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::io::LocalFsStorageFactory;
use iceberg::{Catalog, CatalogBuilder};
use iceberg_catalog_sql::{
    SQL_CATALOG_PROP_BIND_STYLE, SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlBindStyle,
    SqlCatalogBuilder,
};
use serde_json::json;
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

/// Projects a cloud D1 `deployments` row into the unified `Deployment`.
fn deployment_from_d1(row: &serde_json::Value) -> Deployment {
    let target_tables: Vec<String> =
        serde_json::from_str(row["target_tables"].as_str().expect("target_tables"))
            .expect("target_tables JSON array");
    Deployment {
        kind: row["kind"].as_str().expect("kind").to_owned(),
        name: row["name"].as_str().expect("name").to_owned(),
        trigger: row["trigger"].as_str().expect("trigger").to_owned(),
        placement: row["placement"].as_str().expect("placement").to_owned(),
        code: row["code"].as_str().expect("code").to_owned(),
        schedule: row["schedule"].as_str().map(str::to_owned),
        target_tables,
        status: row["status"].as_str().expect("status").to_owned(),
        config: row["config"].as_str().expect("config").to_owned(),
        created_at: row["created_at"]
            .as_str()
            .expect("created_at")
            .parse()
            .expect("created_at timestamp"),
    }
}

/// A local worker row and a cloud deployments row project to the same record.
#[tokio::test]
async fn local_and_cloud_rows_share_one_record_shape() {
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

    let d1 = json!({
        "kind": "worker",
        "name": "flow_alerts",
        "trigger": "cron",
        "placement": "local",
        "code": "bun ingest/flow-alerts.ts",
        "schedule": null,
        "target_tables": "[\"acme.flow_alerts\"]",
        "status": "running",
        "config": "{\"symbol\":\"SPY\"}",
        "created_at": local_row.created_at.to_rfc3339(),
    });
    let cloud = deployment_from_d1(&d1);

    assert_eq!(local, cloud, "local and cloud rows project to one record");
    assert_eq!(
        serde_json::to_value(&local).expect("serialize local"),
        serde_json::to_value(&cloud).expect("serialize cloud"),
        "the two projections serialize to identical JSON",
    );
    assert_eq!(local.target_tables, vec!["acme.flow_alerts".to_owned()]);
}
