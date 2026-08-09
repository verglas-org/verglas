//! Database create contracts for managed and customer-owned resources.

use verglas_database::{
    CreateDatabase, CreateDatabaseRequest, DatabaseKind, DatabasePlan, PlanError,
};

#[test]
fn managed_lakehouse_uses_tenant_lakekeeper_and_managed_storage() {
    let plan = CreateDatabase::new("analytics", DatabaseKind::Lakehouse)
        .plan("tenant-a")
        .expect("managed lakehouse");
    assert!(matches!(plan, DatabasePlan::Lakehouse(_)));
    let lake = plan.lakehouse().expect("lakehouse");
    assert!(lake.data_path().is_none());
    assert!(lake.catalog_uri().is_none());
    assert_eq!(lake.warehouse(), "analytics");
}

#[test]
fn managed_postgres_rejects_lakehouse_arguments() {
    let error = CreateDatabase::new("my_test_db", DatabaseKind::Postgres)
        .with_data_path("s3://customer-bucket/team")
        .plan("tenant-a")
        .expect_err("postgres must reject storage arguments");
    assert_eq!(error, PlanError::PostgresLakehouseOptions);
}

#[test]
fn byo_storage_uses_managed_lakekeeper() {
    let plan = CreateDatabase::new("customer_lake", DatabaseKind::Lakehouse)
        .with_data_path("s3://customer-bucket/team")
        .plan("tenant-a")
        .expect("BYO storage");
    let lake = plan.lakehouse().expect("lakehouse");
    assert_eq!(lake.data_path(), Some("s3://customer-bucket/team"));
    assert!(lake.catalog_uri().is_none());
}

#[test]
fn external_catalog_requires_explicit_warehouse() {
    let error = CreateDatabase::new("external_lake", DatabaseKind::Lakehouse)
        .with_data_path("s3://customer-bucket/team")
        .with_catalog("https://catalog.customer.com")
        .plan("tenant-a")
        .expect_err("external catalog without warehouse");
    assert_eq!(error, PlanError::ExternalCatalogNeedsWarehouse);
}

#[test]
fn external_catalog_and_storage_are_preserved_for_secret_resolution() {
    let plan = CreateDatabase::new("external_lake", DatabaseKind::Lakehouse)
        .with_data_path("s3://customer-bucket/team")
        .with_catalog("https://catalog.customer.com")
        .with_warehouse("customer_warehouse")
        .plan("tenant-a")
        .expect("external lakehouse");
    let lake = plan.lakehouse().expect("lakehouse");
    assert_eq!(lake.data_path(), Some("s3://customer-bucket/team"));
    assert_eq!(lake.catalog_uri(), Some("https://catalog.customer.com"));
    assert_eq!(lake.warehouse(), "customer_warehouse");
}

#[test]
fn cli_wire_contract_deserializes_to_the_same_plan() {
    let request: CreateDatabaseRequest = serde_json::from_value(serde_json::json!({
        "name": "external_lake",
        "type": "lakehouse",
        "storage": {
            "mode": "scoped-secret",
            "data_path": "s3://customer-bucket/team"
        },
        "catalog": {
            "mode": "external",
            "uri": "https://catalog.customer.com",
            "warehouse": "customer_warehouse"
        }
    }))
    .expect("wire request");
    let plan = request.plan("tenant-a").expect("valid plan");
    assert_eq!(
        plan.lakehouse().expect("lakehouse").warehouse(),
        "customer_warehouse"
    );
}
