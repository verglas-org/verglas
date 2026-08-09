//! Database-scoped catalog routing for a tenant with multiple lakehouses.

use verglas_catalog::{
    CatalogBinding, CatalogBindingId, CatalogGateway, CatalogRegistry, CatalogRuntimeRegistry,
    DatabaseId, StorageBindingId,
};
use verglas_core::config::Config;

fn binding(database: &str, catalog: &str, storage: &str) -> CatalogBinding {
    CatalogBinding::new(
        DatabaseId::new(database).expect("database id"),
        CatalogBindingId::new(catalog).expect("catalog binding id"),
        StorageBindingId::new(storage).expect("storage binding id"),
        "https://catalog.example.com",
        Some("warehouse".to_owned()),
        None,
    )
    .expect("catalog binding")
}

#[test]
fn resolves_catalog_and_storage_by_database_not_process_global_config() {
    let registry = CatalogRegistry::default();
    registry
        .insert(binding("analytics", "managed-catalog", "managed-storage"))
        .expect("insert analytics");
    registry
        .insert(binding("customer_lake", "managed-catalog", "customer-s3"))
        .expect("insert customer lake");

    let analytics = registry.get(&DatabaseId::new("analytics").expect("id"));
    let customer = registry.get(&DatabaseId::new("customer_lake").expect("id"));
    assert_eq!(
        analytics.expect("analytics").storage_binding_id().as_str(),
        "managed-storage"
    );
    assert_eq!(
        customer.expect("customer").storage_binding_id().as_str(),
        "customer-s3"
    );
}

#[test]
fn duplicate_database_binding_fails_instead_of_silently_rebinding() {
    let registry = CatalogRegistry::default();
    registry
        .insert(binding("analytics", "catalog-a", "storage-a"))
        .expect("first insert");

    let error = registry
        .insert(binding("analytics", "catalog-b", "storage-b"))
        .expect_err("duplicate database must fail");
    assert!(error.to_string().contains("analytics"));
    assert_eq!(
        registry
            .get(&DatabaseId::new("analytics").expect("id"))
            .expect("original binding")
            .catalog_binding_id()
            .as_str(),
        "catalog-a"
    );
}

#[test]
fn invalid_identifiers_and_catalog_urls_fail_closed() {
    assert!(DatabaseId::new("").is_err());
    assert!(StorageBindingId::new(" ").is_err());
    let result = CatalogBinding::new(
        DatabaseId::new("analytics").expect("id"),
        CatalogBindingId::new("catalog").expect("id"),
        StorageBindingId::new("storage").expect("id"),
        "file:///tmp/catalog",
        None,
        None,
    );
    assert!(result.is_err());
}

/// Builds a gateway whose URI is only inspected as registry identity in this test.
fn gateway(label: &str) -> CatalogGateway {
    let config = Config::from_toml_str(&format!(
        "[cache]\ndir = '/tmp/verglas-catalog-registry-{label}'\ncapacity_bytes = '64MB'\n\
         [backend]\nbucket = 'test'\n[catalog]\nuri = 'https://{label}.example.com'\n"
    ))
    .expect("catalog config");
    CatalogGateway::from_config(config.catalog.as_ref().expect("catalog")).expect("gateway")
}

#[test]
fn runtime_refresh_atomically_replaces_added_and_deleted_databases() {
    let registry = CatalogRuntimeRegistry::default();
    registry
        .replace_all([
            (
                DatabaseId::new("analytics").expect("database"),
                gateway("analytics"),
            ),
            (
                DatabaseId::new("archive").expect("database"),
                gateway("archive"),
            ),
        ])
        .expect("initial refresh");

    registry
        .replace_all([
            (
                DatabaseId::new("analytics").expect("database"),
                gateway("analytics-next"),
            ),
            (
                DatabaseId::new("realtime").expect("database"),
                gateway("realtime"),
            ),
        ])
        .expect("replacement refresh");

    assert!(
        registry
            .get(&DatabaseId::new("archive").expect("database"))
            .is_none(),
        "deleted databases must disappear from the live routing set"
    );
    assert!(
        registry
            .get(&DatabaseId::new("realtime").expect("database"))
            .is_some(),
        "new databases must become routable"
    );
    assert_eq!(registry.len().expect("runtime count"), 2);
}
