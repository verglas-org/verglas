//! Database-scoped catalog routing for a tenant with multiple lakehouses.

use verglas_catalog::{
    CatalogBinding, CatalogBindingId, CatalogRegistry, DatabaseId, StorageBindingId,
};

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
