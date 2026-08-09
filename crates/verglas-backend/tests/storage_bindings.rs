//! Multi-storage-binding routing tests for issue #84.

use std::sync::Arc;

use object_store::memory::InMemory;
use verglas_backend::{
    BackendError, BackendRegistry, BackendStore, BackendStores, MultipartObjectStore,
};

#[test]
fn routes_identical_bucket_names_to_independent_binding_clients() {
    let managed_origin: Arc<dyn MultipartObjectStore> = Arc::new(InMemory::new());
    let customer_origin: Arc<dyn MultipartObjectStore> = Arc::new(InMemory::new());
    let managed = BackendStore::single("managed", "lake", managed_origin);
    let customer = BackendStore::single("customer", "lake", customer_origin);
    let registry = BackendRegistry::new(vec![managed, customer]).expect("valid bindings");

    let managed_registry = registry.clone();
    let managed = std::thread::spawn(move || {
        managed_registry
            .store_for("managed", "lake")
            .expect("managed binding resolves")
    });
    let customer_registry = registry.clone();
    let customer = std::thread::spawn(move || {
        customer_registry
            .store_for("customer", "lake")
            .expect("customer binding resolves")
    });
    let managed_client = managed.join().expect("managed routing thread");
    let customer_client = customer.join().expect("customer routing thread");

    assert!(!Arc::ptr_eq(&managed_client, &customer_client));
    assert!(!Arc::ptr_eq(
        &registry
            .breaker_for("managed", "lake")
            .expect("managed binding")
            .expect("managed breaker"),
        &registry
            .breaker_for("customer", "lake")
            .expect("customer binding")
            .expect("customer breaker"),
    ));
}

#[test]
fn unknown_and_duplicate_bindings_fail_closed() {
    let managed = BackendStore::single("managed", "lake", Arc::new(InMemory::new()));
    let registry = BackendRegistry::new(vec![managed.clone()]).expect("valid binding");
    assert!(matches!(
        registry.store_for("missing", "lake"),
        Err(BackendError::UnknownStorageBinding { .. })
    ));

    let duplicate = BackendRegistry::new(vec![managed.clone(), managed]);
    assert!(matches!(
        duplicate,
        Err(BackendError::DuplicateStorageBinding { .. })
    ));

    let malformed = BackendStore::single("", "lake", Arc::new(InMemory::new()));
    assert!(matches!(
        BackendRegistry::new(vec![malformed]),
        Err(BackendError::InvalidStorageBinding { .. })
    ));
}

#[test]
fn binding_insert_and_remove_take_effect_without_restart() {
    let registry = BackendRegistry::new(Vec::new()).expect("empty dynamic registry");
    assert!(matches!(
        registry.store_for("customer", "lake"),
        Err(BackendError::UnknownStorageBinding { .. })
    ));

    registry
        .insert(BackendStore::single(
            "customer",
            "lake",
            Arc::new(InMemory::new()),
        ))
        .expect("insert customer binding");
    registry
        .store_for("customer", "lake")
        .expect("new binding routes immediately");
    assert!(matches!(
        registry.insert(BackendStore::single(
            "customer",
            "lake",
            Arc::new(InMemory::new()),
        )),
        Err(BackendError::DuplicateStorageBinding { .. })
    ));

    registry.remove("customer").expect("remove binding");
    assert!(matches!(
        registry.store_for("customer", "lake"),
        Err(BackendError::UnknownStorageBinding { .. })
    ));
    assert!(matches!(
        registry.remove("customer"),
        Err(BackendError::UnknownStorageBinding { .. })
    ));
}
