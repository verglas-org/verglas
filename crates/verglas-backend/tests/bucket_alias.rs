//! Logical per-object bucket aliases over globally unique provider buckets.

use std::sync::Arc;

use object_store::memory::InMemory;
use verglas_backend::{BackendStore, BackendStores, BucketAliasStores};

/// A compiled Worker may use one stable logical bucket while each cloud object
/// is isolated in a different physical provider bucket.
#[test]
fn resolves_only_the_declared_logical_bucket() {
    let physical = "verglas-physical-object-bucket";
    let backing: Arc<dyn BackendStores> =
        BackendStore::single("catalog-origin", physical, Arc::new(InMemory::new()));
    let aliased: Arc<dyn BackendStores> = Arc::new(BucketAliasStores::new(
        backing,
        "catalog-origin",
        "lake",
        physical,
    ));

    assert!(aliased.store_for("catalog-origin", "lake").is_ok());
    assert!(aliased.store_for("catalog-origin", physical).is_err());
    assert!(aliased.store_for("other", "lake").is_err());
}
