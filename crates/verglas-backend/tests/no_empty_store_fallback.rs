//! A configured backend must never degrade to an empty in-memory store (#233).
//!
//! Before this fix, a failed typed-client build silently fell back to an
//! `object_store::memory::InMemory`, so every read answered `NotFound`
//! (NoSuchKey) while the daemon looked healthy — every read was wrong. This test
//! pins the invariant: a build failure surfaces as an error, it is never hidden
//! behind an empty store.

use verglas_backend::{BackendError, BackendStore, BackendStores};
use verglas_core::config::{Backend, BackendProvider};

/// A build failure for the configured bucket must surface from `store_for`
/// rather than resolving to an empty in-memory store. An `http://` endpoint
/// without `allow_http` forces the S3 client build to fail deterministically;
/// the daemon must see that failure, not a store that answers NoSuchKey for
/// every GET.
#[test]
fn build_failure_does_not_fall_back_to_an_empty_in_memory_store() {
    let backend = Backend {
        provider: BackendProvider::S3,
        bucket: Some("lake".to_owned()),
        endpoint: Some("http://origin.invalid".to_owned()),
        region: Some("us-east-1".to_owned()),
        // http without allow_http is rejected when the client's settings resolve,
        // so build_typed fails — the exact spot the empty-store fallback lived.
        allow_http: false,
        ..Backend::default()
    };
    let store = BackendStore::from_config(&backend);
    let error = store
        .store_for("lake")
        .expect_err("a build failure must surface, not fall back to an empty in-memory store");
    assert!(
        matches!(error, BackendError::Build { .. }),
        "expected a build error, got {error:?}"
    );
}
