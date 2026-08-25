//! Public-surface guard for the strict S3 cache frontend.
//!
//! This test is intentionally written before the cleanup. It keeps semantic
//! graph, vector, and Iceberg mutation routes out of the protocol crate.

use std::fs;
use std::path::Path;

/// Reads one file from this crate's source tree.
fn read(relative: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(relative);
    fs::read_to_string(path).expect("S3 source file exists")
}

/// The S3 crate contains only auth, frontend, and passthrough adapters.
#[test]
fn s3_public_surface_has_no_semantic_mutation_routes() {
    let lib = read("src/lib.rs");
    let manifest = read("Cargo.toml");
    for forbidden in [
        "pub mod semantic",
        "verglas-graph",
        "verglas-vector",
        "verglas-iceberg",
        "iceberg.workspace",
    ] {
        assert!(!lib.contains(forbidden), "lib.rs retains {forbidden}");
        assert!(
            !manifest.contains(forbidden),
            "Cargo.toml retains {forbidden}"
        );
    }
    assert!(
        !Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/semantic.rs")
            .exists(),
        "semantic route still exists"
    );
}

/// The frontend does not dispatch semantic, graph, vector, or Iceberg writes.
#[test]
fn s3_frontend_has_no_semantic_dispatch() {
    let frontend = read("src/frontend.rs");
    for forbidden in [
        "semantic_router",
        "SemanticApi",
        "verglas_graph",
        "verglas_vector",
        "publish_event",
    ] {
        assert!(
            !frontend.contains(forbidden),
            "frontend.rs retains {forbidden}"
        );
    }
}
