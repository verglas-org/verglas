//! Static dependency guard for the Turso runtime migration.
//!
//! This test keeps deleted CAS, replica, checkpoint, and engine authorities
//! from returning through an accidental runtime import or argument.

use std::fs;
use std::path::Path;

/// Reads one repository file relative to this crate.
fn read(relative: &str) -> Result<String, Box<dyn std::error::Error>> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(relative);
    Ok(fs::read_to_string(path)?)
}

/// Returns the workspace root that owns the runtime crate.
fn workspace_root() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Runtime manifests and sources contain no old engine authority symbols.
#[test]
fn runtime_has_no_old_engine_dependency_or_authority() -> Result<(), Box<dyn std::error::Error>> {
    let manifest = read("Cargo.toml")?;
    assert!(!manifest.contains("verglas-do-engine"));
    for relative in [
        "src/lib.rs",
        "src/worker_storage.rs",
        "src/event_endpoint.rs",
        "src/bin/verglas-runtime.rs",
    ] {
        let source = read(relative)?;
        for forbidden in [
            "verglas_do_engine",
            "CasCommitAuthority",
            "TransactionEnvelope",
        ] {
            assert!(
                !source.contains(forbidden),
                "{relative} contains {forbidden}"
            );
        }
    }
    Ok(())
}

/// The runtime binary has no deleted replica/CAS/offload argument names.
#[test]
fn runtime_has_no_deleted_durability_arguments() -> Result<(), Box<dyn std::error::Error>> {
    let source = read("src/bin/verglas-runtime.rs")?.to_ascii_lowercase();
    for forbidden in [
        "--replica",
        "--cas-",
        "--offload",
        "--checkpoint",
        "replicaendpoint",
    ] {
        assert!(
            !source.contains(forbidden),
            "verglas-runtime contains {forbidden}"
        );
    }
    Ok(())
}

/// The runtime is the only serving process and no clustered cache-node code remains.
#[test]
fn workspace_has_one_non_clustered_runtime_surface() -> Result<(), Box<dyn std::error::Error>> {
    let root = workspace_root();
    assert!(
        root.join("crates/verglas-runtime/src/bin/verglas-runtime.rs")
            .is_file(),
        "verglas-runtime binary is missing"
    );
    assert!(
        !root
            .join("crates/verglas-runtime/src/bin/verglasd.rs")
            .exists(),
        "retired verglasd binary source still exists"
    );
    assert!(
        !root.join("bins/cache-node").exists(),
        "retired cache-node source still exists"
    );
    assert!(
        !root.join("crates/verglas-s3/src/semantic.rs").exists(),
        "retired graph/vector semantic route still exists"
    );
    for retired_core_module in ["node.rs", "peer.rs", "ring.rs"] {
        assert!(
            !root
                .join("crates/verglas-core/src")
                .join(retired_core_module)
                .exists(),
            "retired clustered core module {retired_core_module} still exists"
        );
    }

    for retired_crate in [
        "verglas-block",
        "verglas-cluster",
        "verglas-consensus",
        "verglas-safekeeper",
        "verglas-writeback",
        "verglas-api",
        "verglas-graph",
        "verglas-vector",
        "verglas-catalog",
        "verglas-catalog-storage",
        "verglas-tables",
        "verglas-kv",
        "verglas-instance",
        "verglas-catalog-authz",
        "verglas-catalog-core",
        "verglas-catalog-io",
        "verglas-iceberg-ext",
    ] {
        assert!(
            !root
                .join("crates")
                .join(retired_crate)
                .join("Cargo.toml")
                .exists(),
            "retired crate {retired_crate} still exists"
        );
    }

    let workspace_manifest = fs::read_to_string(root.join("Cargo.toml"))?;
    let runtime_manifest = read("Cargo.toml")?;
    for forbidden in [
        "verglas-cache-node",
        "verglas-block",
        "verglas-cluster",
        "verglas-consensus",
        "verglas-safekeeper",
        "verglas-writeback",
        "verglas-api",
        "verglas-graph",
        "verglas-vector",
        "verglas-catalog-authz",
        "verglas-catalog-core",
        "verglas-catalog-io",
        "verglas-iceberg-ext",
        "iceberg-datafusion",
        "openraft",
    ] {
        assert!(
            !workspace_manifest.contains(forbidden),
            "workspace manifest contains {forbidden}"
        );
        assert!(
            !runtime_manifest.contains(forbidden),
            "runtime manifest contains {forbidden}"
        );
    }
    Ok(())
}

/// The Catalog capability writes immutable proposals and never owns a catalog head.
#[test]
fn catalog_capability_has_no_second_catalog_authority() -> Result<(), Box<dyn std::error::Error>> {
    let source =
        fs::read_to_string(workspace_root().join("crates/verglas-runtime/src/catalog_commit.rs"))?;
    for forbidden in [
        "Arc<dyn Catalog>",
        "TableCache",
        "commit_sink_batch",
        "open_catalog",
    ] {
        assert!(
            !source.contains(forbidden),
            "runtime Catalog capability retained second authority surface {forbidden}"
        );
    }
    Ok(())
}

/// The architecture source of truth contains no retired clustered runtime design.
#[test]
fn whitepaper_has_no_clustered_runtime_architecture() -> Result<(), Box<dyn std::error::Error>> {
    let whitepaper = fs::read_to_string(workspace_root().join("docs/architecture/whitepaper.mdx"))?;
    for forbidden in [
        "Multi-Raft",
        "ReadIndex",
        "coded Raft",
        "safekeeper",
        "write-back fragment",
        "authoritative ring",
    ] {
        assert!(
            !whitepaper.contains(forbidden),
            "whitepaper retains retired {forbidden} architecture"
        );
    }
    Ok(())
}

/// Unreferenced legacy API specifications cannot advertise retired products as active surfaces.
#[test]
fn docs_have_no_retired_standalone_api_surfaces() -> Result<(), Box<dyn std::error::Error>> {
    let root = workspace_root();
    for retired in [
        "docs/reference/openapi/management-open-api.yaml",
        "docs/reference/openapi/generic-table-open-api.yaml",
        "docs/reference/openapi/query-open-api.yaml",
    ] {
        assert!(
            !root.join(retired).exists(),
            "retired API remains: {retired}"
        );
    }
    let docs_config = fs::read_to_string(root.join("docs.json"))?;
    assert!(!docs_config.contains("self-hosted Verglas server"));
    Ok(())
}
