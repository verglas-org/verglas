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

    for retired_crate in [
        "verglas-block",
        "verglas-cluster",
        "verglas-consensus",
        "verglas-safekeeper",
        "verglas-writeback",
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
