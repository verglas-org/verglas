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

/// Runtime manifests and sources contain no old engine authority symbols.
#[test]
fn runtime_has_no_old_engine_dependency_or_authority() -> Result<(), Box<dyn std::error::Error>> {
    let manifest = read("Cargo.toml")?;
    assert!(!manifest.contains("verglas-do-engine"));
    for relative in [
        "src/lib.rs",
        "src/worker_storage.rs",
        "src/event_endpoint.rs",
        "src/bin/verglasd.rs",
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

/// The new binary source has no deleted replica/CAS/offload argument names.
#[test]
fn verglasd_has_no_deleted_durability_arguments() -> Result<(), Box<dyn std::error::Error>> {
    let source = read("src/bin/verglasd.rs")?.to_ascii_lowercase();
    for forbidden in [
        "--replica",
        "--cas-",
        "--offload",
        "--checkpoint",
        "replicaendpoint",
    ] {
        assert!(!source.contains(forbidden), "verglasd contains {forbidden}");
    }
    Ok(())
}
