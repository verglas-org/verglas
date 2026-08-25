//! Static guard for the celld crate's supported supervision surface.
//!
//! This test is written before cleanup so removed Fly placement and obsolete
//! test-process artifacts cannot return as public API or build targets.

use std::fs;
use std::path::{Path, PathBuf};

/// Resolves one path relative to the celld crate.
fn celld_path(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(relative)
}

/// Reads one celld source or manifest file as UTF-8.
fn read(relative: &str) -> String {
    fs::read_to_string(celld_path(relative)).expect("celld source file exists")
}

/// Celld exports only the active local supervision and lifecycle surface.
#[test]
fn public_surface_has_no_retired_fly_provisioner() {
    let lib = read("src/lib.rs");
    for forbidden in [
        "mod fly;",
        "pub use fly::",
        "FlyAuthTokenSource",
        "FlyMachineSize",
        "FlyMachinesConfig",
        "FlyMachinesProvisioner",
    ] {
        assert!(!lib.contains(forbidden), "src/lib.rs retains {forbidden}");
    }
    assert!(
        !celld_path("src/fly.rs").exists(),
        "retired Fly provisioner module still exists"
    );
}

/// Celld has no obsolete test worker target or placeholder support module.
#[test]
fn obsolete_test_targets_are_deleted() {
    let manifest = read("Cargo.toml");
    assert!(
        !manifest.contains("verglas-celld-test-worker"),
        "Cargo.toml retains obsolete test worker target"
    );
    for relative in [
        "src/bin/verglas-celld-test-worker.rs",
        "tests/support/orchestration_worker.rs",
    ] {
        assert!(
            !celld_path(relative).exists(),
            "obsolete celld test artifact {relative} still exists"
        );
    }
}
