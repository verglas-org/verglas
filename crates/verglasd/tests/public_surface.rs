//! Static guard for the verglasd crate's supported supervision surface.
//!
//! This test is written before cleanup so removed Fly placement and obsolete
//! test-process artifacts cannot return as public API or build targets.

use std::fs;
use std::path::{Path, PathBuf};

/// Resolves one path relative to the verglasd crate.
fn daemon_path(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(relative)
}

/// Reads one verglasd source or manifest file as UTF-8.
fn read(relative: &str) -> String {
    fs::read_to_string(daemon_path(relative)).expect("verglasd source file exists")
}

/// Verglasd exports only the active local supervision and lifecycle surface.
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
        !daemon_path("src/fly.rs").exists(),
        "retired Fly provisioner module still exists"
    );
}

/// Verglasd has no obsolete test worker target or placeholder support module.
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
            !daemon_path(relative).exists(),
            "obsolete supervisor test artifact {relative} still exists"
        );
    }
}

/// The supervisor package and executable use the unambiguous Verglas daemon name.
#[test]
fn supervisor_is_named_verglasd_without_an_old_alias() {
    let manifest = read("Cargo.toml");
    assert!(manifest.contains("name = \"verglasd\""));
    assert!(daemon_path("src/bin/verglasd.rs").is_file());
    assert!(!daemon_path("src/bin/verglas-celld.rs").exists());

    let workspace = daemon_path("../..");
    assert!(!workspace.join("crates/verglas-celld").exists());
    let workspace_manifest =
        fs::read_to_string(workspace.join("Cargo.toml")).expect("workspace manifest exists");
    assert!(!workspace_manifest.contains("crates/verglas-celld"));
    let gateway =
        fs::read_to_string(workspace.join("crates/verglas-gateway/src/bin/verglas-gateway.rs"))
            .expect("gateway source file exists");
    assert!(gateway.contains("--verglasd-control"));
    assert!(!gateway.contains("--celld-control"));
}

/// The host daemon exposes one explicit operator path for Catalog runtime startup.
#[test]
fn host_configuration_declares_catalog_host_config_option() {
    let host = read("src/bin/verglasd.rs");
    assert!(host.contains("--catalog-host-config"));
}
