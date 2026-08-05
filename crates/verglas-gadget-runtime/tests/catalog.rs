//! Contract tests for local multi-Gadget and cloud single-Gadget registration.

use std::collections::BTreeMap;

use verglas_gadget_runtime::{
    GadgetBundle, RegisterOutcome, RuntimeCatalog, RuntimeConfig, RuntimeError,
};

/// Builds the smallest valid bundle for one immutable code revision.
fn bundle(version: &str, message: &str) -> GadgetBundle {
    GadgetBundle {
        version: version.to_owned(),
        server_module: format!("export class Gadget {{ message() {{ return {message:?}; }} }}"),
        client_module: "export default {};".to_owned(),
        files: BTreeMap::new(),
    }
}

#[test]
fn local_runtime_registers_multiple_gadgets() {
    let mut catalog = RuntimeCatalog::new(RuntimeConfig::local(8)).expect("local catalog");

    assert!(matches!(
        catalog.register("alpha", bundle("1", "alpha")),
        Ok(RegisterOutcome::Created { .. })
    ));
    assert!(matches!(
        catalog.register("beta", bundle("1", "beta")),
        Ok(RegisterOutcome::Created { .. })
    ));

    assert_eq!(catalog.list().len(), 2);
    assert_eq!(
        catalog.get("alpha").map(|entry| entry.id.as_str()),
        Some("alpha")
    );
    assert_eq!(
        catalog.get("beta").map(|entry| entry.id.as_str()),
        Some("beta")
    );
}

#[test]
fn cloud_runtime_accepts_only_the_configured_gadget() {
    let mut catalog =
        RuntimeCatalog::new(RuntimeConfig::single("workspace-7-gadget-3")).expect("cloud catalog");

    assert!(
        catalog
            .register("workspace-7-gadget-3", bundle("1", "allowed"))
            .is_ok()
    );
    assert!(matches!(
        catalog.register("another-gadget", bundle("1", "denied")),
        Err(RuntimeError::TargetMismatch { .. })
    ));
}

#[test]
fn an_identical_revision_is_idempotent_but_conflicting_bytes_are_rejected() {
    let mut catalog = RuntimeCatalog::new(RuntimeConfig::local(2)).expect("catalog");
    let original = bundle("revision-a", "same");

    let created = catalog.register("alpha", original.clone()).expect("create");
    let existing = catalog
        .register("alpha", original)
        .expect("idempotent register");
    assert!(matches!(created, RegisterOutcome::Created { .. }));
    assert!(matches!(existing, RegisterOutcome::Unchanged { .. }));

    assert!(matches!(
        catalog.register("alpha", bundle("revision-a", "different")),
        Err(RuntimeError::RevisionConflict { .. })
    ));
}

#[test]
fn a_new_revision_replaces_the_selected_bundle() {
    let mut catalog = RuntimeCatalog::new(RuntimeConfig::local(2)).expect("catalog");
    catalog
        .register("alpha", bundle("1", "old"))
        .expect("first revision");

    assert!(matches!(
        catalog.register("alpha", bundle("2", "new")),
        Ok(RegisterOutcome::Replaced { previous_version, .. }) if previous_version == "1"
    ));
    assert_eq!(
        catalog.get("alpha").map(|entry| entry.version.as_str()),
        Some("2")
    );
}

#[test]
fn registration_enforces_capacity_and_safe_bundle_paths() {
    let mut catalog = RuntimeCatalog::new(RuntimeConfig::local(1)).expect("catalog");
    catalog
        .register("alpha", bundle("1", "one"))
        .expect("first gadget");
    assert!(matches!(
        catalog.register("beta", bundle("1", "two")),
        Err(RuntimeError::Capacity { maximum: 1 })
    ));

    let mut unsafe_bundle = bundle("2", "unsafe");
    unsafe_bundle
        .files
        .insert("../outside.js".to_owned(), "export default 1".to_owned());
    assert!(matches!(
        catalog.register("alpha", unsafe_bundle),
        Err(RuntimeError::InvalidBundlePath { .. })
    ));

    let mut reserved_bundle = bundle("2", "reserved");
    reserved_bundle.files.insert(
        "cloudflare-workers.mjs".to_owned(),
        "export const compromised = true".to_owned(),
    );
    assert!(matches!(
        catalog.register("alpha", reserved_bundle),
        Err(RuntimeError::ReservedBundlePath { .. })
    ));
}

#[test]
fn identifiers_are_safe_for_routes_namespaces_and_directories() {
    let mut catalog = RuntimeCatalog::new(RuntimeConfig::local(2)).expect("catalog");

    for invalid in ["", ".", "../other", "with/slash", "white space", "UPPER"] {
        assert!(matches!(
            catalog.register(invalid, bundle("1", "invalid")),
            Err(RuntimeError::InvalidGadgetId { .. })
        ));
    }
}
