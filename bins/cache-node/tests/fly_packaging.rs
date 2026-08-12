//! Enforces the Fly Machines packaging contract for independently scalable
//! cache and query roles.

use std::path::Path;

/// Returns the repository root for packaging assertions.
fn workspace() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root")
}

/// Reads one repository file as UTF-8 text.
fn read(path: &str) -> String {
    std::fs::read_to_string(workspace().join(path)).expect("packaging file")
}

#[test]
fn fly_images_are_role_specific_and_do_not_bootstrap_a_guest_os() {
    let dockerfile = read("Dockerfile");
    let cache = read("deploy/fly/cache-entrypoint.sh");
    let query = read("deploy/fly/query-entrypoint.sh");

    assert!(dockerfile.contains("AS verglas-fly-cache"));
    assert!(dockerfile.contains("AS verglas-fly-query"));
    assert!(dockerfile.contains("ENTRYPOINT [\"/usr/local/bin/verglas-fly-cache-entrypoint\"]"));
    assert!(dockerfile.contains("ENTRYPOINT [\"/usr/local/bin/verglas-fly-query-entrypoint\"]"));

    for forbidden in ["mkfs", "mount ", "e2fsck", "resize2fs", "systemd", "tini"] {
        assert!(
            !cache.contains(forbidden),
            "cache entrypoint contains {forbidden}"
        );
        assert!(
            !query.contains(forbidden),
            "query entrypoint contains {forbidden}"
        );
    }
    assert!(cache.contains("VERGLAS_CACHE_DIR:-/data/cache"));
    assert!(cache.contains("exec setpriv --reuid=verglas --regid=verglas --init-groups"));
    assert!(cache.contains("verglas-cache-node --config"));
    assert!(query.contains("VERGLAS_QUERY_SPILL_DIR:-/tmp/verglas-query-spill"));
    assert!(query.contains("exec verglas-query --config"));
}

#[test]
fn fly_cache_keeps_secrets_and_generated_config_off_persistent_nvme() {
    let cache = read("deploy/fly/cache-entrypoint.sh");

    assert!(cache.contains("RUNTIME_DIR=${VERGLAS_RUNTIME_DIR:-/run/verglas}"));
    assert!(cache.contains("BACKEND_CREDS=${RUNTIME_DIR}/backend-credentials"));
    assert!(cache.contains("ENDPOINT_CREDS=${RUNTIME_DIR}/endpoint-credentials"));
    assert!(!cache.contains("/data/backend-credentials"));
    assert!(!cache.contains("/data/endpoint-credentials"));
}
