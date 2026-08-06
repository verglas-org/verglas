//! Packaging contract for the standalone Gadget runtime image and Compose service.

/// Resolves a repository file from this crate's manifest directory.
fn repository_file(name: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(name)
}

#[test]
fn dockerfile_builds_the_runtime_binary_and_bun_host() {
    let dockerfile =
        std::fs::read_to_string(repository_file("Dockerfile")).expect("read repository Dockerfile");
    assert!(dockerfile.contains("-p verglas-gadget-runtime"));
    assert!(dockerfile.contains("AS verglas-gadget-runtime"));
    assert!(dockerfile.contains("WORKDIR /opt/verglas-gadget-runtime"));
    assert!(dockerfile.contains("sdks/typescript"));
    assert!(dockerfile.contains("runtime/host.mjs"));
    assert!(dockerfile.contains("runtime/verglas-env.mjs"));
    assert!(dockerfile.contains("ENTRYPOINT [\"verglas-gadget-runtime\"]"));
}

#[test]
fn compose_starts_one_multi_gadget_runtime_by_default() {
    let compose = std::fs::read_to_string(repository_file("docker-compose.yml"))
        .expect("read repository Compose file");
    let service = compose
        .split("  verglas-gadget-runtime:")
        .nth(1)
        .expect("Gadget runtime service");
    let service = service
        .lines()
        .take_while(|line| line.is_empty() || line.starts_with("    "))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(service.contains("target: verglas-gadget-runtime"));
    assert!(service.contains("8350:8350"));
    assert!(service.contains("VERGLAS_GADGET_MAX_GADGETS"));
    assert!(service.contains("VERGLAS_GADGET_DATA_ENDPOINT: http://verglas-server:8334"));
    assert!(service.contains("VERGLAS_GADGET_DATA_TOKEN: ${VERGLAS_S3_SECRET_ACCESS_KEY"));
    assert!(!service.contains("VERGLAS_GADGET_KV_ENDPOINT"));
    assert!(!service.contains("VERGLAS_GADGET_KV_TOKEN"));
    assert!(!service.lines().any(|line| line.trim() == "profiles:"));
}

#[test]
fn host_uses_a_valid_exact_kv_namespace_for_each_gadget() {
    let host = std::fs::read_to_string(repository_file(
        "crates/verglas-gadget-runtime/runtime/host.mjs",
    ))
    .expect("read Gadget host");

    assert!(host.contains("`gadget.${gadgetId}`"));
    assert!(!host.contains("`gadget/${gadgetId}`"));
    assert!(host.contains("delete process.env.VERGLAS_GADGET_CAPABILITY_TOKEN"));
    assert!(!host.contains("VERGLAS_GADGET_DATA_TOKEN"));
}

#[test]
fn supervisor_does_not_inherit_control_process_credentials() {
    let supervisor = std::fs::read_to_string(repository_file(
        "crates/verglas-gadget-runtime/src/supervisor.rs",
    ))
    .expect("read Gadget supervisor");
    assert!(supervisor.contains(".env_clear()"));
}
