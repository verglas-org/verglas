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
    assert!(dockerfile.contains("runtime/host.mjs ./host.mjs"));
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
    assert!(!service.lines().any(|line| line.trim() == "profiles:"));
}
