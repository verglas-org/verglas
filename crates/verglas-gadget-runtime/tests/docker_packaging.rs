//! Packaging contract for the standalone Gadget runtime image.

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
fn compose_leaves_gadget_placement_to_the_container_runtime() {
    let compose = std::fs::read_to_string(repository_file("docker-compose.yml"))
        .expect("read repository Compose file");
    assert!(!compose.contains("\n  verglas-gadget-runtime:"));
}
