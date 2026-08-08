//! Packaging contract for the trusted local container runtime manager.

/// Resolves a repository file from this crate's manifest directory.
fn repository_file(name: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(name)
}

#[test]
fn dockerfile_builds_the_runtime_manager() {
    let dockerfile =
        std::fs::read_to_string(repository_file("Dockerfile")).expect("read repository Dockerfile");

    assert!(dockerfile.contains("-p verglas-container-runtime"));
    assert!(dockerfile.contains("AS verglas-container-runtime"));
    assert!(dockerfile.contains(
        "/src/target/release/verglas-container-runtime /usr/local/bin/verglas-container-runtime"
    ));
    assert!(dockerfile.contains("FROM runtime AS verglas-container-runtime"));
    assert!(
        dockerfile
            .contains("/src/target/release/verglas-scheduler /usr/local/bin/verglas-scheduler")
    );
    assert!(!dockerfile.contains("FROM verglas-gadget-runtime AS verglas-container-runtime"));
    assert!(!dockerfile.contains("verglas-gadget-runtime"));
    assert!(!dockerfile.contains("gadget-host"));
    assert!(dockerfile.contains("COPY --from=oven/bun:1.3.8 /usr/local/bin/bun"));
    assert!(dockerfile.contains("FROM oven/bun:1.3.8 AS verglas-integration-runtime"));
    assert!(dockerfile.contains("FROM oven/bun:1.3.8 AS verglas-application-runtime"));
    assert!(dockerfile.contains("ENTRYPOINT [\"verglas-container-runtime\"]"));
    assert!(dockerfile.contains("AS verglas-integration-runtime"));
    assert!(dockerfile.contains("AS verglas-application-runtime"));
    assert!(dockerfile.contains("COPY sdks/typescript/src ./sdk"));
}

#[test]
fn compose_bootstraps_only_server_and_runtime_manager() {
    let compose = std::fs::read_to_string(repository_file("docker-compose.yml"))
        .expect("read repository Compose file");
    let services_block = compose
        .split("services:\n")
        .nth(1)
        .expect("services section")
        .split("\nvolumes:")
        .next()
        .expect("services body");
    let services = services_block
        .lines()
        .filter(|line| {
            line.starts_with("  ") && !line.starts_with("    ") && line.trim_end().ends_with(':')
        })
        .map(str::trim)
        .collect::<Vec<_>>();

    assert_eq!(services, ["verglas-server:", "verglas-container-runtime:"]);
    assert_eq!(compose.matches("/var/run/docker.sock").count(), 2);
    assert!(compose.contains("verglas-runtime-state:/var/lib/verglas-container-runtime"));
    assert!(compose.contains("name: verglas-runtime"));
    assert!(
        compose.contains("VERGLAS_CONTAINER_RUNTIME_URL: http://verglas-container-runtime:8360")
    );
}
