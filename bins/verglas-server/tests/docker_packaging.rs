//! Verifies that the self-hosted Docker application includes and configures
//! every execution role required by the server's fail-closed dispatchers.

use std::path::Path;

#[test]
fn docker_application_packages_execution_workers() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let dockerfile =
        std::fs::read_to_string(workspace.join("Dockerfile")).expect("read workspace Dockerfile");
    let config = std::fs::read_to_string(workspace.join("deploy/docker/verglas.toml"))
        .expect("read Docker config");

    for package in ["verglas-server", "verglas-query", "verglas-write-node"] {
        assert!(
            dockerfile.contains(&format!("-p {package}")),
            "Docker build must compile {package}"
        );
    }
    for binary in ["verglas-server", "verglas-query", "verglas-write"] {
        assert!(
            dockerfile.contains(&format!(
                "/src/target/release/{binary} /usr/local/bin/{binary}"
            )),
            "Docker image must install {binary}"
        );
    }
    assert!(
        config.contains("[query_worker]\nbinary = \"/usr/local/bin/verglas-query\""),
        "Docker config must enable the query worker"
    );
    assert!(
        config.contains("[write_worker]\nbinary = \"/usr/local/bin/verglas-write\""),
        "Docker config must enable the write worker"
    );
}

/// #19: the self-hosted container must fail inside its own resource boundary
/// before an FD leak can exhaust the host-wide file table.
#[test]
fn docker_application_caps_server_file_descriptors() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let compose = std::fs::read_to_string(workspace.join("docker-compose.yml"))
        .expect("read Docker Compose application");

    let server = compose
        .split("  verglas-scheduler:")
        .next()
        .expect("verglas-server service");
    assert!(
        server.contains("ulimits:\n      nofile:\n        soft: 8192\n        hard: 8192"),
        "verglas-server must cap soft and hard nofile at 8192"
    );
}
