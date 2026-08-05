//! Verifies that the self-hosted Docker application includes and configures
//! every execution role required by the server's fail-closed dispatchers.

use std::path::Path;

#[test]
fn docker_application_packages_execution_workers() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let dockerfile =
        std::fs::read_to_string(workspace.join("Dockerfile")).expect("read workspace Dockerfile");
    let compose = std::fs::read_to_string(workspace.join("docker-compose.yml"))
        .expect("read Docker Compose application");

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
        dockerfile.contains("ENTRYPOINT [\"verglas-server\", \"--environment\"]"),
        "the container must load its server configuration from Compose"
    );
    assert!(
        !dockerfile.contains("config.toml") && !compose.contains("config.toml"),
        "the Docker application must not copy or mount a server config.toml"
    );
    assert!(
        !compose.contains("deploy/docker/credentials"),
        "the quickstart must not require a separate credentials directory"
    );
    for variable in [
        "VERGLAS_BACKEND_BUCKET",
        "VERGLAS_BACKEND_ENDPOINT",
        "VERGLAS_BACKEND_REGION",
        "VERGLAS_CACHE_CAPACITY",
        "VERGLAS_CACHE_DRAM",
        "VERGLAS_CATALOG_URI",
        "VERGLAS_CATALOG_WAREHOUSE",
        "VERGLAS_CATALOG_BEARER_TOKEN",
        "VERGLAS_S3_ACCESS_KEY_ID",
        "VERGLAS_S3_SECRET_ACCESS_KEY",
        "VERGLAS_QUERY_WORKER_BINARY",
        "VERGLAS_WRITE_WORKER_BINARY",
        "VERGLAS_RILL_URI",
        "VERGLAS_RILL_BROWSER_URI",
        "VERGLAS_RILL_S3_URI",
    ] {
        assert!(
            compose.contains(variable),
            "Compose must declare {variable}"
        );
    }
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
