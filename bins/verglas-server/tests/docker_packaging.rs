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
    assert!(
        dockerfile.contains("chown -R verglas:verglas /var/lib/verglas")
            && compose.contains("verglas-cache:/var/lib/verglas"),
        "the always-on KV log must use the existing writable persistent data volume"
    );
    assert!(!compose.contains("\n  postgres:"));
    assert!(!compose.contains("\n  rill:"));
    for forbidden in [
        "VERGLAS_BACKEND_BUCKET",
        "VERGLAS_BACKEND_ENDPOINT",
        "VERGLAS_BACKEND_REGION",
        "VERGLAS_CATALOG_URI",
        "VERGLAS_CATALOG_WAREHOUSE",
        "VERGLAS_CATALOG_BEARER_TOKEN",
        "VERGLAS_S3_ACCESS_KEY_ID",
        "VERGLAS_S3_SECRET_ACCESS_KEY",
        "VERGLAS_QUERY_WORKER_BINARY",
        "VERGLAS_WRITE_WORKER_BINARY",
        "R2_BUCKET",
        "R2_ENDPOINT",
        "R2_ACCESS_KEY_ID",
        "R2_SECRET_ACCESS_KEY",
        "R2_CATALOG_URI",
        "R2_CATALOG_WAREHOUSE",
        "R2_CATALOG_TOKEN",
    ] {
        assert!(
            !compose.contains(forbidden),
            "Compose must not hard-code the dynamic provider field {forbidden}"
        );
    }
    for required in [
        "VERGLAS_CACHE_CAPACITY",
        "VERGLAS_CACHE_DRAM",
        "VERGLAS_ACCESS_SERVICE_TOKEN",
    ] {
        assert!(
            compose.contains(required),
            "Compose must declare {required}"
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
        .split("  verglas-container-runtime:")
        .next()
        .expect("verglas-server service");
    assert!(
        server.contains("ulimits:\n      nofile:\n        soft: 8192\n        hard: 8192"),
        "verglas-server must cap soft and hard nofile at 8192"
    );
}

#[test]
fn docker_application_generates_and_persists_its_secret_encryption_key() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let compose = std::fs::read_to_string(workspace.join("docker-compose.yml"))
        .expect("read Docker Compose application");

    assert!(
        compose.contains("verglas-secret-key-init:")
            && compose.contains("verglas-secret-key:/run/verglas-secrets")
            && compose.contains("/run/verglas-secrets/encryption-key"),
        "Compose must generate the encryption key once and persist it outside provider config"
    );
    assert!(
        !compose.contains(&"0".repeat(64)),
        "Compose must not embed a fixed secret encryption key"
    );
}
