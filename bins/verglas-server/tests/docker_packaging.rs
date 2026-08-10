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
    let postgres_runtime =
        std::fs::read_to_string(workspace.join("bins/access-node/src/postgres_runtime.rs"))
            .expect("read managed Postgres runtime");

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
    for variable in [
        "VERGLAS_BACKEND_BUCKET",
        "VERGLAS_BACKEND_ENDPOINT",
        "VERGLAS_BACKEND_REGION",
        "VERGLAS_CACHE_CAPACITY",
        "VERGLAS_CACHE_DRAM",
        "VERGLAS_ACCESS_URI",
        "VERGLAS_INITIAL_OWNER_EMAIL",
        "VERGLAS_TOKEN_SIGNING_KEY",
        "VERGLAS_TARGET_JWT_SIGNING_KEY",
        "VERGLAS_IDENTITY_ASSERTION_KEY",
        "VERGLAS_ACCESS_TOKEN_FILE",
        "VERGLAS_MANAGED_CATALOG_URI",
        "VERGLAS_S3_ACCESS_KEY_ID",
        "VERGLAS_S3_SECRET_ACCESS_KEY",
        "VERGLAS_QUERY_WORKER_BINARY",
        "VERGLAS_WRITE_WORKER_BINARY",
    ] {
        assert!(
            compose.contains(variable),
            "Compose must declare {variable}"
        );
    }
    for singleton in [
        "VERGLAS_CATALOG_URI",
        "VERGLAS_CATALOG_WAREHOUSE",
        "VERGLAS_CATALOG_BEARER_TOKEN",
    ] {
        assert!(
            !compose.contains(singleton),
            "Compose must not declare singleton catalog variable {singleton}"
        );
    }
    for removed_variable in [
        "VERGLAS_ACCESS_SERVICE_TOKEN",
        "VERGLAS_LOCAL_OWNER_BOOTSTRAP",
        "verglas-local-access",
    ] {
        assert!(
            !compose.contains(removed_variable),
            "Compose must not retain the static authorization bypass {removed_variable}"
        );
    }
    let os_service = compose
        .split("  verglas-os:\n")
        .nth(1)
        .expect("Verglas OS service")
        .split("\nvolumes:")
        .next()
        .expect("Verglas OS service body");
    assert!(
        !os_service.contains("VERGLAS_DATA_TOKEN"),
        "Verglas OS must exchange signed user assertions for scoped tokens instead of receiving a static data token"
    );
    assert!(
        os_service.contains(
            "VERGLAS_INITIAL_OWNER_EMAIL: ${VERGLAS_INITIAL_OWNER_EMAIL:?Set VERGLAS_INITIAL_OWNER_EMAIL to the first tenant owner email}"
        ),
        "Verglas OS must derive its ADMINS binding from the same initial owner as the access service"
    );
    for credential_volume in [
        "verglas-access-server-credentials:/var/run/verglas/server",
        "verglas-access-lakekeeper-credentials:/var/run/verglas/lakekeeper",
        "verglas-access-neon-credentials:/var/run/verglas/neon",
    ] {
        assert!(
            compose.contains(credential_volume),
            "Compose must isolate the {credential_volume} credential volume"
        );
    }
    assert!(
        !compose.contains("verglas-access-credentials:"),
        "Compose must not share one access credential volume among unrelated consumers"
    );
    assert!(
        compose.contains("LAKEKEEPER__AUTHZ_BACKEND: verglas"),
        "Lakekeeper must use the tenant Verglas policy adapter"
    );
    assert!(
        compose.contains("LAKEKEEPER__VERGLAS__WORKLOAD_CREDENTIAL_FILE"),
        "Lakekeeper must read only its policy-engine workload credential file"
    );
    assert!(
        !compose.contains("LAKEKEEPER__VERGLAS__DATABASE_RESOURCE_ID"),
        "Lakekeeper must receive the trusted database identity per request, not from a singleton deployment variable"
    );
    assert!(
        !compose.contains("LAKEKEEPER__AUTHZ_BACKEND: allowall"),
        "Lakekeeper must not use the permissive allow-all authorizer"
    );
    assert!(
        compose.contains(
            "https://github.com/verglas-org/lakekeeper.git#34fef9c4580369900211b21c0f1db95cf8f0a876"
        ),
        "Lakekeeper must build from the reviewed immutable Verglas fork revision"
    );
    assert!(
        compose.contains("pull_policy: never"),
        "Compose must not pull an unpublished or upstream Lakekeeper image"
    );
    assert!(
        !compose.contains("verglas-neon-storage-image:")
            && !compose.contains("github.com/verglas-org/neon.git#")
            && postgres_runtime.contains(
                "ghcr.io/verglas-org/neon-storage:294a7b6e99392e60ff18aa6bef08e54b77446f7d"
            )
            && postgres_runtime.contains(
                "ghcr.io/verglas-org/neon-compute-v16:294a7b6e99392e60ff18aa6bef08e54b77446f7d"
            ),
        "the managed Postgres runtime must consume published exact Verglas Neon images without a Compose source-build fallback"
    );
    let access_service = compose
        .split("\n  verglas-access:\n")
        .nth(1)
        .expect("access service")
        .split("\n  verglas-scheduler:")
        .next()
        .expect("access service body");
    assert!(
        access_service.contains("verglas-runtime-state:/var/lib/verglas-container-runtime:ro"),
        "access must read the runtime-generated TLS identity without receiving runtime write authority"
    );
    assert!(
        !access_service.contains("verglas-lakekeeper:\n"),
        "access must not wait for Lakekeeper; Lakekeeper waits for access to avoid a startup cycle"
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
        .split("  verglas-container-runtime:")
        .next()
        .expect("verglas-server service");
    assert!(
        server.contains("ulimits:\n      nofile:\n        soft: 8192\n        hard: 8192"),
        "verglas-server must cap soft and hard nofile at 8192"
    );
}
