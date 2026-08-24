//! Acceptance tests for verified component loading before the `verglasd` bind.

use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tokio::time::sleep;
use verglas_do_wasm::ComponentDigest;

/// A single component option is rejected before any endpoint is created.
#[test]
fn component_options_must_be_supplied_as_a_pair() {
    let directory = tempfile::tempdir().expect("runtime root");
    let socket = directory.path().join("replica.sock");
    let digest = ComponentDigest::compute(b"component");
    let mut child = Command::new(env!("CARGO_BIN_EXE_verglasd"))
        .args([
            "--do-id",
            "pair-test",
            "--replica-id",
            "1",
            "--role",
            "replica",
            "--socket",
        ])
        .arg(&socket)
        .args(["--data-dir"])
        .arg(directory.path().join("data"))
        .args(["--component-digest"])
        .arg(digest.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn pair test");
    let status = child.wait().expect("wait pair test");
    assert!(!status.success(), "a single component option must fail");
    assert!(!socket.exists(), "invalid options must not bind");
}

/// An event socket without a component is rejected before any endpoint bind.
#[test]
fn event_socket_requires_component_arguments() {
    let directory = tempfile::tempdir().expect("runtime root");
    let socket = directory.path().join("worker.sock");
    let event_socket = directory.path().join("events.sock");
    let status = Command::new(env!("CARGO_BIN_EXE_verglasd"))
        .args([
            "--do-id",
            "event-pair-test",
            "--replica-id",
            "1",
            "--role",
            "worker",
            "--socket",
        ])
        .arg(&socket)
        .args(["--data-dir"])
        .arg(directory.path().join("data"))
        .args([
            "--replica-socket",
            "/tmp/verglas-no-replica.sock",
            "--lease-token",
            "token",
            "--lease-generation",
            "1",
            "--start-sequence",
            "0",
            "--event-socket",
        ])
        .arg(&event_socket)
        .status()
        .expect("run event socket argument check");
    assert!(
        !status.success(),
        "event socket without component must fail"
    );
    assert!(
        !socket.exists(),
        "invalid options must not bind worker socket"
    );
}

/// A corrupt digest-named artifact stops a worker before it exposes its socket.
#[tokio::test]
async fn corrupt_component_exits_before_worker_socket_bind() {
    let directory = tempfile::tempdir().expect("runtime root");
    let replica_socket = directory.path().join("replica.sock");
    let worker_socket = directory.path().join("worker.sock");
    let artifact_dir = directory.path().join("components");
    let cache_dir = directory.path().join("cwasm");
    std::fs::create_dir_all(&artifact_dir).expect("component directory");
    let digest = ComponentDigest::compute(b"the expected component");
    std::fs::write(
        artifact_dir.join(format!("{digest}.wasm")),
        b"corrupt component",
    )
    .expect("corrupt artifact");

    let mut replica = spawn_replica(&replica_socket, &directory.path().join("replica"));
    wait_for_socket(&mut replica, &replica_socket).await;
    let mut worker = spawn_worker(
        &worker_socket,
        &directory.path().join("worker"),
        &replica_socket,
        &artifact_dir,
        &cache_dir,
        digest,
    );
    let status = worker.wait().expect("wait for corrupt worker");
    assert!(!status.success(), "corrupt artifact must fail closed");
    assert!(
        !worker_socket.exists(),
        "worker bound before artifact validation"
    );

    stop(replica);
}

/// A digest-verified empty component permits a worker to bind its endpoint.
#[tokio::test]
async fn valid_component_binds_worker_socket_after_verification() {
    let directory = tempfile::tempdir().expect("runtime root");
    let replica_socket = directory.path().join("replica.sock");
    let worker_socket = directory.path().join("worker.sock");
    let artifact_dir = directory.path().join("components");
    let cache_dir = directory.path().join("cwasm");
    std::fs::create_dir_all(&artifact_dir).expect("component directory");
    let bytes = wat::parse_str("(component)").expect("empty component WAT");
    let digest = ComponentDigest::compute(&bytes);
    std::fs::write(artifact_dir.join(format!("{digest}.wasm")), bytes).expect("valid artifact");

    let mut replica = spawn_replica(&replica_socket, &directory.path().join("replica"));
    wait_for_socket(&mut replica, &replica_socket).await;
    let mut worker = spawn_worker(
        &worker_socket,
        &directory.path().join("worker"),
        &replica_socket,
        &artifact_dir,
        &cache_dir,
        digest,
    );
    wait_for_socket(&mut worker, &worker_socket).await;
    assert!(
        std::fs::read_dir(&cache_dir)
            .expect("cache directory")
            .any(|entry| entry
                .expect("cache entry")
                .path()
                .extension()
                .is_some_and(|ext| ext == "cwasm")),
        "worker did not populate the configured cwasm cache"
    );

    stop(worker);
    stop(replica);
}

/// A configured cache path that is not a directory fails before worker bind.
#[tokio::test]
async fn unusable_cwasm_cache_exits_before_worker_socket_bind() {
    let directory = tempfile::tempdir().expect("runtime root");
    let replica_socket = directory.path().join("replica.sock");
    let worker_socket = directory.path().join("worker.sock");
    let artifact_dir = directory.path().join("components");
    let cache_path = directory.path().join("cache-file");
    std::fs::create_dir_all(&artifact_dir).expect("component directory");
    let bytes = wat::parse_str("(component)").expect("empty component WAT");
    let digest = ComponentDigest::compute(&bytes);
    std::fs::write(artifact_dir.join(format!("{digest}.wasm")), bytes).expect("valid artifact");
    std::fs::write(&cache_path, b"not a directory").expect("cache file");

    let mut replica = spawn_replica(&replica_socket, &directory.path().join("replica"));
    wait_for_socket(&mut replica, &replica_socket).await;
    let mut worker = spawn_worker(
        &worker_socket,
        &directory.path().join("worker"),
        &replica_socket,
        &artifact_dir,
        &cache_path,
        digest,
    );
    let status = worker.wait().expect("wait for unusable-cache worker");
    assert!(!status.success(), "an unusable cache must fail closed");
    assert!(
        !worker_socket.exists(),
        "worker bound before cache validation"
    );

    stop(replica);
}

/// Starts a replica child that supplies the worker's replay endpoint.
fn spawn_replica(socket: &std::path::Path, data_dir: &std::path::Path) -> Child {
    Command::new(env!("CARGO_BIN_EXE_verglasd"))
        .args([
            "--do-id",
            "artifact-test",
            "--replica-id",
            "1",
            "--role",
            "replica",
            "--socket",
        ])
        .arg(socket)
        .args(["--data-dir"])
        .arg(data_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn replica")
}

/// Starts a worker child with a content-addressed component artifact.
fn spawn_worker(
    socket: &std::path::Path,
    data_dir: &std::path::Path,
    replica_socket: &std::path::Path,
    component_dir: &std::path::Path,
    cache_dir: &std::path::Path,
    digest: ComponentDigest,
) -> Child {
    Command::new(env!("CARGO_BIN_EXE_verglasd"))
        .args([
            "--do-id",
            "artifact-test",
            "--replica-id",
            "2",
            "--role",
            "worker",
            "--socket",
        ])
        .arg(socket)
        .args(["--data-dir"])
        .arg(data_dir)
        .args(["--replica-socket"])
        .arg(replica_socket)
        .args([
            "--lease-token",
            "artifact-lease",
            "--lease-generation",
            "1",
            "--start-sequence",
            "0",
            "--component-digest",
        ])
        .arg(digest.to_string())
        .args(["--component-dir"])
        .arg(component_dir)
        .args(["--cwasm-cache-dir"])
        .arg(cache_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn worker")
}

/// Waits for a child to create its endpoint or reports an early exit.
async fn wait_for_socket(child: &mut Child, socket: &std::path::Path) {
    let deadline = Instant::now() + Duration::from_secs(3);
    while !socket.exists() {
        if let Some(status) = child.try_wait().expect("inspect child") {
            panic!("child exited before socket bind: {status}");
        }
        assert!(Instant::now() < deadline, "child did not bind socket");
        sleep(Duration::from_millis(5)).await;
    }
}

/// Terminates a child and waits for its process resources to be reclaimed.
fn stop(mut child: Child) {
    child.kill().expect("kill child");
    child.wait().expect("wait child");
}
