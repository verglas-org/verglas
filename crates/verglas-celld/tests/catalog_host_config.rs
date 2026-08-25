//! Catalog host configuration propagation tests for the local process substrate.

use std::path::{Path, PathBuf};

use verglas_celld::{
    HostServiceBinding, LocalProcessProvisioner, ProvisionError, ProvisionRequest, Provisioner,
    WorkerComponent,
};

const DIGEST: &str = "ababaabababaabababaabababaabababaabababaabababaabababaabababaaba";
const CHILD_SCRIPT: &str = "import os,socket,sys,time; d=sys.argv[sys.argv.index('--data-dir')+1]; os.makedirs(d,exist_ok=True); open(os.path.join(d,'argv.txt'),'w').write('\\n'.join(sys.argv)); p=sys.argv[sys.argv.index('--event-socket')+1]; s=socket.socket(socket.AF_UNIX); s.bind(p); s.listen(); time.sleep(30)";

/// Builds one request with an optional exact Catalog host-service declaration.
fn request(root: &Path, host_service: Option<HostServiceBinding>) -> (ProvisionRequest, PathBuf) {
    let data_dir = root.join("do-1");
    let event_socket = data_dir.join("events.sock");
    let request = ProvisionRequest::new(
        "python3",
        vec!["-c".to_owned(), CHILD_SCRIPT.to_owned()],
        "cell-a",
        "do-1",
        &data_dir,
        WorkerComponent::new(DIGEST, root.join("components"), None, &event_socket)
            .expect("component"),
    );
    let request = match host_service {
        Some(host_service) => request.with_host_service(host_service),
        None => request,
    };
    (request, data_dir)
}

/// Stops one test child after its launch arguments have been inspected.
async fn stop_child(
    provisioner: &LocalProcessProvisioner,
    child: &mut verglas_celld::ProvisionedChild,
) {
    provisioner.kill(child).await.expect("kill child");
    provisioner.wait(child).await.expect("wait child");
}

/// A configured Catalog request receives the exact runtime host-config flag.
#[tokio::test]
async fn catalog_request_forwards_configured_host_path() {
    let root = tempfile::tempdir().expect("cell root");
    let config_path = root.path().join("catalog-host.toml");
    let provisioner = LocalProcessProvisioner::new().with_catalog_host_config(&config_path);
    let binding = HostServiceBinding::new("ICEBERG_COMMIT", "verglas-runtime").expect("binding");
    let (request, data_dir) = request(root.path(), Some(binding));

    let mut child = provisioner
        .spawn(request)
        .await
        .expect("spawn Catalog child");
    provisioner
        .await_ready(&mut child)
        .await
        .expect("child ready");
    let argv = std::fs::read_to_string(data_dir.join("argv.txt")).expect("argv dump");
    let lines: Vec<&str> = argv.lines().collect();
    let flag_index = lines
        .iter()
        .position(|line| *line == "--catalog-host-config")
        .expect("Catalog host config flag");
    assert_eq!(lines[flag_index + 1], config_path.display().to_string());
    stop_child(&provisioner, &mut child).await;
}

/// A non-Catalog request never receives the Catalog-only flag, even when configured.
#[tokio::test]
async fn non_catalog_request_never_forwards_catalog_host_path() {
    let root = tempfile::tempdir().expect("cell root");
    let config_path = root.path().join("catalog-host.toml");
    let provisioner = LocalProcessProvisioner::new().with_catalog_host_config(&config_path);
    let (request, data_dir) = request(root.path(), None);

    let mut child = provisioner
        .spawn(request)
        .await
        .expect("spawn ordinary child");
    provisioner
        .await_ready(&mut child)
        .await
        .expect("child ready");
    let argv = std::fs::read_to_string(data_dir.join("argv.txt")).expect("argv dump");
    assert!(!argv.lines().any(|line| line == "--catalog-host-config"));
    stop_child(&provisioner, &mut child).await;
}

/// A Catalog request without operator configuration fails before filesystem or process spawn.
#[tokio::test]
async fn catalog_request_without_host_path_fails_closed_before_spawn() {
    let root = tempfile::tempdir().expect("cell root");
    let provisioner = LocalProcessProvisioner::new();
    let binding = HostServiceBinding::new("ICEBERG_COMMIT", "verglas-runtime").expect("binding");
    let (request, data_dir) = request(root.path(), Some(binding));

    let result = provisioner.spawn(request).await;
    assert!(matches!(
        result,
        Err(ProvisionError::CatalogHostConfigNotConfigured)
    ));
    assert!(
        !data_dir.exists(),
        "fail-closed check must precede spawn setup"
    );
}
