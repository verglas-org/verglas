//! Process-supervision tests for multiplexed local Gadget hosts.

#![cfg(unix)]

use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::time::Duration;

use verglas_gadget_runtime::{GadgetBundle, HostConfig, ProcessSupervisor};

/// Creates a bundle whose version is visible to the fake child host.
fn bundle(version: &str) -> GadgetBundle {
    GadgetBundle {
        version: version.to_owned(),
        server_module: "export class Gadget {}".to_owned(),
        client_module: "export default {};".to_owned(),
        files: BTreeMap::new(),
    }
}

/// Writes a child host that records starts, announces an endpoint, and waits.
fn fake_host(root: &std::path::Path) -> std::path::PathBuf {
    let path = root.join("fake-host.sh");
    std::fs::write(
        &path,
        "#!/bin/sh\nset -eu\nprintf '%s:%s\\n' \"$VERGLAS_GADGET_ID\" \"$VERGLAS_GADGET_VERSION\" >> \"$START_LOG\"\nprintf 'VERGLAS_GADGET_READY=127.0.0.1:45678\\n'\nexec sleep 60\n",
    )
    .expect("write fake host");
    let mut permissions = std::fs::metadata(&path)
        .expect("host metadata")
        .permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&path, permissions).expect("host permissions");
    path
}

#[tokio::test]
async fn supervisor_reuses_replaces_and_stops_per_gadget_children() {
    let root = tempfile::tempdir().expect("test root");
    let log = root.path().join("starts.log");
    let config = HostConfig {
        command: fake_host(root.path()),
        arguments: Vec::new(),
        startup_timeout: Duration::from_secs(2),
        environment: BTreeMap::from([("START_LOG".to_owned(), log.to_string_lossy().into_owned())]),
    };
    let supervisor = ProcessSupervisor::new(
        config,
        "http://127.0.0.1:8350".to_owned(),
        "runtime-control-secret".to_owned(),
    );

    let first = supervisor
        .ensure("alpha", "digest-a", &bundle("1"))
        .await
        .expect("start alpha");
    assert_eq!(first.to_string(), "127.0.0.1:45678");
    supervisor
        .ensure("alpha", "digest-a", &bundle("1"))
        .await
        .expect("reuse alpha");
    supervisor
        .ensure("beta", "digest-b", &bundle("1"))
        .await
        .expect("start beta");
    assert_eq!(supervisor.active().await, vec!["alpha", "beta"]);

    supervisor
        .ensure("alpha", "digest-c", &bundle("2"))
        .await
        .expect("replace alpha");
    assert!(supervisor.stop("beta").await.expect("stop beta"));
    assert_eq!(supervisor.active().await, vec!["alpha"]);

    let starts = std::fs::read_to_string(log).expect("start log");
    assert_eq!(
        starts.lines().collect::<Vec<_>>(),
        ["alpha:1", "beta:1", "alpha:2"]
    );
}
