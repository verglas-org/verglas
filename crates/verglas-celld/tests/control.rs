//! Worker control protocol acceptance tests.

use std::path::Path;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use verglas_celld::{ChildCommand, ControlServer, HostId, HostSupervisor};

const DIGEST: &str = "ababaabababaabababaabababaabababaabababaabababaabababaabababaaba";

/// Sends one strict command through the host control socket.
async fn request(server: &mut ControlServer, path: &Path, command: &str) -> String {
    let client = async {
        let mut stream = UnixStream::connect(path).await.expect("connect control");
        stream
            .write_all(format!("{command}\n").as_bytes())
            .await
            .expect("write command");
        let mut response = String::new();
        BufReader::new(stream)
            .read_line(&mut response)
            .await
            .expect("read response");
        response
    };
    let (served, response) = tokio::join!(server.serve_once(), client);
    served.expect("serve command");
    response
}

/// A fake child records the exact runtime argv and binds the event socket.
fn argv_dump_child() -> ChildCommand {
    ChildCommand::new("python3").arg("-c").arg(
        "import os,signal,socket,sys,time; signal.signal(signal.SIGINT,lambda *_:sys.exit(0)); d=sys.argv[sys.argv.index('--data-dir')+1]; os.makedirs(d,exist_ok=True); open(os.path.join(d,'argv.txt'),'w').write('\\n'.join(sys.argv)); p=sys.argv[sys.argv.index('--event-socket')+1]; s=socket.socket(socket.AF_UNIX); s.bind(p); s.listen(); time.sleep(30)",
    )
}

/// The only worker command forwards the local component launch contract.
#[tokio::test]
async fn spawn_worker_forwards_exact_local_arguments() {
    let root = tempfile::tempdir().expect("cell root");
    let control_path = root.path().join("celld.sock");
    let supervisor = HostSupervisor::new(HostId::new("cell-a"), root.path(), argv_dump_child());
    let mut server = ControlServer::bind(&control_path, supervisor)
        .await
        .expect("bind control");
    let data_dir = root.path().join("do-1");
    let cache_dir = root.path().join("cache");
    let event_socket = data_dir.join("events.sock");
    let command = format!(
        "SPAWN_WORKER do-1 {} {} {} {} {} - -",
        data_dir.display(),
        DIGEST,
        root.path().join("components").display(),
        cache_dir.display(),
        event_socket.display(),
    );
    let response = request(&mut server, &control_path, &command).await;
    assert_eq!(
        response.trim().strip_prefix("OK ").expect("spawn response"),
        event_socket.display().to_string()
    );
    let argv = std::fs::read_to_string(data_dir.join("argv.txt")).expect("child argv dump");
    let lines: Vec<&str> = argv.lines().collect();
    let flag_value = |flag: &str| -> &str {
        let index = lines
            .iter()
            .position(|line| *line == flag)
            .unwrap_or_else(|| panic!("missing {flag} in child argv: {argv}"));
        lines[index + 1]
    };
    assert_eq!(flag_value("--do-id"), "do-1");
    assert_eq!(flag_value("--data-dir"), data_dir.display().to_string());
    assert_eq!(flag_value("--component-digest"), DIGEST);
    assert!(!lines.iter().any(|line| line.starts_with("--turso-")));
    assert_eq!(
        flag_value("--component-dir"),
        root.path().join("components").display().to_string()
    );
    assert_eq!(
        flag_value("--cwasm-cache-dir"),
        cache_dir.display().to_string()
    );
    assert_eq!(
        flag_value("--event-socket"),
        event_socket.display().to_string()
    );
    server.supervisor_mut().shutdown().await.expect("shutdown");
}

/// The local launch contract omits remote database credentials.
#[tokio::test]
async fn spawn_worker_accepts_local_launch_contract_without_remote_credentials() {
    let root = tempfile::tempdir().expect("cell root");
    let control_path = root.path().join("celld.sock");
    let supervisor = HostSupervisor::new(HostId::new("cell-a"), root.path(), argv_dump_child());
    let mut server = ControlServer::bind(&control_path, supervisor)
        .await
        .expect("bind control");
    let data_dir = root.path().join("do-local");
    let event_socket = data_dir.join("events.sock");
    let command = format!(
        "SPAWN_WORKER do-local {} {} {} {} {} - -",
        data_dir.display(),
        DIGEST,
        root.path().join("components").display(),
        root.path().join("cache").display(),
        event_socket.display(),
    );
    let response = request(&mut server, &control_path, &command).await;
    assert!(
        response.starts_with("OK "),
        "local launch failed: {response}"
    );
    server.supervisor_mut().shutdown().await.expect("shutdown");
}

/// Child state and event sockets cannot escape the configured cell root.
#[tokio::test]
async fn spawn_worker_rejects_paths_outside_cell_root() {
    let root = tempfile::tempdir().expect("cell root");
    let outside = tempfile::tempdir().expect("outside root");
    let control_path = root.path().join("celld.sock");
    let supervisor = HostSupervisor::new(HostId::new("cell-a"), root.path(), argv_dump_child());
    let mut server = ControlServer::bind(&control_path, supervisor)
        .await
        .expect("bind control");
    let data_dir = outside.path().join("do-escape");
    let event_socket = data_dir.join("events.sock");
    let command = format!(
        "SPAWN_WORKER do-escape {} {} {} - {} - -",
        data_dir.display(),
        DIGEST,
        root.path().join("components").display(),
        event_socket.display(),
    );
    let response = request(&mut server, &control_path, &command).await;
    assert!(
        response
            .starts_with("ERR invalid Worker launch: local data root must be inside the cell root"),
        "unexpected response: {response}"
    );
}

/// Old replica and managed-CAS commands fail closed without mutating supervision.
#[tokio::test]
async fn removed_durability_commands_are_hard_errors() {
    let root = tempfile::tempdir().expect("cell root");
    let control_path = root.path().join("celld.sock");
    let supervisor = HostSupervisor::new(HostId::new("cell-a"), root.path(), argv_dump_child());
    let mut server = ControlServer::bind(&control_path, supervisor)
        .await
        .expect("bind control");
    for command in [
        "SPAWN do-1 1 follower 0",
        "SPAWN_CAS_WORKER do-1 1 0 http://cas bucket prefix region access secret token 1 0 etag - digest dir event",
        "SPAWN_WORKER do-1 1 0 /tmp/replica token 1 0 -",
    ] {
        let response = request(&mut server, &control_path, command).await;
        assert!(
            response.starts_with("ERR invalid command"),
            "{command}: {response}"
        );
    }
    assert!(server.supervisor_mut().state("do-1").is_none());
}

/// Suspension requires a storage checkpoint, drained outbox, and clean event shutdown.
#[tokio::test]
async fn suspend_requires_storage_checkpoint_outbox_and_clean_shutdown() {
    let root = tempfile::tempdir().expect("cell root");
    let control_path = root.path().join("celld.sock");
    let supervisor = HostSupervisor::new(HostId::new("cell-a"), root.path(), argv_dump_child());
    let mut server = ControlServer::bind(&control_path, supervisor)
        .await
        .expect("bind control");
    let data_dir = root.path().join("do-1");
    let event_socket = data_dir.join("events.sock");
    let command = format!(
        "SPAWN_WORKER do-1 {} {} {} - {} - -",
        data_dir.display(),
        DIGEST,
        root.path().join("components").display(),
        event_socket.display(),
    );
    assert!(
        request(&mut server, &control_path, &command)
            .await
            .starts_with("OK ")
    );
    assert!(
        request(&mut server, &control_path, "SUSPEND do-1 yes no yes")
            .await
            .starts_with("ERR ")
    );
    assert_eq!(
        request(&mut server, &control_path, "SUSPEND do-1 yes yes yes").await,
        "OK\n"
    );
    assert!(
        request(&mut server, &control_path, "PID do-1")
            .await
            .starts_with("ERR ")
    );
    server.supervisor_mut().shutdown().await.expect("shutdown");
}
