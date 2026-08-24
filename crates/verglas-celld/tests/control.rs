//! Unix control-plane protocol for the runnable `celld-host` daemon.

use std::path::Path;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use verglas_celld::{ChildCommand, ControlServer, HostId, HostSupervisor};

fn socket_child() -> ChildCommand {
    ChildCommand::new("python3").arg("-c").arg(
        "import socket,sys,time; p=sys.argv[sys.argv.index('--socket')+1]; s=socket.socket(socket.AF_UNIX); s.bind(p); s.listen(); time.sleep(30)",
    )
}

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

#[tokio::test]
async fn control_socket_spawns_routes_and_suspends_a_child() {
    let root = tempfile::tempdir().expect("cell root");
    let control_path = root.path().join("celld.sock");
    let supervisor = HostSupervisor::new(HostId::new("cell-a"), root.path(), socket_child());
    let mut server = ControlServer::bind(&control_path, supervisor)
        .await
        .expect("bind control");

    let spawn = request(&mut server, &control_path, "SPAWN agent-1 1 leader 4").await;
    assert!(spawn.starts_with("OK "));
    assert!(spawn.trim_end().ends_with("worker.sock"));
    let pid = request(&mut server, &control_path, "PID agent-1").await;
    assert!(pid.starts_with("OK "));
    assert!(pid.trim_start_matches("OK ").trim().parse::<u32>().is_ok());
    let route = request(&mut server, &control_path, "ROUTE_STATEFUL agent-1").await;
    assert!(route.starts_with("OK "));
    let unsafe_suspend = request(&mut server, &control_path, "SUSPEND agent-1 4 3 4").await;
    assert!(unsafe_suspend.starts_with("ERR "));
    let suspend = request(&mut server, &control_path, "SUSPEND agent-1 4 4 4").await;
    assert_eq!(suspend, "OK\n");
    let fenced = request(&mut server, &control_path, "ROUTE_STATEFUL agent-1").await;
    assert!(fenced.starts_with("ERR "));

    let held = request(
        &mut server,
        &control_path,
        "SPAWN_WORKER agent-2 1 0 /tmp/agent-2-replica.sock 68656c642d746f6b656e 2 0 -",
    )
    .await;
    assert!(held.starts_with("OK "));
    assert!(
        request(&mut server, &control_path, "ROUTE_STATEFUL agent-2")
            .await
            .starts_with("OK ")
    );

    server.supervisor_mut().shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn malformed_control_command_fails_without_mutating_supervision() {
    let root = tempfile::tempdir().expect("cell root");
    let control_path = root.path().join("celld.sock");
    let supervisor = HostSupervisor::new(HostId::new("cell-a"), root.path(), socket_child());
    let mut server = ControlServer::bind(&control_path, supervisor)
        .await
        .expect("bind control");

    let response = request(&mut server, &control_path, "SPAWN missing-fields").await;
    assert!(response.starts_with("ERR invalid command"));
    assert!(server.supervisor_mut().state("missing-fields").is_none());
}

/// Allows one logical DO to own a replica endpoint and a Worker endpoint.
#[tokio::test]
async fn control_spawns_replica_and_worker_for_one_do_identity() {
    let root = tempfile::tempdir().expect("cell root");
    let control_path = root.path().join("celld.sock");
    let supervisor = HostSupervisor::new(HostId::new("cell-a"), root.path(), argv_dump_child());
    let mut server = ControlServer::bind(&control_path, supervisor)
        .await
        .expect("bind control");

    let replica = request(&mut server, &control_path, "SPAWN agent-pair 1 follower 0").await;
    assert!(
        replica.starts_with("OK "),
        "replica spawn failed: {replica}"
    );
    let worker = request(
        &mut server,
        &control_path,
        "SPAWN_WORKER agent-pair 1 0 /tmp/agent-pair-replica.sock 68656c642d746f6b656e 11 0 -",
    )
    .await;
    assert!(worker.starts_with("OK "), "worker spawn failed: {worker}");
    assert!(server.supervisor_mut().pid("agent-pair").is_some());
    server.supervisor_mut().shutdown().await.expect("shutdown");
}

/// Fake child that records its argv into the data directory before binding.
fn argv_dump_child() -> ChildCommand {
    ChildCommand::new("python3").arg("-c").arg(
        "import socket,sys,time,os; d=sys.argv[sys.argv.index('--data-dir')+1]; open(os.path.join(d,'argv.txt'),'w').write('\\n'.join(sys.argv)); p=sys.argv[sys.argv.index('--socket')+1]; s=socket.socket(socket.AF_UNIX); s.bind(p); s.listen(); time.sleep(30)",
    )
}

/// A component-bearing SPAWN_WORKER forwards digest, dir, and event socket to the child argv.
#[tokio::test]
async fn spawn_worker_with_component_passes_component_arguments() {
    let root = tempfile::tempdir().expect("cell root");
    let control_path = root.path().join("celld.sock");
    let supervisor = HostSupervisor::new(HostId::new("cell-a"), root.path(), argv_dump_child());
    let mut server = ControlServer::bind(&control_path, supervisor)
        .await
        .expect("bind control");

    let digest = "ab".repeat(32);
    let event_socket = root.path().join("agent-3-events.sock");
    let command = format!(
        "SPAWN_WORKER agent-3 1 0 /tmp/agent-3-replica.sock 68656c642d746f6b656e 2 0 - {digest} /tmp/components {}",
        event_socket.display()
    );
    let response = request(&mut server, &control_path, &command).await;
    assert!(response.starts_with("OK "), "spawn failed: {response}");

    let argv = std::fs::read_to_string(root.path().join("agent-3").join("1").join("argv.txt"))
        .expect("child argv dump");
    let lines: Vec<&str> = argv.lines().collect();
    let flag_value = |flag: &str| -> &str {
        let index = lines
            .iter()
            .position(|line| *line == flag)
            .unwrap_or_else(|| panic!("missing {flag} in child argv: {argv}"));
        lines[index + 1]
    };
    assert_eq!(flag_value("--component-digest"), digest);
    assert_eq!(flag_value("--component-dir"), "/tmp/components");
    assert_eq!(
        flag_value("--event-socket"),
        event_socket.display().to_string()
    );

    server.supervisor_mut().shutdown().await.expect("shutdown");
}

/// A malformed component digest is rejected before any child is spawned.
#[tokio::test]
async fn spawn_worker_with_malformed_component_digest_is_rejected() {
    let root = tempfile::tempdir().expect("cell root");
    let control_path = root.path().join("celld.sock");
    let supervisor = HostSupervisor::new(HostId::new("cell-a"), root.path(), argv_dump_child());
    let mut server = ControlServer::bind(&control_path, supervisor)
        .await
        .expect("bind control");

    let response = request(
        &mut server,
        &control_path,
        "SPAWN_WORKER agent-4 1 0 /tmp/agent-4-replica.sock 68656c642d746f6b656e 2 0 - nothex /tmp/components /tmp/agent-4-events.sock",
    )
    .await;
    assert!(
        response.starts_with("ERR invalid command"),
        "got: {response}"
    );
    assert!(server.supervisor_mut().state("agent-4").is_none());
}
