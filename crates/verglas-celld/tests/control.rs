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
