//! One-owner Turso process supervision tests.

use std::path::Path;

use verglas_celld::{
    ChildCommand, ChildSpec, ChildState, HostId, HostSupervisor, SuspendFence, TursoConfig,
    WorkerComponent,
};

const DIGEST: &str = "ababaabababaabababaabababaabababaabababaabababaabababaabababaaba";

fn worker_spec(root: &Path, do_id: &str) -> ChildSpec {
    let data_dir = root.join(do_id);
    let event_socket = data_dir.join("events.sock");
    ChildSpec::new(do_id)
        .expect("spec")
        .with_data_dir(data_dir)
        .expect("data root")
        .with_turso(
            TursoConfig::new("https://tenant.turso.io/db", root.join("token")).expect("turso"),
        )
        .with_component(
            WorkerComponent::new(DIGEST, root.join("components"), None, event_socket)
                .expect("component"),
        )
}

fn socket_child() -> ChildCommand {
    ChildCommand::new("python3").arg("-c").arg(
        "import socket,sys,time; p=sys.argv[sys.argv.index('--event-socket')+1]; s=socket.socket(socket.AF_UNIX); s.bind(p); s.listen(); time.sleep(30)",
    )
}

#[tokio::test]
async fn host_spawns_routes_suspends_and_restores_one_owner() {
    let root = tempfile::tempdir().expect("cell root");
    let mut supervisor = HostSupervisor::new(HostId::new("cell-a"), root.path(), socket_child());
    let spec = worker_spec(root.path(), "agent-7");
    let child = supervisor.spawn(spec).await.expect("spawn child");
    assert!(child.pid() > 0);
    assert!(child.socket_path().ends_with(Path::new("events.sock")));
    assert_eq!(
        supervisor.route_stateful("agent-7").expect("route"),
        child.socket_path()
    );
    assert!(
        supervisor
            .suspend("agent-7", SuspendFence::new(true, false, true))
            .await
            .is_err()
    );
    supervisor
        .suspend("agent-7", SuspendFence::new(true, true, true))
        .await
        .expect("safe suspend");
    assert_eq!(supervisor.state("agent-7"), Some(ChildState::Suspended));
    assert!(supervisor.route_stateful("agent-7").is_err());
    supervisor
        .start_restore("agent-7")
        .await
        .expect("restore process");
    assert_eq!(supervisor.state("agent-7"), Some(ChildState::Restoring));
    supervisor
        .finish_restore("agent-7")
        .expect("finish restore");
    assert!(supervisor.route_stateful("agent-7").is_ok());
    supervisor.shutdown().await.expect("shutdown");
}

#[test]
fn duplicate_or_unsafe_do_identity_fails_closed() {
    assert!(ChildSpec::new("../escape").is_err());
}
