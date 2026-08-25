//! One-owner process supervision tests.

use std::path::Path;

use verglas_celld::{
    ChildCommand, ChildSpec, ChildState, HostId, HostSupervisor, SuspendFence, WorkerComponent,
};

const DIGEST: &str = "ababaabababaabababaabababaabababaabababaabababaabababaabababaaba";

fn worker_spec(root: &Path, do_id: &str) -> ChildSpec {
    let data_dir = root.join(do_id);
    let event_socket = data_dir.join("events.sock");
    ChildSpec::new(do_id)
        .expect("spec")
        .with_data_dir(data_dir)
        .expect("data root")
        .with_component(
            WorkerComponent::new(DIGEST, root.join("components"), None, event_socket)
                .expect("component"),
        )
}

fn socket_child() -> ChildCommand {
    ChildCommand::new("python3").arg("-c").arg(
        "import signal,socket,sys,time; signal.signal(signal.SIGINT,lambda *_:sys.exit(0)); p=sys.argv[sys.argv.index('--event-socket')+1]; s=socket.socket(socket.AF_UNIX); s.bind(p); s.listen(); time.sleep(30)",
    )
}

fn crash_once_child() -> ChildCommand {
    ChildCommand::new("python3").arg("-c").arg(
        "import os,signal,socket,sys,time; signal.signal(signal.SIGINT,lambda *_:sys.exit(0)); d=sys.argv[sys.argv.index('--data-dir')+1]; p=sys.argv[sys.argv.index('--event-socket')+1]; m=os.path.join(d,'crashed'); s=socket.socket(socket.AF_UNIX); s.bind(p); s.listen(); first=not os.path.exists(m); open(m,'a').close(); time.sleep(0.05 if first else 30)",
    )
}

fn failed_shutdown_child() -> ChildCommand {
    ChildCommand::new("python3").arg("-c").arg(
        "import signal,socket,sys,time; signal.signal(signal.SIGINT,lambda *_:sys.exit(7)); p=sys.argv[sys.argv.index('--event-socket')+1]; s=socket.socket(socket.AF_UNIX); s.bind(p); s.listen(); time.sleep(30)",
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

#[tokio::test]
async fn crashed_child_can_start_restore_without_a_second_lifecycle_transition() {
    let root = tempfile::tempdir().expect("cell root");
    let mut supervisor =
        HostSupervisor::new(HostId::new("cell-a"), root.path(), crash_once_child());
    supervisor
        .spawn(worker_spec(root.path(), "agent-crash"))
        .await
        .expect("spawn child");
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let exited = supervisor.poll_exited().expect("poll exited child");
    assert_eq!(exited.len(), 1);
    assert_eq!(supervisor.state("agent-crash"), Some(ChildState::Restoring));
    supervisor
        .start_restore("agent-crash")
        .await
        .expect("restart crashed child");
    supervisor
        .finish_restore("agent-crash")
        .expect("publish restored child");
    assert!(supervisor.route_stateful("agent-crash").is_ok());
    supervisor.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn shutdown_propagates_a_failed_child_fence() {
    let root = tempfile::tempdir().expect("cell root");
    let mut supervisor =
        HostSupervisor::new(HostId::new("cell-a"), root.path(), failed_shutdown_child());
    supervisor
        .spawn(worker_spec(root.path(), "agent-failed-fence"))
        .await
        .expect("spawn child");
    assert!(supervisor.shutdown().await.is_err());
}

#[test]
fn duplicate_or_unsafe_do_identity_fails_closed() {
    assert!(ChildSpec::new("../escape").is_err());
}
