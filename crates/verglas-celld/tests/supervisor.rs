//! Real child-process supervision and Unix-socket route fencing.

use std::path::Path;

use verglas_celld::{
    ChildCommand, ChildSpec, ChildState, HostId, HostSupervisor, ReplicaRole, SuspendFence,
    WorkerDurability,
};

fn socket_child(seconds: &str, exit_code: i32) -> ChildCommand {
    ChildCommand::new("python3").arg("-c").arg(format!(
        "import socket,sys,time; p=sys.argv[sys.argv.index('--socket')+1]; s=socket.socket(socket.AF_UNIX); s.bind(p); s.listen(); time.sleep({seconds}); sys.exit({exit_code})"
    ))
}

#[tokio::test]
async fn host_spawns_suspends_and_restores_one_isolated_do_child() {
    let root = tempfile::tempdir().expect("cell root");
    let command = socket_child("30", 0);
    let mut supervisor = HostSupervisor::new(HostId::new("cell-a"), root.path(), command);
    let spec = ChildSpec::new("agent-7", 2, ReplicaRole::Leader, 3).expect("child spec");

    let child = supervisor.spawn(spec).await.expect("spawn child");
    assert!(child.pid() > 0);
    assert!(child.socket_path().ends_with(Path::new("worker.sock")));
    assert!(child.socket_path().exists());
    assert_eq!(
        supervisor
            .route_stateful("agent-7")
            .expect("route leader event"),
        child.socket_path()
    );
    let error = supervisor
        .suspend("agent-7", SuspendFence::new(3, 2, 3))
        .await
        .expect_err("unarchived child must stay running");
    assert!(error.to_string().contains("archive sequence 2"));
    assert!(supervisor.pid("agent-7").is_some());

    supervisor
        .suspend("agent-7", SuspendFence::new(3, 3, 3))
        .await
        .expect("safe suspend");
    assert_eq!(supervisor.state("agent-7"), Some(ChildState::Suspended));
    assert!(supervisor.pid("agent-7").is_none());

    supervisor
        .start_restore("agent-7", 3, ReplicaRole::Follower)
        .await
        .expect("start restore process");
    assert_eq!(
        supervisor.state("agent-7"),
        Some(ChildState::Restoring { required: 3 })
    );
    assert!(supervisor.route_stateful("agent-7").is_err());
    supervisor
        .finish_restore("agent-7", ReplicaRole::Follower, 3)
        .expect("finish restore");
    assert!(supervisor.route_snapshot("agent-7", 3).is_ok());
    assert!(supervisor.route_stateful("agent-7").is_err());

    supervisor.shutdown().await.expect("stop child");
}

#[tokio::test]
async fn crashed_child_is_detected_and_all_routes_are_fenced() {
    let root = tempfile::tempdir().expect("cell root");
    let command = socket_child("0.02", 7);
    let mut supervisor = HostSupervisor::new(HostId::new("cell-a"), root.path(), command);
    supervisor
        .spawn(ChildSpec::new("agent-9", 1, ReplicaRole::Leader, 5).expect("spec"))
        .await
        .expect("spawn child");
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let exited = supervisor.poll_exited().expect("poll children");
    assert_eq!(exited.len(), 1);
    assert_eq!(exited[0].do_id(), "agent-9");
    assert_eq!(exited[0].status().code(), Some(7));
    assert_eq!(
        supervisor.state("agent-9"),
        Some(ChildState::Restoring { required: 5 })
    );
    assert!(supervisor.route_stateful("agent-9").is_err());
    assert!(supervisor.route_snapshot("agent-9", 0).is_err());
}

#[tokio::test]
async fn duplicate_or_unsafe_do_identity_fails_closed() {
    let root = tempfile::tempdir().expect("cell root");
    let command = socket_child("30", 0);
    let mut supervisor = HostSupervisor::new(HostId::new("cell-a"), root.path(), command);
    assert!(ChildSpec::new("../escape", 1, ReplicaRole::Follower, 0).is_err());
    let spec = ChildSpec::new("agent-8", 1, ReplicaRole::Follower, 0).expect("spec");
    supervisor.spawn(spec.clone()).await.expect("first child");
    assert!(supervisor.spawn(spec).await.is_err());
    supervisor.shutdown().await.expect("stop child");
}

#[tokio::test]
async fn worker_launch_receives_its_per_do_replica_lease_configuration() {
    let root = tempfile::tempdir().expect("cell root");
    let command = ChildCommand::new("python3").arg("-c").arg(
        "import pathlib,socket,sys,time; d=pathlib.Path(sys.argv[sys.argv.index('--data-dir')+1]); d.joinpath('args.txt').write_text(' '.join(sys.argv)); p=sys.argv[sys.argv.index('--socket')+1]; s=socket.socket(socket.AF_UNIX); s.bind(p); s.listen(); time.sleep(30)"
    );
    let mut supervisor = HostSupervisor::new(HostId::new("cell-a"), root.path(), command);
    let spec = ChildSpec::new("agent-held", 1, ReplicaRole::Leader, 4)
        .expect("spec")
        .with_durability(WorkerDurability::Replica {
            socket: "/tmp/replica-agent-held.sock".into(),
            lease_token: "opaque-held-token".to_owned(),
            generation: 8,
            start_sequence: 4,
            offload_dir: Some("/tmp/managed-offload".into()),
        })
        .expect("durability");

    let child = supervisor.spawn(spec).await.expect("spawn worker");
    let arguments = std::fs::read_to_string(child.data_dir().join("args.txt")).expect("arguments");
    assert!(arguments.contains("--replica-socket /tmp/replica-agent-held.sock"));
    assert!(arguments.contains("--lease-token opaque-held-token"));
    assert!(arguments.contains("--lease-generation 8"));
    assert!(arguments.contains("--start-sequence 4"));
    assert!(arguments.contains("--offload-dir /tmp/managed-offload"));
    supervisor.shutdown().await.expect("stop child");
}
