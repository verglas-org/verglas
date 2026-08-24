//! Supervisor-level orchestration test for fenced drain, checkpoint, clean, and stop.

use verglas_celld::{
    ChildCommand, ChildSpec, ChildState, HostId, HostSupervisor, ReplicaRole, WorkerDurability,
};

/// Launches the dependency-free Rust endpoint helper with a visible drain wait.
fn orchestration_child() -> ChildCommand {
    ChildCommand::new(env!("CARGO_BIN_EXE_verglas-celld-test-worker"))
        .arg("--delay-ms")
        .arg("50")
}

#[tokio::test]
async fn orchestrated_suspend_fences_routes_and_orders_drain_checkpoint_clean() {
    let root = tempfile::tempdir().expect("cell root");
    let replica_socket = root.path().join("replica.sock");
    let mut supervisor =
        HostSupervisor::new(HostId::new("cell-a"), root.path(), orchestration_child());
    let spec = ChildSpec::new("agent-safe", 1, ReplicaRole::Leader, 2)
        .expect("spec")
        .with_durability(WorkerDurability::Replica {
            socket: replica_socket,
            lease_token: "held-token".to_owned(),
            generation: 9,
            start_sequence: 2,
            offload_dir: Some(root.path().join("managed-offload")),
        })
        .expect("durability");
    let child = supervisor.spawn(spec).await.expect("spawn worker");
    assert!(supervisor.route_stateful("agent-safe").is_ok());

    supervisor
        .suspend_orchestrated("agent-safe")
        .await
        .expect("orchestrated suspend");

    assert_eq!(supervisor.state("agent-safe"), Some(ChildState::Suspended));
    assert!(supervisor.pid("agent-safe").is_none());
    assert!(supervisor.route_stateful("agent-safe").is_err());
    assert!(supervisor.route_snapshot("agent-safe", 2).is_err());

    let log = std::fs::read_to_string(child.data_dir().join("commands.txt")).expect("command log");
    let drain = log.find("DRAIN").expect("drain command");
    let checkpoint = log.find("CHECKPOINT").expect("checkpoint command");
    let cover = log.find("REPLICA_COVER").expect("coverage command");
    let clean = log.find("REPLICA_CLEAN").expect("clean command");
    assert!(drain < checkpoint, "DRAIN must precede CHECKPOINT: {log}");
    assert!(
        checkpoint < cover,
        "CHECKPOINT must precede REPLICA_COVER: {log}"
    );
    assert!(
        cover < clean,
        "REPLICA_COVER must precede REPLICA_CLEAN: {log}"
    );

    supervisor.shutdown().await.expect("stop supervisor");
}
