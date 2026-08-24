//! Local-process and provisioner-seam supervision with Unix-socket route fencing.

use std::path::{Path, PathBuf};
use std::process::ExitStatus;
use std::sync::{Arc, Mutex};

use verglas_celld::{
    ChildCommand, ChildDescriptor, ChildSpec, ChildState, HostId, HostSupervisor, ProvisionError,
    ProvisionFuture, ProvisionHandle, ProvisionRequest, ProvisionedChild, Provisioner, ReplicaRole,
    SuspendFence, WorkerDurability,
};

fn socket_child(seconds: &str, exit_code: i32) -> ChildCommand {
    ChildCommand::new("python3").arg("-c").arg(format!(
        "import socket,sys,time; p=sys.argv[sys.argv.index('--socket')+1]; s=socket.socket(socket.AF_UNIX); s.bind(p); s.listen(); time.sleep({seconds}); sys.exit({exit_code})"
    ))
}

/// Proves a component-sized child may spend more than two seconds before binding.
#[tokio::test]
async fn slow_child_startup_is_allowed_by_the_worker_load_budget() {
    let root = tempfile::tempdir().expect("cell root");
    let mut supervisor = HostSupervisor::new(
        HostId::new("cell-slow"),
        root.path(),
        ChildCommand::new("python3").arg("-c").arg(
            "import socket,sys,time; time.sleep(2.5); p=sys.argv[sys.argv.index('--socket')+1]; s=socket.socket(socket.AF_UNIX); s.bind(p); s.listen(); time.sleep(30)",
        ),
    );
    supervisor
        .spawn(ChildSpec::new("slow-worker", 1, ReplicaRole::Leader, 0).expect("spec"))
        .await
        .expect("slow child should become ready");
    supervisor.shutdown().await.expect("stop child");
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

/// Records substrate calls without creating a real child process.
#[derive(Clone)]
struct RecordingProvisioner {
    events: Arc<Mutex<Vec<&'static str>>>,
}

impl RecordingProvisioner {
    /// Creates an empty event recorder shared by all provisioner clones.
    fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Appends one substrate operation to the shared event log.
    fn record(&self, event: &'static str) {
        self.events.lock().expect("events mutex").push(event);
    }

    /// Returns a snapshot of operations observed so far.
    fn events(&self) -> Vec<&'static str> {
        self.events.lock().expect("events mutex").clone()
    }
}

/// Provides a no-op process handle for the recording provisioner.
struct RecordingHandle;

impl ProvisionHandle for RecordingHandle {
    /// Reports that the fake process remains alive.
    fn try_wait(&mut self) -> Result<Option<ExitStatus>, ProvisionError> {
        Ok(None)
    }

    /// Accepts a kill request without creating an operating-system process.
    fn kill<'a>(&'a mut self) -> ProvisionFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    /// Returns a successful fake exit status after a wait request.
    fn wait<'a>(&'a mut self) -> ProvisionFuture<'a, ExitStatus> {
        Box::pin(async { Ok(ExitStatus::default()) })
    }
}

impl Provisioner for RecordingProvisioner {
    /// Records spawning and returns a stable fake descriptor.
    fn spawn<'a>(&'a self, request: ProvisionRequest) -> ProvisionFuture<'a, ProvisionedChild> {
        self.record("spawn");
        Box::pin(async move {
            Ok(ProvisionedChild::new(
                request.do_id().to_owned(),
                ChildDescriptor::new(
                    41,
                    PathBuf::from(request.socket_path()),
                    PathBuf::from(request.data_dir()),
                ),
                Box::new(RecordingHandle),
            ))
        })
    }

    /// Records the readiness fence before the supervisor publishes a child.
    fn await_ready<'a>(&'a self, _child: &'a mut ProvisionedChild) -> ProvisionFuture<'a, ()> {
        self.record("await-ready");
        Box::pin(async { Ok(()) })
    }

    /// Records the nonblocking reap poll used before every route.
    fn try_wait(
        &self,
        _child: &mut ProvisionedChild,
    ) -> Result<Option<ExitStatus>, ProvisionError> {
        self.record("try-wait");
        Ok(None)
    }

    /// Records a kill request issued only after a valid lifecycle fence.
    fn kill<'a>(&'a self, _child: &'a mut ProvisionedChild) -> ProvisionFuture<'a, ()> {
        self.record("kill");
        Box::pin(async { Ok(()) })
    }

    /// Records the reap wait paired with a kill request.
    fn wait<'a>(&'a self, _child: &'a mut ProvisionedChild) -> ProvisionFuture<'a, ExitStatus> {
        self.record("wait");
        Box::pin(async { Ok(ExitStatus::default()) })
    }
}

#[tokio::test]
async fn provisioner_seam_preserves_readiness_and_lifecycle_fences() {
    let root = tempfile::tempdir().expect("cell root");
    let provisioner = Arc::new(RecordingProvisioner::new());
    let mut supervisor = HostSupervisor::with_provisioner(
        HostId::new("cell-fake"),
        root.path(),
        ChildCommand::new("unused"),
        provisioner.clone(),
    );

    supervisor
        .spawn(ChildSpec::new("agent-seam", 1, ReplicaRole::Leader, 12).expect("spec"))
        .await
        .expect("fake spawn");
    assert_eq!(provisioner.events(), vec!["spawn", "await-ready"]);
    assert!(supervisor.route_stateful("agent-seam").is_ok());
    assert_eq!(provisioner.events()[2], "try-wait");

    supervisor
        .suspend("agent-seam", SuspendFence::new(12, 11, 12))
        .await
        .expect_err("archive fence must reject kill");
    assert!(!provisioner.events().contains(&"kill"));

    supervisor
        .suspend("agent-seam", SuspendFence::new(12, 12, 12))
        .await
        .expect("safe suspend");
    let events = provisioner.events();
    assert_eq!(&events[events.len() - 2..], ["kill", "wait"]);

    supervisor
        .start_restore("agent-seam", 12, ReplicaRole::Leader)
        .await
        .expect("fake restore spawn");
    assert!(supervisor.route_stateful("agent-seam").is_err());
    supervisor
        .finish_restore("agent-seam", ReplicaRole::Leader, 12)
        .expect("restore fence");
    assert!(supervisor.route_stateful("agent-seam").is_ok());
    let events = provisioner.events();
    assert_eq!(&events[events.len() - 1..], ["try-wait"]);
}
