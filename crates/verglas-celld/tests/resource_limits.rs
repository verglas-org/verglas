//! Process-level proof that celld applies hard worker resource ceilings.

use std::path::Path;
use std::time::Duration;

use verglas_celld::{
    ChildCommand, ChildSpec, HostId, HostSupervisor, ReplicaRole, WorkerResourceLimits,
};

#[tokio::test]
async fn spawned_worker_receives_configured_and_default_resource_limits() {
    let root = tempfile::tempdir().expect("cell root");
    let helper =
        ChildCommand::new(env!("CARGO_BIN_EXE_verglas-celld-test-worker")).arg("--report-limits");
    let mut supervisor = HostSupervisor::new(HostId::new("cell-a"), root.path(), helper);
    let configured = WorkerResourceLimits::new(1024 * 1024 * 1024, 321).expect("limits");
    let configured_spec = ChildSpec::new("bounded", 1, ReplicaRole::Leader, 0)
        .expect("spec")
        .with_resource_limits(configured);
    let configured_child = supervisor
        .spawn(configured_spec)
        .await
        .expect("bounded child");
    let configured_report = wait_for_report(configured_child.data_dir().join("limits.txt")).await;
    #[cfg(target_os = "macos")]
    assert_eq!(configured_report, format!("{} 321\n", libc::RLIM_INFINITY));
    #[cfg(not(target_os = "macos"))]
    assert_eq!(configured_report, "1073741824 321\n");

    let default_spec = ChildSpec::new("defaulted", 1, ReplicaRole::Leader, 0).expect("spec");
    let default_child = supervisor.spawn(default_spec).await.expect("default child");
    let default_report = wait_for_report(default_child.data_dir().join("limits.txt")).await;
    #[cfg(target_os = "macos")]
    assert_eq!(default_report, format!("{} 1024\n", libc::RLIM_INFINITY));
    #[cfg(not(target_os = "macos"))]
    assert_eq!(default_report, "4294967296 1024\n");

    supervisor.shutdown().await.expect("stop children");
}

async fn wait_for_report(path: impl AsRef<Path>) -> String {
    let path = path.as_ref();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if path.exists() {
            return tokio::fs::read_to_string(path).await.expect("limit report");
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{} not written",
            path.display()
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}
