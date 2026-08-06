//! Opt-in lifecycle verification against an operator-owned Docker Engine.

use verglas_container_runtime::{ContainerSpec, DockerRuntime, ObservedState};

#[tokio::test]
#[ignore = "requires a local Docker Engine and pulls alpine:3.22"]
async fn real_docker_lifecycle() {
    let runtime = DockerRuntime::connect_local().expect("connect to local Docker Engine");
    let deployment_id = format!("integration-{}", std::process::id());
    let spec = ContainerSpec::new(&deployment_id, "alpine:3.22").with_command([
        "sh",
        "-c",
        "while true; do sleep 3600; done",
    ]);

    runtime.reconcile(&spec).await.expect("create and start");
    let running = runtime
        .inspect(&deployment_id)
        .await
        .expect("inspect")
        .expect("managed container");
    assert_eq!(running.state, ObservedState::Running);

    assert!(runtime.stop(&deployment_id).await.expect("stop"));
    assert!(runtime.remove(&deployment_id).await.expect("remove"));
    assert!(
        runtime
            .inspect(&deployment_id)
            .await
            .expect("inspect removed")
            .is_none()
    );
}
