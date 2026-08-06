//! Opt-in lifecycle verification against an operator-owned Docker Engine.

use std::collections::BTreeMap;

use verglas_container_runtime::{
    ContainerSpec, DockerRuntime, ObservedState, TypescriptProject, VesselHttp, VesselProjectSpec,
    VesselRole,
};

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

#[tokio::test]
#[ignore = "requires a local Docker Engine and downloads one npm dependency"]
async fn real_docker_builds_a_dependency_bearing_typescript_vessel() {
    let runtime = DockerRuntime::connect_local().expect("connect to local Docker Engine");
    let name = format!("dependency-app-{}", std::process::id());
    let project = VesselProjectSpec {
        name: name.clone(),
        role: VesselRole::Application,
        project: TypescriptProject {
            files: BTreeMap::from([
                (
                    "package.json".to_owned(),
                    r#"{"scripts":{"start":"bun src/server.ts"},"dependencies":{"hono":"4.8.3"}}"#
                        .to_owned(),
                ),
                (
                    "src/server.ts".to_owned(),
                    "import { Hono } from 'hono'; const app = new Hono().get('/health', c => c.json({ok: true})); Bun.serve({port: 8380, fetch: app.fetch});"
                        .to_owned(),
                ),
            ]),
        },
        environment: BTreeMap::new(),
        http: VesselHttp {
            port: 8380,
            health_path: Some("/health".to_owned()),
        },
    };

    let build = runtime
        .build_project(&project)
        .await
        .expect("build project");
    runtime
        .reconcile(
            &project
                .vessel_spec(build.image)
                .container_spec()
                .expect("container spec"),
        )
        .await
        .expect("run Vessel");
    let running = runtime
        .inspect(&format!("vessel-{name}"))
        .await
        .expect("inspect")
        .expect("managed Vessel");
    assert_eq!(running.state, ObservedState::Running);
    runtime
        .remove(&format!("vessel-{name}"))
        .await
        .expect("remove Vessel");
}
