//! Opt-in lifecycle verification against an operator-owned Docker Engine.

use std::collections::BTreeMap;
use std::path::Path;

use axum::Router;
use axum::body::Bytes;
use axum::routing::get;

use verglas_container_runtime::{
    ContainerSpec, DockerRuntime, ObservedState, TypescriptProject, VesselHttp, VesselProjectSpec,
    VesselRole, WorkerInvocation, WorkerProjectSpec, WorkerResources, WorkerRuntime,
};

#[tokio::test]
#[ignore = "requires a local Docker Engine and pulls alpine:3.22"]
async fn real_docker_lifecycle() {
    let runtime = DockerRuntime::connect_local(std::env::temp_dir().join("verglas-worker-scratch"))
        .expect("connect to local Docker Engine");
    let deployment_id = format!("integration-{}", std::process::id());
    let source = tempfile::NamedTempFile::new().expect("source file");
    std::fs::write(source.path(), "runtime-secret").expect("source contents");
    let spec = ContainerSpec::new(&deployment_id, "alpine:3.22")
        .with_command([
            "sh",
            "-c",
            "test \"$(cat /run/secrets/test)\" = runtime-secret; while true; do sleep 3600; done",
        ])
        .with_file(source.path().to_string_lossy(), "/run/secrets/test", 0o600)
        .with_ephemeral_port(8080);

    runtime.reconcile(&spec).await.expect("create and start");
    let running = runtime
        .inspect(&deployment_id)
        .await
        .expect("inspect")
        .expect("managed container");
    assert_eq!(running.state, ObservedState::Running);
    assert_eq!(running.published_ports.len(), 1);
    assert!(running.published_ports[0].host_port.is_some());

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
#[ignore = "requires Docker, downloads locked Polars wheels, and reads VERGLAS_LARGE_CSV_GZ"]
async fn real_python_worker_parses_a_large_compressed_file() {
    let sample = std::env::var("VERGLAS_LARGE_CSV_GZ")
        .expect("VERGLAS_LARGE_CSV_GZ must point to the compressed CSV sample");
    let bytes = Bytes::from(std::fs::read(&sample).expect("large CSV sample"));
    let app = Router::new().route(
        "/sample.csv.gz",
        get(move || {
            let bytes = bytes.clone();
            async move { bytes }
        }),
    );
    let listener = tokio::net::TcpListener::bind("0.0.0.0:0")
        .await
        .expect("sample listener");
    let port = listener.local_addr().expect("sample address").port();
    tokio::spawn(async move { axum::serve(listener, app).await.expect("sample server") });

    let fixture =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/workers/large-file-parser");
    let read = |name: &str| std::fs::read_to_string(fixture.join(name)).expect("worker file");
    let project = WorkerProjectSpec {
        name: format!("large-parser-{}", std::process::id()),
        runtime: WorkerRuntime::Python,
        entrypoint: vec!["python".to_owned(), "worker.py".to_owned()],
        files: BTreeMap::from([
            ("pyproject.toml".to_owned(), read("pyproject.toml")),
            ("uv.lock".to_owned(), read("uv.lock")),
            ("worker.py".to_owned(), read("worker.py")),
        ]),
    };
    let runtime = DockerRuntime::connect_local(std::env::temp_dir().join("verglas-worker-scratch"))
        .expect("Docker runtime");
    let build = runtime
        .build_worker_project(&project)
        .await
        .expect("locked worker build");
    let result = runtime
        .run_worker(&WorkerInvocation {
            run_id: format!("run-large-parser-{}", std::process::id()),
            worker: project.name,
            image: build.image,
            entrypoint: project.entrypoint,
            environment: BTreeMap::from([(
                "INPUT_URL".to_owned(),
                format!("http://host.docker.internal:{port}/sample.csv.gz"),
            )]),
            target: String::new(),
            endpoint: "http://host.docker.internal:8334".to_owned(),
            token: String::new(),
            network: None,
            event: serde_json::json!({
                "specversion": "1.0",
                "id": "large-file-test",
                "source": "urn:verglas:test",
                "type": "org.verglas.test"
            }),
            resources: WorkerResources {
                vcpus: 4.0,
                memory_mib: 8_192,
                pids: 256,
            },
            timeout_seconds: 1_800,
            scratch_target: None,
        })
        .await
        .expect("bounded worker run");
    assert_eq!(result.rows_produced, 2_346_855);
}

#[tokio::test]
#[ignore = "requires Docker and downloads one locked Bun dependency"]
async fn real_typescript_worker_runs_with_a_locked_dependency() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../examples/workers/typescript-dependency-worker");
    let read = |name: &str| std::fs::read_to_string(fixture.join(name)).expect("worker file");
    let project = WorkerProjectSpec {
        name: format!("typescript-worker-{}", std::process::id()),
        runtime: WorkerRuntime::Bun,
        entrypoint: vec!["bun".to_owned(), "worker.ts".to_owned()],
        files: BTreeMap::from([
            ("package.json".to_owned(), read("package.json")),
            ("bun.lock".to_owned(), read("bun.lock")),
            ("worker.ts".to_owned(), read("worker.ts")),
        ]),
    };
    let runtime = DockerRuntime::connect_local(std::env::temp_dir().join("verglas-worker-scratch"))
        .expect("Docker runtime");
    let build = runtime
        .build_worker_project(&project)
        .await
        .expect("locked Bun build");
    let result = runtime
        .run_worker(&WorkerInvocation {
            run_id: format!("run-typescript-worker-{}", std::process::id()),
            worker: project.name,
            image: build.image,
            entrypoint: project.entrypoint,
            environment: BTreeMap::new(),
            target: String::new(),
            endpoint: "http://host.docker.internal:8334".to_owned(),
            token: String::new(),
            network: None,
            event: serde_json::json!({
                "specversion": "1.0",
                "id": "typescript-test",
                "source": "urn:verglas:test",
                "type": "org.verglas.test"
            }),
            resources: WorkerResources {
                vcpus: 1.0,
                memory_mib: 512,
                pids: 64,
            },
            timeout_seconds: 60,
            scratch_target: None,
        })
        .await
        .expect("bounded Bun worker run");
    assert_eq!(result.rows_produced, 7);
}

#[tokio::test]
#[ignore = "requires a local Docker Engine and downloads one npm dependency"]
async fn real_docker_builds_a_dependency_bearing_typescript_vessel() {
    let runtime = DockerRuntime::connect_local(std::env::temp_dir().join("verglas-worker-scratch"))
        .expect("connect to local Docker Engine");
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
