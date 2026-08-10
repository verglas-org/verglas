//! Lifecycle contract tests for the Docker placement adapter.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use super::{
    ContainerSpec, DockerApi, DockerRuntimeCore, EngineContainer, EngineCreateRequest,
    LABEL_MANAGED, LABEL_SPEC_DIGEST, ObservedState, ReconcileOutcome, RuntimeError,
    TypescriptProject, VesselHttp, VesselProjectSpec, VesselRole, VesselSpec,
    ensure_local_postgres_tls,
};

#[derive(Clone, Default)]
struct FakeDocker {
    state: Arc<Mutex<FakeState>>,
}

#[derive(Default)]
struct FakeState {
    containers: BTreeMap<String, EngineContainer>,
    networks: BTreeMap<String, BTreeMap<String, String>>,
    events: VecDeque<String>,
    builds: BTreeMap<String, Vec<u8>>,
}

impl FakeDocker {
    /// Adds an engine container without exercising runtime reconciliation.
    fn insert(&self, container: EngineContainer) {
        self.state
            .lock()
            .expect("fake state lock")
            .containers
            .insert(container.name.clone(), container);
    }

    /// Returns the ordered engine operations recorded by the fake.
    fn events(&self) -> Vec<String> {
        self.state
            .lock()
            .expect("fake state lock")
            .events
            .iter()
            .cloned()
            .collect()
    }
}

#[async_trait]
impl DockerApi for FakeDocker {
    /// Builds one immutable image from a normalized tar context.
    async fn build(&self, image: &str, context: Vec<u8>) -> Result<(), RuntimeError> {
        let mut state = self.state.lock().expect("fake state lock");
        state.events.push_back(format!("build:{image}"));
        state.builds.insert(image.to_owned(), context);
        Ok(())
    }

    /// Finds a container by its exact engine name.
    async fn inspect(&self, name: &str) -> Result<Option<EngineContainer>, RuntimeError> {
        Ok(self
            .state
            .lock()
            .expect("fake state lock")
            .containers
            .get(name)
            .cloned())
    }

    /// Creates a stopped container from a normalized request.
    async fn create(&self, request: EngineCreateRequest) -> Result<(), RuntimeError> {
        let mut state = self.state.lock().expect("fake state lock");
        state.events.push_back(format!("create:{}", request.name));
        state.containers.insert(
            request.name.clone(),
            EngineContainer {
                id: format!("id-{}", request.name),
                name: request.name,
                labels: request.labels,
                state: ObservedState::Stopped,
                published_ports: request
                    .published_ports
                    .into_iter()
                    .map(|mut port| {
                        if port.host_port.is_none() {
                            port.host_port = Some(40_000 + port.container_port % 10_000);
                        }
                        port
                    })
                    .collect(),
            },
        );
        Ok(())
    }

    /// Starts an existing stopped container.
    async fn start(&self, name: &str) -> Result<(), RuntimeError> {
        let mut state = self.state.lock().expect("fake state lock");
        state.events.push_back(format!("start:{name}"));
        if let Some(container) = state.containers.get_mut(name) {
            container.state = ObservedState::Running;
        }
        Ok(())
    }

    /// Stops an existing running container.
    async fn stop(&self, name: &str) -> Result<(), RuntimeError> {
        let mut state = self.state.lock().expect("fake state lock");
        state.events.push_back(format!("stop:{name}"));
        if let Some(container) = state.containers.get_mut(name) {
            container.state = ObservedState::Stopped;
        }
        Ok(())
    }

    /// Removes an existing stopped container.
    async fn remove(&self, name: &str) -> Result<(), RuntimeError> {
        let mut state = self.state.lock().expect("fake state lock");
        state.events.push_back(format!("remove:{name}"));
        state.containers.remove(name);
        Ok(())
    }

    /// Lists every container known to the fake engine.
    async fn list(&self) -> Result<Vec<EngineContainer>, RuntimeError> {
        Ok(self
            .state
            .lock()
            .expect("fake state lock")
            .containers
            .values()
            .cloned()
            .collect())
    }

    /// Finds a network and returns its engine labels.
    async fn inspect_network(
        &self,
        name: &str,
    ) -> Result<Option<BTreeMap<String, String>>, RuntimeError> {
        Ok(self
            .state
            .lock()
            .expect("fake state lock")
            .networks
            .get(name)
            .cloned())
    }

    /// Creates one labelled bridge network.
    async fn create_network(
        &self,
        name: &str,
        labels: BTreeMap<String, String>,
    ) -> Result<(), RuntimeError> {
        let mut state = self.state.lock().expect("fake state lock");
        state.events.push_back(format!("network:{name}"));
        state.networks.insert(name.to_owned(), labels);
        Ok(())
    }
}

fn typescript_project() -> VesselProjectSpec {
    VesselProjectSpec {
        name: "shipping-map".to_owned(),
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
                    "import { Hono } from 'hono';\nBun.serve({fetch: new Hono().get('/', c => c.text('ok')).fetch, port: 8380});\n"
                        .to_owned(),
                ),
            ]),
        },
        environment: BTreeMap::new(),
        http: VesselHttp {
            port: 8380,
            health_path: Some("/health".to_owned()),
        },
    }
}

#[test]
fn typescript_project_is_content_addressed_and_generates_a_owned_build() {
    let project = typescript_project();

    let build = project.build_context().expect("build context");

    assert!(
        build
            .image
            .starts_with("verglas/vessel-shipping-map:sha256-")
    );
    assert!(build.dockerfile.contains("FROM oven/bun:"));
    assert!(build.dockerfile.contains("RUN bun install"));
    assert!(build.dockerfile.contains("RUN bun run --if-present build"));
    assert!(
        build
            .dockerfile
            .contains("CMD [\"bun\", \"run\", \"start\"]")
    );
    assert!(!build.context.is_empty());
}

#[test]
fn typescript_project_digest_changes_with_a_dependency() {
    let original = typescript_project().build_context().expect("original");
    let mut changed = typescript_project();
    changed.project.files.insert(
        "package.json".to_owned(),
        r#"{"scripts":{"start":"bun src/server.ts"},"dependencies":{"hono":"4.8.4"}}"#.to_owned(),
    );

    let changed = changed.build_context().expect("changed");

    assert_ne!(original.image, changed.image);
}

#[test]
fn typescript_project_rejects_unsafe_or_incomplete_projects() {
    let mut traversal = typescript_project();
    traversal
        .project
        .files
        .insert("../secret".to_owned(), "no".to_owned());
    assert!(matches!(
        traversal.build_context(),
        Err(RuntimeError::InvalidProjectPath { .. })
    ));

    let mut dockerfile = typescript_project();
    dockerfile
        .project
        .files
        .insert("Dockerfile".to_owned(), "FROM scratch".to_owned());
    assert!(matches!(
        dockerfile.build_context(),
        Err(RuntimeError::InvalidProjectPath { .. })
    ));

    let mut missing_package = typescript_project();
    missing_package.project.files.remove("package.json");
    assert!(matches!(
        missing_package.build_context(),
        Err(RuntimeError::MissingProjectFile { .. })
    ));
}

fn fixture() -> ContainerSpec {
    ContainerSpec::new("scheduler", "verglas/verglas-scheduler:local")
        .with_command(["verglas-scheduler"])
        .with_environment("VERGLAS_SCHEDULER_QUEUE", "local")
}

#[test]
fn vessel_maps_to_one_unpublished_managed_container() {
    let vessel = VesselSpec {
        name: "demo".to_owned(),
        role: VesselRole::Integration,
        image: "verglas/integration-demo:local".to_owned(),
        command: Vec::new(),
        entrypoint: Vec::new(),
        environment: BTreeMap::new(),
        http: VesselHttp {
            port: 8371,
            health_path: Some("/health".to_owned()),
        },
    };

    let container = vessel.container_spec().expect("container specification");

    assert_eq!(container.deployment_id, "vessel-demo");
    assert!(container.published_ports.is_empty());
    assert_eq!(container.image, vessel.image);
}

#[tokio::test]
async fn reconcile_creates_and_starts_missing_container() {
    let api = FakeDocker::default();
    let runtime = DockerRuntimeCore::new(api.clone());

    let outcome = runtime.reconcile(&fixture()).await.expect("reconcile");

    assert_eq!(outcome, ReconcileOutcome::Created);
    assert_eq!(
        api.events(),
        ["create:verglas-scheduler", "start:verglas-scheduler"]
    );
}

#[tokio::test]
async fn reconcile_is_noop_for_unchanged_running_container() {
    let api = FakeDocker::default();
    let runtime = DockerRuntimeCore::new(api.clone());
    runtime
        .reconcile(&fixture())
        .await
        .expect("first reconcile");

    let outcome = runtime
        .reconcile(&fixture())
        .await
        .expect("second reconcile");

    assert_eq!(outcome, ReconcileOutcome::Unchanged);
    assert_eq!(api.events().len(), 2);
}

#[tokio::test]
async fn reconcile_starts_unchanged_stopped_container() {
    let api = FakeDocker::default();
    let runtime = DockerRuntimeCore::new(api.clone());
    runtime
        .reconcile(&fixture())
        .await
        .expect("first reconcile");
    runtime.stop("scheduler").await.expect("stop");

    let outcome = runtime
        .reconcile(&fixture())
        .await
        .expect("second reconcile");

    assert_eq!(outcome, ReconcileOutcome::Started);
    assert_eq!(api.events().last(), Some(&"start:verglas-scheduler".into()));
}

#[tokio::test]
async fn reconcile_replaces_changed_immutable_specification() {
    let api = FakeDocker::default();
    let runtime = DockerRuntimeCore::new(api.clone());
    runtime
        .reconcile(&fixture())
        .await
        .expect("first reconcile");
    let changed = fixture().with_environment("VERGLAS_SCHEDULER_QUEUE", "priority");

    let outcome = runtime
        .reconcile(&changed)
        .await
        .expect("changed reconcile");

    assert_eq!(outcome, ReconcileOutcome::Replaced);
    assert_eq!(
        &api.events()[2..],
        [
            "stop:verglas-scheduler",
            "remove:verglas-scheduler",
            "create:verglas-scheduler",
            "start:verglas-scheduler"
        ]
    );
}

#[tokio::test]
async fn stop_and_remove_are_idempotent() {
    let api = FakeDocker::default();
    let runtime = DockerRuntimeCore::new(api.clone());
    runtime.reconcile(&fixture()).await.expect("reconcile");

    assert!(runtime.stop("scheduler").await.expect("first stop"));
    assert!(!runtime.stop("scheduler").await.expect("second stop"));
    assert!(runtime.remove("scheduler").await.expect("first remove"));
    assert!(!runtime.remove("scheduler").await.expect("second remove"));
}

#[tokio::test]
async fn unmanaged_name_collision_fails_closed() {
    let api = FakeDocker::default();
    api.insert(EngineContainer {
        id: "foreign-id".into(),
        name: "verglas-scheduler".into(),
        labels: BTreeMap::new(),
        state: ObservedState::Running,
        published_ports: Vec::new(),
    });
    let runtime = DockerRuntimeCore::new(api.clone());

    let error = runtime.reconcile(&fixture()).await.expect_err("collision");

    assert!(matches!(error, RuntimeError::UnmanagedCollision { .. }));
    assert!(api.events().is_empty());
}

#[tokio::test]
async fn list_filters_foreign_containers_and_normalizes_identity() {
    let api = FakeDocker::default();
    let runtime = DockerRuntimeCore::new(api.clone());
    runtime.reconcile(&fixture()).await.expect("reconcile");
    api.insert(EngineContainer {
        id: "foreign-id".into(),
        name: "foreign".into(),
        labels: BTreeMap::new(),
        state: ObservedState::Running,
        published_ports: Vec::new(),
    });

    let containers = runtime.list().await.expect("list");

    assert_eq!(containers.len(), 1);
    assert_eq!(containers[0].deployment_id, "scheduler");
    assert_eq!(containers[0].state, ObservedState::Running);
}

#[test]
fn workload_cannot_receive_docker_authority() {
    let socket = ContainerSpec::new("unsafe", "alpine:3.22")
        .with_bind_mount("/var/run/docker.sock", "/var/run/docker.sock");
    let host = ContainerSpec::new("unsafe", "alpine:3.22")
        .with_environment("DOCKER_HOST", "unix:///var/run/docker.sock");
    let credentials = ContainerSpec::new("unsafe", "alpine:3.22")
        .with_environment("DOCKER_AUTH_CONFIG", "secret");

    assert!(matches!(
        socket.validate(),
        Err(RuntimeError::DockerAuthority { .. })
    ));
    assert!(matches!(
        host.validate(),
        Err(RuntimeError::DockerAuthority { .. })
    ));
    assert!(matches!(
        credentials.validate(),
        Err(RuntimeError::DockerAuthority { .. })
    ));
}

/// Carries a declared OCI platform into the normalized Docker request.
#[test]
fn declared_platform_reaches_the_engine_create_request() {
    let request = ContainerSpec::new("amd64-workload", "example/image:sha")
        .with_platform("linux/amd64")
        .create_request()
        .expect("request");

    assert_eq!(request.platform.as_deref(), Some("linux/amd64"));
}

#[test]
fn canonical_digest_is_stable_across_environment_insertion_order() {
    let first = ContainerSpec::new("job", "alpine:3.22")
        .with_environment("B", "2")
        .with_environment("A", "1");
    let second = ContainerSpec::new("job", "alpine:3.22")
        .with_environment("A", "1")
        .with_environment("B", "2");

    assert_eq!(
        first.digest().expect("first"),
        second.digest().expect("second")
    );
}

#[test]
fn managed_labels_include_ownership_and_spec_digest() {
    let labels = fixture().labels().expect("labels");

    assert_eq!(labels.get(LABEL_MANAGED).map(String::as_str), Some("true"));
    assert!(labels.contains_key(LABEL_SPEC_DIGEST));
}

#[tokio::test]
async fn ensure_network_creates_once_and_rejects_foreign_collision() {
    let api = FakeDocker::default();
    let runtime = DockerRuntimeCore::new(api.clone());

    assert!(
        runtime
            .ensure_network("verglas-runtime")
            .await
            .expect("create network")
    );
    assert!(
        !runtime
            .ensure_network("verglas-runtime")
            .await
            .expect("reuse network")
    );

    api.state
        .lock()
        .expect("fake state lock")
        .networks
        .insert("foreign".into(), BTreeMap::new());
    let error = runtime
        .ensure_network("foreign")
        .await
        .expect_err("foreign collision");
    assert!(matches!(error, RuntimeError::UnmanagedCollision { .. }));
}

#[test]
fn published_ports_are_part_of_the_immutable_digest() {
    let private = ContainerSpec::new("service", "alpine:3.22");
    let published = private.clone().with_published_port(8350, 8350);

    assert_ne!(
        private.digest().expect("private digest"),
        published.digest().expect("published digest")
    );
}

#[test]
fn inline_files_and_ephemeral_ports_reach_the_engine_request() {
    let source = tempfile::NamedTempFile::new().expect("source");
    std::fs::write(source.path(), "vgt1.secret").expect("write source");
    let request = ContainerSpec::new("database-proxy", "example/proxy:sha")
        .with_file(
            source.path().to_string_lossy(),
            "/run/secrets/workload-token",
            0o600,
        )
        .with_ephemeral_port(5432)
        .create_request()
        .expect("request");

    assert_eq!(request.files.len(), 1);
    assert_eq!(request.files[0].path, "/run/secrets/workload-token");
    assert_eq!(request.files[0].contents, b"vgt1.secret");
    assert_eq!(request.files[0].mode, 0o600);
    assert_eq!(request.published_ports[0].container_port, 5432);
    assert_eq!(request.published_ports[0].host_port, None);
}

#[tokio::test]
async fn source_file_rotation_replaces_container_without_persisting_bearer() {
    let source = tempfile::NamedTempFile::new().expect("source");
    std::fs::write(source.path(), "first-bearer").expect("first bearer");
    let specification = ContainerSpec::new("database-proxy", "example/proxy:sha").with_file(
        source.path().to_string_lossy(),
        "/run/secrets/workload-token",
        0o600,
    );
    assert!(
        !serde_json::to_string(&specification)
            .expect("serialize")
            .contains("first-bearer")
    );
    let api = FakeDocker::default();
    let runtime = DockerRuntimeCore::new(api);
    runtime
        .reconcile(&specification)
        .await
        .expect("first reconcile");

    std::fs::write(source.path(), "rotated-bearer").expect("rotate bearer");
    let outcome = runtime
        .reconcile(&specification)
        .await
        .expect("rotation reconcile");

    assert_eq!(outcome, ReconcileOutcome::Replaced);
}

#[tokio::test]
async fn ephemeral_host_port_is_observable_after_reconcile() {
    let api = FakeDocker::default();
    let runtime = DockerRuntimeCore::new(api);
    let specification =
        ContainerSpec::new("database-proxy", "example/proxy:sha").with_ephemeral_port(5432);

    runtime.reconcile(&specification).await.expect("reconcile");
    let observed = runtime
        .inspect("database-proxy")
        .await
        .expect("inspect")
        .expect("container");

    assert_eq!(observed.published_ports.len(), 1);
    assert_eq!(observed.published_ports[0].container_port, 5432);
    assert_eq!(observed.published_ports[0].host_port, Some(45_432));
}

#[test]
fn inline_file_paths_and_modes_fail_closed() {
    let relative =
        ContainerSpec::new("service", "alpine:3.22").with_file("/tmp/source", "run/secret", 0o600);
    let traversal = ContainerSpec::new("service", "alpine:3.22").with_file(
        "/tmp/source",
        "/run/../secret",
        0o600,
    );
    let executable =
        ContainerSpec::new("service", "alpine:3.22").with_file("/tmp/source", "/run/secret", 0o700);

    assert!(matches!(
        relative.validate(),
        Err(RuntimeError::InvalidFile { .. })
    ));
    assert!(matches!(
        traversal.validate(),
        Err(RuntimeError::InvalidFile { .. })
    ));
    assert!(matches!(
        executable.validate(),
        Err(RuntimeError::InvalidFile { .. })
    ));
}

#[test]
fn entrypoint_override_is_part_of_the_immutable_digest() {
    let manager = ContainerSpec::new("scheduler", "verglas/verglas-container-runtime:local");
    let scheduler = manager.clone().with_entrypoint(["verglas-scheduler"]);

    assert_ne!(
        manager.digest().expect("manager digest"),
        scheduler.digest().expect("scheduler digest")
    );
}

#[test]
fn local_postgres_tls_identity_is_created_once_in_runtime_state() {
    let directory = tempfile::tempdir().expect("state directory");
    let state = directory.path().join("deployments.json");

    let first = ensure_local_postgres_tls(&state).expect("create identity");
    let certificate = std::fs::read(&first.certificate).expect("certificate");
    let private_key = std::fs::read(&first.private_key).expect("private key");
    let again = ensure_local_postgres_tls(&state).expect("reuse identity");

    assert_eq!(first, again);
    assert_eq!(
        certificate,
        std::fs::read(&again.certificate).expect("certificate")
    );
    assert_eq!(private_key, std::fs::read(&again.private_key).expect("key"));
    assert!(
        String::from_utf8(certificate)
            .expect("pem")
            .contains("BEGIN CERTIFICATE")
    );
    assert!(
        String::from_utf8(private_key)
            .expect("pem")
            .contains("BEGIN PRIVATE KEY")
    );
}
