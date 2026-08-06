//! Lifecycle contract tests for the Docker placement adapter.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use super::{
    ContainerSpec, DockerApi, DockerRuntimeCore, EngineContainer, EngineCreateRequest,
    LABEL_MANAGED, LABEL_SPEC_DIGEST, ObservedState, ReconcileOutcome, RuntimeError,
};

#[derive(Clone, Default)]
struct FakeDocker {
    state: Arc<Mutex<FakeState>>,
}

#[derive(Default)]
struct FakeState {
    containers: BTreeMap<String, EngineContainer>,
    events: VecDeque<String>,
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
}

fn fixture() -> ContainerSpec {
    ContainerSpec::new("scheduler", "verglas/verglas-scheduler:local")
        .with_command(["verglas-scheduler"])
        .with_environment("VERGLAS_SCHEDULER_QUEUE", "local")
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

    assert!(matches!(
        socket.validate(),
        Err(RuntimeError::DockerAuthority { .. })
    ));
    assert!(matches!(
        host.validate(),
        Err(RuntimeError::DockerAuthority { .. })
    ));
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
