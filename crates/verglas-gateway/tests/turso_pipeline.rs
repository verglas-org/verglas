//! Pipeline binding and local Worker launch acceptance tests.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, UnixListener};
use tokio::sync::Mutex;
use verglas_gateway::{
    ArtifactProduct, CelldSpawner, DoSpawner, Gateway, GatewayError, Manifest, SpawnRequest,
};

const DIGEST: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

fn manifest_source(extra: &str) -> String {
    format!(
        r#"{{
            "name": "stream-worker",
            "main": "worker.js",
            "durable_objects": {{"bindings": [{{"name":"COUNTER","class_name":"Counter"}}]}},
            "pipelines": [{{"binding":"STREAM","stream":"stream-identity"}}],
            "artifacts":{{
                "worker":{{"digest":"{DIGEST}","component_dir":"components"}},
                "durable_object":{{"digest":"{DIGEST}","component_dir":"components"}},
                "stream":{{"digest":"{DIGEST}","component_dir":"components"}}
            }},
            "data_root": "state"
            {extra}
        }}"#
    )
}

/// Pipeline bindings remain separate from durable-object namespace bindings.
#[test]
fn pipeline_binding_resolves_stream_identity() {
    let manifest = Manifest::parse(&manifest_source("")).expect("manifest");
    assert!(manifest.binding("STREAM").is_none());
    let pipeline = manifest.pipeline("STREAM").expect("pipeline");
    assert_eq!(pipeline.stream(), "stream-identity");
}

/// Removed remote deployment fields fail closed as unknown manifest keys.
#[test]
fn removed_remote_deployment_fields_are_rejected() {
    let source = manifest_source(
        ",\n            \"turso\":{\"url_template\":\"https://turso.test/{binding}/{do_id}\",\"token_file\":\"/tokens/{binding}.token\"}",
    );
    let error = Manifest::parse(&source).expect_err("removed deployment field");
    assert!(
        error
            .to_string()
            .contains("unknown top-level manifest key: turso")
    );
}

#[derive(Default)]
struct RecordingSpawner;

#[async_trait]
impl DoSpawner for RecordingSpawner {
    /// Records no process and returns a deterministic event endpoint for routing tests.
    async fn spawn(&self, request: SpawnRequest) -> Result<std::path::PathBuf, GatewayError> {
        Ok(request.data_root().join("events.sock"))
    }
}

#[derive(Clone, Default)]
struct CapturingSpawner {
    requests: Arc<Mutex<Vec<SpawnRequest>>>,
}

#[async_trait]
impl DoSpawner for CapturingSpawner {
    /// Captures the selected launch descriptor and serves one empty fetch result.
    async fn spawn(&self, request: SpawnRequest) -> Result<std::path::PathBuf, GatewayError> {
        self.requests.lock().await.push(request.clone());
        let event_path = request
            .data_root()
            .join(request.do_id())
            .join("events.sock");
        tokio::fs::create_dir_all(event_path.parent().expect("event parent"))
            .await
            .map_err(|error| GatewayError::SpawnRejected {
                message: error.to_string(),
            })?;
        let listener =
            UnixListener::bind(&event_path).map_err(|error| GatewayError::SpawnRejected {
                message: error.to_string(),
            })?;
        tokio::spawn(async move {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let (read_half, mut write_half) = stream.into_split();
            let mut lines = BufReader::new(read_half).lines();
            let Ok(Some(line)) = lines.next_line().await else {
                return;
            };
            let Ok(frame) = serde_json::from_str::<Value>(&line) else {
                return;
            };
            let Some(id) = frame.get("id").and_then(Value::as_u64) else {
                return;
            };
            let response = serde_json::json!({
                "type": "fetch-result",
                "id": id,
                "status": 204,
                "headers": [],
                "body_b64": ""
            });
            let _ = write_half
                .write_all(format!("{response}\n").as_bytes())
                .await;
        });
        Ok(event_path)
    }
}

/// Pipeline do-fetch can use the typed binding route without becoming a DO namespace.
#[test]
fn gateway_keeps_pipeline_route_distinct() {
    let manifest = Manifest::parse(&manifest_source("")).expect("manifest");
    let gateway = Gateway::with_spawner(&manifest, "state", Arc::new(RecordingSpawner));
    assert!(
        gateway
            .resolve_binding("STREAM", "stream-identity")
            .is_err()
    );
    assert!(
        gateway
            .resolve_pipeline("STREAM", "stream-identity")
            .is_ok()
    );
}

/// Gateway routing selects each declared product artifact before spawning.
#[tokio::test]
async fn gateway_routes_each_product_to_its_artifact()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let manifest = Manifest::parse(&composition_manifest_source()).expect("composition manifest");
    let directory = tempfile::tempdir()?;
    let spawner = Arc::new(CapturingSpawner::default());
    let gateway = Gateway::with_spawner(&manifest, directory.path().join("state"), spawner.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(gateway.serve(listener));
    let cases = [
        ("COUNTER", "counter-1", DO_DIGEST),
        ("STREAM", "stream-1", STREAM_DIGEST),
        ("PIPELINE", "pipeline-1", PIPELINE_DIGEST),
        ("SINK", "sink-1", SINK_DIGEST),
        ("CATALOG", "catalog-1", CATALOG_DIGEST),
    ];
    for (binding, object, digest) in cases {
        let response = reqwest::get(format!("http://{address}/do/{binding}/{object}")).await?;
        assert_eq!(response.status(), reqwest::StatusCode::NO_CONTENT);
        let requests = spawner.requests.lock().await;
        let request = requests
            .iter()
            .find(|request| request.binding() == binding)
            .expect("spawn request");
        assert_eq!(request.name(), object);
        assert_eq!(request.component_digest(), digest);
    }
    server.abort();
    let _ = server.await;
    Ok(())
}

const WORKER_DIGEST: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const DO_DIGEST: &str = "2222222222222222222222222222222222222222222222222222222222222222";
const STREAM_DIGEST: &str = "3333333333333333333333333333333333333333333333333333333333333333";
const PIPELINE_DIGEST: &str = "4444444444444444444444444444444444444444444444444444444444444444";
const SINK_DIGEST: &str = "5555555555555555555555555555555555555555555555555555555555555555";
const CATALOG_DIGEST: &str = "6666666666666666666666666666666666666666666666666666666666666666";

fn composition_manifest_source() -> String {
    format!(
        r#"{{
            "name":"composed",
            "main":"worker.js",
            "durable_objects":{{"bindings":[{{"name":"COUNTER","class_name":"Counter"}}]}},
            "pipelines":[{{"binding":"STREAM","stream":"stream-1"}}],
            "services":[
                {{"binding":"PIPELINE","service":"pipeline","object":"pipeline-1"}},
                {{"binding":"SINK","service":"sink","object":"sink-1"}},
                {{"binding":"CATALOG","service":"catalog","object":"catalog-1"}},
                {{"binding":"ICEBERG_COMMIT","service":"verglas-runtime"}}
            ],
            "artifacts":{{
                "worker":{{"digest":"{WORKER_DIGEST}","component_dir":"worker"}},
                "durable_object":{{"digest":"{DO_DIGEST}","component_dir":"do"}},
                "stream":{{"digest":"{STREAM_DIGEST}","component_dir":"stream"}},
                "pipeline":{{"digest":"{PIPELINE_DIGEST}","component_dir":"pipeline"}},
                "sink":{{"digest":"{SINK_DIGEST}","component_dir":"sink"}},
                "catalog":{{"digest":"{CATALOG_DIGEST}","component_dir":"catalog"}}
            }},
            "data_root":"state"
        }}"#
    )
}

/// Every named binding selects its product artifact rather than the caller Worker.
#[test]
fn product_bindings_select_distinct_artifacts() {
    let manifest = Manifest::parse(&composition_manifest_source()).expect("composition manifest");
    let cases = [
        (
            "COUNTER",
            "counter-1",
            ArtifactProduct::DurableObject,
            DO_DIGEST,
        ),
        ("STREAM", "stream-1", ArtifactProduct::Stream, STREAM_DIGEST),
        (
            "PIPELINE",
            "pipeline-1",
            ArtifactProduct::Pipeline,
            PIPELINE_DIGEST,
        ),
        ("SINK", "sink-1", ArtifactProduct::Sink, SINK_DIGEST),
        (
            "CATALOG",
            "catalog-1",
            ArtifactProduct::Catalog,
            CATALOG_DIGEST,
        ),
    ];
    for (binding, object, product, digest) in cases {
        assert_eq!(
            manifest
                .product_for_binding(binding, object)
                .expect("binding"),
            product
        );
        assert_eq!(
            manifest
                .artifact_for_binding(binding, object)
                .expect("artifact")
                .digest(),
            digest
        );
    }
    assert_eq!(
        manifest
            .artifact_for_product(ArtifactProduct::Worker)
            .expect("worker")
            .digest(),
        WORKER_DIGEST
    );
    for (binding, service, object) in [
        ("PIPELINE", "pipeline", "pipeline-1"),
        ("SINK", "sink", "sink-1"),
        ("CATALOG", "catalog", "catalog-1"),
    ] {
        let service_binding = manifest
            .services()
            .iter()
            .find(|item| item.binding() == binding)
            .expect("service binding");
        assert_eq!(service_binding.service(), service);
        assert_eq!(service_binding.object(), object);
    }
}

/// Runtime host capabilities remain infrastructure and require no seventh artifact.
#[test]
fn runtime_commit_service_is_a_narrow_host_capability() {
    let manifest = Manifest::parse(&composition_manifest_source()).expect("composition manifest");
    assert_eq!(manifest.host_services().len(), 1);
    let capability = &manifest.host_services()[0];
    assert_eq!(capability.binding(), "ICEBERG_COMMIT");
    assert_eq!(capability.service(), "verglas-runtime");
    assert!(
        manifest
            .product_for_binding("ICEBERG_COMMIT", "verglas-runtime")
            .is_err(),
        "host capability must not resolve to a product artifact"
    );

    let recursive = composition_manifest_source().replace(
        r#"{"binding":"ICEBERG_COMMIT","service":"verglas-runtime"}"#,
        r#"{"binding":"ICEBERG_COMMIT","service":"verglas-runtime","object":"runtime"}"#,
    );
    assert!(Manifest::parse(&recursive).is_err());
    let broad = composition_manifest_source().replace("ICEBERG_COMMIT", "HOST_FETCH");
    assert!(Manifest::parse(&broad).is_err());
}

#[test]
fn bindings_can_target_remote_worker_microvms() {
    let source = composition_manifest_source()
        .replace(
            r#"{"name":"COUNTER","class_name":"Counter"}"#,
            r#"{"name":"COUNTER","class_name":"Counter","origin":"http://counter.internal:8787"}"#,
        )
        .replace(
            r#"{"binding":"STREAM","stream":"stream-1"}"#,
            r#"{"binding":"STREAM","stream":"stream-1","origin":"http://stream.internal:8787"}"#,
        )
        .replace(
            r#"{"binding":"PIPELINE","service":"pipeline","object":"pipeline-1"}"#,
            r#"{"binding":"PIPELINE","service":"pipeline","object":"pipeline-1","origin":"http://pipeline.internal:8787"}"#,
        );
    let manifest = Manifest::parse(&source).expect("remote Worker manifest");
    assert_eq!(
        manifest
            .origin_for_binding("COUNTER", "global")
            .expect("DO binding"),
        Some("http://counter.internal:8787"),
    );
    assert_eq!(
        manifest
            .origin_for_binding("STREAM", "stream-1")
            .expect("Stream binding"),
        Some("http://stream.internal:8787"),
    );
    assert_eq!(
        manifest
            .origin_for_binding("PIPELINE", "pipeline-1")
            .expect("service binding"),
        Some("http://pipeline.internal:8787"),
    );
}

/// The gateway carries the exact runtime capability declaration into one spawn request.
#[tokio::test]
async fn gateway_forwards_exact_host_service_to_spawn_request()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let manifest = Manifest::parse(&composition_manifest_source()).expect("composition manifest");
    let directory = tempfile::tempdir()?;
    let spawner = Arc::new(CapturingSpawner::default());
    let gateway = Gateway::with_spawner(&manifest, directory.path().join("state"), spawner.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(gateway.serve(listener));

    let response = reqwest::get(format!("http://{address}/do/CATALOG/catalog-1")).await?;
    assert_eq!(response.status(), reqwest::StatusCode::NO_CONTENT);
    let requests = spawner.requests.lock().await;
    let request = requests
        .iter()
        .find(|request| request.binding() == "CATALOG")
        .expect("Catalog spawn request");
    let service = request.host_service().expect("runtime host service");
    assert_eq!(service.binding(), "ICEBERG_COMMIT");
    assert_eq!(service.service(), "verglas-runtime");

    server.abort();
    let _ = server.await;
    Ok(())
}

/// ICEBERG_COMMIT is attached only to the selected Catalog artifact spawn.
#[tokio::test]
async fn gateway_attaches_iceberg_commit_only_to_catalog_spawn()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let manifest = Manifest::parse(&composition_manifest_source()).expect("composition manifest");
    let directory = tempfile::tempdir()?;
    let spawner = Arc::new(CapturingSpawner::default());
    let gateway = Gateway::with_spawner(&manifest, directory.path().join("state"), spawner.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(gateway.serve(listener));

    for (binding, object) in [("SINK", "sink-1"), ("CATALOG", "catalog-1")] {
        let response = reqwest::get(format!("http://{address}/do/{binding}/{object}")).await?;
        assert_eq!(response.status(), reqwest::StatusCode::NO_CONTENT);
    }

    let requests = spawner.requests.lock().await;
    let sink = requests
        .iter()
        .find(|request| request.binding() == "SINK")
        .expect("Sink spawn request");
    assert!(
        sink.host_service().is_none(),
        "ICEBERG_COMMIT must not be attached to non-Catalog products"
    );
    let catalog = requests
        .iter()
        .find(|request| request.binding() == "CATALOG")
        .expect("Catalog spawn request");
    let service = catalog
        .host_service()
        .expect("Catalog runtime host service");
    assert_eq!(service.binding(), "ICEBERG_COMMIT");
    assert_eq!(service.service(), "verglas-runtime");

    server.abort();
    let _ = server.await;
    Ok(())
}

/// The gateway serializes the exact host service declaration in the celld command.
#[tokio::test]
async fn celld_spawner_forwards_exact_host_service_declaration()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let manifest = Manifest::parse(&composition_manifest_source()).expect("composition manifest");
    let directory = tempfile::tempdir()?;
    let data_root = directory.path().join("state");
    let control_path = directory.path().join("celld.sock");
    let do_id = "CATALOG--catalog-1";
    let event_path = data_root.join(do_id).join("events.sock");
    let listener = UnixListener::bind(&control_path)?;
    let expected_command = format!(
        "SPAWN_WORKER {do_id} {} {CATALOG_DIGEST} catalog - {} ICEBERG_COMMIT verglas-runtime",
        data_root.join(do_id).display(),
        event_path.display(),
    );
    let event_for_task = event_path.clone();
    let command_task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let (read_half, mut write_half) = stream.into_split();
        let mut lines = BufReader::new(read_half).lines();
        let command = lines.next_line().await?.ok_or("missing worker command")?;
        assert_eq!(command, expected_command);
        tokio::fs::create_dir_all(event_for_task.parent().ok_or("event parent")?).await?;
        let event_listener = UnixListener::bind(&event_for_task)?;
        write_half
            .write_all(format!("OK {}\\n", event_for_task.display()).as_bytes())
            .await?;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        drop(event_listener);
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    });
    let artifact = manifest.artifact_for_product(ArtifactProduct::Catalog)?;
    let request = SpawnRequest::new(
        do_id.to_owned(),
        "CATALOG".to_owned(),
        "catalog-1".to_owned(),
        artifact.digest().to_owned(),
        PathBuf::from("catalog"),
        data_root,
    )
    .with_host_service(manifest.host_services()[0].clone());
    let returned = CelldSpawner::new(control_path).spawn(request).await?;
    assert_eq!(returned, event_path);
    command_task.await??;
    Ok(())
}

/// Every product route forwards its selected digest and stable restart identity.
#[tokio::test]
async fn product_bindings_forward_distinct_commands_and_restart_identity()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let manifest = Manifest::parse(&composition_manifest_source()).expect("composition manifest");
    let cases = [
        (
            "COUNTER",
            "counter-1",
            ArtifactProduct::DurableObject,
            DO_DIGEST,
            "do",
        ),
        (
            "STREAM",
            "stream-1",
            ArtifactProduct::Stream,
            STREAM_DIGEST,
            "stream",
        ),
        (
            "PIPELINE",
            "pipeline-1",
            ArtifactProduct::Pipeline,
            PIPELINE_DIGEST,
            "pipeline",
        ),
        ("SINK", "sink-1", ArtifactProduct::Sink, SINK_DIGEST, "sink"),
        (
            "CATALOG",
            "catalog-1",
            ArtifactProduct::Catalog,
            CATALOG_DIGEST,
            "catalog",
        ),
    ];
    for (binding, object, product, digest, component_dir) in cases {
        assert_eq!(manifest.product_for_binding(binding, object)?, product);
        for _restart in 0..2 {
            let directory = tempfile::tempdir()?;
            let data_root = directory.path().join("state");
            let do_id = format!("{binding}--{object}");
            let event_path = data_root.join(&do_id).join("events.sock");
            let control_path = directory.path().join("celld.sock");
            let listener = UnixListener::bind(&control_path)?;
            let expected_command = format!(
                "SPAWN_WORKER {do_id} {} {digest} {component_dir} - {} - -",
                data_root.join(&do_id).display(),
                event_path.display()
            );
            let event_for_task = event_path.clone();
            let command_task = tokio::spawn(async move {
                let (stream, _) = listener.accept().await?;
                let (read_half, mut write_half) = stream.into_split();
                let mut lines = BufReader::new(read_half).lines();
                let command = lines.next_line().await?.ok_or("missing worker command")?;
                assert_eq!(command, expected_command);
                tokio::fs::create_dir_all(event_for_task.parent().ok_or("event parent")?).await?;
                let event_listener = UnixListener::bind(&event_for_task)?;
                write_half
                    .write_all(format!("OK {}\n", event_for_task.display()).as_bytes())
                    .await?;
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                drop(event_listener);
                Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
            });
            let artifact = manifest.artifact_for_product(product)?;
            let request = SpawnRequest::new(
                do_id.clone(),
                binding.to_owned(),
                object.to_owned(),
                artifact.digest().to_owned(),
                PathBuf::from(component_dir),
                data_root,
            );
            let returned = CelldSpawner::new(control_path).spawn(request).await?;
            assert_eq!(returned, event_path);
            command_task.await??;
        }
    }
    Ok(())
}

/// A declared product route rejects a different object identity.
#[test]
fn wrong_product_object_fails_closed() {
    let manifest = Manifest::parse(&composition_manifest_source()).expect("composition manifest");
    let error = manifest
        .artifact_for_binding("CATALOG", "other-catalog")
        .expect_err("wrong Catalog object");
    assert!(error.to_string().contains("catalog-1"));
}

/// Binding names cannot collide across Durable Object, Stream, and services.
#[test]
fn cross_namespace_binding_collision_fails_closed() {
    let source = composition_manifest_source().replace(
        "{\"binding\":\"SINK\",\"service\":\"sink\"",
        "{\"binding\":\"STREAM\",\"service\":\"sink\"",
    );
    let error = Manifest::parse(&source).expect_err("binding collision");
    assert!(error.to_string().contains("duplicate environment binding"));
}

/// Missing product artifacts are rejected instead of using the caller component.
#[test]
fn missing_product_artifact_fails_closed() {
    let source = composition_manifest_source().replace(
        &format!("\"stream\":{{\"digest\":\"{STREAM_DIGEST}\",\"component_dir\":\"stream\"}},"),
        "",
    );
    let error = Manifest::parse(&source).expect_err("missing Stream artifact");
    assert!(error.to_string().contains("stream"));
}

/// Artifact descriptors reject fields outside the immutable artifact contract.
#[test]
fn unknown_artifact_descriptor_fails_closed() {
    let source = composition_manifest_source().replace(
        "\"component_dir\":\"catalog\"",
        "\"component_dir\":\"catalog\",\"fallback\":true",
    );
    let error = Manifest::parse(&source).expect_err("unknown artifact field");
    assert!(error.to_string().contains("unknown artifact descriptor"));
}

/// A service binding cannot name an unsupported seventh product.
#[test]
fn unknown_service_product_fails_closed() {
    let source =
        composition_manifest_source().replace("\"service\":\"catalog\"", "\"service\":\"worker\"");
    let error = Manifest::parse(&source).expect_err("worker service alias");
    assert!(error.to_string().contains("service"));
}
