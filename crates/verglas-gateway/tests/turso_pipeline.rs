//! Turso deployment and pipeline binding acceptance tests.

use std::sync::Arc;

use async_trait::async_trait;
use verglas_gateway::{DoSpawner, Gateway, GatewayError, Manifest, SpawnRequest};

const DIGEST: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

fn manifest_source(extra: &str) -> String {
    format!(
        r#"{{
            "name": "stream-worker",
            "main": "worker.js",
            "durable_objects": {{"bindings": [{{"name":"COUNTER","class_name":"Counter"}}]}},
            "pipelines": [{{"binding":"STREAM","stream":"stream-identity"}}],
            "component_digest": "{DIGEST}",
            "component_dir": "components",
            "data_root": "state",
            "turso": {{"url_template":"https://turso.test/{{binding}}/{{do_id}}","token_file":"/tokens/{{binding}}.token"}}
            {extra}
        }}"#
    )
}

/// A deployment-level Turso template resolves the exact Worker launch credentials.
#[test]
fn resolves_turso_deployment_for_do_binding() {
    let manifest = Manifest::parse(&manifest_source("")).expect("manifest");
    let deployment = manifest.turso_for("COUNTER").expect("Turso deployment");
    assert_eq!(
        deployment.url("COUNTER", "do-1"),
        "https://turso.test/COUNTER/do-1"
    );
    assert_eq!(
        deployment.token_file("COUNTER", "do-1").to_str(),
        Some("/tokens/COUNTER.token")
    );
}

/// Pipeline bindings remain separate from durable-object namespace bindings.
#[test]
fn pipeline_binding_resolves_stream_identity() {
    let manifest = Manifest::parse(&manifest_source("")).expect("manifest");
    assert!(manifest.binding("STREAM").is_none());
    let pipeline = manifest.pipeline("STREAM").expect("pipeline");
    assert_eq!(pipeline.stream(), "stream-identity");
    assert!(manifest.turso_for("STREAM").is_ok());
}

/// Missing Turso deployment credentials fail closed instead of activating local state.
#[test]
fn missing_turso_config_is_rejected() {
    let source = manifest_source(",\n            \"turso\": null");
    let error = Manifest::parse(&source).expect_err("missing Turso config");
    assert!(error.to_string().contains("turso"));
}

/// Unknown Turso fields fail closed.
#[test]
fn unknown_turso_field_is_rejected() {
    let source = manifest_source("").replace(
        "\"token_file\":\"/tokens/{binding}.token\"",
        "\"token_file\":\"/tokens/{binding}.token\",\"fallback\":true",
    );
    let error = Manifest::parse(&source).expect_err("unknown Turso field");
    assert!(error.to_string().contains("unknown turso"));
}

#[derive(Default)]
struct RecordingSpawner;

#[async_trait]
impl DoSpawner for RecordingSpawner {
    /// Records no process and returns a deterministic event endpoint for routing tests.
    async fn spawn(&self, request: SpawnRequest) -> Result<std::path::PathBuf, GatewayError> {
        assert_eq!(
            request.turso_url(),
            "https://turso.test/STREAM/stream-identity"
        );
        assert_eq!(
            request.turso_token_file().to_str(),
            Some("/tokens/STREAM.token")
        );
        Ok(request.data_root().join("events.sock"))
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
