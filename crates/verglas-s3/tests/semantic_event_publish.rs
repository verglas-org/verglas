//! Frozen acceptance for R7-A: semantic mutation events for tenant workers.
//! RIME candidates must make this pass without editing this file.
//!
//! Contract: `wrap_with_event_publisher(inner, config)` decorates a
//! `SemanticApi` so that every SUCCESSFUL data mutation publishes one
//! fire-and-forget JSON event to the control-plane queue ingress:
//!
//!   PutVectors / DeleteVectors  -> eventType "org.verglas.vector.commit",
//!                                  subject "<vectorBucketName>.<indexName>"
//!   AddNodes / AddEdges         -> eventType "org.verglas.graph.commit",
//!                                  subject "<graphName>"
//!
//! POST body: {"eventType": ..., "tenant_id": ..., "subject": ...}
//! Header:    authorization: Bearer <token>
//!
//! Non-mutating operations, failed operations, and other operation names
//! publish nothing. Publish failures must never affect the client result.
//! `EventPublishConfig::from_env()` reads VERGLAS_EVENT_QUEUE_URL,
//! VERGLAS_CACHE_EVENT_TOKEN, and VERGLAS_TENANT_ID; any of the three absent
//! or empty means None (publishing stays off).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{json, Value};

use verglas_s3::semantic::{
    wrap_with_event_publisher, EventPublishConfig, SemanticApi, SemanticError, SemanticOperation,
};

/// One captured publish call: the `authorization` header value (if present)
/// alongside the decoded JSON event body.
type CapturedEvent = (Option<String>, Value);

#[derive(Clone, Default)]
struct Capture {
    events: Arc<Mutex<Vec<CapturedEvent>>>,
}

async fn record(
    State(capture): State<Capture>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Json<Value> {
    let authorization = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    capture
        .events
        .lock()
        .expect("capture mutex poisoned")
        .push((authorization, body));
    Json(json!({"queued": true}))
}

async fn capture_server() -> (String, Capture) {
    let capture = Capture::default();
    let app = Router::new()
        .route("/v1/table-commits", post(record))
        .with_state(capture.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral capture listener");
    let url = format!(
        "http://{}/v1/table-commits",
        listener
            .local_addr()
            .expect("bound listener has a local address")
    );
    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("capture server exited unexpectedly");
    });
    (url, capture)
}

struct FakeApi {
    fail: bool,
}

#[async_trait]
impl SemanticApi for FakeApi {
    async fn call(
        &self,
        _operation: SemanticOperation,
        _input: Value,
    ) -> Result<Value, SemanticError> {
        if self.fail {
            Err(SemanticError::validation("forced failure"))
        } else {
            Ok(json!({"ok": true}))
        }
    }
}

fn wrapped(url: &str, fail: bool) -> Arc<dyn SemanticApi> {
    wrap_with_event_publisher(
        Arc::new(FakeApi { fail }),
        EventPublishConfig {
            queue_url: url.to_owned(),
            token: "event-token-1".to_owned(),
            tenant_id: "tenant-r7".to_owned(),
        },
    )
}

async fn wait_for_events(capture: &Capture, count: usize) -> Vec<CapturedEvent> {
    for _ in 0..100 {
        {
            let events = capture.events.lock().expect("capture mutex poisoned");
            if events.len() >= count {
                return events.clone();
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    capture
        .events
        .lock()
        .expect("capture mutex poisoned")
        .clone()
}

#[tokio::test]
async fn put_vectors_publishes_a_vector_commit_event() {
    let (url, capture) = capture_server().await;
    let api = wrapped(&url, false);
    let result = api
        .call(
            SemanticOperation::S3Vectors("PutVectors"),
            json!({"vectorBucketName": "embeddings", "indexName": "docs", "vectors": []}),
        )
        .await;
    assert!(result.is_ok());
    let events = wait_for_events(&capture, 1).await;
    assert_eq!(events.len(), 1, "expected exactly one publish: {events:?}");
    let (authorization, body) = &events[0];
    assert_eq!(authorization.as_deref(), Some("Bearer event-token-1"));
    assert_eq!(body["eventType"], "org.verglas.vector.commit");
    assert_eq!(body["tenant_id"], "tenant-r7");
    assert_eq!(body["subject"], "embeddings.docs");
}

#[tokio::test]
async fn delete_vectors_publishes_a_vector_commit_event() {
    let (url, capture) = capture_server().await;
    let api = wrapped(&url, false);
    api.call(
        SemanticOperation::S3Vectors("DeleteVectors"),
        json!({"vectorBucketName": "embeddings", "indexName": "docs", "keys": ["k1"]}),
    )
    .await
    .expect("DeleteVectors should succeed against the fake api");
    let events = wait_for_events(&capture, 1).await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].1["eventType"], "org.verglas.vector.commit");
    assert_eq!(events[0].1["subject"], "embeddings.docs");
}

#[tokio::test]
async fn put_nodes_and_edges_publish_graph_commit_events() {
    let (url, capture) = capture_server().await;
    let api = wrapped(&url, false);
    api.call(
        SemanticOperation::Graph("AddNodes"),
        json!({"graphName": "social", "nodes": []}),
    )
    .await
    .expect("AddNodes should succeed against the fake api");
    api.call(
        SemanticOperation::Graph("AddEdges"),
        json!({"graphName": "social", "edges": []}),
    )
    .await
    .expect("AddEdges should succeed against the fake api");
    let events = wait_for_events(&capture, 2).await;
    assert_eq!(events.len(), 2, "expected two publishes: {events:?}");
    for (_, body) in &events {
        assert_eq!(body["eventType"], "org.verglas.graph.commit");
        assert_eq!(body["tenant_id"], "tenant-r7");
        assert_eq!(body["subject"], "social");
    }
}

#[tokio::test]
async fn reads_failures_and_unknown_operations_publish_nothing() {
    let (url, capture) = capture_server().await;

    // Reads on a working inner API.
    let api = wrapped(&url, false);
    api.call(
        SemanticOperation::S3Vectors("QueryVectors"),
        json!({"vectorBucketName": "embeddings", "indexName": "docs"}),
    )
    .await
    .expect("QueryVectors should succeed against the fake api");
    api.call(
        SemanticOperation::Graph("DescribeGraph"),
        json!({"graphName": "social"}),
    )
    .await
    .expect("DescribeGraph should succeed against the fake api");

    // A failing inner API on a mutating operation.
    let failing = wrapped(&url, true);
    let result = failing
        .call(
            SemanticOperation::Graph("AddNodes"),
            json!({"graphName": "social", "nodes": []}),
        )
        .await;
    assert!(result.is_err());

    // Sentinel mutation: once its event arrives, any earlier wrong publish
    // would already be in the capture ahead of it.
    api.call(
        SemanticOperation::Graph("AddEdges"),
        json!({"graphName": "sentinel", "edges": []}),
    )
    .await
    .expect("sentinel AddEdges should succeed against the fake api");
    let events = wait_for_events(&capture, 1).await;
    assert_eq!(events.len(), 1, "only the sentinel may publish: {events:?}");
    assert_eq!(events[0].1["subject"], "sentinel");
}

#[tokio::test]
async fn an_unreachable_queue_never_fails_the_client_operation() {
    let api = wrapped("http://127.0.0.1:9/v1/table-commits", false);
    let result = api
        .call(
            SemanticOperation::Graph("AddNodes"),
            json!({"graphName": "social", "nodes": []}),
        )
        .await;
    assert!(
        result.is_ok(),
        "publish failure must not surface: {result:?}"
    );
}

#[test]
fn from_env_requires_the_full_trio() {
    // This is the only test in this binary that touches these variables.
    std::env::remove_var("VERGLAS_EVENT_QUEUE_URL");
    std::env::remove_var("VERGLAS_CACHE_EVENT_TOKEN");
    std::env::remove_var("VERGLAS_TENANT_ID");
    assert!(EventPublishConfig::from_env().is_none());

    std::env::set_var(
        "VERGLAS_EVENT_QUEUE_URL",
        "https://api.verglas.dev/v1/table-commits",
    );
    assert!(
        EventPublishConfig::from_env().is_none(),
        "URL alone must not enable publishing"
    );

    std::env::set_var("VERGLAS_CACHE_EVENT_TOKEN", "token-1");
    std::env::set_var("VERGLAS_TENANT_ID", "tenant-1");
    let config = EventPublishConfig::from_env().expect("full trio enables publishing");
    assert_eq!(config.queue_url, "https://api.verglas.dev/v1/table-commits");
    assert_eq!(config.token, "token-1");
    assert_eq!(config.tenant_id, "tenant-1");

    std::env::remove_var("VERGLAS_EVENT_QUEUE_URL");
    std::env::remove_var("VERGLAS_CACHE_EVENT_TOKEN");
    std::env::remove_var("VERGLAS_TENANT_ID");
}
