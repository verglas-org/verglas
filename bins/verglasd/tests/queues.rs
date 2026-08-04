//! Acceptance for the daemon queue routes (#328):
//! `/v1/queues/<name>/{enqueue,poll,ack}` backed by `verglas_harness::queue`.
//!
//! The TS SDK queue verb already targets these three routes; this drives the
//! real admin router (no engine, no catalog needed — a queue is a segment log
//! under a temp dir) and asserts the at-least-once, consumer-group-watermark
//! contract end to end: enqueue appends, poll re-serves un-acked records, and
//! ack advances the watermark monotonically.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

/// Builds the admin router with only the queue routes wired to a temp queue
/// root — the config-less smoke path with one slot filled.
fn queue_app(dir: &std::path::Path) -> axum::Router {
    verglasd::admin::router(
        "test",
        verglasd::admin::Health::ready(),
        verglasd::admin::Slots {
            queues: Some(Arc::new(dir.to_path_buf())),
            ..Default::default()
        },
    )
}

/// Sends one request through the router and returns the status and parsed JSON
/// body.
async fn send(app: &axum::Router, req: Request<Body>) -> (StatusCode, serde_json::Value) {
    let response = app.clone().oneshot(req).await.expect("router response");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .expect("collect body");
    let json = if bytes.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&bytes).expect("json body")
    };
    (status, json)
}

/// A POST with a JSON body.
fn post_json(path: &str, body: serde_json::Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .expect("request")
}

/// enqueue → poll → ack round-trips: poll re-serves un-acked records, and a
/// second poll after ack returns nothing (the watermark advanced).
#[tokio::test]
async fn enqueue_poll_ack_round_trip() {
    let dir = tempfile::tempdir().expect("tempdir");
    let app = queue_app(dir.path());

    // Enqueue three rows.
    let (status, body) = send(
        &app,
        post_json(
            "/v1/queues/orders/enqueue",
            serde_json::json!({ "rows": [{ "id": 1 }, { "id": 2 }, { "id": 3 }] }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["enqueued"], 3);
    assert_eq!(body["endPosition"], 3);

    // Poll from a fresh group: all three, from position 0.
    let (status, body) = send(
        &app,
        Request::builder()
            .method("GET")
            .uri("/v1/queues/orders/poll?group=g1&max=10")
            .body(Body::empty())
            .expect("request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["watermark"], 0);
    let records = body["records"].as_array().expect("records array");
    assert_eq!(records.len(), 3);
    assert_eq!(records[0]["position"], 0);
    assert_eq!(records[0]["row"]["id"], 1);

    // A re-poll before ack re-serves the same records (at-least-once).
    let (_, body) = send(
        &app,
        Request::builder()
            .method("GET")
            .uri("/v1/queues/orders/poll?group=g1")
            .body(Body::empty())
            .expect("request"),
    )
    .await;
    assert_eq!(body["records"].as_array().expect("records").len(), 3);

    // Ack through position 3 (past the last record).
    let (status, body) = send(
        &app,
        post_json(
            "/v1/queues/orders/ack",
            serde_json::json!({ "group": "g1", "position": 3 }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["watermark"], 3);

    // Now the group is caught up: poll returns nothing.
    let (_, body) = send(
        &app,
        Request::builder()
            .method("GET")
            .uri("/v1/queues/orders/poll?group=g1")
            .body(Body::empty())
            .expect("request"),
    )
    .await;
    assert_eq!(body["records"].as_array().expect("records").len(), 0);
    assert_eq!(body["watermark"], 3);
}

/// Ack is monotone: a regressing position never rewinds a group's watermark.
#[tokio::test]
async fn ack_is_monotone() {
    let dir = tempfile::tempdir().expect("tempdir");
    let app = queue_app(dir.path());

    send(
        &app,
        post_json(
            "/v1/queues/q/enqueue",
            serde_json::json!({ "rows": [{ "n": 1 }, { "n": 2 }] }),
        ),
    )
    .await;
    send(
        &app,
        post_json(
            "/v1/queues/q/ack",
            serde_json::json!({ "group": "g", "position": 2 }),
        ),
    )
    .await;
    // A regressing ack is ignored.
    let (_, body) = send(
        &app,
        post_json(
            "/v1/queues/q/ack",
            serde_json::json!({ "group": "g", "position": 1 }),
        ),
    )
    .await;
    assert_eq!(body["watermark"], 2, "a regressing ack must not rewind");
}
