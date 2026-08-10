//! Queue handle contract tests for the Rust SDK.

use axum::extract::{Path, Query};
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use verglas_sdk::{Client, ConnectOptions};

/// The Rust queue handle enqueues, polls, and acks with the same paths and bodies as TypeScript.
#[tokio::test]
async fn queue_handle_round_trips_enqueue_poll_and_ack() {
    async fn enqueue(
        Path(name): Path<String>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> impl IntoResponse {
        assert_eq!(headers["authorization"], "Bearer scoped");
        assert_eq!(name, "events.ingest");
        assert_eq!(body, json!({"rows":[{"id":1},{"id":2}]}));
        Json(json!({"enqueued": 2, "endPosition": 42}))
    }
    async fn poll(
        Path(name): Path<String>,
        Query(query): Query<PollQuery>,
        headers: HeaderMap,
    ) -> impl IntoResponse {
        assert_eq!(headers["authorization"], "Bearer scoped");
        assert_eq!(name, "events.ingest");
        assert_eq!(query.group, "workers");
        assert_eq!(query.max, Some(10));
        Json(json!({
            "records":[{"position":40,"row":{"id":1}},{"position":41,"row":{"id":2}}],
            "watermark":40
        }))
    }
    async fn ack(
        Path(name): Path<String>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> impl IntoResponse {
        assert_eq!(headers["authorization"], "Bearer scoped");
        assert_eq!(name, "events.ingest");
        assert_eq!(body, json!({"group":"workers","position":42}));
        Json(json!({"watermark": 42}))
    }
    let app = Router::new()
        .route("/v1/queues/{name}/enqueue", post(enqueue))
        .route("/v1/queues/{name}/poll", get(poll))
        .route("/v1/queues/{name}/ack", post(ack));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let endpoint = format!("http://{}", listener.local_addr().expect("address"));
    tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });
    let client = Client::connect(
        ConnectOptions::new(&endpoint)
            .with_query_uri(&endpoint)
            .with_s3_endpoint("http://127.0.0.1:8333")
            .with_token("scoped"),
    )
    .await
    .expect("connect");

    let queue = client.queue("events.ingest").expect("queue handle");
    let enqueued = queue
        .enqueue(vec![json!({"id":1}), json!({"id":2})])
        .await
        .expect("enqueue");
    assert_eq!(enqueued.enqueued, 2);
    assert_eq!(enqueued.end_position, 42);

    let polled = queue.poll("workers", Some(10)).await.expect("poll");
    assert_eq!(polled.watermark, 40);
    assert_eq!(polled.records.len(), 2);
    assert_eq!(polled.records[0].position, 40);
    assert_eq!(polled.records[0].row, json!({"id":1}));

    let acked = queue.ack("workers", 42).await.expect("ack");
    assert_eq!(acked.watermark, 42);
}

/// Rejects empty queue names before any HTTP call.
#[tokio::test]
async fn queue_rejects_empty_name() {
    let client = connect_stub().await;
    let error = client.queue("").expect_err("empty name");
    assert!(error.to_string().contains("queue"));
}

/// Query parameters accepted by the queue poll mock.
#[derive(Debug, Deserialize)]
struct PollQuery {
    group: String,
    max: Option<usize>,
}

/// Builds a client against a stub endpoint that never serves queue routes.
async fn connect_stub() -> Client {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let endpoint = format!("http://{}", listener.local_addr().expect("address"));
    tokio::spawn(async move { axum::serve(listener, Router::new()).await.expect("serve") });
    Client::connect(
        ConnectOptions::new(&endpoint)
            .with_query_uri(&endpoint)
            .with_s3_endpoint("http://127.0.0.1:8333"),
    )
    .await
    .expect("connect")
}
