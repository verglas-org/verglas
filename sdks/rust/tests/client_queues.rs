//! Queue handle contract tests for the Rust SDK.

use axum::extract::Path;
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Json, Router};
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
        assert_eq!(body, json!({"messages":[{"id":1},{"id":2}]}));
        Json(json!({"positions": [40, 41]}))
    }
    async fn poll(
        Path(name): Path<String>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> impl IntoResponse {
        assert_eq!(headers["authorization"], "Bearer scoped");
        assert_eq!(name, "events.ingest");
        assert_eq!(
            body,
            json!({
                "group":"workers",
                "owner":"consumer-a",
                "max":10,
                "leaseSeconds":30
            })
        );
        Json(json!({
            "deliveries":[{
                "position":40,
                "payload":{"id":1},
                "receipt":{"position":40,"owner":"consumer-a","generation":1},
                "expiresAt":"2026-08-10T00:00:30Z"
            }]
        }))
    }
    async fn ack(
        Path(name): Path<String>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> impl IntoResponse {
        assert_eq!(headers["authorization"], "Bearer scoped");
        assert_eq!(name, "events.ingest");
        assert_eq!(
            body,
            json!({
                "group":"workers",
                "receipt":{"position":40,"owner":"consumer-a","generation":1}
            })
        );
        axum::http::StatusCode::NO_CONTENT
    }
    let app = Router::new()
        .route("/v1/queues/{name}/enqueue", post(enqueue))
        .route("/v1/queues/{name}/poll", post(poll))
        .route("/v1/queues/{name}/ack", post(ack));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let endpoint = format!("http://{}", listener.local_addr().expect("address"));
    tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });
    let client = Client::connect(
        ConnectOptions::new(&endpoint)
            .with_query_uri("http://127.0.0.1:9")
            .with_access_uri(&endpoint)
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
    assert_eq!(enqueued.positions, vec![40, 41]);

    let polled = queue
        .poll("workers", "consumer-a", Some(10), 30)
        .await
        .expect("poll");
    assert_eq!(polled.deliveries.len(), 1);
    assert_eq!(polled.deliveries[0].position, 40);
    assert_eq!(polled.deliveries[0].payload, json!({"id":1}));

    queue
        .ack("workers", &polled.deliveries[0].receipt)
        .await
        .expect("ack");
}

/// Rejects empty queue names before any HTTP call.
#[tokio::test]
async fn queue_rejects_empty_name() {
    let client = connect_stub().await;
    let error = client.queue("").expect_err("empty name");
    assert!(error.to_string().contains("queue"));
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
