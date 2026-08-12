//! Queue handle contract tests for the Rust SDK.

use axum::extract::Path;
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Json, Router};
use futures::StreamExt;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use verglas_sdk::{Client, ConnectOptions, QueueMessage};

/// The Rust queue handle publishes, subscribes, and acks through one pushed stream.
#[tokio::test]
async fn queue_handle_round_trips_enqueue_subscribe_and_ack() {
    async fn enqueue(
        Path(name): Path<String>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> impl IntoResponse {
        assert_eq!(headers["authorization"], "Bearer scoped");
        assert_eq!(name, "events.ingest");
        assert_eq!(
            body,
            json!({"messages":[{
                "id":"commit-40",
                "topic":"database/trading/table/rlean.custom_points",
                "payload":{"id":1}
            }]})
        );
        Json(json!({"positions": [40, 41]}))
    }
    async fn subscribe(
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
                "topics":["database/trading/table/rlean.custom_points"],
                "max":10,
                "leaseSeconds":30
            })
        );
        (
            [("content-type", "application/x-ndjson")],
            concat!(
                "{\"position\":40,\"topic\":\"database/trading/table/rlean.custom_points\",",
                "\"payload\":{\"id\":1},\"receipt\":{\"position\":40,",
                "\"owner\":\"consumer-a\",\"generation\":1},",
                "\"expiresAt\":\"2026-08-10T00:00:30Z\"}\n"
            ),
        )
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
        .route("/v1/queues/{name}/subscribe", post(subscribe))
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
        .enqueue(vec![QueueMessage {
            id: "commit-40".to_owned(),
            topic: "database/trading/table/rlean.custom_points".to_owned(),
            payload: json!({"id":1}),
        }])
        .await
        .expect("enqueue");
    assert_eq!(enqueued.positions, vec![40, 41]);

    let mut subscription = queue
        .subscribe(
            "workers",
            "consumer-a",
            ["database/trading/table/rlean.custom_points"],
            Some(10),
            30,
        )
        .expect("subscribe");
    let delivery = subscription
        .next()
        .await
        .expect("delivery")
        .expect("valid delivery");
    assert_eq!(delivery.position, 40);
    assert_eq!(delivery.payload, json!({"id":1}));

    queue.ack("workers", &delivery.receipt).await.expect("ack");
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
