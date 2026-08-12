//! Database table-subscription contract tests for the Rust SDK.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Json, Router};
use futures::StreamExt;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use verglas_sdk::{Client, ConnectOptions, TableSubscriptionEvent};

/// One database subscribe call receives a pushed commit and acknowledges its receipt.
#[tokio::test]
async fn database_subscribe_pushes_commit_without_polling() {
    async fn subscribe(
        Path(database): Path<String>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> impl IntoResponse {
        assert_eq!(database, "trading");
        assert_eq!(headers["authorization"], "Bearer scoped");
        assert_eq!(
            body,
            json!({
                "group":"live-deploy-1",
                "owner":"rlean-1",
                "tables":["rlean.custom_points"],
                "max":1,
                "leaseSeconds":60
            })
        );
        (
            [("content-type", "application/x-ndjson")],
            concat!(
                "{\"position\":9,\"topic\":\"database/db-1/table/rlean.custom_points\",",
                "\"payload\":{\"database\":\"db-1\",\"table\":\"rlean.custom_points\",",
                "\"snapshotId\":\"42\",\"committedAt\":\"2026-08-11T18:00:00Z\"},",
                "\"receipt\":{\"position\":9,\"owner\":\"rlean-1\",\"generation\":1},",
                "\"expiresAt\":\"2026-08-11T18:01:00Z\"}\n"
            ),
        )
    }
    async fn ack(
        Path(database): Path<String>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> impl IntoResponse {
        assert_eq!(database, "trading");
        assert_eq!(headers["authorization"], "Bearer scoped");
        assert_eq!(
            body,
            json!({
                "group":"live-deploy-1",
                "receipt":{"position":9,"owner":"rlean-1","generation":1}
            })
        );
        axum::http::StatusCode::NO_CONTENT
    }
    let app = Router::new()
        .route("/v1/databases/{database}/subscribe", post(subscribe))
        .route("/v1/databases/{database}/subscriptions/ack", post(ack));
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
    let database = client.database("trading").expect("database");
    let mut events = database
        .subscribe(
            "live-deploy-1",
            "rlean-1",
            ["rlean.custom_points"],
            Some(1),
            60,
        )
        .expect("subscribe");

    assert!(matches!(
        events
            .next()
            .await
            .expect("connected")
            .expect("valid event"),
        TableSubscriptionEvent::Connected
    ));
    let delivery = match events.next().await.expect("delivery").expect("valid event") {
        TableSubscriptionEvent::Delivery(delivery) => delivery,
        other => panic!("expected delivery, got {other:?}"),
    };
    assert_eq!(delivery.change.table, "rlean.custom_points");
    assert_eq!(delivery.change.snapshot_id, "42");
    database
        .ack("live-deploy-1", &delivery.receipt)
        .await
        .expect("ack");
}

/// A temporary access-node failure does not terminate the durable subscription.
#[tokio::test]
async fn database_subscribe_reconnects_after_transient_http_failure() {
    async fn subscribe(State(calls): State<Arc<AtomicUsize>>) -> impl IntoResponse {
        if calls.fetch_add(1, Ordering::SeqCst) == 0 {
            return (axum::http::StatusCode::SERVICE_UNAVAILABLE, "restarting").into_response();
        }
        (
            [("content-type", "application/x-ndjson")],
            concat!(
                "{\"position\":9,\"topic\":\"database/db-1/table/rlean.custom_points\",",
                "\"payload\":{\"database\":\"db-1\",\"table\":\"rlean.custom_points\",",
                "\"snapshotId\":\"42\",\"committedAt\":\"\"},",
                "\"receipt\":{\"position\":9,\"owner\":\"rlean-1\",\"generation\":1},",
                "\"expiresAt\":\"2026-08-11T18:01:00Z\"}\n"
            ),
        )
            .into_response()
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let app = Router::new()
        .route("/v1/databases/{database}/subscribe", post(subscribe))
        .with_state(calls.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let endpoint = format!("http://{}", listener.local_addr().expect("address"));
    tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });
    let client = Client::connect(
        ConnectOptions::new(&endpoint)
            .with_query_uri("http://127.0.0.1:9")
            .with_access_uri(&endpoint)
            .with_s3_endpoint("http://127.0.0.1:8333"),
    )
    .await
    .expect("connect");
    let database = client.database("trading").expect("database");
    let mut events = database
        .subscribe(
            "live-deploy-1",
            "rlean-1",
            ["rlean.custom_points"],
            Some(1),
            60,
        )
        .expect("subscribe");

    let event = tokio::time::timeout(Duration::from_secs(2), events.next())
        .await
        .expect("reconnect deadline")
        .expect("event")
        .expect("valid event");
    assert!(matches!(event, TableSubscriptionEvent::Connected));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}
