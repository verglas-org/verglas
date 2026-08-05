//! End-to-end contract tests for resumable catalog change following.

use futures::{SinkExt, StreamExt};
use serde_json::json;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{WebSocketStream, accept_async};
use verglas_sdk::{Client, ConnectOptions};

/// A dropped feed reconnects from its last delivered sequence without a
/// duplicate or gap.
#[tokio::test]
async fn follow_reconnects_and_resumes_exactly_once() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind feed");
    let endpoint = format!("http://{}", listener.local_addr().expect("feed address"));
    let server = tokio::spawn(async move {
        let (first, _) = listener.accept().await.expect("first socket");
        let mut first = accept_async(first).await.expect("first websocket");
        send_json(&mut first, json!({"type":"hello","cursor":5})).await;
        assert_subscribe(&mut first, Some(5)).await;
        send_json(
            &mut first,
            json!({
                "type":"change", "seq":6, "table":"sdk.events",
                "snapshot_id":"10", "committed_at":"2026-08-03T00:00:00Z"
            }),
        )
        .await;
        first.close(None).await.expect("drop first feed");
        drop(first);

        let (second, _) = listener.accept().await.expect("second socket");
        let mut second = accept_async(second).await.expect("second websocket");
        send_json(&mut second, json!({"type":"hello","cursor":6})).await;
        assert_subscribe(&mut second, Some(6)).await;
        send_json(
            &mut second,
            json!({
                "type":"change", "seq":6, "table":"sdk.events",
                "snapshot_id":"10", "committed_at":"2026-08-03T00:00:00Z"
            }),
        )
        .await;
        send_json(
            &mut second,
            json!({
                "type":"change", "seq":7, "table":"sdk.events",
                "snapshot_id":"11", "committed_at":"2026-08-03T00:01:00Z"
            }),
        )
        .await;
    });

    let client = Client::connect(
        ConnectOptions::new("http://127.0.0.1:1")
            .with_query_uri("http://127.0.0.1:1")
            .with_catalog_uri(&endpoint)
            .with_s3_endpoint("http://127.0.0.1:8333")
            .with_token("feed-token"),
    )
    .await
    .expect("client");
    let mut changes = client
        .follow(["sdk.events"], Some(5))
        .expect("follow stream");
    assert_eq!(next_change(&mut changes).await.seq, 6);
    assert_eq!(next_change(&mut changes).await.seq, 7);
    server.await.expect("feed server");
}

/// An aged-out replay cursor is a distinct error instead of a silent jump.
#[tokio::test]
async fn follow_reports_cursor_expiry_distinctly() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind feed");
    let endpoint = format!("http://{}", listener.local_addr().expect("feed address"));
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("feed socket");
        let mut socket = accept_async(stream).await.expect("websocket");
        send_json(&mut socket, json!({"type":"hello","cursor":50})).await;
        assert_subscribe(&mut socket, Some(1)).await;
        send_json(
            &mut socket,
            json!({"type":"resync","reason":"cursor-too-old"}),
        )
        .await;
    });
    let client = Client::connect(
        ConnectOptions::new("http://127.0.0.1:1")
            .with_query_uri("http://127.0.0.1:1")
            .with_catalog_uri(&endpoint)
            .with_s3_endpoint("http://127.0.0.1:8333"),
    )
    .await
    .expect("client");
    let mut changes = client.follow(["sdk.events"], Some(1)).expect("follow");
    let error = changes
        .next()
        .await
        .expect("cursor error")
        .expect_err("expired cursor must fail");
    assert!(matches!(
        error,
        verglas_sdk::ClientError::CursorExpired { reason } if reason == "cursor-too-old"
    ));
}

/// Waits a bounded time for the next successful change.
async fn next_change(stream: &mut verglas_sdk::FollowStream) -> verglas_sdk::worker::ChangeEvent {
    tokio::time::timeout(std::time::Duration::from_secs(3), stream.next())
        .await
        .expect("change deadline")
        .expect("change item")
        .expect("change")
}

/// Sends one JSON text frame.
async fn send_json(socket: &mut WebSocketStream<TcpStream>, value: serde_json::Value) {
    socket
        .send(Message::Text(value.to_string()))
        .await
        .expect("send feed frame");
}

/// Asserts the cursor carried by a client subscribe frame.
async fn assert_subscribe(socket: &mut WebSocketStream<TcpStream>, cursor: Option<i64>) {
    let frame = socket
        .next()
        .await
        .expect("subscribe frame")
        .expect("valid subscribe");
    let Message::Text(text) = frame else {
        panic!("subscribe was not text");
    };
    let value: serde_json::Value = serde_json::from_str(&text).expect("subscribe JSON");
    assert_eq!(value, json!({"type":"subscribe","cursor":cursor}));
}
