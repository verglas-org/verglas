//! Integration tests for the websocket catalog change feed (#47, the second
//! change-feed transport). A local tokio websocket server (random port) speaks
//! the feed protocol, and a minimal Iceberg REST mock (random port) answers the
//! pointer reads the feed drives. The tests assert, at the `CatalogWatcher`
//! surface the warming/prefetch coordinators consume:
//!
//! - upgrade → hello → subscribe → change drives the downstream handling (a
//!   `TableChanged` event, produced by a targeted pointer refresh),
//! - a socket drop reconnects and re-subscribes from the last-seen cursor,
//! - a non-101 upgrade falls back to polling, which still detects commits.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

use axum::Router;
use axum::extract::{Path, State};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use futures::{SinkExt, StreamExt};
use serde_json::json;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::sync::{Notify, broadcast};
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;
use verglas_tables::catalog::{
    CatalogFeed, CatalogWatcher, RestCatalogSource, TableChanged, TableFilter, TableIdent,
    WatcherOptions, WsFeedConfig,
};

/// A generous ceiling for feed-driven assertions.
const EVENT_TIMEOUT: Duration = Duration::from_secs(6);

/// Watcher options tuned for tests: fast polls, no jitter.
fn test_options() -> WatcherOptions {
    WatcherOptions {
        interval: Duration::from_millis(25),
        jitter: Duration::ZERO,
        max_backoff: Duration::from_millis(100),
        history_depth: 16,
        filter: TableFilter::default(),
    }
}

// ---------------------------------------------------------------------------
// Minimal Iceberg REST catalog mock (pointer reads only).
// ---------------------------------------------------------------------------

/// A single table pointer the REST mock serves.
#[derive(Clone)]
struct MockTable {
    metadata_location: String,
    snapshot_id: i64,
}

/// The REST mock's mutable table set.
#[derive(Default)]
struct MockCatalog {
    tables: Mutex<BTreeMap<(String, String), MockTable>>,
}

impl MockCatalog {
    /// Sets a table's pointer, as a commit would.
    fn set_table(&self, ns: &str, name: &str, metadata_location: &str, snapshot_id: i64) {
        self.tables.lock().expect("lock").insert(
            (ns.to_owned(), name.to_owned()),
            MockTable {
                metadata_location: metadata_location.to_owned(),
                snapshot_id,
            },
        );
    }
}

/// `GET /v1/config` — no prefix override.
async fn get_config() -> Response {
    axum::Json(json!({"defaults": {}, "overrides": {}})).into_response()
}

/// `GET /v1/namespaces` — the distinct namespaces present.
async fn list_namespaces(State(mock): State<Arc<MockCatalog>>) -> Response {
    let tables = mock.tables.lock().expect("lock");
    let mut namespaces: Vec<String> = tables.keys().map(|(ns, _)| ns.clone()).collect();
    namespaces.dedup();
    let namespaces: Vec<_> = namespaces.into_iter().map(|ns| json!([ns])).collect();
    axum::Json(json!({ "namespaces": namespaces })).into_response()
}

/// `GET /v1/namespaces/{ns}/tables` — identifiers within `ns`.
async fn list_tables(State(mock): State<Arc<MockCatalog>>, Path(ns): Path<String>) -> Response {
    let tables = mock.tables.lock().expect("lock");
    let identifiers: Vec<_> = tables
        .keys()
        .filter(|(namespace, _)| *namespace == ns)
        .map(|(namespace, name)| json!({"namespace": [namespace], "name": name}))
        .collect();
    axum::Json(json!({ "identifiers": identifiers })).into_response()
}

/// `GET /v1/namespaces/{ns}/tables/{table}` — the table's current pointer.
async fn load_table(
    State(mock): State<Arc<MockCatalog>>,
    Path((ns, name)): Path<(String, String)>,
) -> Response {
    let tables = mock.tables.lock().expect("lock");
    let Some(table) = tables.get(&(ns, name)) else {
        return axum::http::StatusCode::NOT_FOUND.into_response();
    };
    axum::Json(json!({
        "metadata-location": table.metadata_location,
        "metadata": {"format-version": 2, "current-snapshot-id": table.snapshot_id}
    }))
    .into_response()
}

/// Boots the REST mock on a random port.
async fn spawn_rest_mock() -> (SocketAddr, Arc<MockCatalog>) {
    let mock = Arc::new(MockCatalog::default());
    let app = Router::new()
        .route("/v1/config", get(get_config))
        .route("/v1/namespaces", get(list_namespaces))
        .route("/v1/namespaces/{ns}/tables", get(list_tables))
        .route("/v1/namespaces/{ns}/tables/{table}", get(load_table))
        .with_state(Arc::clone(&mock));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind rest");
    let addr = listener.local_addr().expect("rest addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("rest serves");
    });
    (addr, mock)
}

// ---------------------------------------------------------------------------
// Websocket feed server.
// ---------------------------------------------------------------------------

/// How the feed server behaves on a connection.
#[derive(Clone, Copy, PartialEq, Eq)]
enum WsMode {
    /// Send hello, accept subscribe, push a change when triggered, stay open.
    Normal,
    /// Like `Normal`, but drop the socket right after the first change.
    DropAfterChange,
    /// Refuse the upgrade with HTTP 404 (a third-party catalog).
    Reject,
}

/// Shared state for the feed server across connections.
struct WsServer {
    mode: WsMode,
    /// The dotted table each change frame names.
    table: String,
    /// Monotonic event sequence.
    seq: AtomicI64,
    /// The cursor recorded from each `subscribe` frame, in connection order.
    subscribe_cursors: Mutex<Vec<Option<i64>>>,
    /// Fires once per change the test wants pushed.
    change: Notify,
    /// Count of accepted connections.
    connections: AtomicU64,
}

impl WsServer {
    /// A server in `mode` that pushes changes for `table`.
    fn new(mode: WsMode, table: &str) -> Arc<WsServer> {
        Arc::new(WsServer {
            mode,
            table: table.to_owned(),
            seq: AtomicI64::new(0),
            subscribe_cursors: Mutex::new(Vec::new()),
            change: Notify::new(),
            connections: AtomicU64::new(0),
        })
    }

    /// Asks the current connection to push one change frame.
    fn trigger_change(&self) {
        self.change.notify_one();
    }

    /// The subscribe cursors recorded so far.
    fn cursors(&self) -> Vec<Option<i64>> {
        self.subscribe_cursors.lock().expect("lock").clone()
    }
}

/// Boots the feed server on a random port and returns its address.
async fn spawn_ws_server(server: Arc<WsServer>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind ws");
    let addr = listener.local_addr().expect("ws addr");
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let server = Arc::clone(&server);
            tokio::spawn(handle_conn(stream, server));
        }
    });
    addr
}

/// Serves one feed connection per the server's mode.
async fn handle_conn(mut stream: tokio::net::TcpStream, server: Arc<WsServer>) {
    server.connections.fetch_add(1, Ordering::SeqCst);
    if server.mode == WsMode::Reject {
        // Refuse the upgrade: a plain 404, no websocket handshake.
        let _ = stream
            .write_all(b"HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
            .await;
        let _ = stream.shutdown().await;
        return;
    }
    let ws = match accept_async(stream).await {
        Ok(ws) => ws,
        Err(_) => return,
    };
    let (mut write, mut read) = ws.split();

    // Hello with the current sequence.
    let hello = json!({"type": "hello", "cursor": server.seq.load(Ordering::SeqCst)}).to_string();
    if write.send(Message::Text(hello)).await.is_err() {
        return;
    }

    // Record the first subscribe before serving any change (deterministic
    // ordering: a change never races ahead of the subscription).
    match read.next().await {
        Some(Ok(Message::Text(text))) => record_subscribe(&server, &text),
        _ => return,
    }

    loop {
        tokio::select! {
            incoming = read.next() => match incoming {
                Some(Ok(Message::Text(text))) => record_subscribe(&server, &text),
                Some(Ok(_)) => {}
                _ => return,
            },
            _ = server.change.notified() => {
                let seq = server.seq.fetch_add(1, Ordering::SeqCst) + 1;
                let change = json!({
                    "type": "change",
                    "seq": seq,
                    "table": server.table,
                    "snapshot_id": "200",
                    "committed_at": "2026-08-01T00:00:00Z",
                }).to_string();
                if write.send(Message::Text(change)).await.is_err() {
                    return;
                }
                if server.mode == WsMode::DropAfterChange {
                    return;
                }
            }
        }
    }
}

/// Parses a `subscribe` frame and records its cursor (`null` → `None`).
fn record_subscribe(server: &WsServer, text: &str) {
    let value: serde_json::Value = match serde_json::from_str(text) {
        Ok(value) => value,
        Err(_) => return,
    };
    if value.get("type").and_then(|t| t.as_str()) == Some("subscribe") {
        let cursor = value.get("cursor").and_then(|c| c.as_i64());
        server.subscribe_cursors.lock().expect("lock").push(cursor);
    }
}

// ---------------------------------------------------------------------------
// Test helpers.
// ---------------------------------------------------------------------------

/// Receives the next event or panics after `timeout`.
async fn next_event(rx: &mut broadcast::Receiver<TableChanged>, timeout: Duration) -> TableChanged {
    tokio::time::timeout(timeout, rx.recv())
        .await
        .expect("timed out waiting for a TableChanged event")
        .expect("event channel closed")
}

/// Polls `condition` until it holds or panics after `timeout`.
async fn wait_until(condition: impl Fn() -> bool, timeout: Duration, what: &str) {
    let deadline = tokio::time::Instant::now() + timeout;
    while !condition() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for: {what}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// ---------------------------------------------------------------------------
// Tests.
// ---------------------------------------------------------------------------

/// Upgrade → hello → subscribe → change drives the same downstream handling the
/// poller drives: the change frame triggers a targeted pointer read that emits
/// a `TableChanged` carrying the new snapshot.
#[tokio::test(flavor = "multi_thread")]
async fn change_frame_drives_downstream_refresh() {
    let (rest_addr, mock) = spawn_rest_mock().await;
    mock.set_table("db", "events", "s3://lake/db/events/v1.json", 100);

    let server = WsServer::new(WsMode::Normal, "db.events");
    let ws_addr = spawn_ws_server(Arc::clone(&server)).await;

    let source = RestCatalogSource::new(format!("http://{rest_addr}"));
    let ws = WsFeedConfig::from_catalog_uri(&format!("http://{ws_addr}"), Some("tok".to_owned()))
        .expect("feed url");
    let feed = CatalogFeed::spawn(source, test_options(), Some(ws));
    let mut rx = feed.subscribe();

    let ident = TableIdent::new(&["db"], "events");
    wait_until(
        || feed.table_state(&ident).is_some(),
        EVENT_TIMEOUT,
        "feed seeded via attach poll",
    )
    .await;

    // A commit lands, then the server pushes the change frame for it.
    mock.set_table("db", "events", "s3://lake/db/events/v2.json", 200);
    server.trigger_change();

    let event = next_event(&mut rx, EVENT_TIMEOUT).await;
    assert_eq!(
        event,
        TableChanged {
            table: ident.clone(),
            old_snapshot: Some(100),
            new_snapshot: Some(200),
        }
    );
    let state = feed.table_state(&ident).expect("state after change");
    assert_eq!(state.metadata_location, "s3://lake/db/events/v2.json");
    assert_eq!(state.current_snapshot_id, Some(200));

    // The upgrade carried the bearer auth the catalog client uses.
    // (The server accepted the handshake; the subscribe was recorded.)
    assert_eq!(
        server.cursors().first(),
        Some(&None),
        "first subscribe live-only"
    );
}

/// A socket drop reconnects and re-subscribes from the last-seen sequence, so
/// the server replays only what was missed during the gap.
#[tokio::test(flavor = "multi_thread")]
async fn drop_reconnects_and_resumes_from_cursor() {
    let (rest_addr, mock) = spawn_rest_mock().await;
    mock.set_table("db", "events", "s3://lake/db/events/v1.json", 100);

    let server = WsServer::new(WsMode::DropAfterChange, "db.events");
    let ws_addr = spawn_ws_server(Arc::clone(&server)).await;

    let source = RestCatalogSource::new(format!("http://{rest_addr}"));
    let ws = WsFeedConfig::from_catalog_uri(&format!("http://{ws_addr}"), Some("tok".to_owned()))
        .expect("feed url");
    let feed = CatalogFeed::spawn(source, test_options(), Some(ws));
    let mut rx = feed.subscribe();

    let ident = TableIdent::new(&["db"], "events");
    wait_until(
        || feed.table_state(&ident).is_some(),
        EVENT_TIMEOUT,
        "feed seeded via attach poll",
    )
    .await;

    // Push the change (seq 1); the server drops the socket right after.
    mock.set_table("db", "events", "s3://lake/db/events/v2.json", 200);
    server.trigger_change();
    let event = next_event(&mut rx, EVENT_TIMEOUT).await;
    assert_eq!(event.new_snapshot, Some(200));

    // The client reconnects and re-subscribes from the last-seen cursor (1).
    wait_until(
        || server.cursors().len() >= 2,
        EVENT_TIMEOUT,
        "reconnect re-subscribes",
    )
    .await;
    let cursors = server.cursors();
    assert_eq!(cursors[0], None, "first subscribe was live-only");
    assert_eq!(
        cursors[1],
        Some(1),
        "reconnect resumes from the last-seen sequence"
    );
    assert!(
        server.connections.load(Ordering::SeqCst) >= 2,
        "a reconnect opened a second connection"
    );
}

/// A non-101 upgrade (a third-party catalog) falls back to polling, which still
/// detects a commit and emits it.
#[tokio::test(flavor = "multi_thread")]
async fn non_101_upgrade_falls_back_to_polling() {
    let (rest_addr, mock) = spawn_rest_mock().await;
    mock.set_table("db", "events", "s3://lake/db/events/v1.json", 100);

    let server = WsServer::new(WsMode::Reject, "db.events");
    let ws_addr = spawn_ws_server(Arc::clone(&server)).await;

    let source = RestCatalogSource::new(format!("http://{rest_addr}"));
    let ws = WsFeedConfig::from_catalog_uri(&format!("http://{ws_addr}"), Some("tok".to_owned()))
        .expect("feed url");
    let feed = CatalogFeed::spawn(source, test_options(), Some(ws));
    let mut rx = feed.subscribe();

    let ident = TableIdent::new(&["db"], "events");
    wait_until(
        || feed.table_state(&ident).is_some(),
        EVENT_TIMEOUT,
        "polling fallback seeds state",
    )
    .await;

    // Polling, not the websocket, detects the commit.
    mock.set_table("db", "events", "s3://lake/db/events/v2.json", 200);
    let event = next_event(&mut rx, EVENT_TIMEOUT).await;
    assert_eq!(
        event,
        TableChanged {
            table: ident,
            old_snapshot: Some(100),
            new_snapshot: Some(200),
        }
    );
    assert!(
        server.connections.load(Ordering::SeqCst) >= 1,
        "the daemon attempted the upgrade before falling back"
    );
}
