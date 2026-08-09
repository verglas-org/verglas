//! Integration tests for the catalog watcher (#47), against a mock Iceberg
//! REST catalog: an axum server speaking the minimal REST-catalog subset the
//! watcher uses (config, namespaces, tables, load-table). Each test maps to an
//! acceptance criterion: commit detection within one poll interval,
//! include/exclude filters, outage resilience, and snapshot lineage.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use serde_json::json;
use tokio::sync::broadcast;
use verglas_catalog::{CatalogMutation, TableState};
use verglas_tables::catalog::{
    CatalogWatcher, PollingWatcher, RestCatalogSource, StrongWatcher, TableChanged, TableFilter,
    TableIdent, WatcherOptions,
};

/// Mutable state behind the mock REST catalog: the tables it serves, an
/// outage switch, a required bearer token, and a log of load-table calls.
#[derive(Default)]
struct MockCatalog {
    /// `(namespace, name)` → current pointer. Single-level namespaces only.
    tables: Mutex<BTreeMap<(String, String), MockTable>>,
    /// When set, every endpoint answers 503 — the catalog is "down".
    outage: AtomicBool,
    /// When set, requests without `Authorization: Bearer <this>` get 401.
    required_token: Mutex<Option<String>>,
    /// When set, requests must carry a SigV4 `Authorization: AWS4-HMAC-SHA256`
    /// header whose credential scope names this `region/service`; else 401.
    /// This proves the watcher signs — the mock does not re-derive the signature.
    required_sigv4: Mutex<Option<(String, String)>>,
    /// Whether child namespace enumeration returns AWS S3 Tables' HTTP 400.
    reject_parent_namespaces: AtomicBool,
    /// Number of child namespace requests received.
    parent_namespace_requests: AtomicU64,
    /// Dotted names of tables that have been load-table'd, in order.
    loads: Mutex<Vec<String>>,
}

/// One table's current catalog pointer in the mock.
#[derive(Clone)]
struct MockTable {
    metadata_location: String,
    snapshot_id: i64,
}

impl MockCatalog {
    /// Creates or updates a table's pointer, as an Iceberg commit would.
    fn set_table(&self, namespace: &str, name: &str, metadata_location: &str, snapshot_id: i64) {
        self.tables.lock().expect("mock lock").insert(
            (namespace.to_owned(), name.to_owned()),
            MockTable {
                metadata_location: metadata_location.to_owned(),
                snapshot_id,
            },
        );
    }

    /// Drops a table from the catalog.
    fn remove_table(&self, namespace: &str, name: &str) {
        self.tables
            .lock()
            .expect("mock lock")
            .remove(&(namespace.to_owned(), name.to_owned()));
    }

    /// Flips the outage switch: `true` makes every endpoint answer 503.
    fn set_outage(&self, down: bool) {
        self.outage.store(down, Ordering::SeqCst);
    }

    /// Rejects the request if the catalog is down or auth fails; `None` means
    /// the request may proceed.
    fn gate(&self, headers: &HeaderMap) -> Option<Response> {
        if self.outage.load(Ordering::SeqCst) {
            return Some(StatusCode::SERVICE_UNAVAILABLE.into_response());
        }
        if let Some(token) = self.required_token.lock().expect("mock lock").as_deref() {
            let expected = format!("Bearer {token}");
            let got = headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default();
            if got != expected {
                return Some(StatusCode::UNAUTHORIZED.into_response());
            }
        }
        if let Some((region, service)) = self
            .required_sigv4
            .lock()
            .expect("mock lock")
            .as_ref()
            .cloned()
        {
            let got = headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default();
            // A SigV4 header names the algorithm and a credential scope
            // `.../<date>/<region>/<service>/aws4_request`. Requiring the scope
            // proves the watcher signed with the configured region+service.
            let scope = format!("/{region}/{service}/aws4_request");
            if !got.starts_with("AWS4-HMAC-SHA256 ") || !got.contains(&scope) {
                return Some(StatusCode::UNAUTHORIZED.into_response());
            }
        }
        None
    }
}

/// `GET /v1/config` — advertises the `demo` prefix so every test exercises
/// prefix handling in the client's URL construction.
async fn get_config(State(mock): State<Arc<MockCatalog>>, headers: HeaderMap) -> Response {
    if let Some(rejection) = mock.gate(&headers) {
        return rejection;
    }
    axum::Json(json!({"defaults": {}, "overrides": {"prefix": "demo"}})).into_response()
}

/// `GET /v1/demo/namespaces` — top-level namespaces; children of any parent
/// are empty (the mock uses single-level namespaces only).
async fn list_namespaces(
    State(mock): State<Arc<MockCatalog>>,
    headers: HeaderMap,
    Query(params): Query<BTreeMap<String, String>>,
) -> Response {
    if let Some(rejection) = mock.gate(&headers) {
        return rejection;
    }
    if params.contains_key("parent") {
        mock.parent_namespace_requests
            .fetch_add(1, Ordering::SeqCst);
        if mock.reject_parent_namespaces.load(Ordering::SeqCst) {
            return StatusCode::BAD_REQUEST.into_response();
        }
        return axum::Json(json!({"namespaces": []})).into_response();
    }
    let tables = mock.tables.lock().expect("mock lock");
    let mut namespaces: Vec<&String> = tables.keys().map(|(ns, _)| ns).collect();
    namespaces.dedup();
    let namespaces: Vec<_> = namespaces.into_iter().map(|ns| json!([ns])).collect();
    axum::Json(json!({"namespaces": namespaces})).into_response()
}

/// `GET /v1/demo/namespaces/{ns}/tables` — identifiers of tables in `ns`.
async fn list_tables(
    State(mock): State<Arc<MockCatalog>>,
    headers: HeaderMap,
    Path(ns): Path<String>,
) -> Response {
    if let Some(rejection) = mock.gate(&headers) {
        return rejection;
    }
    let tables = mock.tables.lock().expect("mock lock");
    let identifiers: Vec<_> = tables
        .keys()
        .filter(|(namespace, _)| *namespace == ns)
        .map(|(namespace, name)| json!({"namespace": [namespace], "name": name}))
        .collect();
    axum::Json(json!({"identifiers": identifiers})).into_response()
}

/// `GET /v1/demo/namespaces/{ns}/tables/{table}` — a minimal load-table
/// result: the metadata location plus just enough `metadata` to be shaped
/// like a real response.
async fn load_table(
    State(mock): State<Arc<MockCatalog>>,
    headers: HeaderMap,
    Path((ns, name)): Path<(String, String)>,
) -> Response {
    if let Some(rejection) = mock.gate(&headers) {
        return rejection;
    }
    mock.loads
        .lock()
        .expect("mock lock")
        .push(format!("{ns}.{name}"));
    let tables = mock.tables.lock().expect("mock lock");
    let Some(table) = tables.get(&(ns.clone(), name.clone())) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    axum::Json(json!({
        "metadata-location": table.metadata_location,
        "metadata": {
            "format-version": 2,
            "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
            "location": format!("s3://lake/{ns}/{name}"),
            "current-snapshot-id": table.snapshot_id,
            "snapshots": [
                {"snapshot-id": table.snapshot_id, "timestamp-ms": 1_700_000_000_000_i64,
                 "manifest-list": format!("s3://lake/{ns}/{name}/metadata/snap-{}.avro", table.snapshot_id)}
            ]
        }
    }))
    .into_response()
}

/// `POST .../tables/{table}` simulates an Iceberg commit by advancing the
/// table pointer. The gateway must forward the write and invalidate the cached
/// load-table response before the next read.
async fn commit_table(
    State(mock): State<Arc<MockCatalog>>,
    headers: HeaderMap,
    Path((ns, name)): Path<(String, String)>,
) -> Response {
    if let Some(rejection) = mock.gate(&headers) {
        return rejection;
    }
    let mut tables = mock.tables.lock().expect("mock lock");
    let Some(table) = tables.get_mut(&(ns, name)) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    table.snapshot_id += 1;
    table.metadata_location = format!("s3://lake/metadata/v{}.json", table.snapshot_id);
    StatusCode::NO_CONTENT.into_response()
}

/// Boots the mock catalog on an ephemeral port; returns its address and the
/// handle tests mutate to simulate commits and outages.
async fn spawn_mock() -> (SocketAddr, Arc<MockCatalog>) {
    let mock = Arc::new(MockCatalog::default());
    let app = Router::new()
        .route("/v1/config", get(get_config))
        .route("/v1/demo/namespaces", get(list_namespaces))
        .route("/v1/demo/namespaces/{ns}/tables", get(list_tables))
        .route(
            "/v1/demo/namespaces/{ns}/tables/{table}",
            get(load_table).post(commit_table),
        )
        .with_state(Arc::clone(&mock));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock catalog");
    let addr = listener.local_addr().expect("mock addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("mock serves");
    });
    (addr, mock)
}

/// Watcher options tuned for tests: fast polls, no jitter, quick backoff cap
/// so outage recovery happens within a test-sized timeout.
fn test_options() -> WatcherOptions {
    WatcherOptions {
        interval: Duration::from_millis(25),
        jitter: Duration::ZERO,
        max_backoff: Duration::from_millis(100),
        history_depth: 16,
        filter: TableFilter::default(),
    }
}

/// Polls `condition` until it holds or `timeout` elapses (then panics).
async fn wait_until(condition: impl Fn() -> bool, timeout: Duration, what: &str) {
    let deadline = tokio::time::Instant::now() + timeout;
    while !condition() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for: {what}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Receives the next event or panics after `timeout`.
async fn next_event(rx: &mut broadcast::Receiver<TableChanged>, timeout: Duration) -> TableChanged {
    tokio::time::timeout(timeout, rx.recv())
        .await
        .expect("timed out waiting for a TableChanged event")
        .expect("event channel closed")
}

/// A generous ceiling for "within one poll interval" at a 25ms interval.
const EVENT_TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::test(flavor = "multi_thread")]
async fn commit_detected_within_one_poll_with_correct_snapshot_ids() {
    let (addr, mock) = spawn_mock().await;
    mock.set_table("db", "events", "s3://lake/db/events/metadata/v1.json", 100);

    let source = RestCatalogSource::new(format!("http://{addr}"));
    let watcher = PollingWatcher::spawn(source, test_options());
    let mut rx = watcher.subscribe();

    let ident = TableIdent::new(&["db"], "events");
    wait_until(
        || watcher.table_state(&ident).is_some(),
        EVENT_TIMEOUT,
        "initial state seeded",
    )
    .await;
    let seeded = watcher.table_state(&ident).expect("seeded state");
    assert_eq!(
        seeded.metadata_location,
        "s3://lake/db/events/metadata/v1.json"
    );
    assert_eq!(seeded.current_snapshot_id, Some(100));

    // The commit: new metadata.json, new snapshot id.
    mock.set_table("db", "events", "s3://lake/db/events/metadata/v2.json", 200);

    let event = next_event(&mut rx, EVENT_TIMEOUT).await;
    assert_eq!(
        event,
        TableChanged {
            table: ident.clone(),
            old_snapshot: Some(100),
            new_snapshot: Some(200),
        }
    );
    let state = watcher.table_state(&ident).expect("state after commit");
    assert_eq!(
        state.metadata_location,
        "s3://lake/db/events/metadata/v2.json"
    );
    assert_eq!(state.current_snapshot_id, Some(200));

    // The watcher is usable through the trait object the mapper (#49) holds.
    let as_trait: &dyn CatalogWatcher = &watcher;
    assert_eq!(as_trait.watched_tables(), vec![ident]);
}

/// Eventual mode remains a polling watcher, but a hosted catalog notification
/// can wake the next reconciliation without creating a third consistency mode.
#[tokio::test(flavor = "multi_thread")]
async fn eventual_notification_accelerates_the_next_poll() {
    let (addr, mock) = spawn_mock().await;
    mock.set_table("db", "events", "s3://lake/db/events/metadata/v1.json", 100);
    let mut options = test_options();
    options.interval = Duration::from_secs(60);
    let watcher = PollingWatcher::spawn(RestCatalogSource::new(format!("http://{addr}")), options);
    let ident = TableIdent::new(&["db"], "events");
    wait_until(
        || watcher.table_state(&ident).is_some(),
        EVENT_TIMEOUT,
        "eventual watcher seeded",
    )
    .await;
    let mut events = watcher.subscribe();
    mock.set_table("db", "events", "s3://lake/db/events/metadata/v2.json", 200);

    watcher.request_refresh();
    assert_eq!(
        next_event(&mut events, EVENT_TIMEOUT).await,
        TableChanged {
            table: ident,
            old_snapshot: Some(100),
            new_snapshot: Some(200),
        }
    );
}

/// Strong mode applies the exact quorum-committed pointer and advances its
/// query fence only after the new state is visible.
#[tokio::test(flavor = "multi_thread")]
async fn strong_watcher_applies_ordered_pointer_without_polling() {
    let (addr, mock) = spawn_mock().await;
    mock.set_table("db", "events", "s3://lake/db/events/metadata/v1.json", 100);
    let watcher = StrongWatcher::seed(
        RestCatalogSource::new(format!("http://{addr}")),
        test_options(),
    )
    .await
    .expect("strict initial seed");
    let ident = TableIdent::new(&["db"], "events");
    let mut events = watcher.subscribe();

    let mutation = CatalogMutation {
        sequence: 41,
        event_id: "event-41".into(),
        warehouse_id: "warehouse-1".into(),
        table_id: "table-1".into(),
        namespace: vec!["db".into()],
        table: "events".into(),
        previous_namespace: None,
        previous_table: None,
        operation: "updateTable".into(),
        metadata_location: Some("s3://lake/db/events/metadata/v2.json".into()),
        snapshot_id: Some(200),
    };
    assert!(
        watcher
            .apply(
                &mutation,
                Some(TableState {
                    metadata_location: mutation.metadata_location.clone().expect("pointer"),
                    current_snapshot_id: mutation.snapshot_id,
                }),
            )
            .expect("ordered apply")
    );
    assert_eq!(watcher.applied_sequence(), 41);
    assert_eq!(
        watcher.table_state(&ident).expect("updated state"),
        TableState {
            metadata_location: "s3://lake/db/events/metadata/v2.json".into(),
            current_snapshot_id: Some(200),
        }
    );
    assert_eq!(
        next_event(&mut events, EVENT_TIMEOUT).await,
        TableChanged {
            table: ident,
            old_snapshot: Some(100),
            new_snapshot: Some(200),
        }
    );
    assert!(
        !watcher
            .apply(
                &mutation,
                Some(TableState {
                    metadata_location: "ignored duplicate".into(),
                    current_snapshot_id: Some(999),
                }),
            )
            .expect("duplicate sequence")
    );
}

/// A rename removes the previous identifier in the same state publication.
#[tokio::test(flavor = "multi_thread")]
async fn strong_watcher_renames_without_leaving_stale_identity() {
    let (addr, mock) = spawn_mock().await;
    mock.set_table("old", "events", "s3://lake/events/v1.json", 1);
    let watcher = StrongWatcher::seed(
        RestCatalogSource::new(format!("http://{addr}")),
        test_options(),
    )
    .await
    .expect("strict initial seed");
    let mutation = CatalogMutation {
        sequence: 2,
        event_id: "rename-2".into(),
        warehouse_id: "warehouse-1".into(),
        table_id: "table-1".into(),
        namespace: vec!["new".into()],
        table: "renamed".into(),
        previous_namespace: Some(vec!["old".into()]),
        previous_table: Some("events".into()),
        operation: "renameTable".into(),
        metadata_location: Some("s3://lake/events/v1.json".into()),
        snapshot_id: Some(1),
    };
    watcher
        .apply(
            &mutation,
            Some(TableState {
                metadata_location: "s3://lake/events/v1.json".into(),
                current_snapshot_id: Some(1),
            }),
        )
        .expect("rename apply");

    assert!(
        watcher
            .table_state(&TableIdent::new(&["old"], "events"))
            .is_none()
    );
    assert!(
        watcher
            .table_state(&TableIdent::new(&["new"], "renamed"))
            .is_some()
    );
}

/// `seeded()` starts `false` and flips to `true` once the first successful
/// poll has populated the watched set — the signal a startup consumer (the
/// warming coordinator, #168) awaits before enumerating pre-existing tables.
#[tokio::test(flavor = "multi_thread")]
async fn seeded_signal_flips_after_the_first_successful_poll() {
    let (addr, mock) = spawn_mock().await;
    mock.set_table("db", "events", "s3://lake/db/events/metadata/v1.json", 100);

    let source = RestCatalogSource::new(format!("http://{addr}"));
    let watcher = PollingWatcher::spawn(source, test_options());
    let mut seeded = watcher.seeded();

    // Await the flip (already-true is fine — the first poll may have won).
    tokio::time::timeout(EVENT_TIMEOUT, async {
        while !*seeded.borrow() {
            seeded.changed().await.expect("watcher alive");
        }
    })
    .await
    .expect("seeded never flipped true");

    // Once seeded, the pre-existing table is enumerable — the guarantee the
    // coordinator's startup pass relies on.
    assert_eq!(
        watcher.watched_tables(),
        vec![TableIdent::new(&["db"], "events")]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn include_exclude_filters_honored() {
    let (addr, mock) = spawn_mock().await;
    mock.set_table("db", "a", "s3://lake/db/a/metadata/v1.json", 10);
    mock.set_table("db", "b", "s3://lake/db/b/metadata/v1.json", 20);
    mock.set_table("tmp", "c", "s3://lake/tmp/c/metadata/v1.json", 30);

    let mut options = test_options();
    options.filter = TableFilter::new(&["db.*"], &["db.b"]);
    let watcher = PollingWatcher::spawn(RestCatalogSource::new(format!("http://{addr}")), options);
    let mut rx = watcher.subscribe();

    let watched = TableIdent::new(&["db"], "a");
    wait_until(
        || watcher.table_state(&watched).is_some(),
        EVENT_TIMEOUT,
        "filtered table seeded",
    )
    .await;
    assert_eq!(watcher.watched_tables(), vec![watched.clone()]);
    assert!(
        watcher
            .table_state(&TableIdent::new(&["db"], "b"))
            .is_none()
    );
    assert!(
        watcher
            .table_state(&TableIdent::new(&["tmp"], "c"))
            .is_none()
    );

    // Commit to all three tables; only the watched one may produce an event.
    mock.set_table("db", "a", "s3://lake/db/a/metadata/v2.json", 11);
    mock.set_table("db", "b", "s3://lake/db/b/metadata/v2.json", 21);
    mock.set_table("tmp", "c", "s3://lake/tmp/c/metadata/v2.json", 31);

    let event = next_event(&mut rx, EVENT_TIMEOUT).await;
    assert_eq!(event.table, watched);
    assert_eq!(event.new_snapshot, Some(11));

    // Nothing further arrives for the filtered-out tables.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(matches!(
        rx.try_recv(),
        Err(broadcast::error::TryRecvError::Empty)
    ));

    // Cheapness: excluded tables are never even load-table'd.
    let loads = mock.loads.lock().expect("mock lock").clone();
    assert!(
        loads.iter().all(|t| t == "db.a"),
        "only db.a may be loaded, got: {loads:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn outage_preserves_state_and_events_resume_on_recovery() {
    let (addr, mock) = spawn_mock().await;
    mock.set_table("db", "events", "s3://lake/db/events/metadata/v1.json", 100);

    let watcher = PollingWatcher::spawn(
        RestCatalogSource::new(format!("http://{addr}")),
        test_options(),
    );
    let mut rx = watcher.subscribe();

    let ident = TableIdent::new(&["db"], "events");
    wait_until(
        || watcher.table_state(&ident).is_some(),
        EVENT_TIMEOUT,
        "initial state seeded",
    )
    .await;

    // Catalog goes dark; a commit happens while it is unreachable.
    mock.set_outage(true);
    mock.set_table("db", "events", "s3://lake/db/events/metadata/v2.json", 200);
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Last-known state still served, no bogus events, no panic.
    let state = watcher
        .table_state(&ident)
        .expect("last-known state survives outage");
    assert_eq!(state.current_snapshot_id, Some(100));
    assert_eq!(watcher.watched_tables(), vec![ident.clone()]);
    assert!(matches!(
        rx.try_recv(),
        Err(broadcast::error::TryRecvError::Empty)
    ));

    // Recovery is automatic: the missed commit is observed and emitted.
    mock.set_outage(false);
    let event = next_event(&mut rx, EVENT_TIMEOUT).await;
    assert_eq!(
        event,
        TableChanged {
            table: ident.clone(),
            old_snapshot: Some(100),
            new_snapshot: Some(200),
        }
    );
    assert_eq!(
        watcher
            .table_state(&ident)
            .expect("recovered state")
            .current_snapshot_id,
        Some(200)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn lineage_returns_recent_history_in_order() {
    let (addr, mock) = spawn_mock().await;
    mock.set_table("db", "events", "s3://lake/db/events/metadata/v1.json", 100);

    let watcher = PollingWatcher::spawn(
        RestCatalogSource::new(format!("http://{addr}")),
        test_options(),
    );
    let mut rx = watcher.subscribe();

    let ident = TableIdent::new(&["db"], "events");
    wait_until(
        || watcher.table_state(&ident).is_some(),
        EVENT_TIMEOUT,
        "initial state seeded",
    )
    .await;

    mock.set_table("db", "events", "s3://lake/db/events/metadata/v2.json", 200);
    next_event(&mut rx, EVENT_TIMEOUT).await;
    mock.set_table("db", "events", "s3://lake/db/events/metadata/v3.json", 300);
    next_event(&mut rx, EVENT_TIMEOUT).await;

    let lineage = watcher.lineage(&ident);
    let ids: Vec<i64> = lineage.iter().map(|e| e.snapshot_id).collect();
    assert_eq!(
        ids,
        vec![100, 200, 300],
        "oldest → newest, ending at current"
    );
    assert_eq!(
        lineage.last().expect("non-empty lineage").metadata_location,
        "s3://lake/db/events/metadata/v3.json"
    );
    // Unknown tables have no lineage.
    assert!(
        watcher
            .lineage(&TableIdent::new(&["db"], "nope"))
            .is_empty()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn table_appearance_and_disappearance_emit_optional_endpoints() {
    let (addr, mock) = spawn_mock().await;
    mock.set_table("db", "a", "s3://lake/db/a/metadata/v1.json", 100);

    let watcher = PollingWatcher::spawn(
        RestCatalogSource::new(format!("http://{addr}")),
        test_options(),
    );
    let mut rx = watcher.subscribe();

    let a = TableIdent::new(&["db"], "a");
    wait_until(
        || watcher.table_state(&a).is_some(),
        EVENT_TIMEOUT,
        "initial state seeded",
    )
    .await;

    // A table created after watch start emits old=None.
    mock.set_table("db", "b", "s3://lake/db/b/metadata/v1.json", 7);
    let b = TableIdent::new(&["db"], "b");
    let event = next_event(&mut rx, EVENT_TIMEOUT).await;
    assert_eq!(
        event,
        TableChanged {
            table: b.clone(),
            old_snapshot: None,
            new_snapshot: Some(7),
        }
    );

    // A dropped table emits new=None and leaves the watched set.
    mock.remove_table("db", "a");
    let event = next_event(&mut rx, EVENT_TIMEOUT).await;
    assert_eq!(
        event,
        TableChanged {
            table: a.clone(),
            old_snapshot: Some(100),
            new_snapshot: None,
        }
    );
    wait_until(
        || watcher.table_state(&a).is_none(),
        EVENT_TIMEOUT,
        "dropped table leaves state",
    )
    .await;
    assert_eq!(watcher.watched_tables(), vec![b]);
}

#[tokio::test(flavor = "multi_thread")]
async fn bearer_token_and_config_construction_work() {
    let (addr, mock) = spawn_mock().await;
    *mock.required_token.lock().expect("mock lock") = Some("sekrit".to_owned());
    mock.set_table("db", "events", "s3://lake/db/events/metadata/v1.json", 100);

    let core_config: verglas_core::config::Catalog = toml::de::from_str(&format!(
        "uri = \"http://{addr}\"\npoll_interval_secs = 1\nbearer_token = \"sekrit\"\n"
    ))
    .expect("catalog config parses");
    let source = RestCatalogSource::from_config(&core_config).expect("catalog source builds");

    let watcher = PollingWatcher::spawn(source, test_options());
    let ident = TableIdent::new(&["db"], "events");
    wait_until(
        || watcher.table_state(&ident).is_some(),
        EVENT_TIMEOUT,
        "authenticated seed",
    )
    .await;

    // Options derived from config carry the interval and filters.
    let options = WatcherOptions::from_config(&core_config);
    assert_eq!(options.interval, Duration::from_secs(1));
    assert!(options.jitter <= options.interval);
    assert!(options.filter.matches(&ident));
}

#[tokio::test(flavor = "multi_thread")]
async fn bearer_token_from_credentials_file_authenticates() {
    let (addr, mock) = spawn_mock().await;
    *mock.required_token.lock().expect("mock lock") = Some("filesekrit".to_owned());
    mock.set_table("db", "events", "s3://lake/db/events/metadata/v1.json", 100);

    // The token lives in a file, referenced by catalog.credentials_file — never
    // inline in the config. Trailing whitespace in the file is trimmed.
    let dir = std::env::temp_dir().join(format!("verglas-catcred-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create scratch dir");
    let token_file = dir.join("catalog-token");
    std::fs::write(&token_file, "filesekrit\n").expect("write token file");

    let core_config: verglas_core::config::Catalog = toml::de::from_str(&format!(
        "uri = \"http://{addr}\"\npoll_interval_secs = 1\ncredentials_file = \"{}\"\n",
        token_file.display()
    ))
    .expect("catalog config parses");
    let source = RestCatalogSource::from_config(&core_config).expect("catalog source builds");

    let watcher = PollingWatcher::spawn(source, test_options());
    let ident = TableIdent::new(&["db"], "events");
    wait_until(
        || watcher.table_state(&ident).is_some(),
        EVENT_TIMEOUT,
        "file-token authenticated seed",
    )
    .await;

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test(flavor = "multi_thread")]
async fn sigv4_signed_catalog_is_watched_and_unsigned_is_rejected() {
    // SigV4 catalog auth (#236): the mock rejects any request that is not
    // SigV4-signed for region `us-west-2` and service `s3tables`. The watcher,
    // configured with those SigV4 settings and an AWS-INI credentials file,
    // signs every call (including /v1/config, so the warehouse-prefix bootstrap
    // works signed) and discovers the seeded table. A bearer-only source against
    // the same mock never authenticates.
    let (addr, mock) = spawn_mock().await;
    *mock.required_sigv4.lock().expect("mock lock") =
        Some(("us-west-2".to_owned(), "s3tables".to_owned()));
    mock.reject_parent_namespaces.store(true, Ordering::SeqCst);
    mock.set_table(
        "lean",
        "factor_files",
        "s3://tb/lean/factor_files/v1.json",
        7,
    );

    // AWS-INI credentials file, named from the config (never inline). Static
    // keys let the default profile provider resolve without a live AWS account.
    let dir = std::env::temp_dir().join(format!("verglas-catsigv4-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create scratch dir");
    let creds_file = dir.join("aws-credentials");
    std::fs::write(
        &creds_file,
        "[default]\naws_access_key_id = AKIAEXAMPLE\naws_secret_access_key = secretexamplekey\n",
    )
    .expect("write aws credentials");

    let core_config: verglas_core::config::Catalog = toml::de::from_str(&format!(
        "uri = \"http://{addr}\"\npoll_interval_secs = 1\n\
         warehouse = \"arn:aws:s3tables:us-west-2:1:bucket/rlean-data\"\n\
         sigv4_region = \"us-west-2\"\nsigv4_signing_name = \"s3tables\"\n\
         credentials_file = \"{}\"\n",
        creds_file.display()
    ))
    .expect("catalog config parses");
    assert!(core_config.sigv4_enabled(), "config turns SigV4 on");
    let source = RestCatalogSource::from_config(&core_config).expect("catalog source builds");

    let watcher = PollingWatcher::spawn(source, test_options());
    let ident = TableIdent::new(&["lean"], "factor_files");
    wait_until(
        || watcher.table_state(&ident).is_some(),
        EVENT_TIMEOUT,
        "sigv4-signed seed",
    )
    .await;
    let state = watcher.table_state(&ident).expect("signed table state");
    assert_eq!(state.current_snapshot_id, Some(7));
    assert_eq!(
        mock.parent_namespace_requests.load(Ordering::SeqCst),
        0,
        "S3 Tables supports one namespace level and must not receive parent queries"
    );

    // A bearer-only source (no SigV4) is rejected by the same mock: it never
    // seeds the table within the timeout.
    let bearer_config: verglas_core::config::Catalog = toml::de::from_str(&format!(
        "uri = \"http://{addr}\"\npoll_interval_secs = 1\nbearer_token = \"nope\"\n"
    ))
    .expect("bearer config parses");
    let bearer_source =
        RestCatalogSource::from_config(&bearer_config).expect("bearer source builds");
    let bearer_watcher = PollingWatcher::spawn(bearer_source, test_options());
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        bearer_watcher.table_state(&ident).is_none(),
        "an unsigned (bearer-only) source must not authenticate against a SigV4 catalog"
    );

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn from_config_rejects_both_token_sources() {
    let core_config: verglas_core::config::Catalog = toml::de::from_str(
        "uri = \"http://example.invalid\"\nbearer_token = \"inline\"\ncredentials_file = \"/nope\"\n",
    )
    .expect("catalog config parses");
    let err = RestCatalogSource::from_config(&core_config)
        .expect_err("setting both token sources must be rejected");
    assert!(
        err.to_string().contains("catalog.credentials_file"),
        "error must name the field, got: {err}"
    );
}

#[test]
fn table_filter_wildcards() {
    let ident = |ns: &str, name: &str| TableIdent::new(&[ns], name);
    // Empty include = everything; exclude wins.
    let all = TableFilter::default();
    assert!(all.matches(&ident("db", "a")));
    let filter = TableFilter::new(&["db.*"], &["db.tmp_*"]);
    assert!(filter.matches(&ident("db", "a")));
    assert!(filter.matches(&ident("db", "tmp"))); // `tmp_*` needs the underscore
    assert!(!filter.matches(&ident("db", "tmp_scratch")));
    assert!(!filter.matches(&ident("analytics", "a")));
    // `*` alone matches anything, including dots.
    let star = TableFilter::new(&["*"], &[]);
    assert!(star.matches(&ident("a", "b")));
    // Exact names match without wildcards; multi-level namespaces join with dots.
    let exact = TableFilter::new(&["ns.sub.t"], &[]);
    assert!(exact.matches(&TableIdent::new(&["ns", "sub"], "t")));
    assert!(!exact.matches(&TableIdent::new(&["ns"], "t")));
}
