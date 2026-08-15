//! Wire-level integration test for client-side Iceberg ingest (#145).
//!
//! Unlike the hermetic `MemoryCatalog` unit tests in `ingest::write`, this
//! drives the real `iceberg-catalog-rest` `RestCatalog` client — the same
//! code `verglas_sdk::ingest::catalog::open_catalog` builds — against a mock
//! Iceberg REST server (an axum app on an ephemeral port). The mock records
//! every request's method, path, and headers, and answers with real Iceberg
//! metadata by delegating the actual catalog bookkeeping to an in-process
//! `MemoryCatalog` over a temp-dir warehouse. This proves, end to end:
//!
//! - the wire sequence a create + append makes: `GET /v1/config`, a
//!   namespace check/create, `POST .../tables` (create), `POST
//!   .../tables/{name}` (the fast-append commit), and on a second append a
//!   `GET .../tables/{name}` (load) before another commit;
//! - every request carries `X-Iceberg-Access-Delegation: vended-credentials`
//!   (the catalog is asked to vend per-table credentials on every call) and
//!   the configured bearer token;
//! - `FileIO`, opened from the catalog's response, actually lands Parquet
//!   data files under the warehouse directory — the write path is real, not
//!   just a metadata round trip.
//!
//! No real object store or Iceberg REST catalog is contacted.

use std::collections::HashMap;
use std::io::Write;
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use axum::extract::{Path as AxumPath, Request, State};
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::{get, post};
use axum::{Json, Router};
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCommit, TableCreation, TableIdent};
use iceberg_catalog_rest::{
    CommitTableRequest, CommitTableResponse, CreateTableRequest, LoadTableResult,
};
use iceberg_storage_opendal::OpenDalStorageFactory;
use serde_json::{Value, json};
use verglas_sdk::ingest;

/// The bearer token the mock catalog expects on every request.
const CATALOG_TOKEN: &str = "cat_tok_mock_1";

/// One observed request: method, path, whether it carried the vended-creds
/// delegation header, and whether it carried the expected bearer.
#[derive(Debug, Clone)]
struct Observed {
    method: String,
    path: String,
    delegated: bool,
    bearer_ok: bool,
}

/// Shared state: the real catalog backing every mock handler, plus the
/// request log the test asserts against.
#[derive(Clone)]
struct MockState {
    catalog: Arc<dyn Catalog>,
    log: Arc<Mutex<Vec<Observed>>>,
}

/// Records every request (method, path, delegation header, bearer) before
/// dispatching it to the real handler.
async fn record(State(state): State<MockState>, request: Request, next: Next) -> Response {
    let method = request.method().to_string();
    let path = request.uri().path().to_owned();
    let delegated = request
        .headers()
        .get("x-iceberg-access-delegation")
        .and_then(|v| v.to_str().ok())
        == Some("vended-credentials");
    let bearer_ok = request
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        == Some(&format!("Bearer {CATALOG_TOKEN}"));
    state.log.lock().expect("lock").push(Observed {
        method,
        path,
        delegated,
        bearer_ok,
    });
    next.run(request).await
}

/// `GET /v1/config` — no route prefix, no overrides.
async fn get_config() -> Json<Value> {
    Json(json!({ "defaults": {}, "overrides": {} }))
}

/// `GET /v1/namespaces/{ns}` — 200 if the namespace exists, else 404.
async fn get_namespace(
    State(state): State<MockState>,
    AxumPath(ns): AxumPath<String>,
) -> (StatusCode, Json<Value>) {
    let ident = NamespaceIdent::new(ns);
    match state.catalog.get_namespace(&ident).await {
        Ok(_) => (
            StatusCode::OK,
            Json(json!({ "namespace": ident.as_ref(), "properties": {} })),
        ),
        Err(_) => (StatusCode::NOT_FOUND, Json(json!({}))),
    }
}

/// `POST /v1/namespaces` — creates the namespace named in the body.
async fn create_namespace(
    State(state): State<MockState>,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let levels: Vec<String> = body["namespace"]
        .as_array()
        .expect("namespace array")
        .iter()
        .map(|v| v.as_str().expect("string level").to_owned())
        .collect();
    let ident = NamespaceIdent::from_vec(levels).expect("namespace ident");
    state
        .catalog
        .create_namespace(&ident, HashMap::new())
        .await
        .expect("create namespace");
    (
        StatusCode::OK,
        Json(json!({ "namespace": ident.as_ref(), "properties": {} })),
    )
}

/// `POST /v1/namespaces/{ns}/tables` — creates the table, delegating the real
/// schema/metadata bookkeeping to the backing `MemoryCatalog`, and answers
/// with a real `LoadTableResult`.
async fn create_table(
    State(state): State<MockState>,
    AxumPath(ns): AxumPath<String>,
    Json(req): Json<CreateTableRequest>,
) -> Json<LoadTableResult> {
    let namespace = NamespaceIdent::new(ns);
    let creation = TableCreation::builder()
        .name(req.name)
        .schema(req.schema)
        .partition_spec_opt(req.partition_spec)
        .properties(req.properties)
        .build();
    let table = state
        .catalog
        .create_table(&namespace, creation)
        .await
        .expect("create table");
    Json(LoadTableResult {
        metadata_location: table.metadata_location().map(str::to_owned),
        metadata: table.metadata().clone(),
        config: HashMap::new(),
        storage_credentials: None,
    })
}

/// `GET /v1/namespaces/{ns}/tables/{table}` — loads the current table state.
async fn load_table(
    State(state): State<MockState>,
    AxumPath((ns, table)): AxumPath<(String, String)>,
) -> Json<LoadTableResult> {
    let ident = TableIdent::new(NamespaceIdent::new(ns), table);
    let table = state.catalog.load_table(&ident).await.expect("load table");
    Json(LoadTableResult {
        metadata_location: table.metadata_location().map(str::to_owned),
        metadata: table.metadata().clone(),
        config: HashMap::new(),
        storage_credentials: None,
    })
}

/// `POST /v1/namespaces/{ns}/tables/{table}` — commits a fast-append (or any
/// other requirements/updates pair) through the backing catalog.
async fn commit_table(
    State(state): State<MockState>,
    AxumPath((ns, table)): AxumPath<(String, String)>,
    Json(req): Json<CommitTableRequest>,
) -> Json<CommitTableResponse> {
    let ident = TableIdent::new(NamespaceIdent::new(ns), table);
    let commit = TableCommit::from_parts(ident, req.requirements, req.updates);
    let table = state
        .catalog
        .update_table(commit)
        .await
        .expect("commit table");
    Json(CommitTableResponse {
        metadata_location: table
            .metadata_location()
            .expect("metadata location after commit")
            .to_owned(),
        metadata: table.metadata().clone(),
    })
}

/// Spawns the mock catalog on an ephemeral port; returns its base URL, the
/// request log, and the warehouse tempdir (kept alive for the test's
/// duration — data files land under it).
fn spawn_mock_catalog() -> (String, Arc<Mutex<Vec<Observed>>>, tempfile::TempDir) {
    let warehouse = tempfile::tempdir().expect("warehouse tempdir");
    let log = Arc::new(Mutex::new(Vec::new()));
    let warehouse_path = warehouse.path().to_str().expect("utf8 path").to_owned();
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    listener.set_nonblocking(true).expect("nonblocking");
    let addr = listener.local_addr().expect("addr");
    let log_for_thread = log.clone();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        rt.block_on(async move {
            let catalog: Arc<dyn Catalog> = Arc::new(
                MemoryCatalogBuilder::default()
                    .load(
                        "memory",
                        HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse_path)]),
                    )
                    .await
                    .expect("memory catalog"),
            );
            let state = MockState {
                catalog,
                log: log_for_thread,
            };
            let app = Router::new()
                .route("/v1/config", get(get_config))
                .route("/v1/namespaces/{ns}", get(get_namespace))
                .route("/v1/namespaces", post(create_namespace))
                .route("/v1/namespaces/{ns}/tables", post(create_table))
                .route(
                    "/v1/namespaces/{ns}/tables/{table}",
                    get(load_table).post(commit_table),
                )
                .layer(middleware::from_fn_with_state(state.clone(), record))
                .with_state(state);
            let listener = tokio::net::TcpListener::from_std(listener).expect("tokio listener");
            axum::serve(listener, app).await.expect("serve");
        });
    });
    (format!("http://{addr}"), log, warehouse)
}

/// Writes `contents` to a temp file named `name`; the file's tempdir is
/// leaked to keep the path valid for the test's duration.
fn source_file(name: &str, contents: &str) -> PathBuf {
    let dir = tempfile::tempdir().expect("source tempdir");
    let path = dir.path().join(name);
    let mut file = std::fs::File::create(&path).expect("create source");
    file.write_all(contents.as_bytes()).expect("write source");
    std::mem::forget(dir);
    path
}

/// Builds the same kind of `RestCatalog` production code opens (see
/// `ingest::catalog::open_catalog`), but with an `Fs` storage factory
/// instead of `S3` so `FileIO` lands data files under the mock's local
/// warehouse instead of requiring a real object store. This is the
/// substitution the issue's TDD plan calls for: "table load with vended
/// creds pointing FileIO at a temp dir ... instead of a real object store".
async fn rest_catalog_over_fs(base_url: &str) -> Arc<dyn iceberg::Catalog> {
    use iceberg::CatalogBuilder;
    use iceberg_catalog_rest::RestCatalogBuilder;

    let mut props = HashMap::new();
    props.insert(
        iceberg_catalog_rest::REST_CATALOG_PROP_URI.to_owned(),
        base_url.to_owned(),
    );
    props.insert("token".to_owned(), CATALOG_TOKEN.to_owned());
    props.insert(
        "header.X-Iceberg-Access-Delegation".to_owned(),
        "vended-credentials".to_owned(),
    );
    let catalog = RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalStorageFactory::Fs))
        .load("verglas-sdk-test", props)
        .await
        .expect("open rest catalog");
    Arc::new(catalog)
}

/// Acceptance (#145): create → append against a mock REST catalog exercises
/// the real wire sequence, sends the vended-credentials delegation header and
/// bearer on every request, and actually writes Parquet data files.
#[tokio::test]
async fn create_then_append_speaks_the_expected_wire_sequence() {
    let (base_url, log, warehouse) = spawn_mock_catalog();
    let catalog = rest_catalog_over_fs(&base_url).await;
    let ident = ingest::parse_table_ident("mockns.orders").expect("ident");

    let first = source_file("orders.csv", "id,amount\n1,10.5\n2,20.0\n3,5.25\n");
    let create = ingest::create_table_with_catalog(catalog.as_ref(), &ident, &first, None)
        .await
        .expect("create table against the mock catalog");
    assert_eq!(create.records_added, 3);
    assert!(create.data_files_added >= 1);
    assert!(create.snapshot_id.is_some());

    let second = source_file("more.csv", "id,amount\n4,9.99\n");
    let append = ingest::append_with_catalog(catalog.as_ref(), &ident, &second)
        .await
        .expect("append against the mock catalog");
    assert_eq!(append.records_added, 1);
    assert_ne!(append.snapshot_id, create.snapshot_id);

    // Every request the client made must have asked for vended credentials
    // and presented the configured bearer.
    let observed = log.lock().expect("lock").clone();
    assert!(!observed.is_empty(), "the mock must have seen requests");
    for request in &observed {
        assert!(
            request.delegated,
            "{} {} missing X-Iceberg-Access-Delegation: vended-credentials",
            request.method, request.path
        );
        assert!(
            request.bearer_ok,
            "{} {} missing or wrong bearer",
            request.method, request.path
        );
    }

    // The expected wire sequence: config, a namespace probe/create, a table
    // create (POST .../tables), then at least one commit (POST
    // .../tables/{name}) for the create's own initial append, plus a load
    // and a second commit for the append call.
    let methods_paths: Vec<(String, String)> = observed
        .iter()
        .map(|o| (o.method.clone(), o.path.clone()))
        .collect();
    assert!(
        methods_paths
            .iter()
            .any(|(m, p)| m == "GET" && p == "/v1/config"),
        "must GET /v1/config: {methods_paths:?}"
    );
    assert!(
        methods_paths
            .iter()
            .any(|(m, p)| m == "POST" && p == "/v1/namespaces/mockns/tables"),
        "must POST the table create: {methods_paths:?}"
    );
    let commits = methods_paths
        .iter()
        .filter(|(m, p)| m == "POST" && p == "/v1/namespaces/mockns/tables/orders")
        .count();
    assert!(
        commits >= 2,
        "expected at least 2 commits (create's append + the second append), got {commits}: {methods_paths:?}"
    );
    let loads = methods_paths
        .iter()
        .filter(|(m, p)| m == "GET" && p == "/v1/namespaces/mockns/tables/orders")
        .count();
    assert!(
        loads >= 1,
        "append must load the table before committing: {methods_paths:?}"
    );

    // Real Parquet data files landed under the warehouse — the write path is
    // real, not a metadata-only round trip.
    let mut parquet_files = Vec::new();
    for entry in walk(warehouse.path()) {
        if entry.extension().and_then(|e| e.to_str()) == Some("parquet") {
            parquet_files.push(entry);
        }
    }
    assert!(
        !parquet_files.is_empty(),
        "expected at least one Parquet data file under {}",
        warehouse.path().display()
    );
}

/// Recursively lists every file under `dir`.
fn walk(dir: &std::path::Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            out.extend(walk(&path));
        } else {
            out.push(path);
        }
    }
    out
}
