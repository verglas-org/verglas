//! The query node's one route: `POST /v1/query`.
//!
//! No authentication guards this surface in v1 — org-network-only, the same
//! trust model the cache node's admin port uses. A request runs to
//! completion, times out, or is refused with a clean JSON error; nothing
//! panics and nothing is answered with a raw 5xx for a caller-fixable
//! mistake (bad JSON, bad SQL, an over-cap memory request).

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Json;
use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use futures::StreamExt;
use iceberg::Catalog;
use serde::{Deserialize, Serialize};
use verglas_iceberg::{AgentError, PreparedCatalog, batch_to_json_rows_fragment};

/// Shared server state: the long-lived catalog client handle plus the node's
/// fixed query limits.
///
/// A fresh DataFusion catalog session is opened per query in [`run_query`]
/// rather than reused for the process lifetime. `IcebergCatalogProvider`
/// snapshots the namespace/table list once, at construction
/// (`iceberg-datafusion` 0.10.1's `schema.rs` says so directly: "tables might
/// become stale"), so a session opened once at boot would never see a table
/// created afterward — exactly what the frozen local-lite protocol does
/// (`up` boots this node, `check` creates `main.pairing_events` afterward).
/// Rebuilding costs one catalog walk per request; for v1 that is the honest
/// trade against silently serving stale schema. A cache keyed on a real
/// catalog-generation signal is future work — `verglas_iceberg::query`'s own
/// module docs anticipate it, but no such signal is public yet, and adding
/// one is outside this change's scope (no new engine features).
#[derive(Clone)]
pub struct AppState {
    catalog: Arc<dyn Catalog>,
    memory_limit_bytes: usize,
    query_timeout: Duration,
}

impl AppState {
    /// Builds server state over an already-open catalog client handle.
    pub fn new(
        catalog: Arc<dyn Catalog>,
        memory_limit_bytes: usize,
        query_timeout: Duration,
    ) -> Self {
        Self {
            catalog,
            memory_limit_bytes,
            query_timeout,
        }
    }
}

/// Builds the query node's router.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/v1/query", post(handle_query))
        .with_state(state)
}

/// The accepted request body: `{"sql": "..."}`, or the `/v0` form
/// `{"q": "..."}`. `memory_limit_bytes`, when present, is validated against
/// the node's cap but never used to shrink it — the node's one prepared
/// session already runs at the node's configured ceiling.
#[derive(Debug, Deserialize)]
struct QueryRequest {
    sql: Option<String>,
    q: Option<String>,
    memory_limit_bytes: Option<usize>,
}

/// One result column's name and Arrow type label (e.g. `"Int64"`, `"Utf8"`).
#[derive(Debug, Serialize, PartialEq)]
struct ColumnMeta {
    name: String,
    #[serde(rename = "type")]
    data_type: String,
}

/// Parses the request, runs the query under the node's wall-clock timeout,
/// and answers the `/v0` result shape (`{meta, data, rows, statistics}`) on
/// success.
async fn handle_query(State(state): State<AppState>, body: Bytes) -> Response {
    let request: QueryRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(error) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                format!("invalid JSON body: {error}"),
            );
        }
    };
    let sql = match request.sql.or(request.q) {
        Some(sql) if !sql.trim().is_empty() => sql,
        _ => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "request body must set \"sql\" (or \"q\")".to_owned(),
            );
        }
    };
    if let Some(requested) = request.memory_limit_bytes
        && requested > state.memory_limit_bytes
    {
        return error_response(
            StatusCode::BAD_REQUEST,
            format!(
                "requested memory_limit_bytes {requested} exceeds this node's cap of {} bytes",
                state.memory_limit_bytes
            ),
        );
    }

    match tokio::time::timeout(
        state.query_timeout,
        run_query(Arc::clone(&state.catalog), state.memory_limit_bytes, &sql),
    )
    .await
    {
        Ok(Ok(outcome)) => outcome.into_response(),
        Ok(Err(error)) => error_response(StatusCode::BAD_REQUEST, error.to_string()),
        Err(_elapsed) => error_response(
            StatusCode::REQUEST_TIMEOUT,
            format!(
                "query exceeded the {}s node timeout",
                state.query_timeout.as_secs()
            ),
        ),
    }
}

/// One successfully executed query's result.
struct QueryOutcome {
    meta: Vec<ColumnMeta>,
    /// The inner bytes of a JSON array of row objects — no enclosing `[`/`]`;
    /// see [`batch_to_json_rows_fragment`].
    data: Vec<u8>,
    rows: usize,
    elapsed_ms: u128,
}

impl IntoResponse for QueryOutcome {
    /// Assembles the final JSON body by splicing the pre-rendered `data`
    /// fragment between `[`/`]`, rather than re-parsing and re-serializing
    /// it — the whole point of streaming batches through
    /// `batch_to_json_rows_fragment` in the first place.
    fn into_response(self) -> Response {
        let meta = serde_json::to_vec(&self.meta).unwrap_or_else(|_| b"[]".to_vec());
        let mut body = Vec::with_capacity(self.data.len() + meta.len() + 64);
        body.extend_from_slice(b"{\"meta\":");
        body.extend_from_slice(&meta);
        body.extend_from_slice(b",\"data\":[");
        body.extend_from_slice(&self.data);
        body.extend_from_slice(b"],\"rows\":");
        body.extend_from_slice(self.rows.to_string().as_bytes());
        body.extend_from_slice(b",\"statistics\":{\"elapsed\":");
        body.extend_from_slice(self.elapsed_ms.to_string().as_bytes());
        body.extend_from_slice(b",\"rows_read\":");
        body.extend_from_slice(self.rows.to_string().as_bytes());
        body.extend_from_slice(b"}}");
        (
            StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            body,
        )
            .into_response()
    }
}

/// Runs `sql` to completion against a freshly opened, memory-bounded
/// DataFusion session: planning surfaces a syntax/unknown-table error before
/// any batch is pulled, then every batch streams through
/// `batch_to_json_rows_fragment` — the whole result is never collected in
/// memory at once.
async fn run_query(
    catalog: Arc<dyn Catalog>,
    memory_limit_bytes: usize,
    sql: &str,
) -> Result<QueryOutcome, AgentError> {
    let start = Instant::now();
    let prepared =
        PreparedCatalog::open_with_memory_limit(catalog, memory_limit_bytes, None).await?;
    let mut execution = prepared.query_stream(sql).await?;
    let schema = execution.batches.schema();

    let mut data = Vec::new();
    let mut rows = 0usize;
    let mut wrote_any = false;
    while let Some(batch) = execution.batches.next().await {
        let batch = batch.map_err(|error| AgentError::Query(error.to_string()))?;
        let (fragment, count) = batch_to_json_rows_fragment(&batch)?;
        if count == 0 {
            continue;
        }
        if wrote_any {
            data.push(b',');
        }
        data.extend_from_slice(&fragment);
        wrote_any = true;
        rows += count;
    }

    let meta = schema
        .fields()
        .iter()
        .map(|field| ColumnMeta {
            name: field.name().clone(),
            data_type: field.data_type().to_string(),
        })
        .collect();

    Ok(QueryOutcome {
        meta,
        data,
        rows,
        elapsed_ms: start.elapsed().as_millis(),
    })
}

/// Builds a clean JSON error body — never a hang, never a raw 5xx, for a
/// request the caller can fix (bad JSON, bad SQL, an over-cap memory
/// request, a timed-out query).
fn error_response(status: StatusCode, message: String) -> Response {
    (status, Json(serde_json::json!({ "error": message }))).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::io::Write as _;

    use iceberg::CatalogBuilder;
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};

    /// Builds an in-process memory catalog over a fresh temp warehouse path —
    /// the same hermetic pattern `crates/verglas-iceberg/tests` uses, so this
    /// server logic is exercised without MinIO or a REST catalog.
    async fn memory_catalog() -> Arc<dyn Catalog> {
        let warehouse = tempfile::tempdir().expect("warehouse tempdir");
        let catalog = MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    warehouse.path().to_str().expect("utf8 path").to_string(),
                )]),
            )
            .await
            .expect("memory catalog");
        std::mem::forget(warehouse);
        Arc::new(catalog)
    }

    /// Writes `contents` to a temp CSV file, keeping the tempdir alive for the
    /// test process's lifetime.
    fn csv_source(contents: &str) -> std::path::PathBuf {
        let dir = tempfile::tempdir().expect("source tempdir");
        let path = dir.path().join("rows.csv");
        let mut file = std::fs::File::create(&path).expect("create source");
        file.write_all(contents.as_bytes()).expect("write source");
        file.flush().expect("flush source");
        std::mem::forget(dir);
        path
    }

    /// Serves `app` on a loopback ephemeral port; the test process exit reaps
    /// the detached listener task.
    async fn serve(app: Router) -> std::net::SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        addr
    }

    /// A table created before the server starts is queryable, and the `/v0`
    /// shape (`meta`/`data`/`rows`/`statistics`) is exactly what the frozen
    /// local-lite protocol's step 9 checks: a `COUNT(*)` query answers 200
    /// with the right row count in `data`.
    #[tokio::test]
    async fn count_query_answers_the_v0_result_shape() {
        let catalog = memory_catalog().await;
        let ident = verglas_iceberg::parse_table_ident("main.pairing_events").expect("ident");
        let source =
            csv_source("id,event\n1,created\n2,appended\n3,appended\n4,appended\n5,appended\n");
        verglas_iceberg::write::create_table(catalog.as_ref(), &ident, &source, None)
            .await
            .expect("create table");

        let state = AppState::new(catalog, 64 * 1024 * 1024, Duration::from_secs(5));
        let addr = serve(router(state)).await;

        let response = reqwest::Client::new()
            .post(format!("http://{addr}/v1/query"))
            .json(&serde_json::json!({ "sql": "SELECT COUNT(*) AS n FROM main.pairing_events" }))
            .send()
            .await
            .expect("request");
        assert_eq!(response.status(), 200);
        let body: serde_json::Value = response.json().await.expect("json body");
        assert_eq!(body["rows"], 1);
        assert_eq!(body["data"][0]["n"], 5);
        assert_eq!(body["meta"][0]["name"], "n");
        assert_eq!(
            body["statistics"]["rows_read"]
                .as_u64()
                .expect("rows_read is a number"),
            1
        );
    }

    /// A table created AFTER the server started is still queryable — proving
    /// `run_query` opens a fresh catalog session per request instead of
    /// caching a snapshot from before the table existed (see `AppState`'s
    /// docs for why a process-lifetime session would fail this).
    #[tokio::test]
    async fn table_created_after_boot_is_visible_to_a_later_query() {
        let catalog = memory_catalog().await;
        let state = AppState::new(
            Arc::clone(&catalog),
            64 * 1024 * 1024,
            Duration::from_secs(5),
        );
        let addr = serve(router(state)).await;

        let ident = verglas_iceberg::parse_table_ident("main.late_table").expect("ident");
        let source = csv_source("id\n1\n2\n");
        verglas_iceberg::write::create_table(catalog.as_ref(), &ident, &source, None)
            .await
            .expect("create table after boot");

        let response = reqwest::Client::new()
            .post(format!("http://{addr}/v1/query"))
            .json(&serde_json::json!({ "sql": "SELECT COUNT(*) AS n FROM main.late_table" }))
            .send()
            .await
            .expect("request");
        assert_eq!(response.status(), 200);
        let body: serde_json::Value = response.json().await.expect("json body");
        assert_eq!(body["data"][0]["n"], 2);
    }

    /// The `/v0` request shape (`{"q": ...}`) is accepted alongside `sql`.
    #[tokio::test]
    async fn the_v0_q_field_is_accepted() {
        let catalog = memory_catalog().await;
        let ident = verglas_iceberg::parse_table_ident("main.q_form").expect("ident");
        let source = csv_source("id\n1\n");
        verglas_iceberg::write::create_table(catalog.as_ref(), &ident, &source, None)
            .await
            .expect("create table");

        let state = AppState::new(catalog, 64 * 1024 * 1024, Duration::from_secs(5));
        let addr = serve(router(state)).await;

        let response = reqwest::Client::new()
            .post(format!("http://{addr}/v1/query"))
            .json(&serde_json::json!({ "q": "SELECT COUNT(*) AS n FROM main.q_form" }))
            .send()
            .await
            .expect("request");
        assert_eq!(response.status(), 200);
    }

    /// A SQL syntax error answers a clean 4xx JSON error, never a hang or a
    /// raw 5xx — the frozen protocol's other step-9 assertion.
    #[tokio::test]
    async fn a_syntax_error_answers_a_clean_4xx_json_error() {
        let catalog = memory_catalog().await;
        let state = AppState::new(catalog, 64 * 1024 * 1024, Duration::from_secs(5));
        let addr = serve(router(state)).await;

        let response = reqwest::Client::new()
            .post(format!("http://{addr}/v1/query"))
            .json(&serde_json::json!({ "sql": "SELEKT this is not sql" }))
            .send()
            .await
            .expect("request");
        assert!(
            response.status().is_client_error(),
            "status: {}",
            response.status()
        );
        let body: serde_json::Value = response.json().await.expect("json body");
        assert!(body["error"].is_string());
    }

    /// A query against a table that does not exist answers a clean 4xx, not
    /// a 5xx or a hang.
    #[tokio::test]
    async fn an_unknown_table_answers_a_clean_4xx_json_error() {
        let catalog = memory_catalog().await;
        let state = AppState::new(catalog, 64 * 1024 * 1024, Duration::from_secs(5));
        let addr = serve(router(state)).await;

        let response = reqwest::Client::new()
            .post(format!("http://{addr}/v1/query"))
            .json(&serde_json::json!({ "sql": "SELECT * FROM main.does_not_exist" }))
            .send()
            .await
            .expect("request");
        assert!(
            response.status().is_client_error(),
            "status: {}",
            response.status()
        );
    }

    /// A request body with neither `sql` nor `q` is a clean 400, not a panic.
    #[tokio::test]
    async fn a_request_missing_sql_answers_400() {
        let catalog = memory_catalog().await;
        let state = AppState::new(catalog, 64 * 1024 * 1024, Duration::from_secs(5));
        let addr = serve(router(state)).await;

        let response = reqwest::Client::new()
            .post(format!("http://{addr}/v1/query"))
            .json(&serde_json::json!({}))
            .send()
            .await
            .expect("request");
        assert_eq!(response.status(), 400);
    }

    /// A request naming a `memory_limit_bytes` above the node's cap is
    /// refused before any query runs.
    #[tokio::test]
    async fn an_over_cap_memory_request_is_refused() {
        let catalog = memory_catalog().await;
        let state = AppState::new(catalog, 1024, Duration::from_secs(5));
        let addr = serve(router(state)).await;

        let response = reqwest::Client::new()
            .post(format!("http://{addr}/v1/query"))
            .json(&serde_json::json!({ "sql": "SELECT 1", "memory_limit_bytes": 999_999_999_u64 }))
            .send()
            .await
            .expect("request");
        assert_eq!(response.status(), 400);
        let body: serde_json::Value = response.json().await.expect("json body");
        assert!(
            body["error"]
                .as_str()
                .expect("error is a string")
                .contains("memory_limit_bytes")
        );
    }

    /// A catalog wrapper that delays `list_namespaces` — the first call
    /// `PreparedCatalog::open_with_memory_limit` makes while building the
    /// DataFusion session — by `delay`, then delegates everything to `inner`.
    ///
    /// An in-memory catalog with no data resolves every operation
    /// synchronously on its very first poll, so no timeout duration, however
    /// small, can ever race it: `tokio::time::timeout` polls the wrapped
    /// future before checking its deadline, and a future that never returns
    /// `Pending` always wins that race. Forcing one real, deterministic
    /// tokio-timer wait here is what gives the timeout wrapper something
    /// genuine to lose to.
    #[derive(Debug)]
    struct SlowCatalog {
        inner: Arc<dyn Catalog>,
        delay: Duration,
    }

    #[async_trait::async_trait]
    impl Catalog for SlowCatalog {
        async fn list_namespaces(
            &self,
            parent: Option<&iceberg::NamespaceIdent>,
        ) -> iceberg::Result<Vec<iceberg::NamespaceIdent>> {
            tokio::time::sleep(self.delay).await;
            self.inner.list_namespaces(parent).await
        }

        async fn create_namespace(
            &self,
            namespace: &iceberg::NamespaceIdent,
            properties: HashMap<String, String>,
        ) -> iceberg::Result<iceberg::Namespace> {
            self.inner.create_namespace(namespace, properties).await
        }

        async fn get_namespace(
            &self,
            namespace: &iceberg::NamespaceIdent,
        ) -> iceberg::Result<iceberg::Namespace> {
            self.inner.get_namespace(namespace).await
        }

        async fn namespace_exists(
            &self,
            namespace: &iceberg::NamespaceIdent,
        ) -> iceberg::Result<bool> {
            self.inner.namespace_exists(namespace).await
        }

        async fn update_namespace(
            &self,
            namespace: &iceberg::NamespaceIdent,
            properties: HashMap<String, String>,
        ) -> iceberg::Result<()> {
            self.inner.update_namespace(namespace, properties).await
        }

        async fn drop_namespace(&self, namespace: &iceberg::NamespaceIdent) -> iceberg::Result<()> {
            self.inner.drop_namespace(namespace).await
        }

        async fn list_tables(
            &self,
            namespace: &iceberg::NamespaceIdent,
        ) -> iceberg::Result<Vec<iceberg::TableIdent>> {
            self.inner.list_tables(namespace).await
        }

        async fn create_table(
            &self,
            namespace: &iceberg::NamespaceIdent,
            creation: iceberg::TableCreation,
        ) -> iceberg::Result<iceberg::table::Table> {
            self.inner.create_table(namespace, creation).await
        }

        async fn load_table(
            &self,
            table: &iceberg::TableIdent,
        ) -> iceberg::Result<iceberg::table::Table> {
            self.inner.load_table(table).await
        }

        async fn drop_table(&self, table: &iceberg::TableIdent) -> iceberg::Result<()> {
            self.inner.drop_table(table).await
        }

        async fn purge_table(&self, table: &iceberg::TableIdent) -> iceberg::Result<()> {
            self.inner.purge_table(table).await
        }

        async fn table_exists(&self, table: &iceberg::TableIdent) -> iceberg::Result<bool> {
            self.inner.table_exists(table).await
        }

        async fn rename_table(
            &self,
            src: &iceberg::TableIdent,
            dest: &iceberg::TableIdent,
        ) -> iceberg::Result<()> {
            self.inner.rename_table(src, dest).await
        }

        async fn register_table(
            &self,
            table: &iceberg::TableIdent,
            metadata_location: String,
        ) -> iceberg::Result<iceberg::table::Table> {
            self.inner.register_table(table, metadata_location).await
        }

        async fn update_table(
            &self,
            commit: iceberg::TableCommit,
        ) -> iceberg::Result<iceberg::table::Table> {
            self.inner.update_table(commit).await
        }
    }

    /// A query that runs past the node's wall-clock timeout answers a clean
    /// 408, not a hang.
    #[tokio::test]
    async fn a_query_past_the_timeout_answers_408() {
        let catalog: Arc<dyn Catalog> = Arc::new(SlowCatalog {
            inner: memory_catalog().await,
            delay: Duration::from_millis(200),
        });
        let state = AppState::new(catalog, 64 * 1024 * 1024, Duration::from_millis(20));
        let addr = serve(router(state)).await;

        let response = reqwest::Client::new()
            .post(format!("http://{addr}/v1/query"))
            .json(&serde_json::json!({ "sql": "SELECT 1" }))
            .send()
            .await
            .expect("request");
        assert_eq!(response.status(), 408);
    }
}
