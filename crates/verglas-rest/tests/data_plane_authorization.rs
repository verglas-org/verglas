//! Contract tests for fail-closed authorization at the data-plane boundary.

use std::sync::{Arc, Mutex};

use axum::body::{Body, to_bytes};
use axum::extract::{Extension, State};
use axum::http::{Method, Request, StatusCode, header};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{Value, json};
use tower::ServiceExt;
use verglas_rest::data_plane::{AuthenticatedPrincipal, DataPlaneAccess, protect};

type SeenQuestions = Arc<Mutex<Vec<(String, Value)>>>;
type SeenResources = Arc<Mutex<Vec<(String, Value)>>>;

#[derive(Clone)]
struct AuthorityState {
    questions: SeenQuestions,
    resources: SeenResources,
}

/// Starts an access authority that records authorization questions and returns one fixed answer.
async fn authority(status: StatusCode, response: Value) -> (String, SeenQuestions, SeenResources) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let resources = Arc::new(Mutex::new(Vec::new()));
    let state = AuthorityState {
        questions: seen.clone(),
        resources: resources.clone(),
    };
    let app = Router::new()
        .route(
            "/v1/access/authorize",
            post(
                move |State(state): State<AuthorityState>,
                      headers: axum::http::HeaderMap,
                      Json(body): Json<Value>| {
                    let response = response.clone();
                    async move {
                        let bearer = headers
                            .get(header::AUTHORIZATION)
                            .and_then(|value| value.to_str().ok())
                            .unwrap_or_default()
                            .to_owned();
                        state
                            .questions
                            .lock()
                            .expect("seen lock")
                            .push((bearer, body));
                        (status, Json(response))
                    }
                },
            ),
        )
        .route(
            "/v1/access/data-plane/resources",
            post(
                |State(state): State<AuthorityState>,
                 headers: axum::http::HeaderMap,
                 Json(body): Json<Value>| async move {
                    let bearer = headers
                        .get(header::AUTHORIZATION)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default()
                        .to_owned();
                    state
                        .resources
                        .lock()
                        .expect("resources lock")
                        .push((bearer, body));
                    StatusCode::NO_CONTENT
                },
            ),
        )
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind authority");
    let endpoint = format!("http://{}", listener.local_addr().expect("address"));
    tokio::spawn(async move { axum::serve(listener, app).await.expect("serve authority") });
    (endpoint, seen, resources)
}

/// Returns one allowed authorization response for the test principal.
fn allowed() -> Value {
    json!({
        "identity": {
            "tenant_id": "tenant-a",
            "principal_id": "token/local-cli",
            "token_id": "token-1",
            "audience": "data-plane"
        },
        "decision": {
            "allowed": true,
            "reason": "exact_grant",
            "grant_id": "grant-1",
            "matched_resource_id": "database/analytics",
            "policy_version": 4
        }
    })
}

/// Returns an allowed answer for the combined CLI and SDK audience.
fn allowed_cli() -> Value {
    let mut value = allowed();
    value["identity"]["audience"] = json!("verglas-cli");
    value
}

/// Builds a request with an optional bearer credential.
fn request(method: Method, uri: &str, token: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(token) = token {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    builder.body(Body::empty()).expect("request")
}

#[tokio::test]
async fn missing_and_rejected_bearers_fail_closed_before_handlers_run() {
    let (endpoint, seen, _) =
        authority(StatusCode::UNAUTHORIZED, json!({"error":"invalid token"})).await;
    let access = DataPlaneAccess::new(endpoint, "data-plane").expect("access client");
    let app = protect(
        Router::new().route(
            "/v1/databases/{database}/query",
            post(|| async { StatusCode::OK }),
        ),
        access,
    );

    let missing = app
        .clone()
        .oneshot(request(Method::POST, "/v1/databases/analytics/query", None))
        .await
        .expect("missing response");
    assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);
    assert!(seen.lock().expect("seen lock").is_empty());

    let rejected = app
        .oneshot(request(
            Method::POST,
            "/v1/databases/analytics/query",
            Some("rejected"),
        ))
        .await
        .expect("rejected response");
    assert_eq!(rejected.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(seen.lock().expect("seen lock").len(), 1);
}

#[tokio::test]
async fn allowed_requests_forward_the_bearer_and_receive_verified_identity() {
    let (endpoint, seen, _) = authority(StatusCode::OK, allowed()).await;
    let access = DataPlaneAccess::new(endpoint, "data-plane").expect("access client");
    let app = protect(
        Router::new().route(
            "/v1/databases/{database}/query",
            post(
                |Extension(principal): Extension<AuthenticatedPrincipal>| async move {
                    Json(json!({
                        "tenant": principal.tenant_id,
                        "principal": principal.principal_id,
                        "token": principal.token_id,
                    }))
                },
            ),
        ),
        access,
    );

    let response = app
        .oneshot(request(
            Method::POST,
            "/v1/databases/analytics/query",
            Some("scoped-token"),
        ))
        .await
        .expect("allowed response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 4096).await.expect("body");
    assert_eq!(
        serde_json::from_slice::<Value>(&body).expect("identity json"),
        json!({"tenant":"tenant-a","principal":"token/local-cli","token":"token-1"})
    );
    assert_eq!(
        seen.lock().expect("seen lock").as_slice(),
        &[(
            "Bearer scoped-token".to_owned(),
            json!({
                "audience":"data-plane",
                "resource_id":"database/analytics",
                "action":"query"
            })
        )]
    );
}

#[tokio::test]
async fn cli_tokens_are_accepted_by_the_data_plane_boundary() {
    let (endpoint, _, _) = authority(StatusCode::OK, allowed_cli()).await;
    let access = DataPlaneAccess::new(endpoint, "data-plane").expect("access client");
    let app = protect(
        Router::new().route(
            "/v1/databases/{database}/query",
            post(|| async { StatusCode::OK }),
        ),
        access,
    );

    let response = app
        .oneshot(request(
            Method::POST,
            "/v1/databases/analytics/query",
            Some("cli-token"),
        ))
        .await
        .expect("allowed response");
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn create_ingest_declares_the_table_resource_before_writing() {
    let (endpoint, seen, resources) = authority(StatusCode::OK, allowed()).await;
    let access = DataPlaneAccess::new(endpoint, "data-plane").expect("access client");
    let app = protect(
        Router::new().route(
            "/v1/databases/{database}/ingest/{name}",
            post(|| async { StatusCode::OK }),
        ),
        access,
    );

    let response = app
        .oneshot(request(
            Method::POST,
            "/v1/databases/analytics/ingest/events.new?mode=create&format=csv",
            Some("scoped-token"),
        ))
        .await
        .expect("create response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        seen.lock().expect("seen lock").as_slice(),
        &[(
            "Bearer scoped-token".to_owned(),
            json!({
                "audience":"data-plane",
                "resource_id":"database/analytics",
                "action":"create_child"
            })
        )]
    );
    assert_eq!(
        resources.lock().expect("resources lock").as_slice(),
        &[(
            "Bearer scoped-token".to_owned(),
            json!({
                "id":"table/analytics/events.new",
                "kind":"table",
                "parent_id":"database/analytics"
            })
        )]
    );
}

#[tokio::test]
async fn routes_map_to_stable_resources_and_least_privilege_actions() {
    let (endpoint, seen, _) = authority(StatusCode::OK, allowed()).await;
    let access = DataPlaneAccess::new(endpoint, "data-plane").expect("access client");
    let app = protect(
        Router::new()
            .route("/v1/databases/{database}/query", post(|| async {}))
            .route(
                "/v1/databases/{database}/catalog/{*path}",
                get(|| async {}).post(|| async {}),
            )
            .route(
                "/v1/databases/{database}/tables/{name}/{*path}",
                get(|| async {}).post(|| async {}),
            )
            .route("/v1/databases/{database}/ingest/{name}", post(|| async {}))
            .route(
                "/v1/databases/{database}/graphs/{namespace}/{*path}",
                get(|| async {}).post(|| async {}),
            )
            .route(
                "/v1/kv/{namespace}/{*key}",
                get(|| async {}).put(|| async {}),
            )
            .route("/v1/queues/{name}/enqueue", post(|| async {}))
            .route("/v1/queues/{name}/poll", post(|| async {}))
            .route("/v1/queues/{name}/subscribe", post(|| async {}))
            .route("/v1/queues/{name}/ack", post(|| async {}))
            .route("/v1/workers", get(|| async {}).post(|| async {}))
            .route("/v1/workers/{name}", get(|| async {}))
            .route("/v1/workers/{name}/state", axum::routing::put(|| async {}))
            .route("/v1/workers/{name}/run", post(|| async {})),
        access,
    );
    let cases = [
        (Method::POST, "/v1/databases/analytics/query"),
        (Method::GET, "/v1/databases/analytics/catalog/v1/config"),
        (
            Method::POST,
            "/v1/databases/analytics/catalog/v1/warehouse/namespaces",
        ),
        (
            Method::GET,
            "/v1/databases/analytics/tables/events.orders/rows",
        ),
        (
            Method::POST,
            "/v1/databases/analytics/tables/events.orders/ingest",
        ),
        (
            Method::POST,
            "/v1/databases/analytics/ingest/events.new?mode=create&format=csv",
        ),
        (
            Method::POST,
            "/v1/databases/analytics/ingest/events.orders?mode=append&format=csv",
        ),
        (
            Method::POST,
            "/v1/databases/analytics/tables/events.orders/indexes/embedding/search",
        ),
        (
            Method::GET,
            "/v1/databases/analytics/graphs/knowledge/indexes",
        ),
        (
            Method::POST,
            "/v1/databases/analytics/graphs/knowledge/nodes",
        ),
        (Method::GET, "/v1/kv/workshop.blueprints/featured"),
        (Method::PUT, "/v1/kv/workshop.blueprints/featured"),
        (Method::POST, "/v1/queues/ingest/enqueue"),
        (Method::POST, "/v1/queues/ingest/poll"),
        (Method::POST, "/v1/queues/ingest/subscribe"),
        (Method::POST, "/v1/queues/ingest/ack"),
        (Method::GET, "/v1/workers"),
        (Method::POST, "/v1/workers"),
        (Method::GET, "/v1/workers/market-ingest"),
        (Method::PUT, "/v1/workers/market-ingest/state"),
        (Method::POST, "/v1/workers/market-ingest/run"),
    ];
    for (method, uri) in cases {
        let response = app
            .clone()
            .oneshot(request(method, uri, Some("scoped-token")))
            .await
            .expect("mapped response");
        assert_eq!(response.status(), StatusCode::OK, "{uri}");
    }
    let bodies: Vec<Value> = seen
        .lock()
        .expect("seen lock")
        .iter()
        .map(|(_, body)| body.clone())
        .collect();
    assert_eq!(
        bodies,
        vec![
            json!({"audience":"data-plane","resource_id":"database/analytics","action":"query"}),
            json!({"audience":"data-plane","resource_id":"database/analytics","action":"describe"}),
            json!({"audience":"data-plane","resource_id":"database/analytics","action":"create_child"}),
            json!({"audience":"data-plane","resource_id":"table/analytics/events.orders","action":"query"}),
            json!({"audience":"data-plane","resource_id":"table/analytics/events.orders","action":"append"}),
            json!({"audience":"data-plane","resource_id":"database/analytics","action":"create_child"}),
            json!({"audience":"data-plane","resource_id":"table/analytics/events.orders","action":"append"}),
            json!({"audience":"data-plane","resource_id":"vector/analytics/events.orders/embedding","action":"query"}),
            json!({"audience":"data-plane","resource_id":"graph/analytics/knowledge","action":"describe"}),
            json!({"audience":"data-plane","resource_id":"graph/analytics/knowledge","action":"append"}),
            json!({"audience":"data-plane","resource_id":"kv/workshop.blueprints","action":"query"}),
            json!({"audience":"data-plane","resource_id":"kv/workshop.blueprints","action":"modify"}),
            json!({"audience":"data-plane","resource_id":"queue/ingest","action":"append"}),
            json!({"audience":"data-plane","resource_id":"queue/ingest","action":"query"}),
            json!({"audience":"data-plane","resource_id":"queue/ingest","action":"query"}),
            json!({"audience":"data-plane","resource_id":"queue/ingest","action":"modify"}),
            json!({"audience":"data-plane","resource_id":"tenant","action":"discover"}),
            json!({"audience":"data-plane","resource_id":"tenant","action":"create_child"}),
            json!({"audience":"data-plane","resource_id":"tenant","action":"discover"}),
            json!({"audience":"data-plane","resource_id":"tenant","action":"modify"}),
            json!({"audience":"data-plane","resource_id":"tenant","action":"execute"}),
        ]
    );
}
