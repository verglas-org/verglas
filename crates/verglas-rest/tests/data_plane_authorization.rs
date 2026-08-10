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

/// Starts an access authority that records authorization questions and returns one fixed answer.
async fn authority(status: StatusCode, response: Value) -> (String, SeenQuestions) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let app = Router::new()
        .route(
            "/v1/access/authorize",
            post(
                move |State(seen): State<SeenQuestions>,
                      headers: axum::http::HeaderMap,
                      Json(body): Json<Value>| {
                    let response = response.clone();
                    async move {
                        let bearer = headers
                            .get(header::AUTHORIZATION)
                            .and_then(|value| value.to_str().ok())
                            .unwrap_or_default()
                            .to_owned();
                        seen.lock().expect("seen lock").push((bearer, body));
                        (status, Json(response))
                    }
                },
            ),
        )
        .with_state(seen.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind authority");
    let endpoint = format!("http://{}", listener.local_addr().expect("address"));
    tokio::spawn(async move { axum::serve(listener, app).await.expect("serve authority") });
    (endpoint, seen)
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
    let (endpoint, seen) =
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
    let (endpoint, seen) = authority(StatusCode::OK, allowed()).await;
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
    let (endpoint, _) = authority(StatusCode::OK, allowed_cli()).await;
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
async fn routes_map_to_stable_resources_and_least_privilege_actions() {
    let (endpoint, seen) = authority(StatusCode::OK, allowed()).await;
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
            .route("/v1/queues/{name}/ack", post(|| async {})),
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
        (Method::POST, "/v1/queues/ingest/ack"),
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
            json!({"audience":"data-plane","resource_id":"vector/analytics/events.orders/embedding","action":"query"}),
            json!({"audience":"data-plane","resource_id":"graph/analytics/knowledge","action":"describe"}),
            json!({"audience":"data-plane","resource_id":"graph/analytics/knowledge","action":"append"}),
            json!({"audience":"data-plane","resource_id":"kv/workshop.blueprints","action":"query"}),
            json!({"audience":"data-plane","resource_id":"kv/workshop.blueprints","action":"modify"}),
            json!({"audience":"data-plane","resource_id":"queue/ingest","action":"append"}),
            json!({"audience":"data-plane","resource_id":"queue/ingest","action":"query"}),
            json!({"audience":"data-plane","resource_id":"queue/ingest","action":"modify"}),
        ]
    );
}
