//! Standalone Linear API integration Vessel.
//!
//! The service owns its external API credential and exposes a bounded HTTP
//! contract. The runtime manager can proxy this contract without receiving the
//! Linear credential or granting the container Docker authority.

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::sync::RwLock;

const LINEAR_GRAPHQL_ENDPOINT: &str = "https://api.linear.app/graphql";

/// Shared secret and upstream client state for one Linear Vessel.
struct AppState {
    token: RwLock<Option<String>>,
    client: reqwest::Client,
    graphql_endpoint: String,
}

/// Builds a newly unconfigured Linear integration HTTP application.
pub fn router() -> Router {
    router_with_endpoint(LINEAR_GRAPHQL_ENDPOINT)
}

/// Builds the application against an explicit GraphQL endpoint for testing.
pub fn router_with_endpoint(graphql_endpoint: impl Into<String>) -> Router {
    let state = Arc::new(AppState {
        token: RwLock::new(None),
        client: reqwest::Client::new(),
        graphql_endpoint: graphql_endpoint.into(),
    });
    Router::new()
        .route("/health", get(health))
        .route("/v1/config", get(config_status).put(configure))
        .route("/v1/config/schema", get(config_schema))
        .route("/v1/viewer", get(viewer))
        .route("/v1/query", axum::routing::post(query))
        .with_state(state)
}

/// Reports process liveness independently of user configuration.
async fn health() -> StatusCode {
    StatusCode::NO_CONTENT
}

/// Returns the self-describing setup form rendered by Verglas OS.
async fn config_schema() -> Json<Value> {
    Json(json!({
        "title": "Linear",
        "description": "Connect a Linear workspace with a personal API key.",
        "helpUrl": "https://linear.app/settings/account/security",
        "instructions": [
            "Open Linear Settings, then Security & access.",
            "Create a personal API key with access to the workspace you want to connect.",
            "Paste the key below. The key remains inside this integration Vessel."
        ],
        "fields": [{
            "name": "apiToken",
            "label": "Linear API key",
            "type": "password",
            "secret": true,
            "required": true,
            "placeholder": "lin_api_..."
        }]
    }))
}

/// Returns readiness without exposing the configured credential.
async fn config_status(State(state): State<Arc<AppState>>) -> Json<Value> {
    Json(json!({ "configured": state.token.read().await.is_some() }))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ConfigureRequest {
    api_token: String,
}

/// Replaces the in-memory Linear credential without returning or logging it.
async fn configure(
    State(state): State<Arc<AppState>>,
    Json(request): Json<ConfigureRequest>,
) -> Result<StatusCode, ApiError> {
    if request.api_token.trim().is_empty() {
        return Err(ApiError::BadRequest(
            "apiToken must not be empty".to_owned(),
        ));
    }
    *state.token.write().await = Some(request.api_token);
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct QueryRequest {
    operation: String,
    #[serde(default)]
    limit: Option<u16>,
    #[serde(default)]
    cursor: Option<String>,
}

/// Dispatches one bounded read operation supported by this integration.
async fn query(
    State(state): State<Arc<AppState>>,
    Json(request): Json<QueryRequest>,
) -> Result<Json<Value>, ApiError> {
    match request.operation.as_str() {
        "viewer" => load_viewer(&state).await.and_then(to_json).map(Json),
        "teams" => load_connection(&state, ConnectionKind::Teams, request.limit, request.cursor)
            .await
            .map(Json),
        "issues" => load_connection(
            &state,
            ConnectionKind::Issues,
            request.limit,
            request.cursor,
        )
        .await
        .map(Json),
        operation => Err(ApiError::BadRequest(format!(
            "unsupported Linear query operation: {operation}"
        ))),
    }
}

/// Bounded Linear connection exposed through the semantic query endpoint.
enum ConnectionKind {
    Teams,
    Issues,
}

/// Executes one bounded, cursor-paginated Linear collection query.
async fn load_connection(
    state: &AppState,
    kind: ConnectionKind,
    limit: Option<u16>,
    cursor: Option<String>,
) -> Result<Value, ApiError> {
    let first = limit.unwrap_or(25).clamp(1, 100);
    let (query, field) = match kind {
        ConnectionKind::Teams => (
            "query($first:Int!,$after:String){ teams(first:$first,after:$after){ nodes { id key name description private } pageInfo { hasNextPage endCursor } } }",
            "teams",
        ),
        ConnectionKind::Issues => (
            "query($first:Int!,$after:String){ issues(first:$first,after:$after){ nodes { id identifier url title description priority createdAt updatedAt } pageInfo { hasNextPage endCursor } } }",
            "issues",
        ),
    };
    let data = graphql(state, query, json!({ "first": first, "after": cursor })).await?;
    data.get(field)
        .cloned()
        .ok_or_else(|| ApiError::Upstream(format!("Linear returned no {field} connection")))
}

/// Returns the authenticated Linear user and workspace.
async fn viewer(State(state): State<Arc<AppState>>) -> Result<Json<ViewerResult>, ApiError> {
    load_viewer(&state).await.map(Json)
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ViewerResult {
    user: LinearUser,
    organization: LinearOrganization,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LinearUser {
    id: String,
    name: String,
    display_name: Option<String>,
    email: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LinearOrganization {
    id: String,
    name: String,
    url_key: String,
}

#[derive(Deserialize)]
struct GraphqlResponse<T> {
    data: Option<T>,
    #[serde(default)]
    errors: Vec<GraphqlError>,
}

#[derive(Deserialize)]
struct GraphqlError {
    message: String,
}

/// Executes the fixed viewer query with the Vessel-owned credential.
async fn load_viewer(state: &AppState) -> Result<ViewerResult, ApiError> {
    let data = graphql(
        state,
        "query { viewer { id name displayName email } organization { id name urlKey } }",
        json!({}),
    )
    .await?;
    serde_json::from_value(data).map_err(|error| ApiError::Upstream(error.to_string()))
}

/// Sends one fixed GraphQL document and returns only its data object.
async fn graphql(state: &AppState, query: &str, variables: Value) -> Result<Value, ApiError> {
    let token = state
        .token
        .read()
        .await
        .clone()
        .ok_or(ApiError::NotConfigured)?;
    let response = state
        .client
        .post(&state.graphql_endpoint)
        .bearer_auth(token)
        .json(&json!({
            "query": query,
            "variables": variables,
        }))
        .send()
        .await
        .map_err(|error| ApiError::Upstream(error.to_string()))?;
    let status = response.status();
    let body: GraphqlResponse<Value> = response
        .json()
        .await
        .map_err(|error| ApiError::Upstream(error.to_string()))?;
    if let Some(error) = body.errors.into_iter().next() {
        return Err(ApiError::Upstream(error.message));
    }
    if !status.is_success() {
        return Err(ApiError::Upstream(format!("Linear returned HTTP {status}")));
    }
    body.data
        .ok_or_else(|| ApiError::Upstream("Linear returned no data".to_owned()))
}

/// Converts a typed response to the query endpoint's JSON representation.
fn to_json<T: Serialize>(value: T) -> Result<Value, ApiError> {
    serde_json::to_value(value).map_err(|error| ApiError::Upstream(error.to_string()))
}

#[derive(Debug, Error)]
enum ApiError {
    #[error("Linear integration is not configured; submit apiToken to PUT /v1/config")]
    NotConfigured,
    #[error("{0}")]
    BadRequest(String),
    #[error("Linear API request failed: {0}")]
    Upstream(String),
}

impl IntoResponse for ApiError {
    /// Converts integration failures into bounded JSON responses.
    fn into_response(self) -> Response {
        let status = match self {
            Self::NotConfigured => StatusCode::PRECONDITION_REQUIRED,
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::Upstream(_) => StatusCode::BAD_GATEWAY,
        };
        (status, Json(json!({ "error": self.to_string() }))).into_response()
    }
}

#[cfg(test)]
mod tests {
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    use axum::http::HeaderMap;
    use axum::routing::post;
    use axum::{Json, Router};

    use super::{router, router_with_endpoint};

    /// Configuration metadata explains how to obtain and submit a Linear token.
    #[tokio::test]
    async fn configuration_schema_is_self_describing() {
        let response = router()
            .oneshot(
                Request::get("/v1/config/schema")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("body");
        let value: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(value["fields"][0]["name"], "apiToken");
        assert_eq!(value["fields"][0]["secret"], true);
        assert!(
            value["helpUrl"]
                .as_str()
                .expect("help URL")
                .contains("linear.app")
        );
    }

    /// Querying before user configuration fails with an actionable status.
    #[tokio::test]
    async fn query_requires_configuration() {
        let response = router()
            .oneshot(
                Request::post("/v1/query")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"operation":"viewer"}"#))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::PRECONDITION_REQUIRED);
    }

    /// A configured query forwards bearer authority only to Linear's fixed endpoint.
    #[tokio::test]
    async fn configured_viewer_query_calls_linear() {
        let upstream = Router::new().route(
            "/graphql",
            post(|headers: HeaderMap| async move {
                assert_eq!(headers["authorization"], "Bearer test-linear-token");
                Json(serde_json::json!({
                    "data": {
                        "user": {"id":"user-1","name":"Test","displayName":"Test","email":"test@example.com"},
                        "organization": {"id":"org-1","name":"Example","urlKey":"example"}
                    }
                }))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let endpoint = format!("http://{}/graphql", listener.local_addr().expect("address"));
        tokio::spawn(async move { axum::serve(listener, upstream).await.expect("serve") });

        let app = router_with_endpoint(endpoint);
        let configured = app
            .clone()
            .oneshot(
                Request::put("/v1/config")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"apiToken":"test-linear-token"}"#))
                    .expect("request"),
            )
            .await
            .expect("configure");
        assert_eq!(configured.status(), StatusCode::NO_CONTENT);

        let response = app
            .oneshot(
                Request::post("/v1/query")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"operation":"viewer"}"#))
                    .expect("request"),
            )
            .await
            .expect("query");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("body");
        let value: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(value["organization"]["name"], "Example");
    }
}
