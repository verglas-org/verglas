//! End-to-end route composition tests for the on-prem REST service.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::{Body, to_bytes};
use axum::extract::State;
use axum::http::{HeaderMap, Request, StatusCode, header};
use axum::routing::get;
use axum::{Json, Router};
use serde_json::json;
use tokio::net::TcpListener;
use tower::ServiceExt;
use verglas_catalog::{CatalogGateway, CatalogRuntimeRegistry, DatabaseId};
use verglas_core::config::Config;
use verglas_rest::data_plane::{
    AuthenticatedPrincipal, AuthorizationFailure, AuthorizationQuestion, DataPlaneAuthorizer,
};

/// Upstream authorization and trusted-database headers observed by the catalog stub.
type SeenCatalogRequests = Arc<Mutex<Vec<(String, String)>>>;

/// Test authorizer that treats each non-empty bearer as a distinct verified principal.
#[derive(Clone)]
struct BearerAuthorizer;

#[async_trait]
impl DataPlaneAuthorizer for BearerAuthorizer {
    /// Catalog requests in these tests target the ordinary data-plane audience.
    fn audience(&self) -> &str {
        "data-plane"
    }

    /// Accepts a strict bearer and returns a stable identity derived from that bearer.
    async fn authorize(
        &self,
        authorization: &str,
        _question: AuthorizationQuestion,
    ) -> Result<AuthenticatedPrincipal, AuthorizationFailure> {
        let token = authorization
            .strip_prefix("Bearer ")
            .filter(|token| !token.is_empty())
            .ok_or(AuthorizationFailure::Unauthenticated)?;
        Ok(AuthenticatedPrincipal {
            tenant_id: "tenant-a".to_owned(),
            principal_id: format!("token/{token}"),
            token_id: token.to_owned(),
            audience: "data-plane".to_owned(),
        })
    }
}

/// Query routes and the mounted catalog proxy remain usable on one listener.
#[tokio::test]
async fn query_and_catalog_share_the_composed_listener() {
    let upstream = Router::new().route(
        "/v1/config",
        get(|| async { Json(json!({"defaults": {}, "overrides": {}})) }),
    );
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind catalog");
    let uri = format!("http://{}", listener.local_addr().expect("catalog address"));
    tokio::spawn(async move {
        axum::serve(listener, upstream)
            .await
            .expect("serve catalog")
    });
    let config = Config::from_toml_str(&format!(
        "[cache]\ndir = '/tmp/verglas-rest-test'\ncapacity_bytes = '64MB'\n\
         [backend]\nbucket = 'test'\n[catalog]\nuri = '{uri}'\n"
    ))
    .expect("valid config");
    let catalog = CatalogGateway::from_config(config.catalog.as_ref().expect("catalog"))
        .expect("catalog gateway");
    let query = Router::new().route("/v1/query", get(|| async { "query" }));
    let app = verglas_rest::compose_query_and_catalog(query, catalog);

    let response = app
        .clone()
        .oneshot(
            Request::get("/v1/query")
                .body(Body::empty())
                .expect("query"),
        )
        .await
        .expect("query response");
    assert_eq!(response.status(), StatusCode::OK);

    let response = app
        .oneshot(
            Request::get("/catalog/v1/config")
                .body(Body::empty())
                .expect("catalog"),
        )
        .await
        .expect("catalog response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("catalog body");
    assert!(serde_json::from_slice::<serde_json::Value>(&body).is_ok());
}

/// Database-scoped routes select the requested catalog instead of one process-global gateway.
#[tokio::test]
async fn database_catalog_routes_are_selected_by_database_id() {
    async fn upstream(label: &'static str) -> (String, tokio::task::JoinHandle<()>) {
        let app = Router::new().route(
            "/v1/config",
            get(move || async move { Json(json!({"catalog": label})) }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let uri = format!("http://{}", listener.local_addr().expect("address"));
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve");
        });
        (uri, task)
    }

    let (analytics_uri, _analytics) = upstream("analytics").await;
    let (customer_uri, _customer) = upstream("customer").await;
    let runtimes = CatalogRuntimeRegistry::default();
    for (database, uri) in [
        ("analytics", analytics_uri),
        ("customer_lake", customer_uri),
    ] {
        let config = Config::from_toml_str(&format!(
            "[cache]\ndir = '/tmp/verglas-rest-{database}'\ncapacity_bytes = '64MB'\n\
             [backend]\nbucket = 'test'\n[catalog]\nuri = '{uri}'\n"
        ))
        .expect("config");
        runtimes
            .insert(
                DatabaseId::new(database).expect("database id"),
                CatalogGateway::from_config(config.catalog.as_ref().expect("catalog"))
                    .expect("gateway"),
            )
            .expect("runtime");
    }
    let app = verglas_rest::data_plane::protect(
        verglas_rest::compose_database_catalogs(Router::new(), runtimes),
        BearerAuthorizer,
    );

    for (database, expected) in [("analytics", "analytics"), ("customer_lake", "customer")] {
        let response = app
            .clone()
            .oneshot(
                Request::get(format!("/v1/databases/{database}/catalog/v1/config"))
                    .header(header::AUTHORIZATION, "Bearer catalog-reader")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        assert_eq!(
            status,
            StatusCode::OK,
            "catalog response: {}",
            String::from_utf8_lossy(&body)
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).expect("json")["catalog"],
            expected
        );
    }
}

/// A database catalog forwards the verified caller credential and never shares
/// an authenticated response with a different principal.
#[tokio::test]
async fn authenticated_database_catalog_forwards_bearers_without_shared_caching() {
    let seen: SeenCatalogRequests = Arc::new(Mutex::new(Vec::new()));
    let upstream = Router::new()
        .route(
            "/v1/config",
            get(
                |headers: HeaderMap, State(seen): State<SeenCatalogRequests>| async move {
                    let authorization = headers
                        .get(header::AUTHORIZATION)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or("missing")
                        .to_owned();
                    let database = headers
                        .get("x-verglas-database-id")
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or("missing")
                        .to_owned();
                    seen.lock()
                        .expect("upstream bearer lock")
                        .push((authorization.clone(), database.clone()));
                    Json(json!({"authorization": authorization, "database": database}))
                },
            ),
        )
        .with_state(seen.clone());
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind catalog");
    let uri = format!("http://{}", listener.local_addr().expect("catalog address"));
    tokio::spawn(async move {
        axum::serve(listener, upstream)
            .await
            .expect("serve catalog")
    });

    let config = Config::from_toml_str(&format!(
        "[cache]\ndir = '/tmp/verglas-rest-authenticated-catalog'\ncapacity_bytes = '64MB'\n\
         [backend]\nbucket = 'test'\n[catalog]\nuri = '{uri}'\n"
    ))
    .expect("valid config");
    let runtimes = CatalogRuntimeRegistry::default();
    runtimes
        .insert(
            DatabaseId::new("analytics").expect("database id"),
            CatalogGateway::from_config(config.catalog.as_ref().expect("catalog"))
                .expect("gateway"),
        )
        .expect("runtime");
    let app = verglas_rest::data_plane::protect(
        verglas_rest::compose_database_catalogs(Router::new(), runtimes),
        BearerAuthorizer,
    );

    let missing = app
        .clone()
        .oneshot(
            Request::get("/v1/databases/analytics/catalog/v1/config")
                .body(Body::empty())
                .expect("missing credential request"),
        )
        .await
        .expect("missing credential response");
    assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);

    for token in ["user-a", "user-a", "user-b"] {
        let response = app
            .clone()
            .oneshot(
                Request::get("/v1/databases/analytics/catalog/v1/config")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .header("x-verglas-database-id", "attacker-selected")
                    .body(Body::empty())
                    .expect("catalog request"),
            )
            .await
            .expect("catalog response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("catalog body");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).expect("catalog json"),
            json!({"authorization": format!("Bearer {token}"), "database": "analytics"})
        );
    }
    assert_eq!(
        seen.lock().expect("upstream bearer lock").as_slice(),
        [
            ("Bearer user-a".to_owned(), "analytics".to_owned()),
            ("Bearer user-a".to_owned(), "analytics".to_owned()),
            ("Bearer user-b".to_owned(), "analytics".to_owned()),
        ]
    );
}
