//! End-to-end route composition tests for the on-prem REST service.

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use axum::routing::get;
use axum::{Json, Router};
use serde_json::json;
use tokio::net::TcpListener;
use tower::ServiceExt;
use verglas_catalog::{CatalogGateway, CatalogRuntimeRegistry, DatabaseId};
use verglas_core::config::Config;

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
    let app = verglas_rest::compose_database_catalogs(Router::new(), runtimes);

    for (database, expected) in [("analytics", "analytics"), ("customer_lake", "customer")] {
        let response = app
            .clone()
            .oneshot(
                Request::get(format!("/v1/databases/{database}/catalog/v1/config"))
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
