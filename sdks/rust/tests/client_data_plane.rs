//! Connection-contract tests for the Rust catalog client.

use axum::{Json, Router, routing::get};
use tokio::net::TcpListener;
use verglas_core::admin::{ACCESS_PATH, LocalAccess};
use verglas_sdk::{Client, ConnectOptions};

/// A local client discovers the real catalog while retaining the daemon only
/// as its S3 cache/data path.
#[tokio::test]
async fn connect_separates_catalog_from_daemon_cache() {
    let access = LocalAccess {
        s3_endpoint: "http://127.0.0.1:8333".to_owned(),
        catalog_uri: Some("https://tenant.catalog.verglas.dev".to_owned()),
        warehouse: Some("s3://warehouse/tenant".to_owned()),
        region: "auto".to_owned(),
        bucket: Some("warehouse".to_owned()),
        access_key_id: Some("VGKEY".to_owned()),
    };
    let app = Router::new().route(
        ACCESS_PATH,
        get({
            let access = access.clone();
            move || async move { Json(access) }
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock");
    let endpoint = format!("http://{}", listener.local_addr().expect("mock address"));
    tokio::spawn(async move { axum::serve(listener, app).await.expect("mock server") });

    let client = Client::connect(ConnectOptions::new(endpoint))
        .await
        .expect("connect client");
    assert_eq!(client.catalog_uri(), "https://tenant.catalog.verglas.dev");
    assert_eq!(client.s3_endpoint(), Some("http://127.0.0.1:8333"));
}

/// Fully injected container coordinates never require the daemon admin port.
#[tokio::test]
async fn container_environment_shape_connects_without_admin_service() {
    let client = Client::connect(
        ConnectOptions::new("http://127.0.0.1:1")
            .with_catalog_uri("https://tenant.catalog.verglas.dev")
            .with_warehouse("s3://warehouse/tenant")
            .with_s3_endpoint("http://verglas:8333")
            .with_s3_credentials("auto", "VGKEY", "secret")
            .with_token("catalog-token"),
    )
    .await
    .expect("connect without admin service");
    assert_eq!(client.catalog_uri(), "https://tenant.catalog.verglas.dev");
    assert_eq!(client.s3_endpoint(), Some("http://verglas:8333"));
}
