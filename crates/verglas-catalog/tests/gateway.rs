//! Behavioral tests for the shallow cached catalog gateway.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::body::Bytes;
use axum::http::{HeaderMap, Method};
use axum::routing::{any, get};
use axum::{Json, Router};
use serde_json::json;
use tokio::net::TcpListener;
use verglas_catalog::{CatalogGateway, CatalogMutation, TableState};
use verglas_core::config::Config;

/// Successful reads are reused, and a successful mutation forces the next read
/// back to the customer catalog.
#[tokio::test]
async fn reads_cache_and_mutations_invalidate() {
    let reads = Arc::new(AtomicUsize::new(0));
    let app = Router::new()
        .route(
            "/v1/config",
            get({
                let reads = reads.clone();
                move || async move {
                    reads.fetch_add(1, Ordering::SeqCst);
                    Json(json!({"defaults": {}, "overrides": {}}))
                }
            }),
        )
        .route("/v1/namespaces", any(|| async { Json(json!({})) }));
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind catalog");
    let uri = format!("http://{}", listener.local_addr().expect("catalog address"));
    tokio::spawn(async move { axum::serve(listener, app).await.expect("serve catalog") });

    let config = Config::from_toml_str(&format!(
        "[cache]\ndir = '/tmp/verglas-catalog-test'\ncapacity_bytes = '64MB'\n\
         [backend]\nbucket = 'test'\n[catalog]\nuri = '{uri}'\n"
    ))
    .expect("valid config");
    let gateway =
        CatalogGateway::from_config(config.catalog.as_ref().expect("catalog")).expect("gateway");
    assert_eq!(gateway.generation(), 0);

    gateway
        .request(Method::GET, "/v1/config", HeaderMap::new(), Bytes::new())
        .await
        .expect("first read");
    assert_eq!(gateway.generation(), 0);
    gateway
        .request(Method::GET, "/v1/config", HeaderMap::new(), Bytes::new())
        .await
        .expect("cached read");
    assert_eq!(
        gateway.generation(),
        0,
        "cache hits do not invalidate sessions"
    );
    assert_eq!(reads.load(Ordering::SeqCst), 1);

    gateway
        .request(
            Method::POST,
            "/v1/namespaces",
            HeaderMap::new(),
            Bytes::from_static(b"{}"),
        )
        .await
        .expect("write through mutation");
    assert_eq!(gateway.generation(), 1);
    gateway
        .request(Method::GET, "/v1/config", HeaderMap::new(), Bytes::new())
        .await
        .expect("read after mutation");
    assert_eq!(reads.load(Ordering::SeqCst), 2);
}

/// Strong application consumes the durable pointer without re-fetching an
/// historical version that the catalog can no longer serve.
#[tokio::test]
async fn strong_mutation_applies_durable_pointer_without_upstream_io() {
    let app = Router::new().route(
        "/v1/config",
        get(|| async { Json(json!({"defaults": {}, "overrides": {}})) }),
    );
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind catalog");
    let uri = format!("http://{}", listener.local_addr().expect("catalog address"));
    tokio::spawn(async move { axum::serve(listener, app).await.expect("serve catalog") });
    let config = Config::from_toml_str(&format!(
        "[cache]\ndir = '/tmp/verglas-catalog-strong-test'\ncapacity_bytes = '64MB'\n\
         [backend]\nbucket = 'test'\n[catalog]\nuri = '{uri}'\n"
    ))
    .expect("valid config");
    let gateway =
        CatalogGateway::from_config(config.catalog.as_ref().expect("catalog")).expect("gateway");
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
        metadata_location: Some("s3://lake/events/v2.metadata.json".into()),
        snapshot_id: Some(200),
    };

    assert_eq!(
        gateway
            .apply_mutation(&mutation)
            .await
            .expect("durable pointer applies"),
        Some(TableState {
            metadata_location: "s3://lake/events/v2.metadata.json".into(),
            current_snapshot_id: Some(200),
        })
    );
    assert_eq!(gateway.applied_sequence(), 41);

    assert_eq!(gateway.applied_sequence(), 41);
}
