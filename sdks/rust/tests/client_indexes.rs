//! Table vector-index handle contract tests for the Rust SDK.

use axum::extract::Path;
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use verglas_sdk::vector::{DeclareIndexRequest, IndexParams, SearchRequest};
use verglas_sdk::{Client, ConnectOptions};

/// The Rust table handle declares, lists, and searches vector indexes like TypeScript.
#[tokio::test]
async fn table_index_handle_round_trips_declare_list_and_search() {
    async fn declare(
        Path(name): Path<String>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> impl IntoResponse {
        assert_eq!(headers["authorization"], "Bearer scoped");
        assert_eq!(name, "demo.embeddings");
        assert_eq!(
            body,
            json!({
                "field":"embedding",
                "metric":"cosine",
                "idField":"row_id",
                "params":{"r":32,"l":50,"alpha":1.1}
            })
        );
        Json(json!({
            "target":"tbl:demo.embeddings",
            "field":"embedding",
            "metric":"cosine",
            "reflectedSnapshot":7,
            "fullBuild":true,
            "inserts":10,
            "deletes":0,
            "consolidated":false,
            "liveCount":10,
            "tombstones":0,
            "blobLocation":"s3://x/idx.puffin",
            "blobBytes":2048
        }))
    }
    async fn list(Path(name): Path<String>, headers: HeaderMap) -> impl IntoResponse {
        assert_eq!(headers["authorization"], "Bearer scoped");
        assert_eq!(name, "demo.embeddings");
        Json(json!({
            "indexes":[{
                "target":"tbl:demo.embeddings",
                "field":"embedding",
                "metric":"cosine",
                "reflectedSnapshot":7,
                "liveCount":10
            }]
        }))
    }
    async fn search(
        Path((name, field)): Path<(String, String)>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> impl IntoResponse {
        assert_eq!(headers["authorization"], "Bearer scoped");
        assert_eq!(name, "demo.embeddings");
        assert_eq!(field, "embedding");
        assert_eq!(body, json!({"vector":[0.1,0.2,0.3],"k":2,"l":8}));
        Json(json!({
            "source":"index",
            "neighbors":[{"id":1,"distance":0.01},{"id":2,"distance":0.02}]
        }))
    }
    let app = Router::new()
        .route(
            "/v1/databases/analytics/tables/{name}/indexes",
            post(declare).get(list),
        )
        .route(
            "/v1/databases/analytics/tables/{name}/indexes/{field}/search",
            post(search),
        );
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let endpoint = format!("http://{}", listener.local_addr().expect("address"));
    tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });
    let client = Client::connect(
        ConnectOptions::new(&endpoint)
            .with_query_uri(&endpoint)
            .with_s3_endpoint("http://127.0.0.1:8333")
            .with_token("scoped"),
    )
    .await
    .expect("connect");

    let table = client
        .database("analytics")
        .expect("database")
        .table("demo.embeddings")
        .expect("table handle");
    let report = table
        .add_index(
            "embedding",
            DeclareIndexRequest {
                field: String::new(),
                metric: "cosine".to_owned(),
                id_field: Some("row_id".to_owned()),
                params: Some(IndexParams {
                    r: Some(32),
                    l: Some(50),
                    alpha: Some(1.1),
                }),
            },
        )
        .await
        .expect("add index");
    assert_eq!(report.field, "embedding");
    assert_eq!(report.live_count, 10);
    assert_eq!(report.blob_bytes, 2048);

    let indexes = table.list_indexes().await.expect("list indexes");
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].field, "embedding");
    assert_eq!(indexes[0].live_count, Some(10));

    let search = table
        .search_index(
            "embedding",
            SearchRequest {
                vector: vec![0.1, 0.2, 0.3],
                k: 2,
                l: Some(8),
            },
        )
        .await
        .expect("search");
    assert_eq!(search.source, "index");
    assert_eq!(search.neighbors.len(), 2);
    assert_eq!(search.neighbors[0].id, 1);
}

/// Rejects empty table names before any HTTP call.
#[tokio::test]
async fn table_rejects_empty_name() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let endpoint = format!("http://{}", listener.local_addr().expect("address"));
    tokio::spawn(async move { axum::serve(listener, Router::new()).await.expect("serve") });
    let client = Client::connect(
        ConnectOptions::new(&endpoint)
            .with_query_uri(&endpoint)
            .with_s3_endpoint("http://127.0.0.1:8333"),
    )
    .await
    .expect("connect");
    let error = client
        .database("analytics")
        .expect("database")
        .table("")
        .expect_err("empty name");
    assert!(error.to_string().contains("table"));
}
