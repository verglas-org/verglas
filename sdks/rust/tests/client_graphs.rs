//! Graph handle contract tests for the Rust SDK.

use axum::extract::Path;
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{Map, Value, json};
use tokio::net::TcpListener;
use verglas_sdk::graph::{
    EdgeInput, GraphDirection, GraphOp, NeighborView, NodeInput, ReachedView,
};
use verglas_sdk::{Client, ConnectOptions, GraphReadOptions};

/// The Rust graph handle creates, mutates, indexes, and queries with the TypeScript path surface.
#[tokio::test]
async fn graph_handle_round_trips_lifecycle_and_queries() {
    async fn create(
        Path(namespace): Path<String>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> impl IntoResponse {
        assert_eq!(headers["authorization"], "Bearer scoped");
        assert_eq!(namespace, "kg");
        assert_eq!(body, json!({}));
        Json(json!({
            "namespace":"kg",
            "nodesTable":"kg.nodes",
            "edgesTable":"kg.edges"
        }))
    }
    async fn show(Path(namespace): Path<String>, headers: HeaderMap) -> impl IntoResponse {
        assert_eq!(headers["authorization"], "Bearer scoped");
        assert_eq!(namespace, "kg");
        Json(json!({
            "namespace":"kg",
            "nodesTable":"kg.nodes",
            "edgesTable":"kg.edges",
            "nodeCount":1,
            "edgeCount":1,
            "indexed":true,
            "snapshotId":9
        }))
    }
    async fn nodes(Path(namespace): Path<String>, Json(body): Json<Value>) -> impl IntoResponse {
        assert_eq!(namespace, "kg");
        assert_eq!(
            body,
            json!({"nodes":[{"id":"a","labels":["Person"],"properties":{}}]})
        );
        Json(json!({"snapshotId": 1, "count": 1}))
    }
    async fn edges(Path(namespace): Path<String>, Json(body): Json<Value>) -> impl IntoResponse {
        assert_eq!(namespace, "kg");
        assert_eq!(
            body,
            json!({
                "edges":[{
                    "srcId":"a",
                    "predicate":"knows",
                    "dstId":"b",
                    "confidence":1.0,
                    "provenance":"m1",
                    "properties":{}
                }]
            })
        );
        Json(json!({"snapshotId": 2, "count": 1}))
    }
    async fn index(Path(namespace): Path<String>, Json(body): Json<Value>) -> impl IntoResponse {
        assert_eq!(namespace, "kg");
        assert_eq!(body, json!({}));
        Json(json!({
            "built":true,
            "snapshotId":2,
            "nodeCount":2,
            "edgeCount":1,
            "blobPath":"s3://x/index.puffin",
            "blobBytes":128,
            "mode":"full"
        }))
    }
    async fn query(Path(namespace): Path<String>, Json(body): Json<Value>) -> impl IntoResponse {
        assert_eq!(namespace, "kg");
        match body["op"].as_str() {
            Some("neighbors") => {
                assert_eq!(body["start"], "a");
                assert_eq!(body["direction"], "out");
                assert_eq!(body["filter"]["predicate"], "knows");
                Json(json!({
                    "op":"neighbors",
                    "backend":"index",
                    "snapshotId":2,
                    "neighbors":[{
                        "nodeId":"b",
                        "predicate":"knows",
                        "confidence":1.0,
                        "edgeId":"e1",
                        "provenance":"m1",
                        "direction":"out"
                    }]
                }))
            }
            Some("kHop") => {
                assert_eq!(body["start"], "a");
                assert_eq!(body["k"], 2);
                Json(json!({
                    "op":"kHop",
                    "backend":"index",
                    "snapshotId":2,
                    "reached":[{"nodeId":"b","hops":1,"pathConfidence":1.0}]
                }))
            }
            Some("paths") => {
                assert_eq!(body["start"], "a");
                assert_eq!(body["dst"], "b");
                assert_eq!(body["maxHops"], 3);
                Json(json!({
                    "op":"paths",
                    "backend":"scan",
                    "snapshotId":2,
                    "paths":[{
                        "nodes":["a","b"],
                        "edges":[{
                            "srcId":"a",
                            "predicate":"knows",
                            "dstId":"b",
                            "confidence":1.0,
                            "edgeId":"e1",
                            "provenance":"m1"
                        }],
                        "confidence":1.0
                    }]
                }))
            }
            other => panic!("unexpected op {other:?}"),
        }
    }
    let app = Router::new()
        .route("/v1/graphs/{namespace}", post(create).get(show))
        .route("/v1/graphs/{namespace}/nodes", post(nodes))
        .route("/v1/graphs/{namespace}/edges", post(edges))
        .route("/v1/graphs/{namespace}/index", post(index))
        .route("/v1/graphs/{namespace}/query", post(query));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let endpoint = format!("http://{}", listener.local_addr().expect("address"));
    tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });
    let client = Client::connect(
        ConnectOptions::new(&endpoint)
            .with_query_uri(&endpoint)
            .with_catalog_uri("http://127.0.0.1:1")
            .with_s3_endpoint("http://127.0.0.1:8333")
            .with_token("scoped"),
    )
    .await
    .expect("connect");

    let graph = client.graph("kg").expect("graph handle");
    let created = graph.create().await.expect("create");
    assert_eq!(created.namespace, "kg");
    assert_eq!(created.nodes_table, "kg.nodes");

    let inserted = graph
        .insert_nodes(vec![NodeInput {
            id: "a".to_owned(),
            labels: vec!["Person".to_owned()],
            properties: Map::new(),
            agent_id: None,
            namespace: None,
        }])
        .await
        .expect("insert nodes");
    assert_eq!(inserted.count, 1);
    assert_eq!(inserted.snapshot_id, Some(1));

    let edges = graph
        .insert_edges(vec![EdgeInput {
            edge_id: None,
            src_id: "a".to_owned(),
            predicate: "knows".to_owned(),
            dst_id: "b".to_owned(),
            confidence: 1.0,
            provenance: "m1".to_owned(),
            supersedes: None,
            valid_from: None,
            agent_id: None,
            namespace: None,
            properties: Map::new(),
        }])
        .await
        .expect("insert edges");
    assert_eq!(edges.count, 1);

    let indexed = graph.build_index().await.expect("build index");
    assert!(indexed.built);
    assert_eq!(indexed.edge_count, 1);

    let shown = graph.show().await.expect("show");
    assert!(shown.indexed);
    assert_eq!(shown.snapshot_id, Some(9));

    let neighbors = graph
        .neighbors(
            "a",
            GraphReadOptions {
                predicate: Some("knows".to_owned()),
                direction: GraphDirection::Out,
                ..GraphReadOptions::default()
            },
        )
        .await
        .expect("neighbors");
    assert_eq!(
        neighbors,
        vec![NeighborView {
            node_id: "b".to_owned(),
            predicate: "knows".to_owned(),
            confidence: 1.0,
            edge_id: "e1".to_owned(),
            provenance: "m1".to_owned(),
            direction: GraphDirection::Out,
        }]
    );

    let reached = graph
        .k_hop("a", 2, GraphReadOptions::default())
        .await
        .expect("k_hop");
    assert_eq!(
        reached,
        vec![ReachedView {
            node_id: "b".to_owned(),
            hops: 1,
            path_confidence: 1.0,
        }]
    );

    let paths = graph
        .paths("a", "b", 3, GraphReadOptions::default())
        .await
        .expect("paths");
    assert_eq!(paths.len(), 1);
    assert_eq!(paths[0].nodes, vec!["a".to_owned(), "b".to_owned()]);
    assert_eq!(paths[0].confidence, 1.0);

    // Keep GraphOp in the test surface so the query body enum stays linked.
    assert_eq!(format!("{:?}", GraphOp::Neighbors), "Neighbors");
}

/// Rejects empty graph namespaces before any HTTP call.
#[tokio::test]
async fn graph_rejects_empty_namespace() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let endpoint = format!("http://{}", listener.local_addr().expect("address"));
    tokio::spawn(async move { axum::serve(listener, Router::new()).await.expect("serve") });
    let client = Client::connect(
        ConnectOptions::new(&endpoint)
            .with_query_uri(&endpoint)
            .with_catalog_uri("http://127.0.0.1:1")
            .with_s3_endpoint("http://127.0.0.1:8333"),
    )
    .await
    .expect("connect");
    let error = client.graph("").expect_err("empty namespace");
    assert!(error.to_string().contains("graph"));
}
