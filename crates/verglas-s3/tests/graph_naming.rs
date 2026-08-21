//! Route names of the Verglas Graph API.
//!
//! The operation vocabulary follows Graphiti's add/search/retrieve verbs, so
//! these assertions pin the exact names callers sign and dispatch against.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use tower::ServiceExt;
use verglas_s3::semantic::{SemanticApi, SemanticError, SemanticOperation, router};

/// Records graph operation dispatches so a test can assert which one ran.
struct RecordingGraphApi(Mutex<Vec<SemanticOperation>>);

#[async_trait]
impl SemanticApi for RecordingGraphApi {
    /// Records a Graph operation and returns an empty response object.
    async fn call(
        &self,
        operation: SemanticOperation,
        _input: Value,
    ) -> Result<Value, SemanticError> {
        if !matches!(operation, SemanticOperation::Graph(_)) {
            return Err(SemanticError::validation("expected graph operation"));
        }
        self.0
            .lock()
            .map_err(|error| SemanticError::unavailable(error.to_string()))?
            .push(operation);
        Ok(json!({}))
    }
}

/// Posts an empty JSON body to one route and returns its status.
async fn post(app: &axum::Router, uri: &str) -> StatusCode {
    let request = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from("{}"))
        .expect("request builds");
    app.clone()
        .oneshot(request)
        .await
        .expect("router responds")
        .status()
}

/// Every shipped graph route dispatches to a Graph operation.
#[tokio::test]
async fn graph_routes_dispatch_by_name() {
    let api = Arc::new(RecordingGraphApi(Mutex::new(Vec::new())));
    let app = router(api.clone());
    let names = [
        "CreateGraph",
        "ListGraphs",
        "DescribeGraph",
        "DeleteGraph",
        "AddNodes",
        "AddEdges",
        "RetrieveNeighbors",
        "SearchKHop",
        "SearchNeighborhood",
        "SearchPaths",
        "SearchPrecedents",
        "BuildIndex",
    ];
    for name in names {
        assert_eq!(
            post(&app, &format!("/{name}")).await,
            StatusCode::OK,
            "{name}"
        );
    }
    let recorded = api.0.lock().expect("recorded operations");
    assert_eq!(recorded.len(), names.len());
}

/// The ad-hoc names the Graphiti-style vocabulary replaced are gone, so a
/// stale caller fails loudly instead of reaching a silently renamed operation.
#[tokio::test]
async fn retired_operation_names_are_absent() {
    let api = Arc::new(RecordingGraphApi(Mutex::new(Vec::new())));
    let app = router(api.clone());
    for name in [
        "PutNodes",
        "PutEdges",
        "GetNeighbors",
        "QueryKHop",
        "QueryNeighborhood",
        "QueryPaths",
        "QueryPrecedents",
        "BuildGraphIndex",
        "GetGraph",
    ] {
        assert_eq!(
            post(&app, &format!("/{name}")).await,
            StatusCode::NOT_FOUND,
            "{name}"
        );
    }
    assert!(api.0.lock().expect("recorded operations").is_empty());
}
