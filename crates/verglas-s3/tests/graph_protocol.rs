//! Endpoint tests for the Verglas Graph REST-JSON contract.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use tower::ServiceExt;
use verglas_s3::semantic::{SemanticApi, SemanticError, SemanticOperation, router};

/// Captures graph requests at the engine boundary.
struct RecordingGraphApi(Mutex<Vec<SemanticOperation>>);

#[async_trait]
impl SemanticApi for RecordingGraphApi {
    /// Rejects accidental S3-vector dispatches and records only Graph operations.
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
        Ok(json!({"ok": true}))
    }
}

/// The public Graph verbs all dispatch through one REST-JSON adapter.
#[tokio::test]
async fn graph_contract_operations_reach_the_engine_adapter() {
    let api = Arc::new(RecordingGraphApi(Mutex::new(Vec::new())));
    let app = router(api.clone());
    for operation in [
        "CreateGraph",
        "DeleteGraph",
        "GetGraph",
        "ListGraphs",
        "PutNodes",
        "PutEdges",
        "GetNeighbors",
        "QueryKHop",
        "QueryNeighborhood",
        "QueryPaths",
        "BuildGraphIndex",
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::post(format!("/{operation}"))
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .expect("request"),
            )
            .await
            .expect("route response");
        assert_eq!(response.status(), StatusCode::OK, "{operation}");
    }
    assert_eq!(api.0.lock().expect("recording lock").len(), 11);
}
