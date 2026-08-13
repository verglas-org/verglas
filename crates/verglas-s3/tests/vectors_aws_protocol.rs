//! Endpoint tests for the frozen AWS S3 Vectors REST-JSON contract.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use tower::ServiceExt;
use verglas_s3::semantic::{SemanticApi, SemanticError, SemanticOperation, router};

/// Records route dispatches without acting as a semantic data store.
struct RecordingApi(Mutex<Vec<SemanticOperation>>);

#[async_trait]
impl SemanticApi for RecordingApi {
    /// Records the route and supplies a JSON response so the test exercises HTTP.
    async fn call(
        &self,
        operation: SemanticOperation,
        input: Value,
    ) -> Result<Value, SemanticError> {
        self.0
            .lock()
            .map_err(|error| SemanticError::unavailable(error.to_string()))?
            .push(operation);
        Ok(json!({"operation": format!("{operation:?}"), "input": input}))
    }
}

/// Every exact AWS URI reaches the durable-adapter boundary and never falls through to S3.
#[tokio::test]
async fn every_frozen_aws_operation_dispatches_on_its_exact_method_and_uri() {
    let api = Arc::new(RecordingApi(Mutex::new(Vec::new())));
    let app = router(api.clone());
    let operations = [
        ("POST", "/CreateIndex"),
        ("POST", "/CreateVectorBucket"),
        ("POST", "/DeleteIndex"),
        ("POST", "/DeleteVectorBucket"),
        ("POST", "/DeleteVectorBucketPolicy"),
        ("POST", "/DeleteVectors"),
        ("POST", "/GetIndex"),
        ("POST", "/GetVectorBucket"),
        ("POST", "/GetVectorBucketPolicy"),
        ("POST", "/GetVectors"),
        ("POST", "/ListIndexes"),
        ("GET", "/tags/arn%3Aexample"),
        ("POST", "/ListVectorBuckets"),
        ("POST", "/ListVectors"),
        ("POST", "/PutVectorBucketPolicy"),
        ("POST", "/PutVectors"),
        ("POST", "/QueryVectors"),
        ("POST", "/tags/arn%3Aexample"),
        ("DELETE", "/tags/arn%3Aexample"),
    ];
    for (method, uri) in operations {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(uri)
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .expect("request"),
            )
            .await
            .expect("route response");
        assert_eq!(response.status(), StatusCode::OK, "{method} {uri}");
    }
    assert_eq!(api.0.lock().expect("recording lock").len(), 19);
}

/// An unlisted URI is not silently treated as a semantic request.
#[tokio::test]
async fn unknown_semantic_uri_is_not_dispatched() {
    let app = router(Arc::new(RecordingApi(Mutex::new(Vec::new()))));
    let response = app
        .oneshot(
            Request::post("/QueryVector")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("route response");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}
