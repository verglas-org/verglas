//! Queue HTTP surface delegates only to the provisioned PostgreSQL store.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;
use verglas_queue::{AckRequest, Delivery, PollRequest, QueueError, QueueStore};
use verglas_queue_service::router;

#[derive(Default)]
struct FakeQueue {
    enqueued: Mutex<Vec<Value>>,
}

#[async_trait]
impl QueueStore for FakeQueue {
    async fn enqueue(&self, payloads: &[Value]) -> Result<Vec<i64>, QueueError> {
        self.enqueued
            .lock()
            .expect("enqueue lock")
            .extend_from_slice(payloads);
        Ok((1..=payloads.len() as i64).collect())
    }

    async fn poll(&self, _request: &PollRequest) -> Result<Vec<Delivery>, QueueError> {
        Ok(Vec::new())
    }

    async fn ack(&self, _request: &AckRequest) -> Result<(), QueueError> {
        Ok(())
    }
}

#[tokio::test]
async fn enqueue_requires_private_token_and_returns_positions() {
    let store = Arc::new(FakeQueue::default());
    let app = router(store.clone(), "queue-secret".to_owned());

    let unauthorized = app
        .clone()
        .oneshot(
            Request::post("/v1/enqueue")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"messages":[{"id":1}]}"#))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

    let response = app
        .oneshot(
            Request::post("/v1/enqueue")
                .header("authorization", "Bearer queue-secret")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"messages":[{"id":1}]}"#))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    assert_eq!(
        serde_json::from_slice::<Value>(&body).expect("json"),
        json!({"positions":[1]})
    );
    assert_eq!(store.enqueued.lock().expect("enqueue lock").len(), 1);
}
