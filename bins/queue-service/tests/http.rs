//! Queue HTTP surface delegates only to the provisioned PostgreSQL store.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use futures::{StreamExt, stream};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;
use verglas_queue::{
    AckRequest, Delivery, DeliveryStream, PollRequest, QueueError, QueueMessage, QueueStore,
    SubscribeRequest,
};
use verglas_queue_service::router;

#[derive(Default)]
struct FakeQueue {
    enqueued: Mutex<Vec<Value>>,
}

#[async_trait]
impl QueueStore for FakeQueue {
    async fn enqueue(&self, messages: &[QueueMessage]) -> Result<Vec<i64>, QueueError> {
        self.enqueued
            .lock()
            .expect("enqueue lock")
            .extend(messages.iter().map(|message| message.payload.clone()));
        Ok((1..=messages.len() as i64).collect())
    }

    async fn subscribe(&self, request: SubscribeRequest) -> Result<DeliveryStream, QueueError> {
        assert_eq!(request.group, "strategy-a");
        assert_eq!(request.owner, "process-1");
        assert_eq!(
            request.topics,
            vec!["database/trading/table/rlean.custom_points"]
        );
        Ok(Box::pin(stream::iter([Ok(Delivery {
            position: 7,
            topic: request.topics[0].clone(),
            payload: json!({"snapshotId":"42"}),
            receipt: verglas_queue::Receipt {
                position: 7,
                owner: request.owner,
                generation: 1,
            },
            expires_at: chrono::Utc::now() + chrono::Duration::seconds(30),
        })])))
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
                .body(Body::from(r#"{"messages":[{"id":"commit-1","topic":"database/trading/table/rlean.custom_points","payload":{"id":1}}]}"#))
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
                .body(Body::from(r#"{"messages":[{"id":"commit-1","topic":"database/trading/table/rlean.custom_points","payload":{"id":1}}]}"#))
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

#[tokio::test]
async fn subscribe_pushes_matching_topic_as_ndjson_without_polling() {
    let app = router(Arc::new(FakeQueue::default()), "queue-secret".to_owned());
    let response = app
        .oneshot(
            Request::post("/v1/subscribe")
                .header("authorization", "Bearer queue-secret")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"group":"strategy-a","owner":"process-1","topics":["database/trading/table/rlean.custom_points"],"leaseSeconds":30}"#,
                ))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body().into_data_stream();
    let frame = body
        .next()
        .await
        .expect("pushed frame")
        .expect("body bytes");
    let delivery: Delivery = serde_json::from_slice(&frame).expect("delivery frame");
    assert_eq!(delivery.position, 7);
    assert_eq!(delivery.topic, "database/trading/table/rlean.custom_points");
}
