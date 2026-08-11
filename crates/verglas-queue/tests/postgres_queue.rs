//! Real PostgreSQL coverage for exclusive delivery, lease expiry, and fencing.

use chrono::{Duration, Utc};
use futures::StreamExt;
use serde_json::json;
use verglas_queue::{AckRequest, PgQueue, PollRequest, QueueMessage, QueueStore, SubscribeRequest};

#[tokio::test]
#[ignore = "requires VERGLAS_TEST_POSTGRES_URL"]
async fn postgres_queue_redelivers_expired_and_fences_stale_receipts() {
    let database_url = std::env::var("VERGLAS_TEST_POSTGRES_URL")
        .expect("VERGLAS_TEST_POSTGRES_URL is required for this ignored test");
    let queue = PgQueue::connect(&database_url)
        .await
        .expect("connect queue");
    let positions = queue
        .enqueue(&[
            QueueMessage {
                id: "event-1".to_owned(),
                topic: "orders".to_owned(),
                payload: json!({"event": 1}),
            },
            QueueMessage {
                id: "event-2".to_owned(),
                topic: "orders".to_owned(),
                payload: json!({"event": 2}),
            },
        ])
        .await
        .expect("enqueue");
    assert_eq!(positions.len(), 2);

    let now = Utc::now();
    let first = queue
        .poll(&PollRequest {
            group: "workers".to_owned(),
            owner: "consumer-a".to_owned(),
            topics: vec!["orders".to_owned()],
            max: 1,
            now,
            lease_seconds: 10,
        })
        .await
        .expect("first poll");
    assert_eq!(first.len(), 1);

    let competing = queue
        .poll(&PollRequest {
            group: "workers".to_owned(),
            owner: "consumer-b".to_owned(),
            topics: vec!["orders".to_owned()],
            max: 1,
            now: now + Duration::seconds(1),
            lease_seconds: 10,
        })
        .await
        .expect("competing poll");
    assert_eq!(competing.len(), 1);
    assert_ne!(first[0].position, competing[0].position);

    let redelivered = queue
        .poll(&PollRequest {
            group: "workers".to_owned(),
            owner: "consumer-c".to_owned(),
            topics: vec!["orders".to_owned()],
            max: 1,
            now: now + Duration::seconds(11),
            lease_seconds: 10,
        })
        .await
        .expect("expired poll");
    assert_eq!(redelivered[0].position, first[0].position);
    assert!(redelivered[0].receipt.generation > first[0].receipt.generation);

    let stale = queue
        .ack(&AckRequest {
            group: "workers".to_owned(),
            receipt: first[0].receipt.clone(),
            now: now + Duration::seconds(12),
        })
        .await
        .expect_err("stale receipt must fail");
    assert!(stale.to_string().contains("stale queue receipt"));

    queue
        .ack(&AckRequest {
            group: "workers".to_owned(),
            receipt: redelivered[0].receipt.clone(),
            now: now + Duration::seconds(12),
        })
        .await
        .expect("current receipt acks");
}

#[tokio::test]
#[ignore = "requires VERGLAS_TEST_POSTGRES_URL"]
async fn postgres_subscription_wakes_for_a_committed_matching_topic() {
    let database_url = std::env::var("VERGLAS_TEST_POSTGRES_URL")
        .expect("VERGLAS_TEST_POSTGRES_URL is required for this ignored test");
    let queue = PgQueue::connect(&database_url)
        .await
        .expect("connect queue");
    let suffix = Utc::now().timestamp_nanos_opt().expect("timestamp");
    let topic = format!("table-events-{suffix}");
    let group = format!("strategy-{suffix}");
    let mut subscription = queue
        .subscribe(SubscribeRequest {
            group: group.clone(),
            owner: "subscriber-a".to_owned(),
            topics: vec![topic.clone()],
            max: 16,
            lease_seconds: 30,
        })
        .await
        .expect("subscribe");

    queue
        .enqueue(&[QueueMessage {
            id: format!("snapshot-{suffix}"),
            topic: topic.clone(),
            payload: json!({"snapshotId": "42"}),
        }])
        .await
        .expect("enqueue commit event");

    let delivery = tokio::time::timeout(std::time::Duration::from_secs(2), subscription.next())
        .await
        .expect("push deadline")
        .expect("delivery")
        .expect("valid delivery");
    assert_eq!(delivery.topic, topic);
    assert_eq!(delivery.payload, json!({"snapshotId": "42"}));
    queue
        .ack(&AckRequest {
            group,
            receipt: delivery.receipt,
            now: Utc::now(),
        })
        .await
        .expect("ack pushed delivery");
}
