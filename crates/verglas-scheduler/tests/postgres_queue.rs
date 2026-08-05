//! Postgres integration coverage for durable queue fencing and idempotency.

use chrono::{Duration, Utc};
use verglas_scheduler::{
    ClaimRequest, CompleteRequest, Completion, Invocation, PgQueue, RunQueue, TriggerSource,
};
use verglas_sdk::worker::TriggerEvent;

/// Exercises the real SQL schema and transactional claim path.
#[tokio::test]
#[ignore = "requires VERGLAS_TEST_POSTGRES_URL"]
async fn postgres_queue_lifecycle() {
    let database_url = std::env::var("VERGLAS_TEST_POSTGRES_URL")
        .expect("VERGLAS_TEST_POSTGRES_URL is required for this ignored test");
    let queue_name = format!(
        "test-{}",
        Utc::now().timestamp_nanos_opt().expect("timestamp")
    );
    let queue = PgQueue::connect(&database_url, queue_name)
        .await
        .expect("connect queue");
    let now = Utc::now();
    let invocation = Invocation::new(
        "worker-a",
        TriggerSource::Manual {
            request_id: "request-1".to_owned(),
        },
        TriggerEvent::Manual,
        now,
    );

    let first = queue.enqueue(&invocation).await.expect("first enqueue");
    let duplicate = queue.enqueue(&invocation).await.expect("duplicate enqueue");
    assert_eq!(first.job_id(), duplicate.job_id());
    assert_eq!(queue.jobs().await.expect("jobs").len(), 1);

    let claimed = queue
        .claim(&ClaimRequest {
            owner: "consumer-a".to_owned(),
            now,
            lease_seconds: 60,
        })
        .await
        .expect("claim")
        .expect("ready job");
    assert!(
        queue
            .claim(&ClaimRequest {
                owner: "consumer-b".to_owned(),
                now: now + Duration::seconds(1),
                lease_seconds: 60,
            })
            .await
            .expect("competing claim")
            .is_none()
    );

    queue
        .complete(&CompleteRequest {
            lease: claimed.lease,
            completion: Completion::Succeeded { rows_produced: 3 },
            now: now + Duration::seconds(2),
        })
        .await
        .expect("complete");
    let attempts = queue.attempts(first.job_id()).await.expect("attempts");
    assert_eq!(attempts.len(), 1);
    assert_eq!(
        attempts[0].completion,
        Some(Completion::Succeeded { rows_produced: 3 })
    );
}
