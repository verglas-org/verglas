//! Queue declarations and their two managed deployments have one transactional lifecycle.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use verglas_queue::{
    CreateQueueRequest, MemoryQueueRepository, QueueManager, QueuePlacement, QueuePlan,
    QueueProvisioner, QueueService,
};

#[derive(Default)]
struct FakeProvisioner {
    events: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl QueueProvisioner for FakeProvisioner {
    async fn ensure(&self, plan: &QueuePlan) -> Result<QueuePlacement, String> {
        self.events
            .lock()
            .expect("events")
            .push(format!("ensure:{}", plan.name()));
        Ok(QueuePlacement::new(
            plan.database_name(),
            plan.database_deployment_id(),
            plan.container_deployment_id(),
        ))
    }

    async fn delete(&self, placement: &QueuePlacement) -> Result<(), String> {
        self.events
            .lock()
            .expect("events")
            .push(format!("delete:{}", placement.container_deployment_id()));
        Ok(())
    }
}

#[tokio::test]
async fn create_and_delete_cover_both_dedicated_deployments() {
    let repository = MemoryQueueRepository::default();
    let events = Arc::new(Mutex::new(Vec::new()));
    let service = QueueService::new(
        repository,
        FakeProvisioner {
            events: events.clone(),
        },
    );
    let plan = CreateQueueRequest {
        name: "events".to_owned(),
    }
    .plan("tenant-a")
    .expect("plan");

    let queue = service.create_queue(plan).await.expect("create");
    assert_eq!(queue.name, "events");
    assert_eq!(queue.database_deployment_id, "queue-events-postgres");
    assert_eq!(queue.container_deployment_id, "queue-events-service");

    service
        .delete_queue("tenant-a", "events")
        .await
        .expect("delete");
    assert!(
        service
            .list_queues("tenant-a")
            .await
            .expect("list")
            .is_empty()
    );
    assert_eq!(
        *events.lock().expect("events"),
        vec!["ensure:events", "delete:queue-events-service"]
    );
}

#[tokio::test]
async fn recovery_reconciles_every_explicit_queue_deployment() {
    let repository = MemoryQueueRepository::default();
    let events = Arc::new(Mutex::new(Vec::new()));
    let service = QueueService::new(
        repository,
        FakeProvisioner {
            events: events.clone(),
        },
    );
    service
        .create_queue(
            CreateQueueRequest {
                name: "events".to_owned(),
            }
            .plan("tenant-a")
            .expect("plan"),
        )
        .await
        .expect("create");
    events.lock().expect("events").clear();

    assert!(
        service
            .recover("tenant-a")
            .await
            .expect("recover")
            .is_empty()
    );
    assert_eq!(*events.lock().expect("events"), vec!["ensure:events"]);
}
