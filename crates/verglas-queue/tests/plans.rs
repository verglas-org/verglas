//! Queue resource plans must always own distinct storage and serving deployments.

use verglas_queue::CreateQueueRequest;

#[test]
fn queue_plan_owns_neon_and_container_deployments() {
    let plan = CreateQueueRequest {
        name: "events".to_owned(),
    }
    .plan("tenant-a")
    .expect("valid queue plan");

    assert_eq!(plan.name(), "events");
    assert_eq!(plan.database_name(), "queue-events");
    assert_eq!(plan.database_deployment_id(), "queue-events-postgres");
    assert_eq!(plan.container_deployment_id(), "queue-events-service");
    assert_ne!(
        plan.database_deployment_id(),
        plan.container_deployment_id()
    );
}

#[test]
fn invalid_queue_names_fail_before_provisioning() {
    let error = CreateQueueRequest {
        name: "Not A Queue".to_owned(),
    }
    .plan("tenant-a")
    .expect_err("invalid queue name");

    assert_eq!(error.to_string(), "invalid queue name: Not A Queue");
}
