//! Queue resource routes create explicit independently provisioned deployments.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;
use verglas_queue::{QueueManager, QueuePlan, QueueServiceError, QueueView};
use verglas_rest::data_plane::AuthenticatedPrincipal;
use verglas_rest::queue::{
    QueueAuthorization, QueueAuthorizationError, QueueProxy, QueueProxyResponse,
};

struct PanicProxy;

#[async_trait]
impl QueueProxy for PanicProxy {
    async fn request(
        &self,
        _queue: &QueueView,
        _operation: &str,
        _body: axum::body::Bytes,
    ) -> Result<QueueProxyResponse, String> {
        panic!("unknown queues must not reach a container")
    }
}

#[derive(Default)]
struct FakeManager {
    plans: Mutex<Vec<String>>,
}

#[derive(Default)]
struct FakeAuthorization {
    created: Mutex<Vec<String>>,
    deleted: Mutex<Vec<String>>,
}

#[async_trait]
impl QueueAuthorization for FakeAuthorization {
    async fn create_queue_resource(
        &self,
        principal: &AuthenticatedPrincipal,
        queue: &str,
    ) -> Result<(), QueueAuthorizationError> {
        self.created
            .lock()
            .expect("created")
            .push(format!("{}/{}", principal.principal_id, queue));
        Ok(())
    }

    async fn delete_queue_resource(
        &self,
        principal: &AuthenticatedPrincipal,
        queue: &str,
    ) -> Result<(), QueueAuthorizationError> {
        self.deleted
            .lock()
            .expect("deleted")
            .push(format!("{}/{}", principal.principal_id, queue));
        Ok(())
    }
}

#[async_trait]
impl QueueManager for FakeManager {
    async fn create_queue(&self, plan: QueuePlan) -> Result<QueueView, QueueServiceError> {
        self.plans
            .lock()
            .expect("plans")
            .push(format!("{}/{}", plan.tenant_id(), plan.name()));
        Ok(QueueView {
            name: plan.name().to_owned(),
            database_deployment_id: plan.database_deployment_id().to_owned(),
            container_deployment_id: plan.container_deployment_id().to_owned(),
        })
    }

    async fn list_queues(&self, _tenant_id: &str) -> Result<Vec<QueueView>, QueueServiceError> {
        Ok(Vec::new())
    }

    async fn get_queue(&self, tenant_id: &str, name: &str) -> Result<QueueView, QueueServiceError> {
        Err(QueueServiceError::NotFound {
            tenant_id: tenant_id.to_owned(),
            name: name.to_owned(),
        })
    }

    async fn delete_queue(&self, tenant_id: &str, name: &str) -> Result<(), QueueServiceError> {
        Err(QueueServiceError::NotFound {
            tenant_id: tenant_id.to_owned(),
            name: name.to_owned(),
        })
    }
}

fn principal() -> AuthenticatedPrincipal {
    AuthenticatedPrincipal {
        tenant_id: "tenant-a".to_owned(),
        principal_id: "user/owner@example.com".to_owned(),
        token_id: "token-a".to_owned(),
        audience: "verglas-cli".to_owned(),
    }
}

#[tokio::test]
async fn create_queue_injects_tenant_and_returns_both_deployments() {
    let manager = Arc::new(FakeManager::default());
    let authorization = Arc::new(FakeAuthorization::default());
    let app = verglas_rest::queue::router(
        manager.clone(),
        authorization.clone(),
        "tenant-a".to_owned(),
    )
    .layer(axum::Extension(principal()));
    let response = app
        .oneshot(
            Request::post("/v1/queues")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"name":"events"}"#))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let queue: serde_json::Value = serde_json::from_slice(&body).expect("queue");
    assert_eq!(queue["databaseDeploymentId"], "queue-events-postgres");
    assert_eq!(queue["containerDeploymentId"], "queue-events-service");
    assert_eq!(
        manager.plans.lock().expect("plans").as_slice(),
        ["tenant-a/events"]
    );
    assert_eq!(
        authorization.created.lock().expect("created").as_slice(),
        ["user/owner@example.com/events"]
    );
}

#[tokio::test]
async fn unknown_queue_data_operation_fails_without_implicit_creation() {
    let manager = Arc::new(FakeManager::default());
    let app =
        verglas_rest::queue::data_router(manager, Arc::new(PanicProxy), "tenant-a".to_owned());
    let response = app
        .oneshot(
            Request::post("/v1/queues/missing/enqueue")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"messages":[1]}"#))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}
