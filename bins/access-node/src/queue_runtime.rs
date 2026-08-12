//! Provisions a dedicated queue container after its queue-owned Neon database.

use async_trait::async_trait;
use axum::body::Bytes;
use hmac::{Hmac, Mac};
use reqwest::Url;
use sha2::Sha256;
use verglas_container_runtime::ContainerSpec;
use verglas_queue::{CreateQueueRequest, QueuePlacement, QueuePlan, QueueProvisioner, QueueView};
use verglas_rest::queue::{QueueProxy, QueueProxyResponse};

use crate::postgres_runtime::ManagedPostgresProvisioner;

pub(crate) const QUEUE_IMAGE: &str = "verglas/verglas-queue-service:local";
const QUEUE_PORT: u16 = 8_370;
const READY_ATTEMPTS: usize = 90;

/// Configuration retained by the queue lifecycle provisioner.
pub(crate) struct QueueRuntimeConfig {
    /// Authenticated desired-container service origin.
    pub(crate) runtime_endpoint: String,
    /// Bearer accepted by the desired-container service.
    pub(crate) runtime_token: String,
    /// Key deriving restart-stable per-queue private credentials.
    pub(crate) credential_key: Vec<u8>,
}

/// Reconciles queue-owned Neon and service containers in dependency order.
#[derive(Clone)]
pub(crate) struct ManagedQueueProvisioner {
    postgres: ManagedPostgresProvisioner,
    runtime_endpoint: Url,
    runtime_token: String,
    credential_key: Vec<u8>,
    http: reqwest::Client,
}

#[async_trait]
impl QueueProxy for ManagedQueueProvisioner {
    async fn request(
        &self,
        queue: &QueueView,
        operation: &str,
        body: Bytes,
    ) -> Result<QueueProxyResponse, String> {
        if !matches!(operation, "enqueue" | "poll" | "subscribe" | "ack") {
            return Err("unsupported queue operation".to_owned());
        }
        let plan = CreateQueueRequest {
            name: queue.name.clone(),
        }
        .plan(self.postgres.tenant_id())
        .map_err(|error| error.to_string())?;
        if plan.container_deployment_id() != queue.container_deployment_id {
            return Err("queue placement does not match its declaration".to_owned());
        }
        let endpoint = Url::parse(&format!(
            "http://verglas-{}:{QUEUE_PORT}/v1/{operation}",
            queue.container_deployment_id
        ))
        .map_err(|error| format!("invalid queue upstream URL: {error}"))?;
        let response = self
            .http
            .post(endpoint)
            .bearer_auth(self.token(&plan)?)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .map_err(|error| format!("queue upstream failed: {error}"))?;
        let status = response.status().as_u16();
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("application/octet-stream")
            .to_owned();
        let body = axum::body::Body::from_stream(response.bytes_stream());
        Ok(QueueProxyResponse {
            status,
            content_type,
            body,
        })
    }
}

impl ManagedQueueProvisioner {
    /// Validates queue runtime configuration before accepting mutations.
    pub(crate) fn new(
        postgres: ManagedPostgresProvisioner,
        config: QueueRuntimeConfig,
    ) -> Result<Self, String> {
        let mut runtime_endpoint = Url::parse(&config.runtime_endpoint)
            .map_err(|error| format!("invalid queue container runtime URL: {error}"))?;
        if !matches!(runtime_endpoint.scheme(), "http" | "https") {
            return Err("queue container runtime URL must use http or https".to_owned());
        }
        if !runtime_endpoint.path().ends_with('/') {
            runtime_endpoint.set_path(&format!("{}/", runtime_endpoint.path()));
        }
        if config.runtime_token.is_empty() || config.credential_key.len() != 32 {
            return Err("queue runtime token and 256-bit credential key are required".to_owned());
        }
        Ok(Self {
            postgres,
            runtime_endpoint,
            runtime_token: config.runtime_token,
            credential_key: config.credential_key,
            http: reqwest::Client::new(),
        })
    }

    /// Stores and starts one queue service declaration through the container runtime.
    async fn put_container(&self, spec: &ContainerSpec) -> Result<(), String> {
        let endpoint = self
            .runtime_endpoint
            .join(&format!("v1/containers/{}", spec.deployment_id))
            .map_err(|error| format!("invalid queue container URL: {error}"))?;
        let response = self
            .http
            .put(endpoint)
            .bearer_auth(&self.runtime_token)
            .json(spec)
            .send()
            .await
            .map_err(|error| format!("queue container request failed: {error}"))?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(format!(
                "container runtime rejected {} with HTTP {}",
                spec.deployment_id,
                response.status()
            ))
        }
    }

    /// Removes one queue service declaration from the container runtime.
    async fn delete_container(&self, deployment_id: &str) -> Result<(), String> {
        let endpoint = self
            .runtime_endpoint
            .join(&format!("v1/containers/{deployment_id}"))
            .map_err(|error| format!("invalid queue container URL: {error}"))?;
        let response = self
            .http
            .delete(endpoint)
            .bearer_auth(&self.runtime_token)
            .send()
            .await
            .map_err(|error| format!("queue container delete failed: {error}"))?;
        if response.status().is_success() || response.status() == reqwest::StatusCode::NOT_FOUND {
            Ok(())
        } else {
            Err(format!(
                "container runtime rejected deletion of {deployment_id} with HTTP {}",
                response.status()
            ))
        }
    }

    /// Waits until the independently started queue container serves its health endpoint.
    async fn wait_for_queue(&self, deployment_id: &str) -> Result<(), String> {
        let endpoint = Url::parse(&format!(
            "http://verglas-{deployment_id}:{QUEUE_PORT}/healthz"
        ))
        .map_err(|error| format!("invalid queue health URL: {error}"))?;
        for attempt in 0..READY_ATTEMPTS {
            match self.http.get(endpoint.clone()).send().await {
                Ok(response) if response.status().is_success() => return Ok(()),
                _ if attempt + 1 < READY_ATTEMPTS => {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
                _ => {}
            }
        }
        Err(format!(
            "queue container {deployment_id} did not become healthy"
        ))
    }

    /// Derives the queue-private bearer without persisting plaintext in a resource record.
    fn token(&self, plan: &QueuePlan) -> Result<String, String> {
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.credential_key)
            .map_err(|error| format!("queue credential key failed: {error}"))?;
        mac.update(b"verglas-queue-token\0");
        mac.update(plan.tenant_id().as_bytes());
        mac.update(&[0]);
        mac.update(plan.name().as_bytes());
        Ok(hex::encode(mac.finalize().into_bytes()))
    }
}

#[async_trait]
impl QueueProvisioner for ManagedQueueProvisioner {
    async fn ensure(&self, plan: &QueuePlan) -> Result<QueuePlacement, String> {
        self.postgres
            .ensure_private(plan.database_name())
            .await
            .map_err(|error| error.to_string())?;
        let database_url = self
            .postgres
            .private_database_url(plan.database_name())
            .map_err(|error| error.to_string())?;
        let spec = queue_container_spec(
            plan.container_deployment_id(),
            &database_url,
            &self.token(plan)?,
        );
        let container = match self.put_container(&spec).await {
            Ok(()) => self.wait_for_queue(plan.container_deployment_id()).await,
            Err(error) => Err(error),
        };
        if let Err(error) = container {
            self.postgres
                .delete(plan.database_name())
                .await
                .map_err(|rollback| format!("{error}; Neon rollback failed: {rollback}"))?;
            return Err(error);
        }
        Ok(QueuePlacement::new(
            plan.database_name(),
            plan.database_deployment_id(),
            plan.container_deployment_id(),
        ))
    }

    async fn delete(&self, placement: &QueuePlacement) -> Result<(), String> {
        self.delete_container(&placement.container_deployment_id)
            .await?;
        self.postgres
            .delete(&placement.database_name)
            .await
            .map_err(|error| error.to_string())
    }
}

/// Builds the immutable standalone queue declaration served on the private network.
fn queue_container_spec(deployment_id: &str, database_url: &str, token: &str) -> ContainerSpec {
    ContainerSpec::new(deployment_id, QUEUE_IMAGE)
        .with_environment("VERGLAS_QUEUE_DATABASE_URL", database_url)
        .with_environment("VERGLAS_QUEUE_TOKEN", token)
        .with_environment("VERGLAS_QUEUE_LISTEN", format!("0.0.0.0:{QUEUE_PORT}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_container_plan_is_independent_and_uses_only_neon() {
        let spec = queue_container_spec(
            "queue-events-service",
            "postgres://queue@neon/queue-events",
            "secret",
        );

        assert_eq!(spec.deployment_id, "queue-events-service");
        assert_eq!(spec.image, QUEUE_IMAGE);
        assert_eq!(
            spec.environment.get("VERGLAS_QUEUE_DATABASE_URL"),
            Some(&"postgres://queue@neon/queue-events".to_owned())
        );
        assert_eq!(
            spec.environment.get("VERGLAS_QUEUE_TOKEN"),
            Some(&"secret".to_owned())
        );
        assert!(
            spec.environment
                .values()
                .all(|value| !value.contains("postgres:17"))
        );
    }
}
