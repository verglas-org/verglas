//! Trigger ingress from the on-prem REST and catalog surfaces into scheduling.
//!
//! This module never schedules or executes workers. It resolves the current
//! deployment record, validates the trigger, and pushes one complete worker
//! event to the standalone scheduler. Verglas persists queue state through the
//! scheduler REST routes; the standalone service owns execution.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::Deserialize;
use verglas_scheduler::{EnqueueOutcome, Invocation, WorkerRecord, WorkerSpec};
use verglas_sdk::worker::{CloudEvent, HttpCallback, TriggerSpec};

/// CloudEvent type used for caller-requested immediate runs.
const MANUAL_EVENT_TYPE: &str = "org.verglas.worker.manual";
/// CloudEvent type used for complete accepted HTTP callbacks.
const HTTP_EVENT_TYPE: &str = "org.verglas.http.request";

/// An ingress request could not resolve or enqueue a runnable deployment.
#[derive(Debug, thiserror::Error)]
pub enum IngressError {
    /// The named worker does not exist or cannot accept this trigger.
    #[error("{0}")]
    Invalid(String),
    /// A worker's trigger JSON is malformed.
    #[error("worker {worker} triggers are invalid: {source}")]
    Triggers {
        /// Worker whose deployment record could not be decoded.
        worker: String,
        /// JSON decoding failure.
        source: serde_json::Error,
    },
    /// The scheduler event service could not accept the invocation.
    #[error("scheduler event service: {0}")]
    Transport(#[from] reqwest::Error),
}

/// The scheduler's durable enqueue acknowledgement.
#[derive(Deserialize)]
struct EventResponse {
    /// Deterministic run identity.
    job_id: String,
    /// Whether this request created the run.
    created: bool,
}

/// On-prem ingress backed by the deployment registry and scheduler event API.
pub struct SchedulerIngress {
    scheduler_url: String,
    control_token: Arc<str>,
    http: reqwest::Client,
}

impl SchedulerIngress {
    /// Couples the current deployment registry to a pushed-event scheduler.
    pub fn new(scheduler_url: String, control_token: String) -> SchedulerIngress {
        SchedulerIngress {
            scheduler_url: scheduler_url.trim_end_matches('/').to_owned(),
            control_token: Arc::from(control_token),
            http: reqwest::Client::new(),
        }
    }

    /// Lists durable scheduler-owned worker declarations.
    pub async fn list_workers(&self, include_all: bool) -> Result<Vec<WorkerRecord>, IngressError> {
        let view = if include_all { "all" } else { "active" };
        Ok(self
            .authorized(
                self.http
                    .get(format!("{}/v1/workers?view={view}", self.scheduler_url)),
            )
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    /// Registers one durable scheduler-owned worker declaration.
    pub async fn register_worker(&self, spec: WorkerSpec) -> Result<WorkerRecord, IngressError> {
        Ok(self
            .authorized(self.http.post(format!("{}/v1/workers", self.scheduler_url)))
            .json(&spec)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    /// Returns one durable worker declaration when present.
    pub async fn get_worker(&self, name: &str) -> Result<Option<WorkerRecord>, IngressError> {
        let response = self
            .authorized(
                self.http
                    .get(format!("{}/v1/workers/{name}", self.scheduler_url)),
            )
            .send()
            .await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        Ok(Some(response.error_for_status()?.json().await?))
    }

    /// Applies one scheduler-owned worker lifecycle transition.
    pub async fn set_worker_state(
        &self,
        name: &str,
        state: &str,
    ) -> Result<WorkerRecord, IngressError> {
        Ok(self
            .authorized(
                self.http
                    .put(format!("{}/v1/workers/{name}/state", self.scheduler_url)),
            )
            .json(&serde_json::json!({"state": state}))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    /// Enqueues a caller-requested run after validating the worker is running.
    pub async fn manual(
        &self,
        name: &str,
        request_id: String,
    ) -> Result<EnqueueOutcome, IngressError> {
        self.running_worker(name).await?;
        let event = CloudEvent::new(request_id, "urn:verglas:rest", MANUAL_EVENT_TYPE);
        self.enqueue(&Invocation::new(name, event, Utc::now()))
            .await
    }

    /// Enqueues a request routed to one named webhook deployment.
    pub async fn webhook(
        &self,
        name: &str,
        request_id: String,
        request: HttpCallback,
    ) -> Result<EnqueueOutcome, IngressError> {
        let worker = self.running_worker(name).await?;
        if !parse_triggers(&worker.name, &worker.triggers)?
            .iter()
            .any(|trigger| matches!(trigger, TriggerSpec::Webhook { .. }))
        {
            return Err(IngressError::Invalid(format!(
                "worker {name} has no webhook trigger"
            )));
        }
        let mut event = CloudEvent::new(request_id, "urn:verglas:http", HTTP_EVENT_TYPE);
        event.subject = Some(request.path.clone());
        event.datacontenttype = Some("application/json".to_owned());
        event.data = Some(serde_json::to_value(request).map_err(|error| {
            IngressError::Invalid(format!("HTTP callback is not serializable: {error}"))
        })?);
        self.enqueue(&Invocation::new(name, event, Utc::now()))
            .await
    }

    /// Resolves a dynamically configured HTTP path and enqueues its worker.
    pub async fn dynamic_http(
        &self,
        route_path: &str,
        request_id: String,
        request: HttpCallback,
    ) -> Result<EnqueueOutcome, IngressError> {
        let workers = self.list_workers(false).await?;
        for worker in workers {
            if worker.state != "running" {
                continue;
            }
            let matches = parse_triggers(&worker.name, &worker.triggers)?.iter().any(|trigger| {
                matches!(trigger, TriggerSpec::Webhook { path: Some(configured) } if configured == route_path)
            });
            if matches {
                return self.webhook(&worker.name, request_id, request).await;
            }
        }
        Err(IngressError::Invalid(format!(
            "no running worker owns HTTP path `{route_path}`"
        )))
    }

    /// Fans one CloudEvent into every running worker whose event filter matches.
    pub async fn event(
        &self,
        event: CloudEvent,
        ready_at: DateTime<Utc>,
    ) -> Result<Vec<EnqueueOutcome>, IngressError> {
        event
            .validate()
            .map_err(|error| IngressError::Invalid(format!("invalid CloudEvent: {error}")))?;
        let workers = self.list_workers(false).await?;
        let mut outcomes = Vec::new();
        for worker in workers {
            if worker.state != "running"
                || !parse_triggers(&worker.name, &worker.triggers)?
                    .iter()
                    .any(|trigger| trigger.matches(&event))
            {
                continue;
            }
            outcomes.push(
                self.enqueue(&Invocation::new(&worker.name, event.clone(), ready_at))
                    .await?,
            );
        }
        Ok(outcomes)
    }

    /// Reads one running worker or reports why it cannot be dispatched.
    async fn running_worker(&self, name: &str) -> Result<WorkerRecord, IngressError> {
        let worker = self
            .get_worker(name)
            .await?
            .ok_or_else(|| IngressError::Invalid(format!("no worker named {name}")))?;
        if worker.state != "running" {
            return Err(IngressError::Invalid(format!(
                "worker {name} is {}, not running",
                worker.state
            )));
        }
        Ok(worker)
    }

    /// Pushes one complete event to the scheduler, which persists before replying.
    async fn enqueue(&self, invocation: &Invocation) -> Result<EnqueueOutcome, IngressError> {
        let response: EventResponse = self
            .authorized(self.http.post(format!("{}/v1/events", self.scheduler_url)))
            .json(invocation)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(if response.created {
            EnqueueOutcome::Created(response.job_id)
        } else {
            EnqueueOutcome::Existing(response.job_id)
        })
    }

    /// Adds the private scheduler control credential without exposing it to callers.
    fn authorized(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        request.bearer_auth(self.control_token.as_ref())
    }
}

/// Decodes a worker's trigger declarations without silently disabling them.
pub(crate) fn parse_triggers(
    worker: &str,
    triggers: &str,
) -> Result<Vec<TriggerSpec>, IngressError> {
    serde_json::from_str(triggers).map_err(|source| IngressError::Triggers {
        worker: worker.to_owned(),
        source,
    })
}
