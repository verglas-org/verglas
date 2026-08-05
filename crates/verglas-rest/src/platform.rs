//! Trigger ingress from the on-prem REST and catalog surfaces into scheduling.
//!
//! This module never schedules or executes workers. It resolves the current
//! deployment record, validates the trigger, and pushes one complete worker
//! event to the standalone scheduler. Verglas persists queue state through the
//! scheduler REST routes; the standalone service owns execution.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::Deserialize;
use verglas_platform::{SystemCatalog, SystemState, WorkerRow};
use verglas_scheduler::{EnqueueOutcome, Invocation, TriggerSource};
use verglas_sdk::worker::{ChangeEvent, HttpCallback, TriggerEvent, TriggerSpec};

/// An ingress request could not resolve or enqueue a runnable deployment.
#[derive(Debug, thiserror::Error)]
pub enum IngressError {
    /// The platform registry could not be read.
    #[error("worker registry: {0}")]
    Registry(#[from] verglas_platform::PlatformError),
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
    sys: Arc<SystemCatalog>,
    scheduler_url: String,
    http: reqwest::Client,
}

impl SchedulerIngress {
    /// Couples the current deployment registry to a pushed-event scheduler.
    pub fn new(sys: Arc<SystemCatalog>, scheduler_url: String) -> SchedulerIngress {
        SchedulerIngress {
            sys,
            scheduler_url: scheduler_url.trim_end_matches('/').to_owned(),
            http: reqwest::Client::new(),
        }
    }

    /// Enqueues a caller-requested run after validating the worker is running.
    pub async fn manual(
        &self,
        name: &str,
        request_id: String,
    ) -> Result<EnqueueOutcome, IngressError> {
        self.running_worker(name).await?;
        self.enqueue(&Invocation::new(
            name,
            TriggerSource::Manual { request_id },
            TriggerEvent::Manual,
            Utc::now(),
        ))
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
        if !parse_triggers(&worker)?
            .iter()
            .any(|trigger| matches!(trigger, TriggerSpec::Webhook { .. }))
        {
            return Err(IngressError::Invalid(format!(
                "worker {name} has no webhook trigger"
            )));
        }
        self.enqueue(&Invocation::new(
            name,
            TriggerSource::Http {
                request_id,
                path: request.path.clone(),
            },
            TriggerEvent::Webhook { request },
            Utc::now(),
        ))
        .await
    }

    /// Resolves a dynamically configured HTTP path and enqueues its worker.
    pub async fn dynamic_http(
        &self,
        route_path: &str,
        request_id: String,
        request: HttpCallback,
    ) -> Result<EnqueueOutcome, IngressError> {
        let workers = self.sys.list_active_workers().await?;
        for worker in workers {
            if worker.state != SystemState::Running {
                continue;
            }
            let matches = parse_triggers(&worker)?.iter().any(|trigger| {
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

    /// Fans one durable catalog update into matching data-change job objects.
    pub async fn data_update(
        &self,
        table: &str,
        snapshot_id: String,
        sequence: u64,
        committed_at: DateTime<Utc>,
    ) -> Result<Vec<EnqueueOutcome>, IngressError> {
        let workers = self.sys.list_active_workers().await?;
        let mut outcomes = Vec::new();
        for worker in workers {
            if worker.state != SystemState::Running
                || !data_change_tables(&worker)?
                    .iter()
                    .any(|name| name == table)
            {
                continue;
            }
            outcomes.push(
                self.enqueue(&Invocation::new(
                    &worker.name,
                    TriggerSource::DataUpdate {
                        subscription_id: format!("{}:{table}", worker.name),
                        sequence,
                    },
                    TriggerEvent::DataChange {
                        change: ChangeEvent {
                            seq: sequence,
                            table: table.to_owned(),
                            snapshot_id: snapshot_id.clone(),
                            committed_at: committed_at.to_rfc3339(),
                        },
                    },
                    committed_at,
                ))
                .await?,
            );
        }
        Ok(outcomes)
    }

    /// Reads one running worker or reports why it cannot be dispatched.
    async fn running_worker(&self, name: &str) -> Result<WorkerRow, IngressError> {
        let worker = self
            .sys
            .get_worker(name)
            .await?
            .ok_or_else(|| IngressError::Invalid(format!("no worker named {name}")))?;
        if worker.state != SystemState::Running {
            return Err(IngressError::Invalid(format!(
                "worker {name} is {}, not running",
                worker.state.as_str()
            )));
        }
        Ok(worker)
    }

    /// Pushes one complete event to the scheduler, which persists before replying.
    async fn enqueue(&self, invocation: &Invocation) -> Result<EnqueueOutcome, IngressError> {
        let response: EventResponse = self
            .http
            .post(format!("{}/v1/events", self.scheduler_url))
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
}

/// Decodes a worker's trigger declarations without silently disabling them.
pub(crate) fn parse_triggers(worker: &WorkerRow) -> Result<Vec<TriggerSpec>, IngressError> {
    serde_json::from_str(&worker.triggers).map_err(|source| IngressError::Triggers {
        worker: worker.name.clone(),
        source,
    })
}

/// Returns every table subscribed by `data_change` triggers.
fn data_change_tables(worker: &WorkerRow) -> Result<Vec<String>, IngressError> {
    Ok(parse_triggers(worker)?
        .into_iter()
        .filter_map(|trigger| match trigger {
            TriggerSpec::DataChange { table } => Some(table.tables()),
            _ => None,
        })
        .flatten()
        .collect())
}
