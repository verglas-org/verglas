//! Postgres-backed job, lease, retry, and completion state for one tenant queue.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::postgres::{PgPool, PgPoolOptions, PgRow};
use sqlx::{Row, Transaction};
use verglas_sdk::worker::CloudEvent;

/// A scheduler storage, encoding, or input error.
#[derive(Debug, thiserror::Error)]
pub enum SchedulerError {
    /// Postgres rejected an operation.
    #[error("scheduler postgres: {0}")]
    Database(#[from] sqlx::Error),
    /// A durable scheduler value was not valid JSON.
    #[error("scheduler JSON: {0}")]
    Json(#[from] serde_json::Error),
    /// A runtime secret could not be encrypted or authenticated.
    #[error("scheduler secret: {0}")]
    Secret(#[from] verglas_authz::SecretError),
    /// A queue or invocation field is invalid.
    #[error("invalid scheduler input: {0}")]
    Invalid(String),
    /// A cron expression could not be planned.
    #[error("scheduler cron: {0}")]
    Cron(String),
}

/// A completion was attempted under a lease that no longer owns the job.
#[derive(Debug, thiserror::Error)]
pub enum LeaseError {
    /// The durable queue operation itself failed.
    #[error(transparent)]
    Scheduler(#[from] SchedulerError),
    /// A newer claim generation, different owner, or expiry fences this operation.
    #[error("stale lease for job {job_id}: generation {generation}")]
    Stale {
        /// The job whose lease was fenced.
        job_id: String,
        /// The rejected lease generation.
        generation: u64,
    },
}

impl From<sqlx::Error> for LeaseError {
    /// Preserves database failures behind the queue-level error boundary.
    fn from(error: sqlx::Error) -> Self {
        Self::Scheduler(SchedulerError::Database(error))
    }
}

impl From<serde_json::Error> for LeaseError {
    /// Preserves durable JSON failures behind the queue-level error boundary.
    fn from(error: serde_json::Error) -> Self {
        Self::Scheduler(SchedulerError::Json(error))
    }
}

/// One trigger delivery before it is materialized as a durable job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Invocation {
    pub worker: String,
    pub event: CloudEvent,
    pub ready_at: DateTime<Utc>,
}

impl Invocation {
    /// Builds an invocation from ingress-owned identity and payload.
    pub fn new(
        worker: impl Into<String>,
        event: CloudEvent,
        ready_at: DateTime<Utc>,
    ) -> Invocation {
        Invocation {
            worker: worker.into(),
            event,
            ready_at,
        }
    }

    /// Produces the stable identity used by Postgres conflict handling.
    fn id(&self) -> Result<String, SchedulerError> {
        #[derive(Serialize)]
        struct Identity<'a> {
            worker: &'a str,
            source: &'a str,
            event_id: &'a str,
        }
        let bytes = serde_json::to_vec(&Identity {
            worker: &self.worker,
            source: &self.event.source,
            event_id: &self.event.id,
        })?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
}

/// One durable queued run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub queue: String,
    pub worker: String,
    pub event: CloudEvent,
    pub ready_at: DateTime<Utc>,
}

/// Bounded control-plane projection of one durable worker run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobSummary {
    pub job_id: String,
    pub worker: String,
    pub state: String,
    pub ready_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub rows_produced: Option<u64>,
    pub error_message: Option<String>,
}

/// Result of an idempotent enqueue operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnqueueOutcome {
    Created(String),
    Existing(String),
}

impl EnqueueOutcome {
    /// Returns the deterministic job id.
    pub fn job_id(&self) -> &str {
        match self {
            Self::Created(id) | Self::Existing(id) => id,
        }
    }
}

/// A fenced, time-bounded claim on a job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
    pub job_id: String,
    pub owner: String,
    pub generation: u64,
    pub expires_at: DateTime<Utc>,
}

/// A worker attempt outcome recorded against its lease generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum Completion {
    Succeeded {
        rows_produced: u64,
    },
    Failed {
        message: String,
        retry_at: Option<DateTime<Utc>>,
    },
}

/// Remote claim parameters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimRequest {
    pub owner: String,
    pub now: DateTime<Utc>,
    pub lease_seconds: u64,
}

/// A job and the fenced lease under which it may execute.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimedJob {
    pub job: Job,
    pub lease: Lease,
}

/// Completion parameters recorded after execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompleteRequest {
    pub lease: Lease,
    pub completion: Completion,
    pub now: DateTime<Utc>,
}

/// Lease-renewal parameters sent by a running consumer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RenewRequest {
    pub lease: Lease,
    pub now: DateTime<Utc>,
    pub lease_seconds: u64,
}

/// Current time supplied when asking for the next runnable deadline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NextWakeRequest {
    pub now: DateTime<Utc>,
}

/// One execution attempt reconstructed from its claim and result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attempt {
    pub lease: Lease,
    pub completion: Option<Completion>,
}

/// Durable queue contract for scheduler processes.
#[async_trait]
pub trait RunQueue: Send + Sync {
    async fn enqueue(&self, invocation: &Invocation) -> Result<EnqueueOutcome, SchedulerError>;
    async fn jobs(&self) -> Result<Vec<Job>, SchedulerError>;
    async fn claim(&self, request: &ClaimRequest) -> Result<Option<ClaimedJob>, SchedulerError>;
    async fn renew(&self, request: &RenewRequest) -> Result<Lease, LeaseError>;
    async fn complete(&self, request: &CompleteRequest) -> Result<(), LeaseError>;
    async fn attempts(&self, job_id: &str) -> Result<Vec<Attempt>, SchedulerError>;
    async fn next_wake_at(
        &self,
        request: &NextWakeRequest,
    ) -> Result<Option<DateTime<Utc>>, SchedulerError>;
}

/// One logical tenant queue stored in Postgres.
#[derive(Clone)]
pub struct PgQueue {
    pool: PgPool,
    queue: String,
}

impl PgQueue {
    /// Connects to Postgres and creates the scheduler tables when absent.
    pub async fn connect(
        database_url: &str,
        queue: impl Into<String>,
    ) -> Result<PgQueue, SchedulerError> {
        let queue = queue.into();
        validate_queue(&queue)?;
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .connect(database_url)
            .await?;
        let result = PgQueue { pool, queue };
        result.migrate().await?;
        Ok(result)
    }

    /// Installs the prototype schema used by the scheduler service.
    async fn migrate(&self) -> Result<(), SchedulerError> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS verglas_scheduler_jobs (\
             id TEXT NOT NULL, queue TEXT NOT NULL, worker TEXT NOT NULL, \
             event JSONB NOT NULL, ready_at TIMESTAMPTZ NOT NULL, \
             state TEXT NOT NULL DEFAULT 'pending', lease_owner TEXT, \
             lease_generation BIGINT NOT NULL DEFAULT 0, lease_expires_at TIMESTAMPTZ, \
             created_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY (queue, id))",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS verglas_scheduler_attempts (\
             queue TEXT NOT NULL, job_id TEXT NOT NULL, \
             generation BIGINT NOT NULL, owner TEXT NOT NULL, lease_expires_at TIMESTAMPTZ NOT NULL, \
             completion JSONB, completed_at TIMESTAMPTZ, PRIMARY KEY (queue, job_id, generation), \
             FOREIGN KEY (queue, job_id) REFERENCES verglas_scheduler_jobs(queue, id) ON DELETE CASCADE)",
        ).execute(&self.pool).await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS verglas_scheduler_ready \
             ON verglas_scheduler_jobs (queue, state, ready_at, lease_expires_at)",
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Starts a transaction used to atomically fence a claim or completion.
    async fn transaction(&self) -> Result<Transaction<'_, sqlx::Postgres>, SchedulerError> {
        Ok(self.pool.begin().await?)
    }

    /// Lists recent jobs for one worker with a bounded result size.
    pub async fn worker_jobs(
        &self,
        worker: &str,
        limit: u32,
    ) -> Result<Vec<JobSummary>, SchedulerError> {
        let rows = sqlx::query(
            "SELECT j.id AS job_id,j.worker,j.state,j.ready_at,j.created_at,\
             a.completed_at,a.completion FROM verglas_scheduler_jobs j LEFT JOIN LATERAL (\
             SELECT completed_at,completion FROM verglas_scheduler_attempts \
             WHERE queue=j.queue AND job_id=j.id ORDER BY generation DESC LIMIT 1) a ON true \
             WHERE j.queue=$1 AND j.worker=$2 ORDER BY j.created_at DESC,j.id DESC LIMIT $3",
        )
        .bind(&self.queue)
        .bind(worker)
        .bind(i64::from(limit.clamp(1, 100)))
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(row_job_summary).collect()
    }

    /// Returns one job by id within this scheduler queue.
    pub async fn job(&self, job_id: &str) -> Result<Option<JobSummary>, SchedulerError> {
        sqlx::query(
            "SELECT j.id AS job_id,j.worker,j.state,j.ready_at,j.created_at,\
             a.completed_at,a.completion FROM verglas_scheduler_jobs j LEFT JOIN LATERAL (\
             SELECT completed_at,completion FROM verglas_scheduler_attempts \
             WHERE queue=j.queue AND job_id=j.id ORDER BY generation DESC LIMIT 1) a ON true \
             WHERE j.queue=$1 AND j.id=$2",
        )
        .bind(&self.queue)
        .bind(job_id)
        .fetch_optional(&self.pool)
        .await?
        .as_ref()
        .map(row_job_summary)
        .transpose()
    }
}

#[async_trait]
impl RunQueue for PgQueue {
    /// Inserts a job once using its deterministic trigger identity.
    async fn enqueue(&self, invocation: &Invocation) -> Result<EnqueueOutcome, SchedulerError> {
        let id = invocation.id()?;
        let result = sqlx::query(
            "INSERT INTO verglas_scheduler_jobs \
             (id, queue, worker, event, ready_at) VALUES ($1,$2,$3,$4,$5) \
             ON CONFLICT (queue, id) DO NOTHING",
        )
        .bind(&id)
        .bind(&self.queue)
        .bind(&invocation.worker)
        .bind(serde_json::to_value(&invocation.event)?)
        .bind(invocation.ready_at)
        .execute(&self.pool)
        .await?;
        Ok(if result.rows_affected() == 1 {
            EnqueueOutcome::Created(id)
        } else {
            EnqueueOutcome::Existing(id)
        })
    }

    /// Lists jobs in deterministic creation order for cron reconciliation.
    async fn jobs(&self) -> Result<Vec<Job>, SchedulerError> {
        let rows = sqlx::query(
            "SELECT id, queue, worker, event, ready_at FROM verglas_scheduler_jobs \
             WHERE queue=$1 ORDER BY created_at, id",
        )
        .bind(&self.queue)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(row_job).collect()
    }

    /// Claims one runnable job with `SKIP LOCKED` and records its generation.
    async fn claim(&self, request: &ClaimRequest) -> Result<Option<ClaimedJob>, SchedulerError> {
        let lease_seconds = i64::try_from(request.lease_seconds)
            .map_err(|_| SchedulerError::Invalid("lease seconds exceed i64".to_owned()))?;
        let expires_at = request.now + chrono::Duration::seconds(lease_seconds);
        let mut tx = self.transaction().await?;
        let row = sqlx::query(
            "WITH candidate AS (SELECT id FROM verglas_scheduler_jobs \
             WHERE queue=$1 AND ((state IN ('pending','retryable') AND ready_at <= $2) \
             OR (state='running' AND lease_expires_at <= $2)) \
             ORDER BY ready_at, created_at, id FOR UPDATE SKIP LOCKED LIMIT 1) \
             UPDATE verglas_scheduler_jobs j SET state='running', lease_owner=$3, \
             lease_generation=j.lease_generation+1, lease_expires_at=$4 \
             FROM candidate WHERE j.queue=$1 AND j.id=candidate.id \
             RETURNING j.id,j.queue,j.worker,j.event,j.ready_at,j.lease_generation",
        )
        .bind(&self.queue)
        .bind(request.now)
        .bind(&request.owner)
        .bind(expires_at)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(row) = row else {
            tx.commit().await?;
            return Ok(None);
        };
        let generation_i64: i64 = row.try_get("lease_generation")?;
        let generation = u64::try_from(generation_i64)
            .map_err(|_| SchedulerError::Invalid("negative lease generation".to_owned()))?;
        sqlx::query(
            "INSERT INTO verglas_scheduler_attempts \
             (queue,job_id,generation,owner,lease_expires_at) VALUES ($1,$2,$3,$4,$5)",
        )
        .bind(&self.queue)
        .bind(row.try_get::<String, _>("id")?)
        .bind(generation_i64)
        .bind(&request.owner)
        .bind(expires_at)
        .execute(&mut *tx)
        .await?;
        let job = row_job(&row)?;
        tx.commit().await?;
        Ok(Some(ClaimedJob {
            lease: Lease {
                job_id: job.id.clone(),
                owner: request.owner.clone(),
                generation,
                expires_at,
            },
            job,
        }))
    }

    /// Extends only the current, unexpired claim generation.
    async fn renew(&self, request: &RenewRequest) -> Result<Lease, LeaseError> {
        let seconds = i64::try_from(request.lease_seconds)
            .map_err(|_| SchedulerError::Invalid("lease seconds exceed i64".to_owned()))?;
        let expires_at = request.now + chrono::Duration::seconds(seconds);
        let generation = i64::try_from(request.lease.generation)
            .map_err(|_| SchedulerError::Invalid("lease generation exceeds i64".to_owned()))?;
        let mut tx = self.transaction().await?;
        let result = sqlx::query(
            "UPDATE verglas_scheduler_jobs SET lease_expires_at=$1 \
             WHERE queue=$2 AND id=$3 AND state='running' AND lease_owner=$4 AND lease_generation=$5 \
             AND lease_expires_at > $6",
        )
        .bind(expires_at)
        .bind(&self.queue)
        .bind(&request.lease.job_id)
        .bind(&request.lease.owner)
        .bind(generation)
        .bind(request.now)
        .execute(&mut *tx)
        .await?;
        if result.rows_affected() != 1 {
            return Err(stale(&request.lease));
        }
        sqlx::query(
            "UPDATE verglas_scheduler_attempts SET lease_expires_at=$1 \
             WHERE queue=$2 AND job_id=$3 AND generation=$4",
        )
        .bind(expires_at)
        .bind(&self.queue)
        .bind(&request.lease.job_id)
        .bind(generation)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(Lease {
            expires_at,
            ..request.lease.clone()
        })
    }

    /// Records the live generation's result and schedules a retry when requested.
    async fn complete(&self, request: &CompleteRequest) -> Result<(), LeaseError> {
        let generation = i64::try_from(request.lease.generation)
            .map_err(|_| SchedulerError::Invalid("lease generation exceeds i64".to_owned()))?;
        let (state, ready_at) = match &request.completion {
            Completion::Succeeded { .. } => ("succeeded", request.now),
            Completion::Failed {
                retry_at: Some(retry_at),
                ..
            } => ("retryable", *retry_at),
            Completion::Failed { retry_at: None, .. } => ("failed", request.now),
        };
        let completion = serde_json::to_value(&request.completion)?;
        let mut tx = self.transaction().await?;
        let result = sqlx::query(
            "UPDATE verglas_scheduler_jobs SET state=$1, ready_at=$2, lease_owner=NULL, lease_expires_at=NULL \
             WHERE queue=$3 AND id=$4 AND state='running' AND lease_owner=$5 AND lease_generation=$6 \
             AND lease_expires_at > $7",
        ).bind(state).bind(ready_at).bind(&self.queue).bind(&request.lease.job_id).bind(&request.lease.owner)
        .bind(generation).bind(request.now).execute(&mut *tx).await?;
        if result.rows_affected() != 1 {
            return Err(stale(&request.lease));
        }
        sqlx::query(
            "UPDATE verglas_scheduler_attempts SET completion=$1, completed_at=$2 \
             WHERE queue=$3 AND job_id=$4 AND generation=$5",
        )
        .bind(completion)
        .bind(request.now)
        .bind(&self.queue)
        .bind(&request.lease.job_id)
        .bind(generation)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Lists every claim generation and its optional result.
    async fn attempts(&self, job_id: &str) -> Result<Vec<Attempt>, SchedulerError> {
        let rows = sqlx::query(
            "SELECT job_id,generation,owner,lease_expires_at,completion \
             FROM verglas_scheduler_attempts WHERE queue=$1 AND job_id=$2 ORDER BY generation",
        )
        .bind(&self.queue)
        .bind(job_id)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(row_attempt).collect()
    }

    /// Returns the earliest retry or expired-lease deadline in this queue.
    async fn next_wake_at(
        &self,
        request: &NextWakeRequest,
    ) -> Result<Option<DateTime<Utc>>, SchedulerError> {
        let row = sqlx::query(
            "SELECT min(deadline) AS deadline FROM (\
             SELECT ready_at AS deadline FROM verglas_scheduler_jobs \
             WHERE queue=$1 AND state IN ('pending','retryable') \
             UNION ALL SELECT lease_expires_at AS deadline FROM verglas_scheduler_jobs \
             WHERE queue=$1 AND state='running') deadlines",
        )
        .bind(&self.queue)
        .fetch_one(&self.pool)
        .await?;
        let deadline: Option<DateTime<Utc>> = row.try_get("deadline")?;
        Ok(deadline.map(|value| std::cmp::max(value, request.now)))
    }
}

/// Validates the single-tenant queue name used in rows and logs.
pub(crate) fn validate_queue(queue: &str) -> Result<(), SchedulerError> {
    if queue.is_empty() || queue.contains('/') || queue == "." || queue == ".." {
        return Err(SchedulerError::Invalid(format!(
            "queue `{queue}` must be one non-empty component"
        )));
    }
    Ok(())
}

/// Decodes a durable job row.
fn row_job(row: &PgRow) -> Result<Job, SchedulerError> {
    Ok(Job {
        id: row.try_get("id")?,
        queue: row.try_get("queue")?,
        worker: row.try_get("worker")?,
        event: serde_json::from_value(row.try_get("event")?)?,
        ready_at: row.try_get("ready_at")?,
    })
}

fn row_job_summary(row: &PgRow) -> Result<JobSummary, SchedulerError> {
    let completion = row.try_get::<Option<serde_json::Value>, _>("completion")?;
    let rows_produced = completion
        .as_ref()
        .and_then(|value| value.get("rows_produced"))
        .and_then(serde_json::Value::as_u64);
    let error_message = completion
        .as_ref()
        .and_then(|value| value.get("message"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    Ok(JobSummary {
        job_id: row.try_get("job_id")?,
        worker: row.try_get("worker")?,
        state: row.try_get("state")?,
        ready_at: row.try_get("ready_at")?,
        created_at: row.try_get("created_at")?,
        completed_at: row.try_get("completed_at")?,
        rows_produced,
        error_message,
    })
}

/// Decodes one attempt row and checks its generation domain.
fn row_attempt(row: &PgRow) -> Result<Attempt, SchedulerError> {
    let generation: i64 = row.try_get("generation")?;
    Ok(Attempt {
        lease: Lease {
            job_id: row.try_get("job_id")?,
            owner: row.try_get("owner")?,
            generation: u64::try_from(generation)
                .map_err(|_| SchedulerError::Invalid("negative lease generation".to_owned()))?,
            expires_at: row.try_get("lease_expires_at")?,
        },
        completion: row
            .try_get::<Option<serde_json::Value>, _>("completion")?
            .map(serde_json::from_value)
            .transpose()?,
    })
}

/// Constructs the common fencing error without losing the rejected generation.
fn stale(lease: &Lease) -> LeaseError {
    LeaseError::Stale {
        job_id: lease.job_id.clone(),
        generation: lease.generation,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Retry timing does not change the idempotent identity of one trigger event.
    #[test]
    fn invocation_identity_excludes_ready_time() {
        let now = Utc::now();
        let event = CloudEvent::new("request-1", "urn:test", "org.verglas.worker.manual");
        let first = Invocation::new("worker", event.clone(), now);
        let retry = Invocation::new("worker", event, now + chrono::Duration::minutes(1));
        assert_eq!(first.id().expect("first id"), retry.id().expect("retry id"));
    }

    /// Queue names are plain tenant identifiers, never SQL or path namespaces.
    #[test]
    fn queue_name_is_one_component() {
        assert!(validate_queue("tenant-a").is_ok());
        assert!(validate_queue("").is_err());
        assert!(validate_queue("tenant/a").is_err());
    }
}
