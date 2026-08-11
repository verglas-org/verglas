//! Postgres-backed worker declarations owned by the scheduler control plane.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgPool, PgPoolOptions, PgRow};
use sqlx::{Row, Transaction};
use std::sync::Arc;
use verglas_authz::{AeadSecretCipher, SecretCipher};

use crate::SchedulerError;

/// Registry calls are short and serialized by the scheduler service.
const TENANT_POOL_MAX_CONNECTIONS: u32 = 2;

/// A portable worker declaration accepted by the scheduler.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct WorkerSpec {
    pub name: String,
    #[serde(default)]
    pub code: String,
    #[serde(default = "empty_triggers")]
    pub triggers: String,
    pub output: Option<String>,
    #[serde(default = "empty_config")]
    pub config: String,
    pub created_by: String,
}

fn empty_triggers() -> String {
    "[]".to_owned()
}

fn empty_config() -> String {
    "{}".to_owned()
}

/// One immutable revision in the scheduler's worker registry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerRecord {
    pub name: String,
    pub code: String,
    pub triggers: String,
    pub output: Option<String>,
    pub config: String,
    pub state: String,
    pub placement: String,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
    pub revision: i64,
}

/// Queue-scoped worker registry stored beside scheduler jobs and leases.
#[derive(Clone)]
pub struct PgWorkerRegistry {
    pool: PgPool,
    queue: String,
    cipher: Arc<AeadSecretCipher>,
}

impl PgWorkerRegistry {
    /// Connects to Postgres and installs the worker registry schema.
    pub async fn connect(
        database_url: &str,
        queue: impl Into<String>,
        encryption_key: &[u8],
    ) -> Result<Self, SchedulerError> {
        let queue = queue.into();
        super::queue::validate_queue(&queue)?;
        let pool = PgPoolOptions::new()
            .max_connections(TENANT_POOL_MAX_CONNECTIONS)
            .connect(database_url)
            .await?;
        let cipher = Arc::new(AeadSecretCipher::new(encryption_key)?);
        let registry = Self {
            pool,
            queue,
            cipher,
        };
        registry.migrate().await?;
        Ok(registry)
    }

    async fn migrate(&self) -> Result<(), SchedulerError> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS verglas_scheduler_workers (\
             queue TEXT NOT NULL, name TEXT NOT NULL, revision BIGINT NOT NULL, \
             code TEXT NOT NULL, triggers TEXT NOT NULL, output TEXT, config TEXT NOT NULL, \
             state TEXT NOT NULL, placement TEXT NOT NULL, created_by TEXT NOT NULL, \
             created_at TIMESTAMPTZ NOT NULL, revised_at TIMESTAMPTZ NOT NULL DEFAULT now(), \
             PRIMARY KEY (queue, name, revision))",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS verglas_scheduler_workers_current \
             ON verglas_scheduler_workers (queue, name, revision DESC)",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS verglas_scheduler_secrets (\
             queue TEXT NOT NULL, name TEXT NOT NULL, ciphertext BYTEA NOT NULL, \
             updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY (queue, name))",
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Appends a running revision, preserving the declaration's creation time.
    pub async fn register(&self, spec: WorkerSpec) -> Result<WorkerRecord, SchedulerError> {
        validate_spec(&spec)?;
        let mut tx = self.pool.begin().await?;
        lock_worker(&mut tx, &self.queue, &spec.name).await?;
        let current = current_worker(&mut tx, &self.queue, &spec.name).await?;
        let record = WorkerRecord {
            name: spec.name,
            code: spec.code,
            triggers: spec.triggers,
            output: spec.output,
            config: spec.config,
            state: "running".to_owned(),
            placement: "local".to_owned(),
            created_by: spec.created_by,
            created_at: current.as_ref().map_or_else(Utc::now, |row| row.created_at),
            revision: current.map_or(1, |row| row.revision + 1),
        };
        insert_worker(&mut tx, &self.queue, &record).await?;
        tx.commit().await?;
        Ok(record)
    }

    /// Returns every retained revision or the active current projection.
    pub async fn list(
        &self,
        include_all_revisions: bool,
    ) -> Result<Vec<WorkerRecord>, SchedulerError> {
        let query = if include_all_revisions {
            "SELECT name,code,triggers,output,config,state,placement,created_by,created_at,revision \
             FROM verglas_scheduler_workers WHERE queue=$1 ORDER BY name,revision"
        } else {
            "SELECT name,code,triggers,output,config,state,placement,created_by,created_at,revision \
             FROM (SELECT DISTINCT ON (name) name,code,triggers,output,config,state,placement,\
             created_by,created_at,revision FROM verglas_scheduler_workers WHERE queue=$1 \
             ORDER BY name,revision DESC) current WHERE state <> 'archived' ORDER BY name"
        };
        sqlx::query(query)
            .bind(&self.queue)
            .fetch_all(&self.pool)
            .await?
            .iter()
            .map(decode_worker)
            .collect()
    }

    /// Returns the current revision for one worker.
    pub async fn get(&self, name: &str) -> Result<Option<WorkerRecord>, SchedulerError> {
        let mut tx = self.pool.begin().await?;
        let worker = current_worker(&mut tx, &self.queue, name).await?;
        tx.commit().await?;
        Ok(worker)
    }

    /// Appends a lifecycle-state revision for an existing worker.
    pub async fn set_state(
        &self,
        name: &str,
        state: &str,
    ) -> Result<Option<WorkerRecord>, SchedulerError> {
        validate_state(state)?;
        let mut tx = self.pool.begin().await?;
        lock_worker(&mut tx, &self.queue, name).await?;
        let Some(mut record) = current_worker(&mut tx, &self.queue, name).await? else {
            tx.commit().await?;
            return Ok(None);
        };
        record.state = state.to_owned();
        record.revision += 1;
        insert_worker(&mut tx, &self.queue, &record).await?;
        tx.commit().await?;
        Ok(Some(record))
    }

    /// Lists secret binding names without returning their values.
    pub async fn secret_names(&self) -> Result<Vec<String>, SchedulerError> {
        Ok(sqlx::query_scalar(
            "SELECT name FROM verglas_scheduler_secrets WHERE queue=$1 ORDER BY name",
        )
        .bind(&self.queue)
        .fetch_all(&self.pool)
        .await?)
    }

    /// Creates or replaces one queue-scoped runtime secret.
    pub async fn put_secret(&self, name: &str, value: &str) -> Result<(), SchedulerError> {
        validate_secret_name(name)?;
        let ciphertext = self.cipher.seal(value.as_bytes())?;
        sqlx::query(
            "INSERT INTO verglas_scheduler_secrets (queue,name,ciphertext) VALUES ($1,$2,$3) \
             ON CONFLICT (queue,name) DO UPDATE SET ciphertext=excluded.ciphertext,updated_at=now()",
        )
        .bind(&self.queue)
        .bind(name)
        .bind(ciphertext)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Deletes one queue-scoped runtime secret.
    pub async fn delete_secret(&self, name: &str) -> Result<bool, SchedulerError> {
        validate_secret_name(name)?;
        Ok(
            sqlx::query("DELETE FROM verglas_scheduler_secrets WHERE queue=$1 AND name=$2")
                .bind(&self.queue)
                .bind(name)
                .execute(&self.pool)
                .await?
                .rows_affected()
                == 1,
        )
    }

    /// Resolves one queue-scoped secret for execution without exposing it over HTTP.
    pub async fn secret(&self, name: &str) -> Result<Option<String>, SchedulerError> {
        validate_secret_name(name)?;
        let ciphertext: Option<Vec<u8>> = sqlx::query_scalar(
            "SELECT ciphertext FROM verglas_scheduler_secrets WHERE queue=$1 AND name=$2",
        )
        .bind(&self.queue)
        .bind(name)
        .fetch_optional(&self.pool)
        .await?;
        ciphertext
            .map(|ciphertext| {
                let plaintext = self.cipher.open(&ciphertext)?;
                String::from_utf8(plaintext).map_err(|_| {
                    SchedulerError::Invalid(format!("secret `{name}` is not valid UTF-8"))
                })
            })
            .transpose()
    }
}

fn validate_spec(spec: &WorkerSpec) -> Result<(), SchedulerError> {
    if spec.name.is_empty() || spec.name.contains('/') || spec.name == "." || spec.name == ".." {
        return Err(SchedulerError::Invalid(format!(
            "worker `{}` must be one non-empty component",
            spec.name
        )));
    }
    serde_json::from_str::<serde_json::Value>(&spec.code)?;
    serde_json::from_str::<Vec<verglas_sdk::worker::TriggerSpec>>(&spec.triggers)?;
    serde_json::from_str::<serde_json::Value>(&spec.config)?;
    Ok(())
}

fn validate_state(state: &str) -> Result<(), SchedulerError> {
    if matches!(state, "running" | "paused" | "archived") {
        Ok(())
    } else {
        Err(SchedulerError::Invalid(format!(
            "unknown worker state `{state}`: expected running, paused, or archived"
        )))
    }
}

fn validate_secret_name(name: &str) -> Result<(), SchedulerError> {
    if !name.is_empty()
        && name.len() <= 255
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        Ok(())
    } else {
        Err(SchedulerError::Invalid(format!(
            "secret `{name}` must contain only letters, numbers, dot, underscore, or hyphen"
        )))
    }
}

async fn lock_worker(
    tx: &mut Transaction<'_, sqlx::Postgres>,
    queue: &str,
    name: &str,
) -> Result<(), SchedulerError> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("{queue}:{name}"))
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn current_worker(
    tx: &mut Transaction<'_, sqlx::Postgres>,
    queue: &str,
    name: &str,
) -> Result<Option<WorkerRecord>, SchedulerError> {
    sqlx::query(
        "SELECT name,code,triggers,output,config,state,placement,created_by,created_at,revision \
         FROM verglas_scheduler_workers WHERE queue=$1 AND name=$2 ORDER BY revision DESC LIMIT 1",
    )
    .bind(queue)
    .bind(name)
    .fetch_optional(&mut **tx)
    .await?
    .as_ref()
    .map(decode_worker)
    .transpose()
}

async fn insert_worker(
    tx: &mut Transaction<'_, sqlx::Postgres>,
    queue: &str,
    row: &WorkerRecord,
) -> Result<(), SchedulerError> {
    sqlx::query(
        "INSERT INTO verglas_scheduler_workers \
         (queue,name,revision,code,triggers,output,config,state,placement,created_by,created_at) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
    )
    .bind(queue)
    .bind(&row.name)
    .bind(row.revision)
    .bind(&row.code)
    .bind(&row.triggers)
    .bind(&row.output)
    .bind(&row.config)
    .bind(&row.state)
    .bind(&row.placement)
    .bind(&row.created_by)
    .bind(row.created_at)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

fn decode_worker(row: &PgRow) -> Result<WorkerRecord, SchedulerError> {
    Ok(WorkerRecord {
        name: row.try_get("name")?,
        code: row.try_get("code")?,
        triggers: row.try_get("triggers")?,
        output: row.try_get("output")?,
        config: row.try_get("config")?,
        state: row.try_get("state")?,
        placement: row.try_get("placement")?,
        created_by: row.try_get("created_by")?,
        created_at: row.try_get("created_at")?,
        revision: row.try_get("revision")?,
    })
}
