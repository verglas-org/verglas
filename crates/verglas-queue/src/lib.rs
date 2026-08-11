//! Declares independently provisioned queues and stores their messages in PostgreSQL.
//! A queue owns both a managed Neon deployment and a separately scalable service container.

use std::sync::Mutex;
use std::{pin::Pin, sync::Arc};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::Stream;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::postgres::{PgListener, PgPool, PgPoolOptions};
use sqlx::{Row, Transaction};

/// Public request to create one tenant-local queue.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CreateQueueRequest {
    /// Stable tenant-local queue name.
    pub name: String,
}

impl CreateQueueRequest {
    /// Validates the declaration and assigns dedicated database and service identities.
    pub fn plan(self, tenant_id: impl Into<String>) -> Result<QueuePlan, PlanError> {
        validate_name(&self.name)?;
        let tenant_id = tenant_id.into();
        if tenant_id.trim().is_empty() {
            return Err(PlanError::EmptyTenant);
        }
        let database_name = format!("queue-{}", self.name);
        Ok(QueuePlan {
            tenant_id,
            database_deployment_id: format!("{database_name}-postgres"),
            container_deployment_id: format!("{database_name}-service"),
            database_name,
            name: self.name,
        })
    }
}

/// Validated desired state for one independently scalable queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueuePlan {
    tenant_id: String,
    name: String,
    database_name: String,
    database_deployment_id: String,
    container_deployment_id: String,
}

impl QueuePlan {
    /// Returns the tenant that owns the queue.
    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    /// Returns the stable tenant-local queue name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the logical database created inside the dedicated Neon deployment.
    pub fn database_name(&self) -> &str {
        &self.database_name
    }

    /// Returns the dedicated managed Neon deployment identity.
    pub fn database_deployment_id(&self) -> &str {
        &self.database_deployment_id
    }

    /// Returns the independently scalable queue container identity.
    pub fn container_deployment_id(&self) -> &str {
        &self.container_deployment_id
    }
}

/// Queue declaration failures detected before any external mutation.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PlanError {
    /// Queue names are restricted to stable DNS-safe identifiers.
    #[error("invalid queue name: {0}")]
    InvalidName(String),
    /// Every queue must belong to a tenant.
    #[error("queue tenant must not be empty")]
    EmptyTenant,
}

/// Enforces the stable DNS-safe name accepted by PostgreSQL and container placement.
fn validate_name(name: &str) -> Result<(), PlanError> {
    let valid = !name.is_empty()
        && name.len() <= 48
        && name.bytes().enumerate().all(|(index, byte)| match byte {
            b'a'..=b'z' | b'0'..=b'9' => true,
            b'-' => index > 0 && index + 1 < name.len(),
            _ => false,
        });
    if !valid || name.contains("--") {
        return Err(PlanError::InvalidName(name.to_owned()));
    }
    Ok(())
}

/// Opaque proof that one consumer owns a delivery generation.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Receipt {
    /// Stable message position inside the queue.
    pub position: i64,
    /// Consumer process that owns this lease.
    pub owner: String,
    /// Monotonic generation that fences previous deliveries.
    pub generation: u64,
}

/// One leased message returned to a consumer.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Delivery {
    /// Stable ordered message position.
    pub position: i64,
    /// Exact topic used to filter independent subscriptions.
    pub topic: String,
    /// Caller-supplied JSON message.
    pub payload: Value,
    /// Receipt required to acknowledge exactly this delivery generation.
    pub receipt: Receipt,
    /// Deadline after which another consumer may reclaim this message.
    pub expires_at: DateTime<Utc>,
}

/// One idempotent message published to a topic.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueMessage {
    /// Producer-defined idempotency identity.
    pub id: String,
    /// Exact topic used for subscription matching.
    pub topic: String,
    /// Caller-supplied message body.
    pub payload: Value,
}

/// One long-lived topic subscription using normal queue consumer-group semantics.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscribeRequest {
    /// Independent fan-out position. Owners in the same group compete.
    pub group: String,
    /// Stable identity of this subscriber process.
    pub owner: String,
    /// Exact topics delivered to this subscription.
    pub topics: Vec<String>,
    /// Maximum number of messages claimed by one wake-up.
    pub max: u32,
    /// Lease duration before unacknowledged delivery is eligible again.
    pub lease_seconds: u64,
}

/// A push-only sequence of fenced deliveries.
pub type DeliveryStream = Pin<Box<dyn Stream<Item = Result<Delivery, QueueError>> + Send>>;

/// Bounded request for exclusive deliveries in one consumer group.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PollRequest {
    /// Independent consumer-group name.
    pub group: String,
    /// Stable identity of this consumer process.
    pub owner: String,
    /// Exact topics eligible for this claim.
    pub topics: Vec<String>,
    /// Maximum number of messages claimed in this transaction.
    pub max: u32,
    /// Authoritative current time supplied by the service.
    pub now: DateTime<Utc>,
    /// Lease duration before redelivery becomes legal.
    pub lease_seconds: u64,
}

/// Fenced acknowledgement of one delivered message.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AckRequest {
    /// Consumer group whose delivery is being acknowledged.
    pub group: String,
    /// Exact delivery generation returned by poll.
    pub receipt: Receipt,
    /// Authoritative current time supplied by the service.
    pub now: DateTime<Utc>,
}

/// Queue storage and validation failures.
#[derive(Debug, thiserror::Error)]
pub enum QueueError {
    /// PostgreSQL rejected a durable operation.
    #[error("queue postgres: {0}")]
    Database(#[from] sqlx::Error),
    /// A request violated a bounded queue contract.
    #[error("invalid queue request: {0}")]
    Invalid(String),
    /// A newer delivery or expired deadline fenced an acknowledgement.
    #[error("stale queue receipt for position {position}: generation {generation}")]
    StaleReceipt {
        /// Rejected queue position.
        position: i64,
        /// Rejected delivery generation.
        generation: u64,
    },
}

/// Durable message operations implemented by the standalone queue service.
#[async_trait]
pub trait QueueStore: Send + Sync {
    /// Appends a bounded ordered batch and returns its stable positions.
    async fn enqueue(&self, messages: &[QueueMessage]) -> Result<Vec<i64>, QueueError>;

    /// Claims messages transactionally without delivering one generation twice.
    async fn poll(&self, request: &PollRequest) -> Result<Vec<Delivery>, QueueError>;

    /// Pushes matching messages as they commit without client polling.
    async fn subscribe(&self, request: SubscribeRequest) -> Result<DeliveryStream, QueueError>;

    /// Acknowledges only the current unexpired generation of one delivery.
    async fn ack(&self, request: &AckRequest) -> Result<(), QueueError>;
}

/// PostgreSQL queue store used inside one queue-owned Neon deployment.
#[derive(Clone)]
pub struct PgQueue {
    pool: PgPool,
    database_url: Arc<str>,
}

impl PgQueue {
    /// Connects to the queue database and installs its exclusive-delivery schema.
    pub async fn connect(database_url: &str) -> Result<Self, QueueError> {
        let pool = PgPoolOptions::new()
            .max_connections(16)
            .connect(database_url)
            .await?;
        let queue = Self {
            pool,
            database_url: Arc::from(database_url),
        };
        queue.migrate().await?;
        Ok(queue)
    }

    /// Installs the queue-owned schema without any filesystem state.
    async fn migrate(&self) -> Result<(), QueueError> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS verglas_queue_messages (\
             position BIGSERIAL PRIMARY KEY, event_id TEXT NOT NULL UNIQUE, \
             topic TEXT NOT NULL, payload JSONB NOT NULL, \
             created_at TIMESTAMPTZ NOT NULL DEFAULT now())",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS verglas_queue_deliveries (\
             consumer_group TEXT NOT NULL, position BIGINT NOT NULL \
             REFERENCES verglas_queue_messages(position) ON DELETE CASCADE, \
             owner TEXT NOT NULL, generation BIGINT NOT NULL, \
             lease_expires_at TIMESTAMPTZ NOT NULL, acked BOOLEAN NOT NULL DEFAULT false, \
             acked_at TIMESTAMPTZ, PRIMARY KEY (consumer_group, position))",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS verglas_queue_delivery_candidates \
             ON verglas_queue_deliveries (consumer_group, acked, lease_expires_at, position)",
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Begins an atomic enqueue, claim, or acknowledgement operation.
    async fn transaction(&self) -> Result<Transaction<'_, sqlx::Postgres>, QueueError> {
        Ok(self.pool.begin().await?)
    }
}

#[async_trait]
impl QueueStore for PgQueue {
    async fn enqueue(&self, messages: &[QueueMessage]) -> Result<Vec<i64>, QueueError> {
        validate_messages(messages)?;
        if messages.is_empty() || messages.len() > 1_000 {
            return Err(QueueError::Invalid(
                "enqueue batch must contain 1 through 1000 messages".to_owned(),
            ));
        }
        let mut transaction = self.transaction().await?;
        let mut positions = Vec::with_capacity(messages.len());
        for message in messages {
            let row = sqlx::query(
                "INSERT INTO verglas_queue_messages (event_id,topic,payload) VALUES ($1,$2,$3) \
                 ON CONFLICT (event_id) DO UPDATE SET event_id=EXCLUDED.event_id \
                 WHERE verglas_queue_messages.topic=EXCLUDED.topic \
                 AND verglas_queue_messages.payload=EXCLUDED.payload RETURNING position",
            )
            .bind(&message.id)
            .bind(&message.topic)
            .bind(&message.payload)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or_else(|| {
                QueueError::Invalid(format!(
                    "message id `{}` was already used with different content",
                    message.id
                ))
            })?;
            positions.push(row.try_get("position")?);
        }
        sqlx::query("SELECT pg_notify('verglas_queue_messages', '')")
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(positions)
    }

    async fn poll(&self, request: &PollRequest) -> Result<Vec<Delivery>, QueueError> {
        validate_consumer(request)?;
        let lease_seconds = i64::try_from(request.lease_seconds)
            .map_err(|_| QueueError::Invalid("lease seconds exceed i64".to_owned()))?;
        let expires_at = request.now + chrono::Duration::seconds(lease_seconds);
        let rows = sqlx::query(
            "WITH candidates AS (\
               SELECT m.position FROM verglas_queue_messages m \
               LEFT JOIN verglas_queue_deliveries d \
                 ON d.consumer_group=$1 AND d.position=m.position \
               WHERE m.topic = ANY($6) AND \
                 (d.position IS NULL OR (d.acked=false AND d.lease_expires_at <= $2)) \
               ORDER BY m.position FOR UPDATE OF m SKIP LOCKED LIMIT $3\
             ), leased AS (\
               INSERT INTO verglas_queue_deliveries \
                 (consumer_group,position,owner,generation,lease_expires_at,acked,acked_at) \
               SELECT $1,c.position,$4,1,$5,false,NULL FROM candidates c \
               ON CONFLICT (consumer_group,position) DO UPDATE SET \
                 owner=EXCLUDED.owner,generation=verglas_queue_deliveries.generation+1,\
                 lease_expires_at=EXCLUDED.lease_expires_at,acked=false,acked_at=NULL \
               RETURNING position,owner,generation,lease_expires_at\
             ) SELECT l.position,l.owner,l.generation,l.lease_expires_at,m.topic,m.payload \
               FROM leased l JOIN verglas_queue_messages m ON m.position=l.position \
               ORDER BY l.position",
        )
        .bind(&request.group)
        .bind(request.now)
        .bind(i64::from(request.max))
        .bind(&request.owner)
        .bind(expires_at)
        .bind(&request.topics)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                let generation: i64 = row.try_get("generation")?;
                let generation = u64::try_from(generation)
                    .map_err(|_| QueueError::Invalid("negative delivery generation".to_owned()))?;
                let position = row.try_get("position")?;
                let owner = row.try_get("owner")?;
                Ok(Delivery {
                    position,
                    topic: row.try_get("topic")?,
                    payload: row.try_get("payload")?,
                    receipt: Receipt {
                        position,
                        owner,
                        generation,
                    },
                    expires_at: row.try_get("lease_expires_at")?,
                })
            })
            .collect()
    }

    async fn subscribe(&self, request: SubscribeRequest) -> Result<DeliveryStream, QueueError> {
        validate_subscription(&request)?;
        let mut listener = PgListener::connect(&self.database_url).await?;
        listener.listen("verglas_queue_messages").await?;
        let queue = self.clone();
        Ok(Box::pin(async_stream::try_stream! {
            loop {
                let deliveries = queue.poll(&PollRequest {
                    group: request.group.clone(),
                    owner: request.owner.clone(),
                    topics: request.topics.clone(),
                    max: request.max,
                    now: Utc::now(),
                    lease_seconds: request.lease_seconds,
                }).await?;
                if !deliveries.is_empty() {
                    for delivery in deliveries {
                        yield delivery;
                    }
                    continue;
                }
                match queue.next_delivery_at(&request).await? {
                    Some(deadline) => {
                        let wait = (deadline - Utc::now()).to_std().unwrap_or_default();
                        let wake = tokio::select! {
                            notification = listener.recv() => notification.map(|_| ()),
                            _ = tokio::time::sleep(wait) => Ok(()),
                        };
                        wake?;
                    }
                    None => { listener.recv().await?; }
                }
            }
        }))
    }

    async fn ack(&self, request: &AckRequest) -> Result<(), QueueError> {
        if request.group.is_empty() || request.receipt.owner.is_empty() {
            return Err(QueueError::Invalid(
                "ack group and receipt owner must not be empty".to_owned(),
            ));
        }
        let generation = i64::try_from(request.receipt.generation)
            .map_err(|_| QueueError::Invalid("receipt generation exceeds i64".to_owned()))?;
        let result = sqlx::query(
            "UPDATE verglas_queue_deliveries SET acked=true,acked_at=$1 \
             WHERE consumer_group=$2 AND position=$3 AND owner=$4 AND generation=$5 \
             AND acked=false AND lease_expires_at > $1",
        )
        .bind(request.now)
        .bind(&request.group)
        .bind(request.receipt.position)
        .bind(&request.receipt.owner)
        .bind(generation)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() != 1 {
            return Err(QueueError::StaleReceipt {
                position: request.receipt.position,
                generation: request.receipt.generation,
            });
        }
        Ok(())
    }
}

impl PgQueue {
    /// Returns the first lease expiration that can make a subscribed topic deliverable.
    async fn next_delivery_at(
        &self,
        request: &SubscribeRequest,
    ) -> Result<Option<DateTime<Utc>>, QueueError> {
        let row = sqlx::query(
            "SELECT min(d.lease_expires_at) AS ready_at \
             FROM verglas_queue_deliveries d \
             JOIN verglas_queue_messages m ON m.position=d.position \
             WHERE d.consumer_group=$1 AND d.acked=false AND m.topic=ANY($2)",
        )
        .bind(&request.group)
        .bind(&request.topics)
        .fetch_one(&self.pool)
        .await?;
        Ok(row.try_get("ready_at")?)
    }
}

/// Validates the bounded consumer contract before beginning a transaction.
fn validate_consumer(request: &PollRequest) -> Result<(), QueueError> {
    if request.group.is_empty() || request.owner.is_empty() || request.topics.is_empty() {
        return Err(QueueError::Invalid(
            "poll group and owner must not be empty".to_owned(),
        ));
    }
    if !(1..=256).contains(&request.max) {
        return Err(QueueError::Invalid(
            "poll max must be from 1 through 256".to_owned(),
        ));
    }
    if !(1..=3_600).contains(&request.lease_seconds) {
        return Err(QueueError::Invalid(
            "lease seconds must be from 1 through 3600".to_owned(),
        ));
    }
    Ok(())
}

/// Validates a long-lived subscription before opening a PostgreSQL listener.
fn validate_subscription(request: &SubscribeRequest) -> Result<(), QueueError> {
    validate_consumer(&PollRequest {
        group: request.group.clone(),
        owner: request.owner.clone(),
        topics: request.topics.clone(),
        max: request.max,
        now: Utc::now(),
        lease_seconds: request.lease_seconds,
    })
}

/// Rejects empty identities and topics before beginning an enqueue transaction.
fn validate_messages(messages: &[QueueMessage]) -> Result<(), QueueError> {
    if messages
        .iter()
        .any(|message| message.id.trim().is_empty() || message.topic.trim().is_empty())
    {
        return Err(QueueError::Invalid(
            "message id and topic must not be empty".to_owned(),
        ));
    }
    Ok(())
}

/// Concrete managed resources assigned to one queue declaration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueuePlacement {
    /// Queue-owned logical database name.
    pub database_name: String,
    /// Customer-visible managed Neon deployment identity.
    pub database_deployment_id: String,
    /// Independently scalable queue service identity.
    pub container_deployment_id: String,
}

impl QueuePlacement {
    /// Creates a placement only after both managed resources are known.
    pub fn new(
        database_name: impl Into<String>,
        database_deployment_id: impl Into<String>,
        container_deployment_id: impl Into<String>,
    ) -> Self {
        Self {
            database_name: database_name.into(),
            database_deployment_id: database_deployment_id.into(),
            container_deployment_id: container_deployment_id.into(),
        }
    }

    /// Returns the queue service deployment removed before its database.
    pub fn container_deployment_id(&self) -> &str {
        &self.container_deployment_id
    }
}

/// Public declaration and resolved managed-resource identities for one queue.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueView {
    /// Stable tenant-local queue name.
    pub name: String,
    /// Dedicated managed Neon deployment.
    pub database_deployment_id: String,
    /// Independently scalable queue service container.
    pub container_deployment_id: String,
}

/// Durable queue declaration retained in the system Neon database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueRecord {
    id: String,
    tenant_id: String,
    name: String,
    placement: QueuePlacement,
}

impl QueueRecord {
    /// Creates a complete record after runtime provisioning succeeds.
    fn new(tenant_id: String, name: String, placement: QueuePlacement) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            tenant_id,
            name,
            placement,
        }
    }

    /// Projects the public resource without private database connection material.
    fn view(&self) -> QueueView {
        QueueView {
            name: self.name.clone(),
            database_deployment_id: self.placement.database_deployment_id.clone(),
            container_deployment_id: self.placement.container_deployment_id.clone(),
        }
    }
}

/// Queue declaration persistence failures.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum QueueRepositoryError {
    /// A tenant already owns this queue name.
    #[error("queue {tenant_id}/{name} already exists")]
    Duplicate {
        /// Owning tenant.
        tenant_id: String,
        /// Duplicate stable name.
        name: String,
    },
    /// Durable system database access failed.
    #[error("queue repository failed: {0}")]
    Backend(String),
}

/// Durable resource-record boundary independent of runtime placement.
#[async_trait]
pub trait QueueRepository: Send + Sync {
    /// Inserts one fully provisioned record exactly once.
    async fn insert(&self, record: QueueRecord) -> Result<(), QueueRepositoryError>;

    /// Gets one tenant-local queue record.
    async fn get(
        &self,
        tenant_id: &str,
        name: &str,
    ) -> Result<Option<QueueRecord>, QueueRepositoryError>;

    /// Lists one tenant's records in stable name order.
    async fn list(&self, tenant_id: &str) -> Result<Vec<QueueRecord>, QueueRepositoryError>;

    /// Deletes one record after its runtime is gone.
    async fn delete(&self, tenant_id: &str, name: &str) -> Result<bool, QueueRepositoryError>;
}

/// In-memory repository used by lifecycle tests without a hidden runtime fallback.
#[derive(Default)]
pub struct MemoryQueueRepository {
    records: Mutex<Vec<QueueRecord>>,
}

#[async_trait]
impl QueueRepository for MemoryQueueRepository {
    async fn insert(&self, record: QueueRecord) -> Result<(), QueueRepositoryError> {
        let mut records = self
            .records
            .lock()
            .map_err(|error| QueueRepositoryError::Backend(error.to_string()))?;
        if records
            .iter()
            .any(|current| current.tenant_id == record.tenant_id && current.name == record.name)
        {
            return Err(QueueRepositoryError::Duplicate {
                tenant_id: record.tenant_id,
                name: record.name,
            });
        }
        records.push(record);
        Ok(())
    }

    async fn get(
        &self,
        tenant_id: &str,
        name: &str,
    ) -> Result<Option<QueueRecord>, QueueRepositoryError> {
        Ok(self
            .records
            .lock()
            .map_err(|error| QueueRepositoryError::Backend(error.to_string()))?
            .iter()
            .find(|record| record.tenant_id == tenant_id && record.name == name)
            .cloned())
    }

    async fn list(&self, tenant_id: &str) -> Result<Vec<QueueRecord>, QueueRepositoryError> {
        let mut records = self
            .records
            .lock()
            .map_err(|error| QueueRepositoryError::Backend(error.to_string()))?
            .iter()
            .filter(|record| record.tenant_id == tenant_id)
            .cloned()
            .collect::<Vec<_>>();
        records.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(records)
    }

    async fn delete(&self, tenant_id: &str, name: &str) -> Result<bool, QueueRepositoryError> {
        let mut records = self
            .records
            .lock()
            .map_err(|error| QueueRepositoryError::Backend(error.to_string()))?;
        let before = records.len();
        records.retain(|record| record.tenant_id != tenant_id || record.name != name);
        Ok(records.len() != before)
    }
}

/// PostgreSQL queue resource repository inside the system Neon deployment.
#[derive(Clone)]
pub struct PostgresQueueRepository {
    pool: PgPool,
}

impl PostgresQueueRepository {
    /// Connects to the system database and installs the current resource table.
    pub async fn connect(database_url: &str) -> Result<Self, QueueRepositoryError> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await
            .map_err(repository_error)?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS verglas_queues (\
             id TEXT PRIMARY KEY,tenant_id TEXT NOT NULL,name TEXT NOT NULL,\
             database_name TEXT NOT NULL,database_deployment_id TEXT NOT NULL,\
             container_deployment_id TEXT NOT NULL,UNIQUE(tenant_id,name))",
        )
        .execute(&pool)
        .await
        .map_err(repository_error)?;
        Ok(Self { pool })
    }
}

#[async_trait]
impl QueueRepository for PostgresQueueRepository {
    async fn insert(&self, record: QueueRecord) -> Result<(), QueueRepositoryError> {
        let result = sqlx::query(
            "INSERT INTO verglas_queues \
             (id,tenant_id,name,database_name,database_deployment_id,container_deployment_id) \
             VALUES ($1,$2,$3,$4,$5,$6)",
        )
        .bind(&record.id)
        .bind(&record.tenant_id)
        .bind(&record.name)
        .bind(&record.placement.database_name)
        .bind(&record.placement.database_deployment_id)
        .bind(&record.placement.container_deployment_id)
        .execute(&self.pool)
        .await;
        match result {
            Ok(_) => Ok(()),
            Err(error) if is_unique_violation(&error) => Err(QueueRepositoryError::Duplicate {
                tenant_id: record.tenant_id,
                name: record.name,
            }),
            Err(error) => Err(repository_error(error)),
        }
    }

    async fn get(
        &self,
        tenant_id: &str,
        name: &str,
    ) -> Result<Option<QueueRecord>, QueueRepositoryError> {
        sqlx::query(
            "SELECT id,tenant_id,name,database_name,database_deployment_id,container_deployment_id \
             FROM verglas_queues WHERE tenant_id=$1 AND name=$2",
        )
        .bind(tenant_id)
        .bind(name)
        .fetch_optional(&self.pool)
        .await
        .map_err(repository_error)?
        .map(|row| queue_record(&row))
        .transpose()
    }

    async fn list(&self, tenant_id: &str) -> Result<Vec<QueueRecord>, QueueRepositoryError> {
        let rows = sqlx::query(
            "SELECT id,tenant_id,name,database_name,database_deployment_id,container_deployment_id \
             FROM verglas_queues WHERE tenant_id=$1 ORDER BY name",
        )
        .bind(tenant_id)
        .fetch_all(&self.pool)
        .await
        .map_err(repository_error)?;
        rows.iter().map(queue_record).collect()
    }

    async fn delete(&self, tenant_id: &str, name: &str) -> Result<bool, QueueRepositoryError> {
        Ok(
            sqlx::query("DELETE FROM verglas_queues WHERE tenant_id=$1 AND name=$2")
                .bind(tenant_id)
                .bind(name)
                .execute(&self.pool)
                .await
                .map_err(repository_error)?
                .rows_affected()
                == 1,
        )
    }
}

/// Managed Neon and queue-container lifecycle implemented by the access service.
#[async_trait]
pub trait QueueProvisioner: Send + Sync {
    /// Reconciles the dedicated database before starting the queue container.
    async fn ensure(&self, plan: &QueuePlan) -> Result<QueuePlacement, String>;

    /// Removes the queue container before deleting its managed Neon deployment.
    async fn delete(&self, placement: &QueuePlacement) -> Result<(), String>;
}

/// Resource lifecycle failures with no implicit creation or fallback path.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum QueueServiceError {
    /// No declared queue has this tenant-local name.
    #[error("queue {tenant_id}/{name} not found")]
    NotFound {
        /// Tenant searched.
        tenant_id: String,
        /// Missing queue name.
        name: String,
    },
    /// Durable declaration access failed.
    #[error(transparent)]
    Repository(#[from] QueueRepositoryError),
    /// One of the two required managed runtimes failed.
    #[error("queue provisioning failed: {0}")]
    Provisioning(String),
}

/// Object-safe queue resource boundary consumed by REST.
#[async_trait]
pub trait QueueManager: Send + Sync {
    /// Creates one declared queue and both dedicated deployments.
    async fn create_queue(&self, plan: QueuePlan) -> Result<QueueView, QueueServiceError>;

    /// Lists explicitly declared queues for a tenant.
    async fn list_queues(&self, tenant_id: &str) -> Result<Vec<QueueView>, QueueServiceError>;

    /// Gets one explicitly declared queue.
    async fn get_queue(&self, tenant_id: &str, name: &str) -> Result<QueueView, QueueServiceError>;

    /// Deletes both deployments and then their declaration.
    async fn delete_queue(&self, tenant_id: &str, name: &str) -> Result<(), QueueServiceError>;
}

/// Couples durable queue declarations to mandatory managed runtime placement.
pub struct QueueService<R, P> {
    repository: R,
    provisioner: P,
}

impl<R, P> QueueService<R, P> {
    /// Creates a queue lifecycle service over explicit durable dependencies.
    pub fn new(repository: R, provisioner: P) -> Self {
        Self {
            repository,
            provisioner,
        }
    }
}

impl<R, P> QueueService<R, P>
where
    R: QueueRepository,
    P: QueueProvisioner,
{
    /// Reconciles every durable declaration without creating undeclared queues.
    pub async fn recover(&self, tenant_id: &str) -> Result<Vec<String>, QueueServiceError> {
        let records = self.repository.list(tenant_id).await?;
        let mut failures = Vec::new();
        for record in records {
            let plan = match (CreateQueueRequest {
                name: record.name.clone(),
            })
            .plan(&record.tenant_id)
            {
                Ok(plan) => plan,
                Err(error) => {
                    failures.push(format!("{}/{}: {error}", record.tenant_id, record.name));
                    continue;
                }
            };
            match self.provisioner.ensure(&plan).await {
                Ok(placement) if placement == record.placement => {}
                Ok(_) => failures.push(format!(
                    "{}/{}: recovered placement differs from its declaration",
                    record.tenant_id, record.name
                )),
                Err(error) => {
                    failures.push(format!("{}/{}: {error}", record.tenant_id, record.name))
                }
            }
        }
        Ok(failures)
    }
}

#[async_trait]
impl<R, P> QueueManager for QueueService<R, P>
where
    R: QueueRepository,
    P: QueueProvisioner,
{
    async fn create_queue(&self, plan: QueuePlan) -> Result<QueueView, QueueServiceError> {
        let placement = self
            .provisioner
            .ensure(&plan)
            .await
            .map_err(QueueServiceError::Provisioning)?;
        let record = QueueRecord::new(
            plan.tenant_id().to_owned(),
            plan.name().to_owned(),
            placement.clone(),
        );
        if let Err(error) = self.repository.insert(record.clone()).await {
            self.provisioner
                .delete(&placement)
                .await
                .map_err(|rollback| {
                    QueueServiceError::Provisioning(format!(
                        "{error}; provisioned runtime rollback failed: {rollback}"
                    ))
                })?;
            return Err(error.into());
        }
        Ok(record.view())
    }

    async fn list_queues(&self, tenant_id: &str) -> Result<Vec<QueueView>, QueueServiceError> {
        Ok(self
            .repository
            .list(tenant_id)
            .await?
            .iter()
            .map(QueueRecord::view)
            .collect())
    }

    async fn get_queue(&self, tenant_id: &str, name: &str) -> Result<QueueView, QueueServiceError> {
        self.repository
            .get(tenant_id, name)
            .await?
            .map(|record| record.view())
            .ok_or_else(|| QueueServiceError::NotFound {
                tenant_id: tenant_id.to_owned(),
                name: name.to_owned(),
            })
    }

    async fn delete_queue(&self, tenant_id: &str, name: &str) -> Result<(), QueueServiceError> {
        let record = self.repository.get(tenant_id, name).await?.ok_or_else(|| {
            QueueServiceError::NotFound {
                tenant_id: tenant_id.to_owned(),
                name: name.to_owned(),
            }
        })?;
        self.provisioner
            .delete(&record.placement)
            .await
            .map_err(QueueServiceError::Provisioning)?;
        if !self.repository.delete(tenant_id, name).await? {
            return Err(QueueServiceError::NotFound {
                tenant_id: tenant_id.to_owned(),
                name: name.to_owned(),
            });
        }
        Ok(())
    }
}

/// Decodes one complete resource record from the system database.
fn queue_record(row: &sqlx::postgres::PgRow) -> Result<QueueRecord, QueueRepositoryError> {
    Ok(QueueRecord {
        id: row.try_get("id").map_err(repository_error)?,
        tenant_id: row.try_get("tenant_id").map_err(repository_error)?,
        name: row.try_get("name").map_err(repository_error)?,
        placement: QueuePlacement {
            database_name: row.try_get("database_name").map_err(repository_error)?,
            database_deployment_id: row
                .try_get("database_deployment_id")
                .map_err(repository_error)?,
            container_deployment_id: row
                .try_get("container_deployment_id")
                .map_err(repository_error)?,
        },
    })
}

/// Hides PostgreSQL detail behind the durable resource boundary.
fn repository_error(error: sqlx::Error) -> QueueRepositoryError {
    QueueRepositoryError::Backend(error.to_string())
}

/// Detects only PostgreSQL unique-constraint violations.
fn is_unique_violation(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(|database| database.code())
        .is_some_and(|code| code == "23505")
}
