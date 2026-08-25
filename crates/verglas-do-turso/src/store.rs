//! One serialized Turso database per Durable Object.
//!
//! Production stores always use Turso sync with an explicit remote URL and
//! token. The local-only constructor is feature-gated for tests and is never a
//! runtime fallback. Event transactions use explicit `BEGIN IMMEDIATE`,
//! `COMMIT`, and `ROLLBACK` on one connection so their lifetime can cross WIT
//! calls safely.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::Value as JsonValue;
use tokio::sync::{Mutex, OwnedMutexGuard, RwLock};
use turso::sync::AuthTokenFn;
use turso::{Connection, Value};

use crate::error::{Error, Result};
use crate::outbox::{OutboxKey, OutboxRecord, StreamAppenderHandle};
use crate::rows::rows_to_json;
use crate::schema::{
    WORKER_ALARM_TABLE, create_reserved_tables, validate_reserved_tables, validate_tenant_sql,
};

static NEXT_RELAY_ID: AtomicU64 = AtomicU64::new(1);

/// A static or rotating Turso authentication token.
#[derive(Clone)]
pub enum AuthToken {
    /// Uses one bearer token for every sync request.
    Static(String),
    /// Calls the provider before every sync request.
    Provider(AuthTokenFn),
}

impl From<String> for AuthToken {
    /// Converts a static owned token into an authentication configuration.
    fn from(value: String) -> Self {
        Self::Static(value)
    }
}

impl From<&str> for AuthToken {
    /// Converts a static borrowed token into an authentication configuration.
    fn from(value: &str) -> Self {
        Self::Static(value.to_owned())
    }
}

impl From<AuthTokenFn> for AuthToken {
    /// Converts a Turso asynchronous token provider into an authentication configuration.
    fn from(value: AuthTokenFn) -> Self {
        Self::Provider(value)
    }
}

/// Database backend selected by an explicit production or test constructor.
#[derive(Clone)]
enum Backend {
    /// Turso sync database with remote durability.
    Remote(turso::sync::Database),
    /// Local-only database available only through the test-support feature.
    #[cfg(feature = "test-support")]
    Local(turso::Database),
}

impl Backend {
    /// Opens the shared connection for this backend.
    async fn connect(&self) -> Result<Connection> {
        match self {
            Self::Remote(database) => Ok(database.connect().await?),
            #[cfg(feature = "test-support")]
            Self::Local(database) => Ok(database.connect()?),
        }
    }

    /// Pulls remote changes before a store is allowed to serve events.
    async fn pull(&self) -> Result<()> {
        match self {
            Self::Remote(database) => {
                database.pull().await?;
                Ok(())
            }
            #[cfg(feature = "test-support")]
            Self::Local(_) => Ok(()),
        }
    }

    /// Pushes local WAL state to the configured durability boundary.
    async fn push(&self) -> Result<()> {
        match self {
            Self::Remote(database) => Ok(database.push().await?),
            #[cfg(feature = "test-support")]
            Self::Local(_) => Ok(()),
        }
    }
}

/// Shared Turso state for one Durable Object identity.
#[derive(Clone)]
pub struct TursoStore {
    /// DO identity used in deterministic outbox keys.
    source_do_id: Arc<str>,
    /// Local database path whose complete sidecar family stays under the DO root.
    local_path: Arc<PathBuf>,
    /// Explicitly selected Turso local or remote backend.
    backend: Backend,
    /// One connection shared by all event WIT calls.
    connection: Arc<Mutex<Connection>>,
    /// Serializes event and outbox-control transactions.
    event_lock: Arc<Mutex<()>>,
    /// Optional Stream binding injected by the product composition layer.
    appender: Arc<RwLock<Option<StreamAppenderHandle>>>,
    /// Prevents a connection-local router from replacing an explicit appender.
    appender_external: Arc<RwLock<bool>>,
}

impl TursoStore {
    /// Opens a production Turso database and validates its reserved schema.
    ///
    /// This constructor performs remote bootstrap, an explicit pull, schema
    /// creation/validation, and a schema push before returning to the caller.
    /// It never opens a local-only fallback when remote sync fails.
    pub async fn open<P, U, A, N>(
        local_path: P,
        remote_url: U,
        auth_token: A,
        client_name: N,
    ) -> Result<Self>
    where
        P: AsRef<Path>,
        U: Into<String>,
        A: Into<AuthToken>,
        N: Into<String>,
    {
        let path = local_path.as_ref().to_path_buf();
        create_parent(&path).await?;
        let client_name = client_name.into();
        let mut builder = turso::sync::Builder::new_remote(&path_to_string(&path))
            .with_remote_url(remote_url)
            .with_client_name(client_name.clone())
            .with_logical_mvcc_pull(false);
        builder = match auth_token.into() {
            AuthToken::Static(token) => builder.with_auth_token(token),
            AuthToken::Provider(provider) => builder.with_auth_token_fn(move || {
                let provider = provider.clone();
                async move { provider().await }
            }),
        };
        let backend = Backend::Remote(builder.build().await?);
        let store = Self::new(client_name, path, backend).await?;
        store.backend.pull().await?;
        store.ensure_reserved_schema().await?;
        store.validate_schema().await?;
        store.backend.push().await?;
        Ok(store)
    }

    /// Opens an explicit local-only store for tests and local seam fixtures.
    #[cfg(feature = "test-support")]
    pub async fn open_for_test<P, N>(local_path: P, client_name: N) -> Result<Self>
    where
        P: AsRef<Path>,
        N: Into<String>,
    {
        let path = local_path.as_ref().to_path_buf();
        create_parent(&path).await?;
        let backend = Backend::Local(
            turso::Builder::new_local(&path_to_string(&path))
                .build()
                .await?,
        );
        let store = Self::new(client_name, path, backend).await?;
        store.ensure_reserved_schema().await?;
        store.validate_schema().await?;
        Ok(store)
    }

    /// Creates the shared connection and synchronization gates for one backend.
    async fn new<N>(client_name: N, path: PathBuf, backend: Backend) -> Result<Self>
    where
        N: Into<String>,
    {
        let connection = backend.connect().await?;
        Ok(Self {
            source_do_id: Arc::from(client_name.into()),
            local_path: Arc::new(path),
            backend,
            connection: Arc::new(Mutex::new(connection)),
            event_lock: Arc::new(Mutex::new(())),
            appender: Arc::new(RwLock::new(None)),
            appender_external: Arc::new(RwLock::new(false)),
        })
    }

    /// Returns the DO identity used in outbox record identities.
    pub fn source_do_id(&self) -> &str {
        &self.source_do_id
    }

    /// Returns the local Turso database path and its sidecar root.
    pub fn local_path(&self) -> &Path {
        self.local_path.as_path()
    }

    /// Installs an explicit product Stream binding for transactional outbox delivery.
    pub async fn set_stream_appender(&self, appender: StreamAppenderHandle) {
        let mut external = self.appender_external.write().await;
        *external = true;
        *self.appender.write().await = Some(appender);
    }

    /// Installs the current connection's real Stream router unless a product
    /// composition already supplied an explicit appender.
    pub async fn set_runtime_stream_appender(&self, appender: StreamAppenderHandle) {
        let external = self.appender_external.write().await;
        if !*external {
            *self.appender.write().await = Some(appender);
        }
    }

    /// Removes a connection-local appender while preserving explicit product wiring.
    pub async fn clear_runtime_stream_appender(&self) {
        let external = self.appender_external.write().await;
        if !*external {
            *self.appender.write().await = None;
        }
    }

    /// Pulls remote state while no event transaction is active.
    pub async fn pull(&self) -> Result<()> {
        let _guard = self.event_lock.clone().lock_owned().await;
        self.backend.pull().await
    }

    /// Pushes local state to the remote Turso durability boundary.
    pub async fn push(&self) -> Result<()> {
        let _guard = self.event_lock.clone().lock_owned().await;
        self.backend.push().await
    }

    /// Returns the currently committed durable alarm.
    pub async fn alarm(&self) -> Result<Option<u64>> {
        let _guard = self.event_lock.clone().lock_owned().await;
        let connection = self.connection.lock().await;
        read_alarm(&connection).await
    }

    /// Begins one serialized event transaction after draining prior outbox work.
    pub async fn begin_event(&self) -> Result<TursoEvent> {
        self.drain_outbox().await?;
        let event_guard = self.event_lock.clone().lock_owned().await;
        let connection = self.connection.lock().await;
        rollback_dangling(&connection).await?;
        connection.execute("BEGIN IMMEDIATE", ()).await?;
        if let Err(error) = connection
            .execute(
                "UPDATE __verglas_event_sequence SET next_sequence = next_sequence + 1 WHERE id = 1",
                (),
            )
            .await
        {
            rollback_quiet(&connection).await;
            return Err(error.into());
        }
        let mut rows = match connection
            .query(
                "SELECT next_sequence FROM __verglas_event_sequence WHERE id = 1",
                (),
            )
            .await
        {
            Ok(rows) => rows,
            Err(error) => {
                rollback_quiet(&connection).await;
                return Err(error.into());
            }
        };
        let sequence = match rows.next().await {
            Ok(Some(row)) => match row.get_value(0)? {
                Value::Integer(value) if value >= 0 => value as u64,
                value => {
                    rollback_quiet(&connection).await;
                    return Err(Error::InvalidSchema(format!(
                        "event sequence has invalid value {value:?}"
                    )));
                }
            },
            Ok(None) => {
                rollback_quiet(&connection).await;
                return Err(Error::InvalidSchema(
                    "event sequence row is missing".to_owned(),
                ));
            }
            Err(error) => {
                rollback_quiet(&connection).await;
                return Err(error.into());
            }
        };
        drop(connection);
        Ok(TursoEvent {
            source_do_id: Arc::clone(&self.source_do_id),
            event_sequence: sequence,
            backend: self.backend.clone(),
            connection: Arc::clone(&self.connection),
            event_guard,
            next_record_index: AtomicU32::new(0),
            finished: false,
        })
    }

    /// Ensures missing reserved tables exist without replacing user state.
    async fn ensure_reserved_schema(&self) -> Result<()> {
        let _guard = self.event_lock.clone().lock_owned().await;
        let connection = self.connection.lock().await;
        rollback_dangling(&connection).await?;
        connection.execute("BEGIN IMMEDIATE", ()).await?;
        if let Err(error) = create_reserved_tables(&connection).await {
            rollback_quiet(&connection).await;
            return Err(error);
        }
        if let Err(error) = connection.execute("COMMIT", ()).await {
            rollback_quiet(&connection).await;
            return Err(error.into());
        }
        Ok(())
    }

    /// Validates reserved table columns before a caller can serve events.
    async fn validate_schema(&self) -> Result<()> {
        let _guard = self.event_lock.clone().lock_owned().await;
        let connection = self.connection.lock().await;
        validate_reserved_tables(&connection).await
    }

    /// Delivers all pending outbox records through the injected Stream binding.
    pub async fn drain_outbox(&self) -> Result<()> {
        let now = now_millis();
        self.reclaim_expired_outbox(now).await?;
        if self.has_unexpired_inflight(now).await? {
            return Err(Error::OutboxInFlight);
        }
        let pending = self.pending_outbox(256).await?;
        if pending.is_empty() {
            return Ok(());
        }
        // A prior source commit may have reached local WAL while its push failed;
        // never append to Stream until this retry reaches Turso's boundary.
        self.backend.push().await?;
        let appender = self
            .appender
            .read()
            .await
            .clone()
            .ok_or(Error::OutboxUnavailable)?;
        let lease_owner = format!("relay-{}", NEXT_RELAY_ID.fetch_add(1, Ordering::Relaxed));
        let lease_expires_at = now_millis().saturating_add(30_000);
        for record in &pending {
            let claimed = self
                .mark_outbox_inflight(&record.key, &lease_owner, lease_expires_at)
                .await?;
            if !claimed {
                return Err(Error::OutboxLeaseMismatch);
            }
        }
        appender.append(pending.clone()).await?;
        for record in &pending {
            let delivered = self
                .mark_outbox_delivered(&record.key, &lease_owner)
                .await?;
            if !delivered {
                return Err(Error::OutboxLeaseMismatch);
            }
        }
        Ok(())
    }

    /// Lists pending outbox rows in deterministic event order.
    pub async fn pending_outbox(&self, limit: u32) -> Result<Vec<OutboxRecord>> {
        let _guard = self.event_lock.clone().lock_owned().await;
        let connection = self.connection.lock().await;
        let mut statement = connection
            .prepare(
                "SELECT stream_binding, stream_name, source_do_id, event_sequence, record_index, payload
                 FROM __verglas_outbox
                 WHERE state = 'pending'
                 ORDER BY event_sequence, record_index
                 LIMIT ?1",
            )
            .await?;
        let mut rows = statement.query([i64::from(limit)]).await?;
        let mut records = Vec::new();
        while let Some(row) = rows.next().await? {
            let stream_binding = text_value(&row, 0, "outbox Stream binding")?;
            let stream_name = text_value(&row, 1, "outbox Stream name")?;
            let source_do_id = text_value(&row, 2, "outbox source")?;
            let event_sequence = nonnegative_integer(&row, 3, "outbox sequence")?;
            let record_index = nonnegative_integer(&row, 4, "outbox record index")?;
            let payload = text_value(&row, 5, "outbox payload")?;
            records.push(OutboxRecord::new(
                stream_binding,
                stream_name,
                OutboxKey::new(source_do_id, event_sequence, record_index as u32),
                serde_json::from_str(&payload)?,
            ));
        }
        Ok(records)
    }

    /// Reports whether a relay lease remains active after expired claims are reclaimed.
    async fn has_unexpired_inflight(&self, now: u64) -> Result<bool> {
        let _guard = self.event_lock.clone().lock_owned().await;
        let connection = self.connection.lock().await;
        let mut rows = connection
            .query(
                "SELECT 1 FROM __verglas_outbox
                 WHERE state = 'inflight' AND lease_expires_at > ?1 LIMIT 1",
                [now as i64],
            )
            .await?;
        Ok(rows.next().await?.is_some())
    }

    /// Marks one pending outbox row inflight with an expiring relay lease.
    pub async fn mark_outbox_inflight(
        &self,
        key: &OutboxKey,
        lease_owner: &str,
        lease_expires_at: u64,
    ) -> Result<bool> {
        let _guard = self.event_lock.clone().lock_owned().await;
        let connection = self.connection.lock().await;
        begin_control(&connection).await?;
        let updated = match connection
            .execute(
                "UPDATE __verglas_outbox
                 SET state = 'inflight', lease_owner = ?4, lease_expires_at = ?5
                 WHERE source_do_id = ?1 AND event_sequence = ?2 AND record_index = ?3
                   AND state = 'pending'",
                (
                    key.source_do_id.as_str(),
                    key.event_sequence as i64,
                    key.record_index as i64,
                    lease_owner,
                    lease_expires_at as i64,
                ),
            )
            .await
        {
            Ok(updated) => updated == 1,
            Err(error) => {
                rollback_quiet(&connection).await;
                return Err(error.into());
            }
        };
        finish_control(&connection).await?;
        if updated {
            self.backend.push().await?;
        }
        Ok(updated)
    }

    /// Marks one inflight outbox row delivered after Stream acknowledgement.
    pub async fn mark_outbox_delivered(&self, key: &OutboxKey, lease_owner: &str) -> Result<bool> {
        let _guard = self.event_lock.clone().lock_owned().await;
        let connection = self.connection.lock().await;
        begin_control(&connection).await?;
        let updated = match connection
            .execute(
                "UPDATE __verglas_outbox
                 SET state = 'delivered', delivered_at = ?4,
                     lease_owner = NULL, lease_expires_at = NULL
                 WHERE source_do_id = ?1 AND event_sequence = ?2 AND record_index = ?3
                   AND state = 'inflight' AND lease_owner = ?5",
                (
                    key.source_do_id.as_str(),
                    key.event_sequence as i64,
                    key.record_index as i64,
                    now_millis() as i64,
                    lease_owner,
                ),
            )
            .await
        {
            Ok(updated) => updated == 1,
            Err(error) => {
                rollback_quiet(&connection).await;
                return Err(error.into());
            }
        };
        finish_control(&connection).await?;
        if updated {
            self.backend.push().await?;
        }
        Ok(updated)
    }

    /// Reclaims inflight rows whose relay leases have expired.
    pub async fn reclaim_expired_outbox(&self, now: u64) -> Result<u64> {
        let _guard = self.event_lock.clone().lock_owned().await;
        let connection = self.connection.lock().await;
        begin_control(&connection).await?;
        let updated = match connection
            .execute(
                "UPDATE __verglas_outbox
                 SET state = 'pending', lease_owner = NULL, lease_expires_at = NULL
                 WHERE state = 'inflight' AND lease_expires_at <= ?1",
                [now as i64],
            )
            .await
        {
            Ok(updated) => updated,
            Err(error) => {
                rollback_quiet(&connection).await;
                return Err(error.into());
            }
        };
        finish_control(&connection).await?;
        if updated > 0 {
            self.backend.push().await?;
        }
        Ok(updated)
    }
}

/// One WIT-spanning event transaction on the store's single connection.
pub struct TursoEvent {
    /// Source identity copied into every outbox row.
    source_do_id: Arc<str>,
    /// Event sequence allocated inside this transaction.
    event_sequence: u64,
    /// Backend used to push after local commit.
    backend: Backend,
    /// Shared Turso connection used by every event operation.
    connection: Arc<Mutex<Connection>>,
    /// Exclusive event gate held until commit or rollback completes.
    event_guard: OwnedMutexGuard<()>,
    /// Next record index allocated by this event's Stream sends.
    next_record_index: AtomicU32,
    /// Tracks whether an explicit terminal operation completed.
    finished: bool,
}

impl TursoEvent {
    /// Returns the event sequence allocated by the transaction.
    pub fn event_sequence(&self) -> u64 {
        self.event_sequence
    }

    /// Executes one tenant DDL or DML statement in this event transaction.
    pub async fn execute(&self, statement: &str) -> Result<u64> {
        self.ensure_active()?;
        validate_tenant_sql(statement)?;
        let connection = self.connection.lock().await;
        Ok(connection.execute(statement, ()).await?)
    }

    /// Executes one tenant query and returns honest JSON rows.
    pub async fn query_json(&self, statement: &str) -> Result<JsonValue> {
        self.ensure_active()?;
        validate_tenant_sql(statement)?;
        let connection = self.connection.lock().await;
        let mut statement = connection.prepare(statement).await?;
        let mut rows = statement.query(()).await?;
        Ok(JsonValue::Array(rows_to_json(&mut rows).await?))
    }

    /// Reads one staged or committed KV value in the event snapshot.
    pub async fn get_kv(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.ensure_active()?;
        let connection = self.connection.lock().await;
        let mut rows = connection
            .query("SELECT value FROM __worker_kv WHERE key = ?1", [key])
            .await?;
        match rows.next().await? {
            Some(row) => match row.get_value(0)? {
                Value::Null => Ok(None),
                Value::Blob(value) => Ok(Some(value)),
                value => Err(Error::InvalidSchema(format!(
                    "KV value has unexpected type {value:?}"
                ))),
            },
            None => Ok(None),
        }
    }

    /// Lists live KV keys in sorted order with a hard result bound.
    pub async fn list_kv(&self, prefix: &str, limit: u32) -> Result<Vec<String>> {
        self.ensure_active()?;
        let connection = self.connection.lock().await;
        let mut rows = connection
            .query("SELECT key FROM __worker_kv ORDER BY key", ())
            .await?;
        let mut keys = Vec::new();
        while let Some(row) = rows.next().await? {
            let key = text_value(&row, 0, "KV key")?;
            if key.starts_with(prefix) && keys.len() < limit as usize {
                keys.push(key);
            }
        }
        Ok(keys)
    }

    /// Inserts or replaces one KV value in this event transaction.
    pub async fn put_kv(&self, key: &str, value: Vec<u8>) -> Result<()> {
        self.ensure_active()?;
        let connection = self.connection.lock().await;
        connection
            .execute(
                "INSERT INTO __worker_kv (key, value) VALUES (?1, ?2)
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )
            .await?;
        Ok(())
    }

    /// Deletes one KV key and reports whether it existed in the event view.
    pub async fn delete_kv(&self, key: &str) -> Result<bool> {
        self.ensure_active()?;
        let existed = self.get_kv(key).await?.is_some();
        let connection = self.connection.lock().await;
        connection
            .execute("DELETE FROM __worker_kv WHERE key = ?1", [key])
            .await?;
        Ok(existed)
    }

    /// Reads the event's single durable alarm.
    pub async fn get_alarm(&self) -> Result<Option<u64>> {
        self.ensure_active()?;
        let connection = self.connection.lock().await;
        read_alarm(&connection).await
    }

    /// Sets or replaces the event's single durable alarm.
    pub async fn set_alarm(&self, deadline_ms: u64) -> Result<()> {
        self.ensure_active()?;
        let connection = self.connection.lock().await;
        connection
            .execute(
                "INSERT INTO __worker_alarm (id, deadline_ms) VALUES (1, ?1)
                 ON CONFLICT(id) DO UPDATE SET deadline_ms = excluded.deadline_ms",
                [deadline_ms as i64],
            )
            .await?;
        Ok(())
    }

    /// Clears the event's single durable alarm.
    pub async fn delete_alarm(&self) -> Result<()> {
        self.ensure_active()?;
        let connection = self.connection.lock().await;
        connection
            .execute(
                "UPDATE __worker_alarm SET deadline_ms = NULL WHERE id = 1",
                (),
            )
            .await?;
        Ok(())
    }

    /// Reads one WebSocket attachment in the event transaction.
    pub async fn get_attachment(&self, socket: u64) -> Result<Option<Vec<u8>>> {
        self.ensure_active()?;
        let connection = self.connection.lock().await;
        let mut rows = connection
            .query(
                "SELECT value FROM __worker_attachments WHERE socket = ?1",
                [socket as i64],
            )
            .await?;
        match rows.next().await? {
            Some(row) => match row.get_value(0)? {
                Value::Null => Ok(None),
                Value::Blob(value) => Ok(Some(value)),
                value => Err(Error::InvalidSchema(format!(
                    "attachment value has unexpected type {value:?}"
                ))),
            },
            None => Ok(None),
        }
    }

    /// Inserts or replaces one WebSocket attachment in this event.
    pub async fn set_attachment(&self, socket: u64, value: Vec<u8>) -> Result<()> {
        self.ensure_active()?;
        let connection = self.connection.lock().await;
        connection
            .execute(
                "INSERT INTO __worker_attachments (socket, value) VALUES (?1, ?2)
                 ON CONFLICT(socket) DO UPDATE SET value = excluded.value",
                (socket as i64, value),
            )
            .await?;
        Ok(())
    }

    /// Lists sockets with live attachments in ascending order.
    pub async fn attached_sockets(&self) -> Result<Vec<u64>> {
        self.ensure_active()?;
        let connection = self.connection.lock().await;
        let mut rows = connection
            .query(
                "SELECT socket FROM __worker_attachments WHERE value IS NOT NULL ORDER BY socket",
                (),
            )
            .await?;
        let mut sockets = Vec::new();
        while let Some(row) = rows.next().await? {
            sockets.push(nonnegative_integer(&row, 0, "attachment socket")?);
        }
        Ok(sockets)
    }

    /// Adds a JSON array of logical Stream records to this event transaction.
    ///
    /// Record indexes are allocated once per event, so multiple `send` calls
    /// cannot collide and every retry reuses the committed triple identity.
    pub async fn append_stream_records(
        &self,
        stream_binding: &str,
        stream_name: &str,
        payloads: Vec<JsonValue>,
    ) -> Result<Vec<OutboxKey>> {
        self.ensure_active()?;
        if stream_binding.trim().is_empty() || stream_name.trim().is_empty() {
            return Err(Error::InvalidSchema(
                "Stream binding and name must be nonempty".to_owned(),
            ));
        }
        let count = u32::try_from(payloads.len()).map_err(|_| {
            Error::InvalidSchema("Stream send contains too many records".to_owned())
        })?;
        let start = self
            .next_record_index
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(count)
            })
            .map_err(|_| Error::InvalidSchema("event Stream record index overflow".to_owned()))?;
        let connection = self.connection.lock().await;
        let mut keys = Vec::with_capacity(payloads.len());
        for (offset, payload) in payloads.into_iter().enumerate() {
            let record_index = start
                + u32::try_from(offset)
                    .map_err(|_| Error::InvalidSchema("Stream record index overflow".to_owned()))?;
            let key = OutboxKey::new(
                self.source_do_id.to_string(),
                self.event_sequence,
                record_index,
            );
            let payload = serde_json::to_string(&payload)?;
            connection
                .execute(
                    "INSERT INTO __verglas_outbox
                     (stream_binding, stream_name, source_do_id, event_sequence, record_index,
                      event_id, payload, state)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'pending')",
                    (
                        stream_binding,
                        stream_name,
                        key.source_do_id.as_str(),
                        key.event_sequence as i64,
                        key.record_index as i64,
                        key.event_id(),
                        payload,
                    ),
                )
                .await?;
            keys.push(key);
        }
        Ok(keys)
    }

    /// Commits the local event transaction and pushes it to remote Turso.
    pub async fn commit_and_push(mut self) -> Result<()> {
        self.ensure_active()?;
        let connection = self.connection.lock().await;
        if let Err(error) = connection.execute("COMMIT", ()).await {
            rollback_quiet(&connection).await;
            return Err(error.into());
        }
        drop(connection);
        self.finished = true;
        self.backend.push().await
    }

    /// Rolls back every state and outbox mutation in this event.
    pub async fn rollback(mut self) -> Result<()> {
        self.ensure_active()?;
        let connection = self.connection.lock().await;
        connection.execute("ROLLBACK", ()).await?;
        self.finished = true;
        Ok(())
    }

    /// Rejects operations after commit or rollback.
    fn ensure_active(&self) -> Result<()> {
        if self.finished {
            Err(Error::EventFinished)
        } else {
            Ok(())
        }
    }
}

impl Drop for TursoEvent {
    /// Leaves a dropped transaction for the next serialized opener to roll back.
    fn drop(&mut self) {
        // Turso's async API cannot be awaited from Drop. `begin_event` checks
        // autocommit and rolls back this abandoned explicit transaction before
        // opening the next event, while the held gate prevents an interleaving.
        let _ = &self.event_guard;
    }
}

/// Creates the local parent directory while retaining all Turso sidecars nearby.
async fn create_parent(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    Ok(())
}

/// Converts a path to the owned string required by Turso builders.
fn path_to_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

/// Rolls back an abandoned explicit transaction before opening a new event.
async fn rollback_dangling(connection: &Connection) -> Result<()> {
    if !connection.is_autocommit()? {
        connection.execute("ROLLBACK", ()).await?;
    }
    Ok(())
}

/// Attempts a rollback while preserving the original failure being returned.
async fn rollback_quiet(connection: &Connection) {
    let autocommit = match connection.is_autocommit() {
        Ok(value) => value,
        Err(_) => return,
    };
    if autocommit {
        return;
    }
    let _ = connection.execute("ROLLBACK", ()).await;
}

/// Starts one short control transaction used by outbox state transitions.
async fn begin_control(connection: &Connection) -> Result<()> {
    rollback_dangling(connection).await?;
    connection.execute("BEGIN IMMEDIATE", ()).await?;
    Ok(())
}

/// Commits one outbox control transaction.
async fn finish_control(connection: &Connection) -> Result<()> {
    if let Err(error) = connection.execute("COMMIT", ()).await {
        rollback_quiet(connection).await;
        return Err(error.into());
    }
    Ok(())
}

/// Reads the single alarm row and preserves its nullable state.
async fn read_alarm(connection: &Connection) -> Result<Option<u64>> {
    let mut rows = connection
        .query(
            &format!("SELECT deadline_ms FROM {WORKER_ALARM_TABLE} WHERE id = 1"),
            (),
        )
        .await?;
    match rows.next().await? {
        Some(row) => match row.get_value(0)? {
            Value::Null => Ok(None),
            Value::Integer(value) if value >= 0 => Ok(Some(value as u64)),
            value => Err(Error::InvalidSchema(format!(
                "alarm deadline has unexpected value {value:?}"
            ))),
        },
        None => Ok(None),
    }
}

/// Reads a required text column from one Turso row.
fn text_value(row: &turso::Row, index: usize, field: &str) -> Result<String> {
    match row.get_value(index)? {
        Value::Text(value) => Ok(value),
        value => Err(Error::InvalidSchema(format!(
            "{field} has unexpected value {value:?}"
        ))),
    }
}

/// Reads a nonnegative integer column from one Turso row.
fn nonnegative_integer(row: &turso::Row, index: usize, field: &str) -> Result<u64> {
    match row.get_value(index)? {
        Value::Integer(value) if value >= 0 => Ok(value as u64),
        value => Err(Error::InvalidSchema(format!(
            "{field} has unexpected value {value:?}"
        ))),
    }
}

/// Returns the current Unix epoch in milliseconds for outbox leases.
fn now_millis() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => u64::try_from(duration.as_millis()).unwrap_or(u64::MAX),
        Err(_) => 0,
    }
}
