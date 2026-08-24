//! Transactional WorkerStorage backed by one Turso Durable Object database.
//!
//! Every capability call uses the same explicit Turso event transaction. Turso
//! itself supplies read-your-writes for SQL and reserved Worker tables; this
//! adapter only owns transaction lifetime, WIT error conversion, and the
//! commit/push/outbox publication boundary.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::Mutex;
use verglas_do_turso::{OutboxKey, TursoEvent, TursoStore};
use verglas_do_wasm::{HostError, WorkerStorage};

/// Turso-backed transactional storage capability for one Worker event.
///
/// The event is held until [`Self::commit`] or [`Self::rollback`] is called.
/// A successful commit first reaches Turso's local commit and remote `push()`
/// boundary, then drains any enabled Stream outbox before the caller releases
/// its output permit.
#[derive(Clone)]
pub struct TursoWorkerStorage {
    /// Store whose one serialized connection owns the event transaction.
    store: Arc<TursoStore>,
    /// Event transaction shared by all WIT capability calls for this event.
    event: Arc<Mutex<Option<TursoEvent>>>,
}

impl TursoWorkerStorage {
    /// Begins one event transaction after the store has drained prior outbox work.
    pub async fn begin(store: Arc<TursoStore>) -> Result<Self, HostError> {
        let event = store.begin_event().await.map_err(turso_error)?;
        Ok(Self {
            store,
            event: Arc::new(Mutex::new(Some(event))),
        })
    }

    /// Commits locally, pushes to remote Turso, and drains enabled outbox rows.
    pub async fn commit(&self) -> Result<(), HostError> {
        let event = self.take_event().await?;
        event.commit_and_push().await.map_err(turso_error)?;
        self.store.drain_outbox().await.map_err(turso_error)
    }

    /// Rolls back the open event transaction after handler failure.
    pub async fn rollback(&self) -> Result<(), HostError> {
        let event = self.take_event().await?;
        event.rollback().await.map_err(turso_error)
    }

    /// Appends one selected JSON record to this event's transactional outbox.
    pub async fn append_outbox(
        &self,
        record_index: u32,
        payload: Value,
    ) -> Result<OutboxKey, HostError> {
        let event = self.event_ref().await?;
        let event = event
            .as_ref()
            .ok_or_else(|| HostError::backend("event transaction has already been finished"))?;
        event
            .append_outbox(record_index, payload)
            .await
            .map_err(turso_error)
    }

    /// Persists one WebSocket attachment in this event transaction.
    pub async fn set_attachment(&self, socket: u64, value: Vec<u8>) -> Result<(), HostError> {
        let event = self.event_ref().await?;
        let event = event
            .as_ref()
            .ok_or_else(|| HostError::backend("event transaction has already been finished"))?;
        event
            .set_attachment(socket, value)
            .await
            .map_err(turso_error)
    }

    /// Reads one WebSocket attachment from this event transaction.
    pub async fn get_attachment(&self, socket: u64) -> Result<Option<Vec<u8>>, HostError> {
        let event = self.event_ref().await?;
        let event = event
            .as_ref()
            .ok_or_else(|| HostError::backend("event transaction has already been finished"))?;
        event.get_attachment(socket).await.map_err(turso_error)
    }

    /// Lists WebSocket sockets with live attachments in this event transaction.
    pub async fn attached_sockets(&self) -> Result<Vec<u64>, HostError> {
        let event = self.event_ref().await?;
        let event = event
            .as_ref()
            .ok_or_else(|| HostError::backend("event transaction has already been finished"))?;
        event.attached_sockets().await.map_err(turso_error)
    }

    /// Removes the event from the shared slot and reports a terminal-state error.
    async fn take_event(&self) -> Result<TursoEvent, HostError> {
        self.event
            .lock()
            .await
            .take()
            .ok_or_else(|| HostError::backend("event transaction has already been finished"))
    }

    /// Borrows the active event while one capability operation is in flight.
    async fn event_ref(
        &self,
    ) -> Result<tokio::sync::MutexGuard<'_, Option<TursoEvent>>, HostError> {
        let event = self.event.lock().await;
        if event.is_none() {
            return Err(HostError::backend(
                "event transaction has already been finished",
            ));
        }
        Ok(event)
    }
}

/// Converts one Turso failure into the stable WIT host error.
fn turso_error(error: verglas_do_turso::Error) -> HostError {
    HostError::backend(error.to_string())
}

#[async_trait]
impl WorkerStorage for TursoWorkerStorage {
    /// Reads one KV key from the current Turso event snapshot.
    async fn get(&self, key: String) -> Result<Option<Vec<u8>>, HostError> {
        let event = self.event_ref().await?;
        let event = event
            .as_ref()
            .ok_or_else(|| HostError::backend("event transaction has already been finished"))?;
        event.get_kv(&key).await.map_err(turso_error)
    }

    /// Writes one KV value in the current Turso event transaction.
    async fn put(&self, key: String, value: Vec<u8>) -> Result<(), HostError> {
        let event = self.event_ref().await?;
        let event = event
            .as_ref()
            .ok_or_else(|| HostError::backend("event transaction has already been finished"))?;
        event.put_kv(&key, value).await.map_err(turso_error)
    }

    /// Deletes one KV key and reports whether a live value existed.
    async fn delete(&self, key: String) -> Result<bool, HostError> {
        let event = self.event_ref().await?;
        let event = event
            .as_ref()
            .ok_or_else(|| HostError::backend("event transaction has already been finished"))?;
        event.delete_kv(&key).await.map_err(turso_error)
    }

    /// Lists live KV keys by prefix with the WIT result bound.
    async fn list(&self, prefix: String, limit: u32) -> Result<Vec<String>, HostError> {
        let event = self.event_ref().await?;
        let event = event
            .as_ref()
            .ok_or_else(|| HostError::backend("event transaction has already been finished"))?;
        event.list_kv(&prefix, limit).await.map_err(turso_error)
    }

    /// Rejects the removed Arrow IPC SQL capability honestly.
    async fn sql(&self, _statement: String) -> Result<Vec<u8>, HostError> {
        Err(HostError::Unsupported {
            operation: "Arrow IPC SQL was removed; use sql_rows",
        })
    }

    /// Executes SQL and returns Turso rows as one JSON array.
    async fn sql_rows(&self, statement: String) -> Result<String, HostError> {
        let event = self.event_ref().await?;
        let event = event
            .as_ref()
            .ok_or_else(|| HostError::backend("event transaction has already been finished"))?;
        let rows = event.query_json(&statement).await.map_err(turso_error)?;
        serde_json::to_string(&rows).map_err(|error| HostError::backend(error.to_string()))
    }

    /// Sets or replaces the event's single durable alarm.
    async fn set_alarm(&self, epoch_millis: u64) -> Result<(), HostError> {
        let event = self.event_ref().await?;
        let event = event
            .as_ref()
            .ok_or_else(|| HostError::backend("event transaction has already been finished"))?;
        event.set_alarm(epoch_millis).await.map_err(turso_error)
    }

    /// Reads the event's current durable alarm.
    async fn get_alarm(&self) -> Result<Option<u64>, HostError> {
        let event = self.event_ref().await?;
        let event = event
            .as_ref()
            .ok_or_else(|| HostError::backend("event transaction has already been finished"))?;
        event.get_alarm().await.map_err(turso_error)
    }

    /// Clears the event's durable alarm.
    async fn delete_alarm(&self) -> Result<(), HostError> {
        let event = self.event_ref().await?;
        let event = event
            .as_ref()
            .ok_or_else(|| HostError::backend("event transaction has already been finished"))?;
        event.delete_alarm().await.map_err(turso_error)
    }
}
