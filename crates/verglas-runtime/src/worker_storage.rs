//! Transactional WorkerStorage backed by the Durable Object engine.
//!
//! The adapter keeps a per-event overlay for read-your-writes semantics and
//! stages mutations into the engine transaction that the caller commits after
//! the component returns.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_ipc::writer::StreamWriter;
use arrow_json::writer::{JsonArray, WriterBuilder};
use arrow_schema::Schema;
use async_trait::async_trait;
use tokio::sync::Mutex;
use verglas_do_engine::{
    DoEngine, DoSession, DoStorage, DoTransaction, Error as EngineError, SnapshotFence,
    TransactionHandle, WorkerStateView, stage_alarm_clear, stage_alarm_set, stage_attachment,
    stage_kv_delete, stage_kv_put,
};
use verglas_do_wasm::{HostError, WorkerStorage};

/// Private state that makes staged values visible inside the current event.
#[derive(Clone, Default)]
struct EventOverlay {
    /// Latest staged value for each key; `None` is a tombstone.
    kv: BTreeMap<String, Option<Vec<u8>>>,
    /// Staged alarm state; outer `None` means no alarm mutation yet.
    alarm: Option<Option<u64>>,
    /// Latest staged attachment for each socket; `None` detaches it.
    attachments: BTreeMap<u64, Option<Vec<u8>>>,
}

/// Engine-backed transactional storage capability for one Worker event.
///
/// The transaction is owned by this adapter until [`Self::commit`] or
/// [`Self::take_transaction`] is called. A caller must keep the adapter alive
/// until all guest calls finish, then commit it before committing the event's
/// output permit.
#[derive(Clone)]
pub struct EngineWorkerStorage {
    /// Engine whose committed Worker tables provide the event snapshot.
    engine: Arc<DoEngine>,
    /// Fixed committed fence captured when this event transaction began.
    snapshot: SnapshotFence,
    /// Private transaction receiving all staged mutations.
    transaction: Arc<Mutex<Option<Box<dyn DoTransaction>>>>,
    /// Event-local read overlay for staged KV and alarm state.
    overlay: Arc<Mutex<EventOverlay>>,
}

impl EngineWorkerStorage {
    /// Creates a Worker storage capability around one open engine transaction.
    pub fn new(engine: Arc<DoEngine>, transaction: Box<dyn DoTransaction>) -> Self {
        let snapshot = SnapshotFence::at(transaction.envelope().base_commit_sequence());
        Self::new_with_snapshot(engine, transaction, snapshot)
    }

    /// Creates storage with the exact fence captured before the event began.
    pub fn new_with_snapshot(
        engine: Arc<DoEngine>,
        transaction: Box<dyn DoTransaction>,
        snapshot: SnapshotFence,
    ) -> Self {
        Self {
            engine,
            snapshot,
            transaction: Arc::new(Mutex::new(Some(transaction))),
            overlay: Arc::new(Mutex::new(EventOverlay::default())),
        }
    }

    /// Removes the open transaction so the caller can commit it itself.
    pub async fn take_transaction(&self) -> Result<Box<dyn DoTransaction>, HostError> {
        self.transaction
            .lock()
            .await
            .take()
            .ok_or_else(|| HostError::backend("event transaction has already been taken"))
    }

    /// Commits the staged engine transaction and returns its receipt.
    pub async fn commit(&self) -> Result<verglas_do_engine::CommitReceipt, HostError> {
        let transaction = self.take_transaction().await?;
        self.engine.commit(transaction).await.map_err(engine_error)
    }

    /// Stages one mutation while retaining the transaction ownership invariant.
    async fn stage<F>(&self, operation: F) -> Result<(), HostError>
    where
        F: FnOnce(&mut dyn DoTransaction) -> verglas_do_engine::Result<()>,
    {
        let mut transaction = self.transaction.lock().await;
        let transaction = transaction
            .as_deref_mut()
            .ok_or_else(|| HostError::backend("event transaction has already been taken"))?;
        operation(transaction).map_err(engine_error)
    }

    /// Returns a committed-state reader for this adapter's engine.
    fn view(&self) -> WorkerStateView<'_> {
        WorkerStateView::new(self.engine.as_ref())
    }

    /// Returns a previously staged KV value, distinguishing absence from a tombstone.
    async fn staged_kv(&self, key: &str) -> Option<Option<Vec<u8>>> {
        self.overlay.lock().await.kv.get(key).cloned()
    }

    /// Returns the staged alarm mutation, if this event has one.
    async fn staged_alarm(&self) -> Option<Option<u64>> {
        self.overlay.lock().await.alarm
    }

    /// Stages one attachment in the current event transaction.
    pub async fn set_attachment(&self, socket: u64, value: Vec<u8>) -> Result<(), HostError> {
        let staged_value = value.clone();
        self.stage(move |transaction| stage_attachment(transaction, socket, Some(staged_value)))
            .await?;
        self.overlay
            .lock()
            .await
            .attachments
            .insert(socket, Some(value));
        Ok(())
    }

    /// Reads one attachment from the event overlay or committed state.
    pub async fn get_attachment(&self, socket: u64) -> Result<Option<Vec<u8>>, HostError> {
        if let Some(value) = self.overlay.lock().await.attachments.get(&socket).cloned() {
            return Ok(value);
        }
        self.view().attachment(socket).await.map_err(engine_error)
    }

    /// Lists attached sockets after applying event-local attachment changes.
    pub async fn attached_sockets(&self) -> Result<Vec<u64>, HostError> {
        let mut sockets = self
            .view()
            .attached_sockets()
            .await
            .map_err(engine_error)?
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();
        for (socket, value) in &self.overlay.lock().await.attachments {
            if value.is_some() {
                sockets.insert(*socket);
            } else {
                sockets.remove(socket);
            }
        }
        Ok(sockets.into_iter().collect())
    }

    /// Executes one SQL statement through the shared event transaction.
    async fn execute_sql(&self, statement: &str) -> Result<Vec<RecordBatch>, HostError> {
        let transaction = TransactionHandle::from_shared(Arc::clone(&self.transaction));
        let session = DoSession::from_transaction(
            Arc::clone(&self.engine),
            self.engine.table_ids().map_err(engine_error)?,
            self.snapshot,
            transaction,
        )
        .map_err(engine_error)?;
        session.execute(statement).await.map_err(engine_error)
    }
}

/// Converts one engine failure to the stable WIT-facing backend error.
fn engine_error(error: EngineError) -> HostError {
    HostError::backend(error.to_string())
}

#[async_trait]
impl WorkerStorage for EngineWorkerStorage {
    /// Reads staged KV first, then the committed Worker-state snapshot.
    async fn get(&self, key: String) -> Result<Option<Vec<u8>>, HostError> {
        if let Some(value) = self.staged_kv(&key).await {
            return Ok(value);
        }
        self.view().kv_get(&key).await.map_err(engine_error)
    }

    /// Stages a KV value and publishes it to this event's read overlay.
    async fn put(&self, key: String, value: Vec<u8>) -> Result<(), HostError> {
        let staged_key = key.clone();
        let staged_value = value.clone();
        self.stage(move |transaction| stage_kv_put(transaction, &staged_key, staged_value))
            .await?;
        self.overlay.lock().await.kv.insert(key, Some(value));
        Ok(())
    }

    /// Stages a tombstone and reports whether a live value existed in the event view.
    async fn delete(&self, key: String) -> Result<bool, HostError> {
        let existed = self.get(key.clone()).await?.is_some();
        let staged_key = key.clone();
        self.stage(move |transaction| stage_kv_delete(transaction, &staged_key))
            .await?;
        self.overlay.lock().await.kv.insert(key, None);
        Ok(existed)
    }

    /// Lists the committed and staged live keys in sorted bounded order.
    async fn list(&self, prefix: String, limit: u32) -> Result<Vec<String>, HostError> {
        let committed = self
            .view()
            .kv_list(&prefix, u32::MAX)
            .await
            .map_err(engine_error)?;
        let overlay = self.overlay.lock().await.clone();
        let mut keys = BTreeMap::new();
        for key in committed {
            keys.insert(key, ());
        }
        for (key, value) in overlay.kv {
            if !key.starts_with(&prefix) {
                continue;
            }
            match value {
                Some(_) => {
                    keys.insert(key, ());
                }
                None => {
                    keys.remove(&key);
                }
            }
        }
        Ok(keys.into_keys().take(limit as usize).collect())
    }

    /// Executes SQL and returns the Arrow IPC stream produced by the event session.
    async fn sql(&self, statement: String) -> Result<Vec<u8>, HostError> {
        let batches = self.execute_sql(&statement).await?;
        let schema = batches
            .first()
            .map(RecordBatch::schema)
            .unwrap_or_else(|| Arc::new(Schema::empty()));
        let mut bytes = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut bytes, &schema)
                .map_err(|error| HostError::backend(error.to_string()))?;
            for batch in &batches {
                writer
                    .write(batch)
                    .map_err(|error| HostError::backend(error.to_string()))?;
            }
            writer
                .finish()
                .map_err(|error| HostError::backend(error.to_string()))?;
        }
        Ok(bytes)
    }

    /// Executes SQL and returns all rows as one JSON array of objects.
    async fn sql_rows(&self, statement: String) -> Result<String, HostError> {
        let batches = self.execute_sql(&statement).await?;
        let references = batches.iter().collect::<Vec<_>>();
        let mut writer = WriterBuilder::new()
            .with_explicit_nulls(true)
            .build::<_, JsonArray>(Vec::new());
        writer
            .write_batches(&references)
            .map_err(|error| HostError::backend(error.to_string()))?;
        writer
            .finish()
            .map_err(|error| HostError::backend(error.to_string()))?;
        String::from_utf8(writer.into_inner())
            .map_err(|error| HostError::backend(error.to_string()))
    }

    /// Stages a replacement for the event's single durable alarm.
    async fn set_alarm(&self, epoch_millis: u64) -> Result<(), HostError> {
        self.stage(move |transaction| stage_alarm_set(transaction, epoch_millis))
            .await?;
        self.overlay.lock().await.alarm = Some(Some(epoch_millis));
        Ok(())
    }

    /// Reads the staged alarm mutation or the committed alarm row.
    async fn get_alarm(&self) -> Result<Option<u64>, HostError> {
        if let Some(value) = self.staged_alarm().await {
            return Ok(value);
        }
        self.view().alarm().await.map_err(engine_error)
    }

    /// Stages clearing the event's single durable alarm.
    async fn delete_alarm(&self) -> Result<(), HostError> {
        self.stage(stage_alarm_clear).await?;
        self.overlay.lock().await.alarm = Some(None);
        Ok(())
    }
}
