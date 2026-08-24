//! Reserved Worker-state tables: KV, the single durable alarm, and WebSocket
//! attachments stored as ordinary committed relational DO state.
//!
//! These tables ride the engine's one commit history — they replay, checkpoint,
//! and archive with everything else and have no side store. The WASM host layer
//! enforces ABI bounds (attachment size); this module stores what it is given.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::UInt64Type;
use arrow_array::{Array, ArrayRef, BinaryArray, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use futures::TryStreamExt;

use crate::error::{Error, Result};
use crate::storage::{DoEngine, DoStorage, Projection, SnapshotFence};
use crate::transaction::{DoTransaction, MutationDomain, TableId};

/// Reserved relational table holding Worker KV state.
pub const WORKER_KV_TABLE: &str = "__worker_kv";
/// Reserved relational table holding the single durable alarm.
pub const WORKER_ALARM_TABLE: &str = "__worker_alarm";
/// Reserved relational table holding WebSocket connection attachments.
pub const WORKER_ATTACHMENTS_TABLE: &str = "__worker_attachments";

/// Schema of `__worker_kv`: key plus nullable value; a null value is a tombstone.
fn kv_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Binary, true),
    ]))
}

/// Schema of `__worker_alarm`: one nullable deadline; a null clears the alarm.
fn alarm_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "deadline_ms",
        DataType::UInt64,
        true,
    )]))
}

/// Schema of `__worker_attachments`: socket plus nullable blob; null detaches.
fn attachments_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("socket", DataType::UInt64, false),
        Field::new("value", DataType::Binary, true),
    ]))
}

/// Creates any missing reserved Worker table without touching existing state.
///
/// `DoEngine::create_table` replaces a table wholesale, so this checks for an
/// existing schema first — re-ensuring on every wake must never wipe rows.
pub async fn ensure_worker_tables(engine: &DoEngine) -> Result<()> {
    for (name, schema) in [
        (WORKER_KV_TABLE, kv_schema()),
        (WORKER_ALARM_TABLE, alarm_schema()),
        (WORKER_ATTACHMENTS_TABLE, attachments_schema()),
    ] {
        let table = TableId::new(name);
        if engine.table_schema(&table).is_err() {
            engine.create_table(table, schema).await?;
        }
    }
    Ok(())
}

/// Stages one KV write into the transaction's private write set.
pub fn stage_kv_put(transaction: &mut dyn DoTransaction, key: &str, value: Vec<u8>) -> Result<()> {
    stage_kv_row(transaction, key, Some(value))
}

/// Stages one KV tombstone into the transaction's private write set.
pub fn stage_kv_delete(transaction: &mut dyn DoTransaction, key: &str) -> Result<()> {
    stage_kv_row(transaction, key, None)
}

/// Builds and appends one single-row KV batch.
fn stage_kv_row(
    transaction: &mut dyn DoTransaction,
    key: &str,
    value: Option<Vec<u8>>,
) -> Result<()> {
    let keys: ArrayRef = Arc::new(StringArray::from(vec![key]));
    let values: ArrayRef = Arc::new(BinaryArray::from_opt_vec(vec![value.as_deref()]));
    let batch = RecordBatch::try_new(kv_schema(), vec![keys, values]).map_err(Error::Arrow)?;
    transaction.append(
        MutationDomain::Relational,
        TableId::new(WORKER_KV_TABLE),
        batch,
    )
}

/// Stages the durable alarm deadline into the transaction's private write set.
pub fn stage_alarm_set(transaction: &mut dyn DoTransaction, deadline_ms: u64) -> Result<()> {
    stage_alarm_row(transaction, Some(deadline_ms))
}

/// Stages clearing the durable alarm into the transaction's private write set.
pub fn stage_alarm_clear(transaction: &mut dyn DoTransaction) -> Result<()> {
    stage_alarm_row(transaction, None)
}

/// Builds and appends one single-row alarm batch.
fn stage_alarm_row(transaction: &mut dyn DoTransaction, deadline_ms: Option<u64>) -> Result<()> {
    let deadlines: ArrayRef = Arc::new(UInt64Array::from(vec![deadline_ms]));
    let batch = RecordBatch::try_new(alarm_schema(), vec![deadlines]).map_err(Error::Arrow)?;
    transaction.append(
        MutationDomain::Relational,
        TableId::new(WORKER_ALARM_TABLE),
        batch,
    )
}

/// Stages one attachment write (`Some`) or detach (`None`) for a socket.
pub fn stage_attachment(
    transaction: &mut dyn DoTransaction,
    socket: u64,
    value: Option<Vec<u8>>,
) -> Result<()> {
    let sockets: ArrayRef = Arc::new(UInt64Array::from(vec![socket]));
    let values: ArrayRef = Arc::new(BinaryArray::from_opt_vec(vec![value.as_deref()]));
    let batch =
        RecordBatch::try_new(attachments_schema(), vec![sockets, values]).map_err(Error::Arrow)?;
    transaction.append(
        MutationDomain::Relational,
        TableId::new(WORKER_ATTACHMENTS_TABLE),
        batch,
    )
}

/// Committed-state reader over the reserved Worker tables.
///
/// Reads fold scan output last-writer-wins in commit order. Read-your-writes
/// inside an open event is the WASM host adapter's overlay, not this reader's:
/// this view only ever exposes committed state.
pub struct WorkerStateView<'engine> {
    /// The engine whose committed state is read.
    engine: &'engine DoEngine,
}

impl<'engine> WorkerStateView<'engine> {
    /// Creates a reader over one engine's committed Worker state.
    pub fn new(engine: &'engine DoEngine) -> Self {
        Self { engine }
    }

    /// Reads the committed value for one KV key, honoring tombstones.
    pub async fn kv_get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.kv_fold().await?.remove(key).flatten())
    }

    /// Lists live keys with `prefix`, sorted, bounded by `limit`.
    pub async fn kv_list(&self, prefix: &str, limit: u32) -> Result<Vec<String>> {
        Ok(self
            .kv_fold()
            .await?
            .into_iter()
            .filter(|(key, value)| key.starts_with(prefix) && value.is_some())
            .map(|(key, _)| key)
            .take(limit as usize)
            .collect())
    }

    /// Reads the committed alarm deadline, when one is armed.
    pub async fn alarm(&self) -> Result<Option<u64>> {
        let batches = self.scan_all(WORKER_ALARM_TABLE).await?;
        let mut deadline = None;
        for batch in &batches {
            let column = batch.column(0).as_primitive::<UInt64Type>();
            for row in 0..batch.num_rows() {
                deadline = column.is_valid(row).then(|| column.value(row));
            }
        }
        Ok(deadline)
    }

    /// Reads the committed attachment for one socket, honoring detaches.
    pub async fn attachment(&self, socket: u64) -> Result<Option<Vec<u8>>> {
        Ok(self.attachments_fold().await?.remove(&socket).flatten())
    }

    /// Lists sockets with a live committed attachment, ascending.
    pub async fn attached_sockets(&self) -> Result<Vec<u64>> {
        Ok(self
            .attachments_fold()
            .await?
            .into_iter()
            .filter(|(_, value)| value.is_some())
            .map(|(socket, _)| socket)
            .collect())
    }

    /// Scans one reserved table completely at the current committed snapshot.
    async fn scan_all(&self, table: &str) -> Result<Vec<RecordBatch>> {
        self.engine
            .scan(
                TableId::new(table),
                SnapshotFence::at(self.engine.applied_sequence()),
                Projection::all(),
                vec![],
            )
            .await?
            .try_collect::<Vec<_>>()
            .await
            .map_err(Error::DataFusion)
    }

    /// Folds the KV log last-writer-wins into key → latest value.
    async fn kv_fold(&self) -> Result<BTreeMap<String, Option<Vec<u8>>>> {
        let batches = self.scan_all(WORKER_KV_TABLE).await?;
        let mut folded = BTreeMap::new();
        for batch in &batches {
            let keys = batch.column(0).as_string::<i32>();
            let values = batch.column(1).as_binary::<i32>();
            for row in 0..batch.num_rows() {
                let value = values.is_valid(row).then(|| values.value(row).to_vec());
                folded.insert(keys.value(row).to_owned(), value);
            }
        }
        Ok(folded)
    }

    /// Folds the attachment log last-writer-wins into socket → latest blob.
    async fn attachments_fold(&self) -> Result<BTreeMap<u64, Option<Vec<u8>>>> {
        let batches = self.scan_all(WORKER_ATTACHMENTS_TABLE).await?;
        let mut folded = BTreeMap::new();
        for batch in &batches {
            let sockets = batch.column(0).as_primitive::<UInt64Type>();
            let values = batch.column(1).as_binary::<i32>();
            for row in 0..batch.num_rows() {
                let value = values.is_valid(row).then(|| values.value(row).to_vec());
                folded.insert(sockets.value(row), value);
            }
        }
        Ok(folded)
    }
}
