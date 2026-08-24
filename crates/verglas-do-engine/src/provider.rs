//! DataFusion table provider and INSERT sink over one DO transaction snapshot.

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::MemTable;
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, SendableRecordBatchStream,
};
use futures::{StreamExt, TryStreamExt};
use tokio::sync::Mutex;

use crate::error::{Error, Result};
use crate::storage::{DoEngine, DoStorage, Projection, SnapshotFence};
use crate::transaction::{DoTransaction, MutationDomain, TableId};

/// Shared private write set used by every provider in one SQL transaction.
#[derive(Clone)]
pub struct TransactionHandle {
    transaction: Arc<Mutex<Option<Box<dyn DoTransaction>>>>,
}

impl TransactionHandle {
    /// Wraps newly begun private transaction state.
    pub fn new(transaction: Box<dyn DoTransaction>) -> Self {
        Self {
            transaction: Arc::new(Mutex::new(Some(transaction))),
        }
    }

    /// Shares an existing transaction owner with another SQL session.
    pub fn from_shared(transaction: Arc<Mutex<Option<Box<dyn DoTransaction>>>>) -> Self {
        Self { transaction }
    }

    /// Returns the fixed snapshot sequence carried by this transaction.
    pub async fn base_commit_sequence(&self) -> Result<u64> {
        let guard = self.transaction.lock().await;
        let transaction = guard
            .as_ref()
            .ok_or_else(|| Error::InvalidReceipt("transaction is already closed".to_owned()))?;
        Ok(transaction.envelope().base_commit_sequence())
    }

    /// Appends a relational batch produced by one DataFusion DML plan.
    async fn append(&self, table: TableId, batch: arrow_array::RecordBatch) -> Result<()> {
        let mut guard = self.transaction.lock().await;
        let transaction = guard
            .as_mut()
            .ok_or_else(|| Error::InvalidReceipt("transaction is already closed".to_owned()))?;
        transaction.append(MutationDomain::Relational, table, batch)
    }

    /// Returns transaction-local relational batches for read-your-writes scans.
    async fn relational_batches(&self, table: &TableId) -> Result<Vec<arrow_array::RecordBatch>> {
        let guard = self.transaction.lock().await;
        let transaction = guard
            .as_ref()
            .ok_or_else(|| Error::InvalidReceipt("transaction is already closed".to_owned()))?;
        Ok(transaction
            .envelope()
            .mutations()
            .iter()
            .filter(|mutation| {
                mutation.domain() == MutationDomain::Relational && mutation.table() == table
            })
            .map(|mutation| mutation.batch().clone())
            .collect())
    }

    /// Closes the handle and returns the transaction for one commit call.
    pub async fn take(&self) -> Result<Box<dyn DoTransaction>> {
        self.transaction
            .lock()
            .await
            .take()
            .ok_or_else(|| Error::InvalidReceipt("transaction is already closed".to_owned()))
    }
}

/// DataFusion view of one DO table at one immutable transaction fence.
pub struct DoTableProvider {
    engine: Arc<DoEngine>,
    table: TableId,
    snapshot: SnapshotFence,
    schema: SchemaRef,
    transaction: Option<TransactionHandle>,
}

impl DoTableProvider {
    /// Opens a read-only provider after resolving its registered schema.
    pub fn open(engine: Arc<DoEngine>, table: TableId, snapshot: SnapshotFence) -> Result<Self> {
        Self::open_inner(engine, table, snapshot, None)
    }

    /// Opens a provider whose INSERT plans append to one private write set.
    pub fn open_transactional(
        engine: Arc<DoEngine>,
        table: TableId,
        snapshot: SnapshotFence,
        transaction: TransactionHandle,
    ) -> Result<Self> {
        Self::open_inner(engine, table, snapshot, Some(transaction))
    }

    /// Resolves shared provider metadata for read-only and transactional modes.
    fn open_inner(
        engine: Arc<DoEngine>,
        table: TableId,
        snapshot: SnapshotFence,
        transaction: Option<TransactionHandle>,
    ) -> Result<Self> {
        let schema = engine.table_schema(&table)?;
        Ok(Self {
            engine,
            table,
            snapshot,
            schema,
            transaction,
        })
    }
}

impl Debug for DoTableProvider {
    /// Formats provider identity without exposing mutable engine internals.
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DoTableProvider")
            .field("table", &self.table)
            .field("snapshot", &self.snapshot)
            .field("transactional", &self.transaction.is_some())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TableProvider for DoTableProvider {
    /// Returns the registered table schema.
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Identifies live DO state as an ordinary base table.
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Plans a snapshot scan including transaction-local relational writes.
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let mut batches = self
            .engine
            .scan(
                self.table.clone(),
                self.snapshot,
                Projection::all(),
                Vec::new(),
            )
            .await
            .map_err(external_error)?
            .try_collect::<Vec<_>>()
            .await?;
        if let Some(transaction) = &self.transaction {
            batches.extend(
                transaction
                    .relational_batches(&self.table)
                    .await
                    .map_err(external_error)?,
            );
        }
        let memory = MemTable::try_new(self.schema.clone(), vec![batches])?;
        memory.scan(state, projection, filters, limit).await
    }

    /// Converts INSERT input into transaction-local Arrow mutations.
    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if insert_op != InsertOp::Append {
            return Err(DataFusionError::NotImplemented(format!(
                "{insert_op} is not supported for Durable Object tables"
            )));
        }
        let transaction = self.transaction.clone().ok_or_else(|| {
            DataFusionError::Plan("INSERT requires an active Durable Object transaction".to_owned())
        })?;
        let sink = TransactionSink {
            schema: self.schema.clone(),
            table: self.table.clone(),
            transaction,
        };
        Ok(Arc::new(DataSinkExec::new(input, Arc::new(sink), None)))
    }
}

/// DataFusion sink that appends every input batch to a private write set.
struct TransactionSink {
    schema: SchemaRef,
    table: TableId,
    transaction: TransactionHandle,
}

impl Debug for TransactionSink {
    /// Formats the target table for EXPLAIN output.
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TransactionSink")
            .field("table", &self.table)
            .finish()
    }
}

impl DisplayAs for TransactionSink {
    /// Displays the transaction sink in DataFusion physical plans.
    fn fmt_as(
        &self,
        _format: DisplayFormatType,
        formatter: &mut Formatter<'_>,
    ) -> std::fmt::Result {
        write!(formatter, "DoTransactionSink table={}", self.table.as_str())
    }
}

#[async_trait]
impl DataSink for TransactionSink {
    /// Returns the target table schema.
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Consumes one DML stream and appends all rows without committing files.
    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> DataFusionResult<u64> {
        let mut rows = 0_u64;
        while let Some(batch) = data.next().await {
            let batch = batch?;
            rows = rows.saturating_add(batch.num_rows() as u64);
            self.transaction
                .append(self.table.clone(), batch)
                .await
                .map_err(external_error)?;
        }
        Ok(rows)
    }
}

/// Wraps a DO error as a DataFusion external execution error.
fn external_error(error: Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
