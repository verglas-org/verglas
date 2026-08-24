//! DataFusion table provider and transactional DML over one DO snapshot.

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::arrow::compute::kernels::zip::zip;
use datafusion::arrow::compute::{and, concat_batches, filter_record_batch};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::DFSchema;
use datafusion::datasource::MemTable;
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_expr::{PhysicalExpr, create_physical_expr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use futures::{StreamExt, TryStreamExt};
use tokio::sync::Mutex;

use crate::error::{Error, Result};
use crate::storage::{DoEngine, DoStorage, Projection, SnapshotFence, apply_relational_mutation};
use crate::transaction::{DoTransaction, MutationBatch, MutationDomain, MutationKind, TableId};

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

    /// Adds a durable table schema declaration to the shared transaction.
    pub async fn append_schema_change(
        &self,
        table: TableId,
        schema: arrow_schema::SchemaRef,
    ) -> Result<()> {
        let mut guard = self.transaction.lock().await;
        let transaction = guard
            .as_mut()
            .ok_or_else(|| Error::InvalidReceipt("transaction is already closed".to_owned()))?;
        transaction.append_schema_change(table, schema)
    }

    /// Appends a relational insert batch produced by one DataFusion DML plan.
    async fn append(&self, table: TableId, batch: RecordBatch) -> Result<()> {
        self.append_with_kind(MutationKind::Insert, table, batch)
            .await
    }

    /// Appends a typed relational mutation to the private write set.
    async fn append_with_kind(
        &self,
        kind: MutationKind,
        table: TableId,
        batch: RecordBatch,
    ) -> Result<()> {
        let mut guard = self.transaction.lock().await;
        let transaction = guard
            .as_mut()
            .ok_or_else(|| Error::InvalidReceipt("transaction is already closed".to_owned()))?;
        transaction.append_with_kind(kind, MutationDomain::Relational, table, batch)
    }

    /// Returns transaction-local relational mutations for read-your-writes scans.
    async fn relational_mutations(&self, table: &TableId) -> Result<Vec<MutationBatch>> {
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
            .cloned()
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

    /// Opens a provider whose DML plans append to one private write set.
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

    /// Materializes committed rows at the provider fence plus private mutations.
    async fn visible_batches(&self) -> Result<Vec<RecordBatch>> {
        let committed = self
            .engine
            .scan(
                self.table.clone(),
                self.snapshot,
                Projection::all(),
                Vec::new(),
            )
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        let mut visible = committed;
        if let Some(transaction) = &self.transaction {
            for mutation in transaction.relational_mutations(&self.table).await? {
                apply_relational_mutation(&mut visible, mutation.kind(), mutation.batch())?;
            }
        }
        Ok(visible)
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
        let batches = self.visible_batches().await.map_err(external_error)?;
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

    /// Converts DELETE predicates into one private replacement mutation.
    async fn delete_from(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let transaction = self.transaction.clone().ok_or_else(|| {
            DataFusionError::Plan("DELETE requires an active Durable Object transaction".to_owned())
        })?;
        let batches = self.visible_batches().await.map_err(external_error)?;
        let current = combine_batches(&self.schema, &batches)?;
        let df_schema = DFSchema::try_from(self.schema.clone())?;
        let mask =
            evaluate_filters_to_mask(&filters, &current, &df_schema, state.execution_props())?;
        let (remaining, deleted) = match mask {
            Some(mask) => {
                let deleted = mask.iter().filter(|value| *value == Some(true)).count();
                let keep =
                    BooleanArray::from_iter(mask.iter().map(|value| Some(value != Some(true))));
                (filter_record_batch(&current, &keep)?, deleted as u64)
            }
            None => (
                RecordBatch::new_empty(self.schema.clone()),
                current.num_rows() as u64,
            ),
        };
        transaction
            .append_with_kind(MutationKind::Replace, self.table.clone(), remaining)
            .await
            .map_err(external_error)?;
        Ok(Arc::new(DmlResultExec::new(deleted)))
    }

    /// Converts UPDATE assignments and predicates into one private replacement mutation.
    async fn update(
        &self,
        state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let transaction = self.transaction.clone().ok_or_else(|| {
            DataFusionError::Plan("UPDATE requires an active Durable Object transaction".to_owned())
        })?;
        let batches = self.visible_batches().await.map_err(external_error)?;
        let current = combine_batches(&self.schema, &batches)?;
        let df_schema = DFSchema::try_from(self.schema.clone())?;
        let physical_assignments = assignments
            .iter()
            .map(|(name, expression)| {
                if self.schema.field_with_name(name).is_err() {
                    return Err(DataFusionError::Plan(format!(
                        "UPDATE column '{name}' does not exist"
                    )));
                }
                let physical =
                    create_physical_expr(expression, &df_schema, state.execution_props())?;
                Ok((name.clone(), physical))
            })
            .collect::<DataFusionResult<Vec<_>>>()?;
        let mask =
            evaluate_filters_to_mask(&filters, &current, &df_schema, state.execution_props())?;
        let (updated, changed) =
            update_batch(&current, &self.schema, &physical_assignments, mask.as_ref())?;
        transaction
            .append_with_kind(MutationKind::Replace, self.table.clone(), updated)
            .await
            .map_err(external_error)?;
        Ok(Arc::new(DmlResultExec::new(changed)))
    }
}

/// Combines visible batches into one batch for DataFusion expression evaluation.
fn combine_batches(schema: &SchemaRef, batches: &[RecordBatch]) -> DataFusionResult<RecordBatch> {
    if batches.is_empty() {
        Ok(RecordBatch::new_empty(schema.clone()))
    } else {
        Ok(concat_batches(schema, batches.iter())?)
    }
}

/// Evaluates all filter expressions into one SQL three-valued boolean mask.
fn evaluate_filters_to_mask(
    filters: &[Expr],
    batch: &RecordBatch,
    df_schema: &DFSchema,
    execution_props: &datafusion::logical_expr::execution_props::ExecutionProps,
) -> DataFusionResult<Option<BooleanArray>> {
    if filters.is_empty() {
        return Ok(None);
    }
    let mut combined_mask = None;
    for filter_expr in filters {
        let physical = create_physical_expr(filter_expr, df_schema, execution_props)?;
        let array = physical.evaluate(batch)?.into_array(batch.num_rows())?;
        let mask = array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "filter expression did not evaluate to boolean".to_owned(),
                )
            })?
            .clone();
        combined_mask = Some(match combined_mask {
            Some(existing) => and(&existing, &mask)?,
            None => mask,
        });
    }
    Ok(combined_mask)
}

/// Applies physical assignments to matching rows and reports the affected count.
fn update_batch(
    batch: &RecordBatch,
    schema: &SchemaRef,
    assignments: &[(String, Arc<dyn PhysicalExpr>)],
    mask: Option<&BooleanArray>,
) -> DataFusionResult<(RecordBatch, u64)> {
    let update_mask = mask
        .cloned()
        .unwrap_or_else(|| BooleanArray::from(vec![true; batch.num_rows()]));
    let changed = update_mask
        .iter()
        .filter(|value| *value == Some(true))
        .count() as u64;
    let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    for field in schema.fields() {
        let original = batch.column_by_name(field.name()).ok_or_else(|| {
            DataFusionError::Internal(format!("column '{}' was not found", field.name()))
        })?;
        let new_column = if let Some((_, physical)) =
            assignments.iter().find(|(name, _)| name == field.name())
        {
            let values = physical.evaluate_selection(batch, &update_mask)?;
            let values = values.into_array(batch.num_rows())?;
            let new_values: &dyn Array = values.as_ref();
            let old_values: &dyn Array = original.as_ref();
            zip(&update_mask, &new_values, &old_values)?
        } else {
            original.clone()
        };
        new_columns.push(new_column);
    }
    Ok((RecordBatch::try_new(schema.clone(), new_columns)?, changed))
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
        formatter: &mut std::fmt::Formatter<'_>,
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

/// Execution plan returning the count of rows affected by a private DML mutation.
#[derive(Debug)]
struct DmlResultExec {
    rows_affected: u64,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl DmlResultExec {
    /// Builds a one-row DML result with the affected-row count.
    fn new(rows_affected: u64) -> Self {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]));
        let properties = PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            rows_affected,
            schema,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for DmlResultExec {
    /// Displays the affected-row count in a DataFusion physical plan.
    fn fmt_as(
        &self,
        _format: DisplayFormatType,
        formatter: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(
            formatter,
            "DmlResultExec rows_affected={}",
            self.rows_affected
        )
    }
}

impl ExecutionPlan for DmlResultExec {
    /// Returns the stable plan name.
    fn name(&self) -> &str {
        "DmlResultExec"
    }

    /// Returns the one-column count schema.
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Returns the plan properties used by the DataFusion scheduler.
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    /// Returns no child plans because the DML has already staged its mutation.
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    /// Returns this terminal plan when DataFusion rewrites children.
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    /// Produces one Arrow count batch for the DML statement.
    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let count = UInt64Array::from(vec![self.rows_affected]);
        let batch = RecordBatch::try_new(self.schema.clone(), vec![Arc::new(count)])?;
        let stream = futures::stream::iter(vec![Ok(batch)]);
        Ok(Box::pin(
            datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                self.schema.clone(),
                stream,
            ),
        ))
    }
}

/// Wraps a DO error as a DataFusion external execution error.
fn external_error(error: Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
