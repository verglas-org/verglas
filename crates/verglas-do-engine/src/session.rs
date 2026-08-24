//! Thin BEGIN/statement/COMMIT session around DataFusion.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::logical_expr::{DdlStatement, LogicalPlan};
use datafusion::prelude::SessionContext;

use crate::error::Result;
use crate::provider::{DoTableProvider, TransactionHandle};
use crate::storage::{DoEngine, DoStorage, SnapshotFence};
use crate::transaction::{CommitReceipt, IsolationLevel, TableId};

/// Extracts a native table registration from DataFusion's supported CREATE TABLE plans.
fn ddl_table(plan: &LogicalPlan) -> Option<(TableId, SchemaRef)> {
    match plan {
        LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(command)) => Some((
            TableId::new(command.name.to_string()),
            Arc::clone(command.input.schema().inner()),
        )),
        LogicalPlan::Ddl(DdlStatement::CreateExternalTable(command)) => Some((
            TableId::new(command.name.to_string()),
            Arc::clone(command.schema.inner()),
        )),
        _ => None,
    }
}

/// One SQL transaction with a fixed DataFusion snapshot and private write set.
pub struct DoSession {
    engine: Arc<DoEngine>,
    context: SessionContext,
    transaction: TransactionHandle,
}

impl DoSession {
    /// Implements BEGIN and registers the requested DO tables in DataFusion.
    pub async fn begin(
        engine: Arc<DoEngine>,
        tables: impl IntoIterator<Item = TableId>,
        isolation: IsolationLevel,
    ) -> Result<Self> {
        let transaction = TransactionHandle::new(engine.begin(isolation).await?);
        let snapshot = SnapshotFence::at(transaction.base_commit_sequence().await?);
        Self::from_transaction(engine, tables, snapshot, transaction)
    }

    /// Registers a caller-owned transaction at its fixed event snapshot.
    pub fn from_transaction(
        engine: Arc<DoEngine>,
        tables: impl IntoIterator<Item = TableId>,
        snapshot: SnapshotFence,
        transaction: TransactionHandle,
    ) -> Result<Self> {
        let context = SessionContext::new();
        for table in tables {
            let provider = DoTableProvider::open_transactional(
                engine.clone(),
                table.clone(),
                snapshot,
                transaction.clone(),
            )?;
            context.register_table(table.as_str(), Arc::new(provider))?;
        }
        Ok(Self {
            engine,
            context,
            transaction,
        })
    }

    /// Plans and executes one SQL statement against the transaction snapshot.
    pub async fn execute(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let plan = self.context.state().create_logical_plan(sql).await?;
        let table = ddl_table(&plan);
        let batches = self
            .context
            .execute_logical_plan(plan)
            .await?
            .collect()
            .await?;
        if let Some((table, schema)) = table
            && self.engine.table_schema(&table).is_err()
        {
            self.engine.create_table(table, schema).await?;
        }
        Ok(batches)
    }

    /// Implements COMMIT through the engine's sole authority.
    pub async fn commit(self) -> Result<CommitReceipt> {
        let transaction = self.transaction.take().await?;
        self.engine.commit(transaction).await
    }
}
