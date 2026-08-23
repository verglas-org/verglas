//! Thin BEGIN/statement/COMMIT session around DataFusion.

use std::sync::Arc;

use arrow_array::RecordBatch;
use datafusion::prelude::SessionContext;

use crate::error::Result;
use crate::provider::{DoTableProvider, TransactionHandle};
use crate::storage::{DoEngine, DoStorage, SnapshotFence};
use crate::transaction::{CommitReceipt, IsolationLevel, TableId};

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
        let snapshot = SnapshotFence::at(engine.applied_sequence());
        let transaction = TransactionHandle::new(engine.begin(isolation).await?);
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
        Ok(self.context.sql(sql).await?.collect().await?)
    }

    /// Implements COMMIT through the engine's sole authority.
    pub async fn commit(self) -> Result<CommitReceipt> {
        let transaction = self.transaction.take().await?;
        self.engine.commit(transaction).await
    }
}
