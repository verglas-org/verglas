//! Non-durable DataFusion query compute retaining an explicit dataset cache pin.

use std::sync::Arc;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::prelude::SessionContext;

use crate::{Error, Result};

/// Opaque cache reservation retained for the query object's lifetime.
pub trait DatasetCachePin: Send + Sync {}

/// Cache service capable of pinning one external or managed dataset.
#[async_trait]
pub trait DatasetCache: Send + Sync {
    /// Acquires a reservation whose drop releases the pinned cache state.
    async fn pin(&self, dataset_id: &str) -> Result<Box<dyn DatasetCachePin>>;
}

/// Ephemeral DataFusion object over a pinned source dataset.
pub struct QueryObject {
    dataset_id: String,
    context: SessionContext,
    _pin: Box<dyn DatasetCachePin>,
}

impl QueryObject {
    /// Acquires the cache pin before exposing query execution.
    pub async fn new(dataset_id: impl Into<String>, cache: Arc<dyn DatasetCache>) -> Result<Self> {
        let dataset_id = dataset_id.into();
        if dataset_id.is_empty() {
            return Err(Error::InvalidObjectPolicy(
                "query object dataset identity cannot be empty".to_owned(),
            ));
        }
        let pin = cache.pin(&dataset_id).await?;
        Ok(Self {
            dataset_id,
            context: SessionContext::new(),
            _pin: pin,
        })
    }

    /// Returns the source identity whose cache state remains pinned.
    pub fn dataset_id(&self) -> &str {
        &self.dataset_id
    }

    /// Registers one read provider belonging to the pinned dataset.
    pub fn register_table(&self, name: &str, provider: Arc<dyn TableProvider>) -> Result<()> {
        if name.is_empty() {
            return Err(Error::InvalidObjectPolicy(
                "query object table name cannot be empty".to_owned(),
            ));
        }
        self.context.register_table(name, provider)?;
        Ok(())
    }

    /// Plans and executes SQL without creating an authoritative transaction stream.
    pub async fn execute(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        Ok(self.context.sql(sql).await?.collect().await?)
    }
}
