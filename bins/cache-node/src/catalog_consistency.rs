//! Strong catalog orchestration for the cache-node binary.
//!
//! The EC log establishes the committed order. Catch-up refreshes the exact
//! authoritative pointer and publishes it to the watcher before a strong read
//! or acknowledgment can proceed.

use std::sync::Arc;

use verglas_catalog::{CatalogGateway, CatalogMutation};
use verglas_tables::catalog::{StrongApplyError, StrongWatcher};
use verglas_write::catalog_log::{CatalogLogError, CatalogMutationAck, EcCatalogLog};

/// A failure to make the local query metadata current with the EC log.
#[derive(Debug, thiserror::Error)]
pub enum StrongCatalogError {
    /// The EC mutation log could not prove or append a committed position.
    #[error(transparent)]
    Log(#[from] CatalogLogError),
    /// The authoritative catalog could not resolve the committed pointer.
    #[error(transparent)]
    Catalog(#[from] verglas_catalog::CatalogError),
    /// The watcher refused to publish an incomplete or mismatched mutation.
    #[error(transparent)]
    Apply(#[from] StrongApplyError),
    /// Catch-up completed without reaching the quorum-committed sequence.
    #[error("catalog catch-up stopped at {applied} below committed sequence {committed}")]
    Behind {
        /// Highest locally applied sequence.
        applied: u64,
        /// Sequence proven by the ring read quorum.
        committed: u64,
    },
}

/// One strong catalog view shared by event and query routes.
pub struct StrongCatalog {
    gateway: CatalogGateway,
    watcher: Arc<StrongWatcher>,
    log: Arc<EcCatalogLog>,
    catch_up_lock: tokio::sync::Mutex<()>,
}

impl StrongCatalog {
    /// Builds an empty strong view without contacting the ring.
    ///
    /// Ring members start concurrently, so requiring a read quorum here creates
    /// a bootstrap deadlock: every member exits before its peer listeners can
    /// become available. Strong reads and mutation acknowledgements still call
    /// [`Self::catch_up`] and therefore remain unavailable until a quorum can
    /// prove the committed tail.
    pub fn new(
        gateway: CatalogGateway,
        watcher: StrongWatcher,
        log: Arc<EcCatalogLog>,
    ) -> Arc<Self> {
        Arc::new(Self {
            gateway,
            watcher: Arc::new(watcher),
            log,
            catch_up_lock: tokio::sync::Mutex::new(()),
        })
    }

    /// Quorum-appends one event, catches up locally, and returns its proof.
    pub async fn append_and_apply(
        &self,
        mutation: CatalogMutation,
    ) -> Result<CatalogMutationAck, StrongCatalogError> {
        let ack = self.log.append(mutation).await?;
        self.catch_up().await?;
        if self.watcher.applied_sequence() < ack.sequence {
            return Err(StrongCatalogError::Behind {
                applied: self.watcher.applied_sequence(),
                committed: ack.sequence,
            });
        }
        Ok(ack)
    }

    /// Replays the EC log through the authoritative gateway and local watcher.
    pub async fn catch_up(&self) -> Result<(), StrongCatalogError> {
        let _guard = self.catch_up_lock.lock().await;
        let applied = self.watcher.applied_sequence();
        for mutation in self.log.read_after(applied).await? {
            let state = self.gateway.apply_mutation(&mutation).await?;
            self.watcher.apply(&mutation, state)?;
        }
        let committed = self.log.committed_sequence().await?;
        let applied = self.watcher.applied_sequence();
        if applied < committed {
            return Err(StrongCatalogError::Behind { applied, committed });
        }
        Ok(())
    }

    /// Returns the gateway after callers have passed the catch-up fence.
    pub fn gateway(&self) -> &CatalogGateway {
        &self.gateway
    }

    /// Returns the query-session generation after callers pass the fence.
    pub fn generation(&self) -> u64 {
        self.gateway.generation()
    }

    /// Returns the applied durable sequence exposed with the generation.
    pub fn applied_sequence(&self) -> u64 {
        self.watcher.applied_sequence()
    }
}
