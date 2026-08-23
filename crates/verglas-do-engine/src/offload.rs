//! Ordered asynchronous archive contract for committed managed DO transactions.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::error::{Error, Result};

/// One committed canonical envelope ready for the managed object-store archive.
#[derive(Debug, Clone)]
pub struct ManagedTransactionArchive {
    do_id: String,
    transaction_id: Uuid,
    commit_sequence: u64,
    canonical_envelope: Vec<u8>,
}

impl ManagedTransactionArchive {
    /// Creates one immutable archive unit after its transaction applies.
    pub(crate) fn new(
        do_id: String,
        transaction_id: Uuid,
        commit_sequence: u64,
        canonical_envelope: Vec<u8>,
    ) -> Self {
        Self {
            do_id,
            transaction_id,
            commit_sequence,
            canonical_envelope,
        }
    }

    /// Returns the managed Durable Object identity.
    pub fn do_id(&self) -> &str {
        &self.do_id
    }

    /// Returns the retry-stable transaction identity.
    pub fn transaction_id(&self) -> Uuid {
        self.transaction_id
    }

    /// Returns the exact sequence assigned by the commit authority.
    pub fn commit_sequence(&self) -> u64 {
        self.commit_sequence
    }

    /// Returns the canonical bytes to store under the sequence identity.
    pub fn canonical_envelope(&self) -> &[u8] {
        &self.canonical_envelope
    }
}

/// Flush thresholds shared by compacted SQLite archive and Iceberg publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OffloadBatchPolicy {
    max_delay: Duration,
    max_bytes: usize,
}

impl OffloadBatchPolicy {
    /// Returns the production ten-second or sixteen-MiB thresholds.
    pub fn production() -> Self {
        Self {
            max_delay: Duration::from_secs(10),
            max_bytes: 16 * 1024 * 1024,
        }
    }

    /// Creates explicit positive thresholds for deterministic coordination tests.
    pub fn new(max_delay: Duration, max_bytes: usize) -> Result<Self> {
        if max_delay.is_zero() || max_bytes == 0 {
            return Err(Error::Archive(
                "offload batch thresholds must be positive".to_owned(),
            ));
        }
        Ok(Self {
            max_delay,
            max_bytes,
        })
    }

    /// Returns the oldest transaction age that triggers publication.
    pub fn max_delay(self) -> Duration {
        self.max_delay
    }

    /// Returns the canonical-byte threshold that triggers publication.
    pub fn max_bytes(self) -> usize {
        self.max_bytes
    }
}

/// One contiguous transaction range compacted by a single offload flush.
#[derive(Debug, Clone)]
pub struct OffloadBatch {
    transactions: Vec<ManagedTransactionArchive>,
    bytes: usize,
}

impl OffloadBatch {
    /// Returns the first transaction sequence in this compacted range.
    pub fn from_sequence(&self) -> u64 {
        self.transactions[0].commit_sequence
    }

    /// Returns the highest transaction sequence in this compacted range.
    pub fn through_sequence(&self) -> u64 {
        self.transactions[self.transactions.len() - 1].commit_sequence
    }

    /// Returns the ordered exact transactions used by SQLite and Iceberg sinks.
    pub fn transactions(&self) -> &[ManagedTransactionArchive] {
        &self.transactions
    }

    /// Returns the canonical bytes accumulated by this range.
    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

/// Deterministic threshold state shared by archive and lake materialization workers.
pub struct OffloadBatcher {
    policy: OffloadBatchPolicy,
    transactions: Vec<ManagedTransactionArchive>,
    bytes: usize,
    first_at: Option<Instant>,
}

impl OffloadBatcher {
    /// Creates an empty compacting batcher under one explicit policy.
    pub fn new(policy: OffloadBatchPolicy) -> Self {
        Self {
            policy,
            transactions: Vec::new(),
            bytes: 0,
            first_at: None,
        }
    }

    /// Adds one contiguous transaction and flushes when the byte ceiling is reached.
    pub fn push(
        &mut self,
        transaction: ManagedTransactionArchive,
        now: Instant,
    ) -> Result<Option<OffloadBatch>> {
        if let Some(previous) = self.transactions.last() {
            if transaction.do_id != previous.do_id
                || transaction.commit_sequence != previous.commit_sequence.saturating_add(1)
            {
                return Err(Error::Archive(
                    "offload batch transactions must be same-DO and contiguous".to_owned(),
                ));
            }
        } else {
            self.first_at = Some(now);
        }
        self.bytes = self
            .bytes
            .saturating_add(transaction.canonical_envelope.len());
        self.transactions.push(transaction);
        if self.bytes >= self.policy.max_bytes {
            Ok(self.take())
        } else {
            Ok(None)
        }
    }

    /// Flushes when the oldest transaction reaches the configured delay.
    pub fn flush_due(&mut self, now: Instant) -> Option<OffloadBatch> {
        if self
            .first_at
            .is_some_and(|first| now.saturating_duration_since(first) >= self.policy.max_delay)
        {
            self.take()
        } else {
            None
        }
    }

    /// Flushes all pending work during suspension or explicit lifecycle drain.
    pub fn drain(&mut self) -> Option<OffloadBatch> {
        self.take()
    }

    /// Removes the current batch while resetting every threshold counter.
    fn take(&mut self) -> Option<OffloadBatch> {
        if self.transactions.is_empty() {
            return None;
        }
        let transactions = std::mem::take(&mut self.transactions);
        let bytes = std::mem::take(&mut self.bytes);
        self.first_at = None;
        Some(OffloadBatch {
            transactions,
            bytes,
        })
    }
}

/// Verified identity of one compacted archive object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffloadBatchReceipt {
    from_sequence: u64,
    through_sequence: u64,
    transactions: usize,
    etag: String,
}

impl OffloadBatchReceipt {
    /// Returns the first archived sequence.
    pub fn from_sequence(&self) -> u64 {
        self.from_sequence
    }

    /// Returns the highest archived sequence.
    pub fn through_sequence(&self) -> u64 {
        self.through_sequence
    }

    /// Returns the number of transactions compacted into the object.
    pub fn transactions(&self) -> usize {
        self.transactions
    }

    /// Returns the verified SHA-256 object identity.
    pub fn etag(&self) -> &str {
        &self.etag
    }
}

/// Managed sink for one immutable contiguous offload range.
#[async_trait]
pub trait OffloadBatchArchive: Send + Sync {
    /// Publishes one compacted object and verifies exact retry identity.
    async fn archive(&self, batch: &OffloadBatch) -> Result<OffloadBatchReceipt>;
}

/// Object-store implementation of immutable compacted transaction batches.
pub struct ObjectStoreOffloadBatchArchive {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
}

impl ObjectStoreOffloadBatchArchive {
    /// Creates a compacted archive beneath one managed prefix.
    pub fn new(store: Arc<dyn ObjectStore>, prefix: Path) -> Self {
        Self { store, prefix }
    }
}

#[async_trait]
impl OffloadBatchArchive for ObjectStoreOffloadBatchArchive {
    /// Creates one range-named object, then reads it back before acknowledging.
    async fn archive(&self, batch: &OffloadBatch) -> Result<OffloadBatchReceipt> {
        let first = batch
            .transactions
            .first()
            .ok_or_else(|| Error::Archive("cannot publish an empty offload batch".to_owned()))?;
        let path = self.prefix.clone().join(first.do_id()).join(format!(
            "{:020}-{:020}.batch",
            batch.from_sequence(),
            batch.through_sequence()
        ));
        let mut bytes = Vec::with_capacity(batch.bytes.saturating_add(64));
        bytes.extend_from_slice(b"VDO_BATCH");
        bytes.extend_from_slice(&(batch.transactions.len() as u64).to_le_bytes());
        for transaction in &batch.transactions {
            bytes.extend_from_slice(&transaction.commit_sequence.to_le_bytes());
            bytes.extend_from_slice(transaction.transaction_id.as_bytes());
            bytes.extend_from_slice(&(transaction.canonical_envelope.len() as u64).to_le_bytes());
            bytes.extend_from_slice(&transaction.canonical_envelope);
        }
        match self
            .store
            .put_opts(
                &path,
                Bytes::copy_from_slice(&bytes).into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) | Err(object_store::Error::AlreadyExists { .. }) => {}
            Err(error) => return Err(Error::Archive(error.to_string())),
        }
        let actual = self
            .store
            .get(&path)
            .await
            .map_err(|error| Error::Archive(error.to_string()))?
            .bytes()
            .await
            .map_err(|error| Error::Archive(error.to_string()))?;
        if actual.as_ref() != bytes {
            return Err(Error::Archive(format!(
                "verification mismatch for compacted range {path}"
            )));
        }
        Ok(OffloadBatchReceipt {
            from_sequence: batch.from_sequence(),
            through_sequence: batch.through_sequence(),
            transactions: batch.transactions.len(),
            etag: hex::encode(Sha256::digest(&actual)),
        })
    }
}

/// Verified identity of one transaction object in managed object storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveReceipt {
    commit_sequence: u64,
    etag: String,
}

impl ArchiveReceipt {
    /// Creates a receipt after the archive implementation verifies the object.
    pub fn new(commit_sequence: u64, etag: impl Into<String>) -> Self {
        Self {
            commit_sequence,
            etag: etag.into(),
        }
    }

    /// Returns the archived transaction sequence.
    pub fn commit_sequence(&self) -> u64 {
        self.commit_sequence
    }

    /// Returns the verified object content identity.
    pub fn etag(&self) -> &str {
        &self.etag
    }
}

/// Managed object-store sink for canonical committed transactions.
#[async_trait]
pub trait TransactionArchive: Send + Sync {
    /// Uploads and verifies one idempotently named transaction archive object.
    async fn archive(&self, transaction: &ManagedTransactionArchive) -> Result<ArchiveReceipt>;
}

/// Provider-neutral managed transaction archive backed by an object store.
pub struct ObjectStoreTransactionArchive {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
}

impl ObjectStoreTransactionArchive {
    /// Creates an archive beneath `prefix` in one managed DO bucket.
    pub fn new(store: Arc<dyn ObjectStore>, prefix: Path) -> Self {
        Self { store, prefix }
    }

    /// Builds the deterministic object path for an exact transaction retry.
    fn path(&self, transaction: &ManagedTransactionArchive) -> Path {
        self.prefix.clone().join(transaction.do_id()).join(format!(
            "{:020}-{}.arrow",
            transaction.commit_sequence(),
            transaction.transaction_id()
        ))
    }
}

#[async_trait]
impl TransactionArchive for ObjectStoreTransactionArchive {
    /// Atomically uploads the canonical bytes, reads them back, and verifies identity.
    async fn archive(&self, transaction: &ManagedTransactionArchive) -> Result<ArchiveReceipt> {
        let path = self.path(transaction);
        let expected = transaction.canonical_envelope();
        match self
            .store
            .put_opts(
                &path,
                Bytes::copy_from_slice(expected).into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) | Err(object_store::Error::AlreadyExists { .. }) => {}
            Err(error) => return Err(Error::Archive(error.to_string())),
        }
        let actual = self
            .store
            .get(&path)
            .await
            .map_err(|error| Error::Archive(error.to_string()))?
            .bytes()
            .await
            .map_err(|error| Error::Archive(error.to_string()))?;
        if actual.as_ref() != expected {
            return Err(Error::Archive(format!(
                "verification mismatch for {}",
                path
            )));
        }
        let content_identity = hex::encode(Sha256::digest(&actual));
        Ok(ArchiveReceipt::new(
            transaction.commit_sequence(),
            content_identity,
        ))
    }
}

/// Progress made by one ordered offload pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OffloadReport {
    archived_transactions: usize,
    through: u64,
}

impl OffloadReport {
    /// Creates an offload progress report.
    pub(crate) fn new(archived_transactions: usize, through: u64) -> Self {
        Self {
            archived_transactions,
            through,
        }
    }

    /// Returns the number of newly verified archive objects.
    pub fn archived_transactions(self) -> usize {
        self.archived_transactions
    }

    /// Returns the contiguous verified archive watermark.
    pub fn through(self) -> u64 {
        self.through
    }
}
