//! The commit barrier (#286): the durability gate a table commit crosses before
//! it may publish.
//!
//! ## Why a barrier exists
//!
//! Write-back acks a data-file PUT once it is durable on the buffer (a fragment
//! quorum in the §6 EC path, or one local-NVMe fragment plus the journal in the
//! single-node path), and propagates it to the origin in the background. Iceberg
//! data files are invisible until a commit references them, so fast-acking the
//! data files is safe on its own. What is *not* safe is publishing the commit —
//! the metadata write and the catalog commit POST — before those referenced data
//! files have reached the origin: another device reading through the catalog and
//! direct S3 (no Verglas) would then see a table pointing at files that are not
//! there yet.
//!
//! The barrier closes that gap. A commit awaits propagation of the data files it
//! references before it is forwarded; if that propagation cannot complete in
//! time (origin down), the commit is refused with a clear error and the table
//! stays consistent. The rule is *slow is acceptable, wrong is never*: a commit
//! may wait, or fail, but it can never publish files absent from the origin.
//!
//! ## One abstraction over both durability backends
//!
//! The durability state the barrier reads is the write-back journal: an acked
//! object is `Dirty` until its background propagation to the origin succeeds,
//! then `Clean`. Both durability backends record their acks in the *same*
//! [`JournalStore`] — the §6 EC quorum and the #286 single-node local fsync
//! differ only in how a fragment becomes durable, not in how propagation is
//! tracked. So a single barrier over the journal serves both backends, and
//! [`CommitBarrier`] is the interface a future backend that tracked durability
//! differently would implement instead of [`JournalBarrier`].
//!
//! ## The transport-level-only retry rule
//!
//! The barrier's bounded wait governs how long a *commit* blocks, never the
//! in-flight S3 propagation itself. When the deadline passes, the barrier stops
//! waiting and the commit fails, but the buffered object stays put and its
//! propagation keeps retrying — a request in flight is progress and is never
//! abandoned on a wall clock. When the origin returns, propagation drains and a
//! retried commit passes the barrier.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use verglas_core::CacheKey;

use crate::journal::JournalStore;

/// How often the barrier re-checks the journal while waiting. Small enough that
/// the barrier adds only a short tail once propagation completes, large enough
/// that a long wait is not a busy-loop. Not a tuning knob: it is a poll cadence,
/// invisible to configuration.
const BARRIER_POLL: Duration = Duration::from_millis(15);

/// Why a commit could not cross the barrier.
#[derive(Debug, thiserror::Error)]
pub enum BarrierError {
    /// Referenced data files were still buffered locally when the bounded wait
    /// elapsed — typically the origin is unreachable. The commit is refused so
    /// the table never publishes files absent from the origin; the buffered data
    /// is intact and its propagation continues, so a retried commit succeeds once
    /// the origin returns.
    #[error(
        "commit barrier: {pending} write-back object(s) still buffered locally after {waited_ms}ms; \
         the referenced data has not reached the origin (is it reachable?) — commit refused so the \
         table stays consistent; the data is safe and propagation continues"
    )]
    Timeout {
        /// Objects still dirty when the wait elapsed.
        pending: usize,
        /// How long the barrier waited before giving up.
        waited_ms: u64,
    },
}

/// What one barrier crossing did: how many referenced objects were still dirty
/// when it began and had to be awaited. `awaited == 0` is the fast path — every
/// referenced file was already at the origin, so the commit paid nothing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BarrierOutcome {
    /// Referenced objects that were dirty at entry and were waited on.
    pub awaited: usize,
}

/// The durability gate a table commit crosses before it may publish (#286).
///
/// Implementations block until the referenced write-back objects are durable at
/// the origin, or fail with [`BarrierError`] when that cannot be reached in the
/// bounded wait. [`JournalBarrier`] is the implementation over the shared
/// write-back journal; both durability backends feed that journal, so it serves
/// both (see the module docs).
#[async_trait]
pub trait CommitBarrier: Send + Sync {
    /// Await propagation of exactly the `referenced` origin keys. Keys already
    /// clean (or never write-back-buffered) return immediately; dirty keys are
    /// awaited up to `deadline`. This is the precise barrier — it costs the tail
    /// of the referenced files' propagation, which is usually already in flight,
    /// not the sum.
    async fn await_referenced(
        &self,
        referenced: &[CacheKey],
        deadline: Duration,
    ) -> Result<BarrierOutcome, BarrierError>;

    /// Await propagation of *every* currently-dirty write-back object. The
    /// conservative barrier a customer-invoked commit uses when it has not
    /// resolved its exact referenced keys (where parsing the commit body's
    /// manifests to enumerate data files would be disproportionate): a
    /// correct superset of [`await_referenced`], since a commit can only
    /// reference files that were written before it, and all such buffered files
    /// are in the dirty set.
    async fn await_all_dirty(&self, deadline: Duration) -> Result<BarrierOutcome, BarrierError>;
}

/// The commit barrier over the write-back journal.
///
/// Holds a share of the same [`JournalStore`] the coordinator acks and
/// propagates against. Reading it is cheap: [`JournalStore::is_idle`] is a
/// single relaxed atomic load when nothing is dirty, so a barrier crossing on an
/// idle buffer takes the fast path with no lock — the common case, and never on
/// a serve or ack hot path (the barrier is on the commit path only).
pub struct JournalBarrier {
    /// The shared dirty journal both durability backends record against.
    journals: Arc<JournalStore>,
}

impl JournalBarrier {
    /// Builds a barrier over the coordinator's journal store. Pass
    /// [`crate::WriteCoordinator::journals`]`().clone()` so the barrier reads the
    /// exact dirty state the ack and propagation paths write.
    pub fn new(journals: Arc<JournalStore>) -> Self {
        Self { journals }
    }

    /// Objects among `referenced` that are still dirty right now.
    fn pending_referenced(&self, referenced: &[CacheKey]) -> usize {
        referenced
            .iter()
            .filter(|k| self.journals.find_dirty(&k.bucket, &k.key).is_some())
            .count()
    }
}

#[async_trait]
impl CommitBarrier for JournalBarrier {
    async fn await_referenced(
        &self,
        referenced: &[CacheKey],
        deadline: Duration,
    ) -> Result<BarrierOutcome, BarrierError> {
        let start = Instant::now();
        let awaited = self.pending_referenced(referenced);
        loop {
            let pending = self.pending_referenced(referenced);
            if pending == 0 {
                return Ok(BarrierOutcome { awaited });
            }
            if start.elapsed() >= deadline {
                return Err(BarrierError::Timeout {
                    pending,
                    waited_ms: start.elapsed().as_millis() as u64,
                });
            }
            tokio::time::sleep(BARRIER_POLL).await;
        }
    }

    async fn await_all_dirty(&self, deadline: Duration) -> Result<BarrierOutcome, BarrierError> {
        let start = Instant::now();
        let awaited = self.journals.dirty_object_ids().len();
        loop {
            if self.journals.is_idle() {
                return Ok(BarrierOutcome { awaited });
            }
            if start.elapsed() >= deadline {
                return Err(BarrierError::Timeout {
                    pending: self.journals.dirty_object_ids().len(),
                    waited_ms: start.elapsed().as_millis() as u64,
                });
            }
            tokio::time::sleep(BARRIER_POLL).await;
        }
    }
}
