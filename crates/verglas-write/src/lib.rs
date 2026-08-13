//! Erasure-coded NVMe write-back tier (#180).
//!
//! ## Durability contract (opt-in, per prefix, off by default)
//!
//! For a prefix that opts in, a PUT is acked once `w` erasure-coded fragments
//! (`k` data + `m` parity, any `k` reconstruct) are fsynced on `w` distinct
//! live pod nodes, with `w >= k + f`. During the window between that ack and
//! the background upload to the origin, **the pod is the only copy of the
//! object**, held as fragments. Losing more than `n - k` (`n = k + m`) of the
//! fragment-holding nodes within that window is data loss. This is a durability
//! contract change and is therefore opt-in per prefix and off by default.
//!
//! ## Configured-ring shortfall
//!
//! Write-back acks over a quorum only when the live gossip view can place `w`
//! fragments on distinct live nodes. A configured ring below `w` rejects the
//! write before consuming its body; it never bypasses the EC contract through
//! the origin. A genuine single-node deployment remains explicitly
//! write-through, because it has no configured fragment quorum.
//!
//! ## Turn-off semantics
//!
//! "Verglas off" for a write-back prefix means flush-then-stop: drain the dirty
//! states to the origin, then stop. A crash mid-window is replayed on the next
//! start by scanning quorum-proven immutable state records and fragments. If more than `n - k` fragment
//! nodes are also lost before propagation, the unpropagated writes in that
//! window are lost — the stated cost of the contract.
//!
//! ## Iceberg ordering — the commit barrier
//!
//! A commit that references a data file must not be published before that file
//! is durable at the origin. With write-back, "durable at the origin" is the
//! propagation point, not the ack. The safe rule is **flush-before-commit**:
//! wait for the referenced objects to propagate (a release record) before writing
//! the Iceberg commit. Read-your-writes covers intra-pod readers in the
//! meantime; a direct-to-origin reader does not see the object until it
//! propagates (the lag is bounded and observable via the counters).
//!
//! [`CommitBarrier`] (and [`TransactionRecordBarrier`], its implementation over the shared
//! state) is that rule made into a primitive the commit path calls: it awaits
//! the referenced data files' propagation before the commit is forwarded, or
//! refuses the commit with a clear error when propagation cannot complete
//! (#286). It sits on the commit path, never the ack or serve hot path.
//!
//! ## Single-node write-through
//!
//! A one-node deployment ([`SingleNodeMembership`]) cannot form a durability
//! quorum. It therefore writes synchronously to the origin and acknowledges only
//! after the origin accepts the complete object. It creates no dirty EC state.

pub mod barrier;
pub mod catalog_log;
pub mod coordinator;
pub mod membership;
pub mod meta;
pub mod metrics;
pub mod policy;
pub mod reader;
pub mod state;
pub mod transport;
pub mod writer;

pub use barrier::{BarrierError, BarrierOutcome, CommitBarrier, TransactionRecordBarrier};
pub use coordinator::{ScrubReport, WriteCoordinator, WritebackError};
pub use membership::{AgentMembership, LiveMembership, SingleNodeMembership};
pub use metrics::{WritebackMetrics, WritebackMetricsSnapshot};
pub use policy::{PrefixRule, WritebackPolicy};
pub use reader::WritebackReader;
pub use state::{Placement, StateIndex, TransactionRecord, TransactionState};
pub use transport::{FragmentTransport, PeerFragmentTransport, TransportError};
pub use writer::WritebackWriter;

use std::sync::Arc;
use std::time::Duration;

use verglas_core::write::ObjectWrite;

/// The assembled write-back tier: a shared coordinator plus the reader and
/// writer wrappers the server inserts into the S3 read and write paths.
///
/// Built once at construction. When no prefix opts in, the server does not
/// build this at all and the read/write paths are exactly today's — the tier
/// adds zero hot-path cost when disabled.
pub struct WritebackTier<R, W: ObjectWrite> {
    /// The read wrapper (serves read-your-writes, delegates otherwise).
    pub reader: WritebackReader<R, W>,
    /// The write wrapper (quorum-acks eligible PUTs, delegates otherwise).
    pub writer: WritebackWriter<W>,
    /// The shared coordinator, exposed for the repair loop and metrics.
    pub coordinator: Arc<WriteCoordinator<W>>,
}

impl<R, W> WritebackTier<R, W>
where
    R: verglas_core::read::ObjectRead,
    W: ObjectWrite,
{
    /// Assembles the tier from its coordinator, the inner read path, the origin
    /// writer, and the opt-in policy. Resumes any dirty state propagation left
    /// by a previous run. There is no buffer cap: the coordinator streams every
    /// eligible write and gates on NVMe headroom, not object size.
    pub fn new(
        coordinator: Arc<WriteCoordinator<W>>,
        inner_reader: R,
        origin: Arc<W>,
        policy: Arc<WritebackPolicy>,
    ) -> Self {
        coordinator.resume_propagation();
        let reader = WritebackReader::new(inner_reader, Arc::clone(&coordinator));
        let writer = WritebackWriter::new(Arc::clone(&coordinator), origin, policy);
        Self {
            reader,
            writer,
            coordinator,
        }
    }
}

/// Spawns the background repair loop: on each membership-epoch change, repair
/// every dirty object that lost a fragment node. Returns the task handle so the
/// server can abort it on shutdown.
pub fn spawn_repair_loop<W: ObjectWrite>(
    coordinator: Arc<WriteCoordinator<W>>,
    membership: Arc<dyn LiveMembership>,
    poll: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut last_epoch = membership.epoch();
        loop {
            tokio::time::sleep(poll).await;
            let epoch = membership.epoch();
            if epoch != last_epoch {
                last_epoch = epoch;
                if let Err(error) = coordinator.repair_once().await {
                    eprintln!("writeback: repair pass failed: {error}");
                }
            }
        }
    })
}

/// Spawns the background scrubber (#220): every `interval`, walk the stored
/// fragments, verify each checksum, and re-encode any that are corrupt or
/// missing before the object drops below `k`. This catches silent bit-rot, which
/// (unlike node loss) fires no membership event for the repair loop. Returns the
/// task handle so the server can abort it on shutdown.
///
/// The pass yields between objects so it never monopolizes the runtime ahead of
/// organic serving traffic. The interval is the durability knob: it must be
/// short enough to repair a fault before enough independent faults accumulate to
/// cross `m` (see [`WriteCoordinator::scrub_once`]).
pub fn spawn_scrub_loop<W: ObjectWrite>(
    coordinator: Arc<WriteCoordinator<W>>,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            match coordinator.scrub_once().await {
                Ok(report) if report.corrupt > 0 || report.repaired > 0 => {
                    eprintln!(
                        "writeback: scrub verified {} fragments, found {} corrupt, repaired {}",
                        report.scrubbed, report.corrupt, report.repaired
                    );
                }
                Ok(_) => {}
                Err(error) => eprintln!("writeback: scrub pass failed: {error}"),
            }
        }
    })
}
