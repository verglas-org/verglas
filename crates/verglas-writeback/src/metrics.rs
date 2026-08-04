//! Write-back tier counters (#180): plain atomics, no locks on the write path.
//!
//! The issue requires counting how a write was acked — via a fragment quorum or
//! via the write-through fallback — and counting mode transitions between the
//! two. Failed propagations and repairs are counted too so the bounded-lag and
//! durability contract is observable.

use std::sync::atomic::{AtomicU64, Ordering};

/// Monotonic write-back counters. Relaxed ordering: each counter is independent
/// and only added to, so a snapshot may observe mid-flight values.
#[derive(Debug, Default)]
pub struct WritebackMetrics {
    /// Writes acked after a fragment quorum became durable on distinct nodes.
    pub acked_via_quorum: AtomicU64,
    /// Writes acked by the synchronous write-through fallback (degraded mode:
    /// membership could not support the quorum, or fan-out fell short).
    pub acked_via_write_through: AtomicU64,
    /// Transitions between write-back and write-through mode. Rises when the
    /// pod crosses the quorum-membership threshold in either direction.
    pub mode_transitions: AtomicU64,
    /// Objects fully propagated to the origin and marked clean.
    pub propagated: AtomicU64,
    /// Propagation attempts that failed and will be retried.
    pub propagation_failures: AtomicU64,
    /// Fragments re-encoded and re-placed by node-loss repair or scrubbing.
    pub fragments_repaired: AtomicU64,
    /// Fragments checked by the background scrubber (#220). Rises as the scrub
    /// loop walks stored fragments verifying their checksums.
    pub fragments_scrubbed: AtomicU64,
    /// Fragments the scrubber found corrupt — present but failing their checksum
    /// (bit-rot) — before re-encoding them (#220).
    pub corrupt_fragments_found: AtomicU64,
}

/// A point-in-time copy of the write-back counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WritebackMetricsSnapshot {
    /// See [`WritebackMetrics::acked_via_quorum`].
    pub acked_via_quorum: u64,
    /// See [`WritebackMetrics::acked_via_write_through`].
    pub acked_via_write_through: u64,
    /// See [`WritebackMetrics::mode_transitions`].
    pub mode_transitions: u64,
    /// See [`WritebackMetrics::propagated`].
    pub propagated: u64,
    /// See [`WritebackMetrics::propagation_failures`].
    pub propagation_failures: u64,
    /// See [`WritebackMetrics::fragments_repaired`].
    pub fragments_repaired: u64,
    /// See [`WritebackMetrics::fragments_scrubbed`].
    pub fragments_scrubbed: u64,
    /// See [`WritebackMetrics::corrupt_fragments_found`].
    pub corrupt_fragments_found: u64,
}

impl WritebackMetrics {
    /// Adds 1 to a counter.
    pub(crate) fn bump(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Adds `n` to a counter.
    pub(crate) fn add(counter: &AtomicU64, n: u64) {
        counter.fetch_add(n, Ordering::Relaxed);
    }

    /// Copies every counter (not atomic across counters).
    pub fn snapshot(&self) -> WritebackMetricsSnapshot {
        let read = |c: &AtomicU64| c.load(Ordering::Relaxed);
        WritebackMetricsSnapshot {
            acked_via_quorum: read(&self.acked_via_quorum),
            acked_via_write_through: read(&self.acked_via_write_through),
            mode_transitions: read(&self.mode_transitions),
            propagated: read(&self.propagated),
            propagation_failures: read(&self.propagation_failures),
            fragments_repaired: read(&self.fragments_repaired),
            fragments_scrubbed: read(&self.fragments_scrubbed),
            corrupt_fragments_found: read(&self.corrupt_fragments_found),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The scrub counters (#220) start at zero and the snapshot reflects the
    /// added values.
    #[test]
    fn scrub_counters_snapshot() {
        let m = WritebackMetrics::default();
        let zero = m.snapshot();
        assert_eq!(zero.fragments_scrubbed, 0);
        assert_eq!(zero.corrupt_fragments_found, 0);
        WritebackMetrics::add(&m.fragments_scrubbed, 5);
        WritebackMetrics::add(&m.corrupt_fragments_found, 2);
        WritebackMetrics::bump(&m.fragments_repaired);
        let snap = m.snapshot();
        assert_eq!(snap.fragments_scrubbed, 5);
        assert_eq!(snap.corrupt_fragments_found, 2);
        assert_eq!(snap.fragments_repaired, 1);
    }
}
