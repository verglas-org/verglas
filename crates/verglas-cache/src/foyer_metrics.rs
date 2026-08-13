//! A foyer metrics registry that surfaces the block cache's silent flush-buffer
//! drops (#278). foyer's flusher drains queued disk writes into a bounded buffer
//! per rotation and discards any entry that does not fit, recording only its
//! internal `storage_queue_buffer_overflow` counter. verglas sizes the buffer to
//! hold two block entries so back-to-back writes both land, but an overflow can
//! still happen (a future geometry, or more than two writes stacked in one
//! rotation under CPU starvation), and a dropped write means a block that was
//! "warmed and flushed" is on neither tier and refills from the origin later.
//!
//! foyer only exposes that event through a `mixtrics` registry. This module is a
//! registry that reads back exactly one of foyer's counters — the block cache's
//! `buffer_overflow` — into a verglas [`AtomicU64`] and emits a WARN on each
//! drop (fitting the #61 logging), so the loss is observable instead of silent.
//! Every other foyer metric is discarded (verglas has no scrape wiring yet; #46
//! is where the rest get surfaced).

use std::borrow::Cow;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use mixtrics::metrics::{
    BoxedCounter, BoxedCounterVec, BoxedGaugeVec, BoxedHistogramVec, CounterOps, CounterVecOps,
    RegistryOps,
};
use mixtrics::registry::noop::NoopMetricsRegistry;

/// foyer's counter-vec name for its flusher's inner operations. The
/// `buffer_overflow` label within it is the silent write-drop count this
/// registry surfaces (foyer 0.22.3, `foyer-common` metrics).
const INNER_OP_COUNTER_VEC: &str = "foyer_storage_inner_op_total";

/// foyer's label value for the flush-buffer overflow counter within
/// [`INNER_OP_COUNTER_VEC`].
const BUFFER_OVERFLOW_LABEL: &str = "buffer_overflow";

/// A foyer metrics registry that tracks only the block cache's flush-buffer
/// overflow counter and drops every other metric. Installed on the data-block
/// hybrid cache via `HybridCacheBuilder::with_metrics_registry`.
#[derive(Debug)]
pub(crate) struct BlockCacheMetricsRegistry {
    /// The verglas counter foyer's `buffer_overflow` drops accumulate into.
    /// Shared with [`crate::counters::CacheCounters`] so a snapshot reads it.
    overflows: Arc<AtomicU64>,
}

impl BlockCacheMetricsRegistry {
    /// Builds a registry that accumulates flush-buffer drops into `overflows`.
    pub(crate) fn new(overflows: Arc<AtomicU64>) -> Self {
        Self { overflows }
    }
}

impl RegistryOps for BlockCacheMetricsRegistry {
    /// Returns a readable counter vec only for foyer's flusher inner-op counter;
    /// every other counter vec is discarded.
    fn register_counter_vec(
        &self,
        name: Cow<'static, str>,
        _desc: Cow<'static, str>,
        _label_names: &'static [&'static str],
    ) -> BoxedCounterVec {
        if name == INNER_OP_COUNTER_VEC {
            Box::new(InnerOpCounterVec {
                overflows: Arc::clone(&self.overflows),
            })
        } else {
            Box::new(NoopMetricsRegistry)
        }
    }

    /// Gauges carry no write-drop signal; discarded.
    fn register_gauge_vec(
        &self,
        _name: Cow<'static, str>,
        _desc: Cow<'static, str>,
        _label_names: &'static [&'static str],
    ) -> BoxedGaugeVec {
        Box::new(NoopMetricsRegistry)
    }

    /// Histograms carry no write-drop signal; discarded.
    fn register_histogram_vec(
        &self,
        _name: Cow<'static, str>,
        _desc: Cow<'static, str>,
        _label_names: &'static [&'static str],
    ) -> BoxedHistogramVec {
        Box::new(NoopMetricsRegistry)
    }

    /// Histograms carry no write-drop signal; discarded.
    fn register_histogram_vec_with_buckets(
        &self,
        _name: Cow<'static, str>,
        _desc: Cow<'static, str>,
        _label_names: &'static [&'static str],
        _buckets: Vec<f64>,
    ) -> BoxedHistogramVec {
        Box::new(NoopMetricsRegistry)
    }
}

/// The counter vec backing foyer's flusher inner operations. It hands back a
/// readable counter for the `buffer_overflow` label and a noop counter for
/// every other label (queue rotations, channel overflow, and the like).
#[derive(Debug)]
struct InnerOpCounterVec {
    /// Shared overflow accumulator (see [`BlockCacheMetricsRegistry`]).
    overflows: Arc<AtomicU64>,
}

impl CounterVecOps for InnerOpCounterVec {
    /// Returns the overflow counter for the `buffer_overflow` label, otherwise a
    /// noop counter. foyer labels this counter `[name, "buffer_overflow"]`.
    fn counter(&self, labels: &[Cow<'static, str>]) -> BoxedCounter {
        if labels.iter().any(|l| l == BUFFER_OVERFLOW_LABEL) {
            Box::new(BufferOverflowCounter {
                overflows: Arc::clone(&self.overflows),
            })
        } else {
            Box::new(NoopMetricsRegistry)
        }
    }
}

/// foyer's flush-buffer overflow counter, mirrored into a verglas counter. Each
/// increment is a disk write foyer dropped because it did not fit the flush
/// buffer — a block that will refill from the origin later. Incrementing here
/// runs on foyer's flusher thread, never on a verglas serve path.
#[derive(Debug)]
struct BufferOverflowCounter {
    /// Shared overflow accumulator (see [`BlockCacheMetricsRegistry`]).
    overflows: Arc<AtomicU64>,
}

impl CounterOps for BufferOverflowCounter {
    /// Records `val` dropped writes into the verglas counter and emits a WARN so
    /// the loss is visible to operators. A drop is degradation, not corruption:
    /// the block simply refills from the origin on its next read.
    fn increase(&self, val: u64) {
        let total = self.overflows.fetch_add(val, Ordering::Relaxed) + val;
        tracing::warn!(
            dropped_writes = val,
            total_dropped_writes = total,
            "foyer flush buffer overflow: a data-block disk write was dropped and will \
             refill from the origin on next read (#278)"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `MakeWriter` that appends every formatted log line to a shared buffer,
    /// so the test can read back the structured event foyer's drop would emit.
    #[derive(Clone, Default)]
    struct SharedBuf(Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for SharedBuf {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("buffer lock").extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for SharedBuf {
        type Writer = SharedBuf;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    /// Registering through the verglas registry and driving foyer's
    /// `buffer_overflow` counter must both bump the shared verglas counter and
    /// emit a WARN-level structured event — the observability #278 requires.
    #[test]
    fn a_buffer_overflow_bumps_the_counter_and_emits_a_warn() {
        let buf = SharedBuf::default();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .flatten_event(true)
            .with_max_level(tracing::Level::DEBUG)
            .with_writer(buf.clone())
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let overflows = Arc::new(AtomicU64::new(0));
        let registry = BlockCacheMetricsRegistry::new(Arc::clone(&overflows));

        // Register exactly as foyer does, then fetch the labelled counter and
        // simulate two dropped writes.
        let vec = registry.register_counter_vec(
            INNER_OP_COUNTER_VEC.into(),
            "foyer disk cache inner op".into(),
            &["name", "op"],
        );
        let counter = vec.counter(&["verglas".into(), BUFFER_OVERFLOW_LABEL.into()]);
        counter.increase(1);
        counter.increase(1);

        assert_eq!(
            overflows.load(Ordering::Relaxed),
            2,
            "each dropped write increments the verglas overflow counter"
        );

        let events: Vec<serde_json::Value> = {
            let bytes = buf.0.lock().expect("buffer lock").clone();
            String::from_utf8(bytes)
                .expect("log output is utf8")
                .lines()
                .filter(|l| !l.trim().is_empty())
                .map(|l| serde_json::from_str(l).expect("each log line is one JSON object"))
                .collect()
        };
        let overflow_events: Vec<_> = events
            .iter()
            .filter(|e| {
                e.get("message")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|m| m.contains("flush buffer overflow"))
            })
            .collect();
        assert_eq!(
            overflow_events.len(),
            2,
            "each dropped write emits one structured event, got: {events:#?}"
        );
        assert_eq!(
            overflow_events[0]
                .get("level")
                .and_then(serde_json::Value::as_str),
            Some("WARN"),
            "a dropped disk write is a warning, not a silent debug line"
        );
        assert_eq!(
            overflow_events[1]
                .get("total_dropped_writes")
                .and_then(serde_json::Value::as_u64),
            Some(2),
            "the event carries the running drop total"
        );
    }

    /// Non-overflow labels within the same counter vec, and every other metric
    /// family, are discarded: driving them must not touch the overflow counter.
    #[test]
    fn other_metrics_do_not_touch_the_overflow_counter() {
        let overflows = Arc::new(AtomicU64::new(0));
        let registry = BlockCacheMetricsRegistry::new(Arc::clone(&overflows));

        let inner = registry.register_counter_vec(
            INNER_OP_COUNTER_VEC.into(),
            "foyer disk cache inner op".into(),
            &["name", "op"],
        );
        inner
            .counter(&["verglas".into(), "queue_rotate".into()])
            .increase(5);

        let other = registry.register_counter_vec(
            "foyer_storage_op_total".into(),
            "foyer disk cache op".into(),
            &["name", "op"],
        );
        other.counter(&["verglas".into(), "hit".into()]).increase(5);

        registry
            .register_gauge_vec("foyer_memory_usage".into(), "usage".into(), &["name"])
            .gauge(&["verglas".into()])
            .increase(5);

        assert_eq!(
            overflows.load(Ordering::Relaxed),
            0,
            "only the buffer_overflow label feeds the verglas counter"
        );
    }
}
