//! Hot-path cost of one per-table telemetry record (#60).
//!
//! Release-blocking discipline: the serving path must add near-zero cost. This
//! benchmark measures exactly what a served read pays to record its access — one
//! [`Telemetry::record`] (thread-local shard lookup + one tape append). It does
//! NOT include `classify()` (measured in a separate classification bench) or
//! body streaming; it isolates the tape write itself.
//!
//! Run with `cargo bench -p verglas-core --bench telemetry_tape`. The expected
//! result is single-digit nanoseconds per record — orders of magnitude below the
//! tens-of-microseconds a warm serve takes, so the tape is well under 1% of
//! serve cost at full load.

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use verglas_core::telemetry::tape::AccessEvent;
use verglas_core::telemetry::{Op, Rollup, Telemetry, Tier};

fn event(table: u32) -> AccessEvent {
    AccessEvent {
        table,
        tier: Tier::Dram.code(),
        op: Op::Get.code(),
        _pad: 0,
        bytes: 65_536,
        latency_us: 40,
    }
}

fn bench_record(c: &mut Criterion) {
    let telemetry = Telemetry::with_shards(8, Rollup::new());
    let mut table = 0u32;
    c.bench_function("telemetry_record", |b| {
        b.iter(|| {
            // Vary the table id so the write is not trivially predictable, the
            // way a real mix of tables would look.
            table = table.wrapping_add(1) & 0x3ff;
            telemetry.record(black_box(event(table)));
        })
    });
}

criterion_group!(benches, bench_record);
criterion_main!(benches);
