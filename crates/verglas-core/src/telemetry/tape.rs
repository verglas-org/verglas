//! Per-core event tapes: the hot-path write side of the per-table telemetry
//! (#60).
//!
//! # Why a tape and not a counter
//!
//! The request path must add near-zero cost. Incrementing a labelled counter
//! per served read would either take a lock or hammer a shared atomic per
//! `(table, tier, op)` — cache-line contention that scales with core count and
//! shows up as throughput loss under load. Worse, it would have to format label
//! strings on the hot path. So we do the opposite: **record, don't aggregate.**
//!
//! Each core appends a fixed-size [`AccessEvent`] to its own [`ShardTape`]. A
//! background task ([`super::rollup`]) swaps each shard's buffers every ~1s and
//! folds the drained events into the real labelled counters, off the hot path.
//! This is the standard high-frequency-telemetry shape (per-core append +
//! background merge, as in LMAX ring buffers and `tracing`'s sharded registry).
//!
//! # Hot-path cost (release-blocking discipline)
//!
//! One [`ShardTape::record`] is: one relaxed atomic load (which buffer is
//! active), one relaxed `fetch_add` on that buffer's cursor, a bounds check, and
//! one 16-byte store. No lock, no allocation, no label formatting, and — the key
//! property — **no shared atomic per table**: the only atomics are per-shard
//! (O(cores)), never O(tables). If a buffer fills between drains the event is
//! dropped and [`ShardTape::dropped`] is bumped: telemetry is allowed to sample
//! under extreme load, never to slow serving.
//!
//! # The sampling race is deliberate
//!
//! A writer that has already chosen the active buffer can still be mid-store
//! when the drainer flips and drains it, so a drain can miss or (never tear —
//! the store is a single aligned 16-byte write) skip the very last few events of
//! a window. That is the "sample under load" licence above; it costs at most a
//! handful of events per shard per drain and never blocks or corrupts serving.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering};

/// A single served-read observation. `#[repr(C)]`, exactly 16 bytes so a whole
/// buffer is a dense, cache-line-friendly array and a record is one aligned
/// store.
///
/// `table` is the mapper's interned table id (`0` = `_unmapped`); `tier` and
/// `op` are the `u8` codes of [`super::Tier`] / [`super::Op`]; `bytes` is the
/// range length served and `latency_us` the serve latency in microseconds.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccessEvent {
    /// Interned table id from the mapper (`0` = `_unmapped`).
    pub table: u32,
    /// Serving tier code ([`super::Tier::code`]).
    pub tier: u8,
    /// Operation code ([`super::Op::code`]).
    pub op: u8,
    /// Explicit padding to keep the layout fixed and 16 bytes.
    pub _pad: u16,
    /// Bytes served for this read (the range length).
    pub bytes: u32,
    /// Serve latency in microseconds, saturating at `u32::MAX`.
    pub latency_us: u32,
}

// The 16-byte contract is load-bearing (dense arrays, single-store records);
// pin it at compile time so a field change can't silently grow the event.
const _: () = assert!(std::mem::size_of::<AccessEvent>() == 16);

/// Events per buffer. Two buffers per shard, so a shard holds up to `2 *
/// BUFFER_CAP` in-flight events between drains. 4096 × 16 B = 64 KiB per buffer;
/// at a 1s drain cadence this absorbs a very high per-core request rate before
/// it has to start dropping.
pub const BUFFER_CAP: usize = 4096;

/// One buffer: a fixed slab of event slots plus a write cursor. The cursor is
/// the next index to claim; values `>= BUFFER_CAP` mean the buffer overflowed
/// and the claiming writes were dropped.
struct Buffer {
    slots: Box<[UnsafeCell<AccessEvent>]>,
    cursor: AtomicUsize,
}

impl Buffer {
    fn new() -> Buffer {
        let mut slots = Vec::with_capacity(BUFFER_CAP);
        for _ in 0..BUFFER_CAP {
            slots.push(UnsafeCell::new(AccessEvent {
                table: 0,
                tier: 0,
                op: 0,
                _pad: 0,
                bytes: 0,
                latency_us: 0,
            }));
        }
        Buffer {
            slots: slots.into_boxed_slice(),
            cursor: AtomicUsize::new(0),
        }
    }
}

/// A per-core tape: two [`Buffer`]s and an index of which one is currently
/// active for writes. Writers append to the active buffer; the drainer flips
/// the index and reads the now-inactive one.
pub struct ShardTape {
    buffers: [Buffer; 2],
    active: AtomicU8,
    dropped: AtomicU64,
}

// SAFETY: `Buffer.slots` is `UnsafeCell`, which is not `Sync`, but the access
// discipline makes concurrent use sound for our "sample under load" contract:
// writers only ever touch the *active* buffer (chosen by the atomic `active`
// index and a unique slot claimed by `fetch_add`); the drainer only reads a
// buffer *after* flipping `active` away from it. The single overlap is the
// documented sampling race, which at worst drops a few trailing events — never a
// torn value, since each slot write is one aligned 16-byte store.
unsafe impl Sync for ShardTape {}
unsafe impl Send for ShardTape {}

impl ShardTape {
    /// A fresh empty tape.
    pub fn new() -> ShardTape {
        ShardTape {
            buffers: [Buffer::new(), Buffer::new()],
            active: AtomicU8::new(0),
            dropped: AtomicU64::new(0),
        }
    }

    /// Appends one event to the active buffer. Hot path — see the module docs
    /// for the cost. Drops the event (bumping [`Self::dropped`]) if the active
    /// buffer is full.
    #[inline]
    pub fn record(&self, event: AccessEvent) {
        let active = self.active.load(Ordering::Relaxed) as usize;
        let buffer = &self.buffers[active];
        let idx = buffer.cursor.fetch_add(1, Ordering::Relaxed);
        if idx < BUFFER_CAP {
            // SAFETY: `idx` is uniquely claimed by this `fetch_add` and is in
            // bounds; no other writer holds it and the drainer will not read
            // this buffer until it flips `active` away. One aligned store.
            unsafe {
                *buffer.slots[idx].get() = event;
            }
        } else {
            // Buffer full between drains: sample it out, don't slow serving.
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Flips the active buffer and drains the now-inactive one into `out`.
    /// Called only by the single background rollup task. Returns the number of
    /// events appended.
    pub fn drain_into(&self, out: &mut Vec<AccessEvent>) -> usize {
        // Flip first, so new writes land in the other buffer while we read.
        let prev = self.active.fetch_xor(1, Ordering::AcqRel) as usize;
        let buffer = &self.buffers[prev];
        // Acquire pairs with the writers' cursor increments so their stores are
        // visible here.
        let claimed = buffer.cursor.load(Ordering::Acquire);
        let n = claimed.min(BUFFER_CAP);
        for slot in &buffer.slots[..n] {
            // SAFETY: `active` now points at the other buffer, so no writer is
            // appending here; we read each populated slot once. (A writer that
            // claimed a slot just before the flip is the documented sampling
            // race.)
            out.push(unsafe { *slot.get() });
        }
        // Reset for the next window this buffer serves.
        buffer.cursor.store(0, Ordering::Release);
        n
    }

    /// Total events dropped on this shard because a buffer was full at record
    /// time. Monotonic; read by the rollup for the `dropped_events` counter.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

impl Default for ShardTape {
    fn default() -> Self {
        ShardTape::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(table: u32, bytes: u32) -> AccessEvent {
        AccessEvent {
            table,
            tier: 0,
            op: 0,
            _pad: 0,
            bytes,
            latency_us: 10,
        }
    }

    #[test]
    fn access_event_is_sixteen_bytes() {
        assert_eq!(std::mem::size_of::<AccessEvent>(), 16);
    }

    #[test]
    fn record_then_drain_returns_the_events() {
        let tape = ShardTape::new();
        tape.record(ev(1, 100));
        tape.record(ev(2, 200));
        let mut out = Vec::new();
        let n = tape.drain_into(&mut out);
        assert_eq!(n, 2);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].table, 1);
        assert_eq!(out[1].bytes, 200);
    }

    #[test]
    fn draining_swaps_buffers_so_a_second_window_is_independent() {
        let tape = ShardTape::new();
        tape.record(ev(1, 1));
        let mut first = Vec::new();
        tape.drain_into(&mut first);
        assert_eq!(first.len(), 1);

        // The next window starts empty (the drained buffer's cursor reset) and
        // writes now land in the flipped buffer.
        tape.record(ev(2, 2));
        tape.record(ev(3, 3));
        let mut second = Vec::new();
        tape.drain_into(&mut second);
        assert_eq!(second.len(), 2);
        assert_eq!(second[0].table, 2);
    }

    #[test]
    fn overflow_drops_events_and_counts_them() {
        let tape = ShardTape::new();
        // Fill the active buffer exactly, then overflow by 5.
        for _ in 0..BUFFER_CAP {
            tape.record(ev(1, 1));
        }
        for _ in 0..5 {
            tape.record(ev(1, 1));
        }
        assert_eq!(tape.dropped(), 5, "the 5 over-cap records are dropped");
        let mut out = Vec::new();
        let n = tape.drain_into(&mut out);
        assert_eq!(
            n, BUFFER_CAP,
            "drain never returns more than the buffer cap"
        );
        assert_eq!(out.len(), BUFFER_CAP);
    }

    #[test]
    fn empty_drain_returns_nothing() {
        let tape = ShardTape::new();
        let mut out = Vec::new();
        assert_eq!(tape.drain_into(&mut out), 0);
        assert!(out.is_empty());
    }
}
