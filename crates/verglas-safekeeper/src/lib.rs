//! Neon-compatible safekeeper storage on the Verglas EC ring (#13).
//!
//! # The append-log contract
//!
//! This crate is the safekeeper component embedded in `verglas-cache-node`.
//! Neon compute connects through a workload-local pool to any cache node using
//! the ordinary safekeeper PostgreSQL protocol. The protocol adapter translates WAL pushes
//! into the [`AppendLog`] contract, and [`EcAppendLog`] commits the new bytes to
//! the same fragment ring and NVMe substrate the cache node already operates.
//! There is no second safekeeper deployment or Neon durability quorum: the
//! active Verglas ingress coordinates one EC quorum append, and another ingress
//! recovers the same logical stream from ring-replicated state after failure.
//!
//! The one primitive Postgres needs that a read cache does not provide is a
//! *durable commit*: a WAL record must be safe before it reaches S3. Everything
//! else (layer files, Iceberg data) is cache-over-S3 and is never erasure-coded.
//! The append log is the only place erasure coding applies, and only for the
//! window between a commit's ack and its flush to S3.
//!
//! ## The logical log
//!
//! The log is a single, dense, append-only byte stream. A position in it is an
//! [`Lsn`] — a `u64` byte offset, the same shape as a Postgres LSN. The stream
//! starts at a base LSN and only ever grows at the tail. There is exactly one
//! logical stream per append log (one Postgres timeline); ordering is total.
//!
//! ## 1. Append semantics — sync, quorum-acked, durable
//!
//! [`AppendLog::append`] takes the PostgreSQL start LSN plus an opaque run of WAL
//! bytes and returns only once
//! those bytes are durable. "Durable" means: the bytes were erasure-coded into
//! `k + m` fragments (any `k` reconstruct) and at least `w` of those fragments
//! are fsynced on `w` distinct live nodes, and the append's manifest record is
//! fsynced. The call blocks until that holds — it is synchronous. A Postgres
//! commit therefore blocks precisely until its WAL is quorum-durable, never on
//! an S3 round-trip. The return value is the half-open [`Appended`] range the
//! bytes occupy.
//!
//! An append is **all-or-nothing**: it either reaches the quorum and its range
//! is assigned, or it fails and the tail does not move. There is never a
//! sub-quorum ack, and there is never a synchronous-S3 fallback on the append
//! path — falling back to S3 would reintroduce the very latency the buffer
//! exists to remove. If `w` fragments cannot be placed the append fails with
//! [`AppendError::QuorumUnavailable`] and the writer retries; the WAL is not
//! lost, it is simply not yet acked. (This differs from the object write-back
//! tier, where a degraded pod safely writes *through* to S3 — a WAL commit
//! cannot, because there is no per-append S3 object to fall through to without
//! paying the latency we are eliminating.)
//!
//! ## 2. Ordering and sequencing
//!
//! Appends are totally ordered and their ranges are contiguous and monotonic.
//! Neon reconnects may resend an exact or partially overlapping range; overlap
//! is byte-validated and only a new suffix is placed. A conflicting byte or a
//! gap is rejected without moving the tail. The log is single-writer by term,
//! and appends are serialized so durable order is commit order.
//!
//! ## 3. Read-back of the un-flushed tail
//!
//! [`AppendLog::read`] returns the exact bytes of any `[from, to)` sub-range
//! that has not been truncated, whether that range is still in the EC buffer
//! (the tail) or already flushed to S3. This is what a restarting pageserver
//! replays: it reads from its last durable position to [`AppendLog::tail`] and
//! re-applies the WAL. Tail bytes are reassembled from fragments (tolerating up
//! to the codec's erasure budget of lost or corrupt fragments); flushed bytes
//! are read back from their S3 segment object. Reads never fabricate bytes — a
//! range that cannot be reconstructed fails loudly.
//!
//! ## 4. Flush-to-S3 lifecycle
//!
//! Acked appends accumulate into **segments** — contiguous LSN runs. When a
//! 16 MiB PostgreSQL WAL segment fills, a one-second low-volume deadline expires,
//! or an explicit [`AppendLog::flush`] runs, it is sealed, its
//! bytes are reassembled in order into one object, and that object is written to
//! S3. Only after S3 confirms the object durable does the log drop the segment's
//! local fragments and advance [`AppendLog::flushed_through`]. The order is
//! strict: **S3 first, then drop** — the buffer is never the sole copy of bytes
//! it has already forgotten. After a full flush the entire log is readable from
//! S3 alone (the turn-off test: the WAL is in S3, Verglas-independent).
//!
//! ## 5. Truncation / GC
//!
//! Once the pageserver has ingested WAL up to some LSN into layer files, that
//! prefix of the log is no longer needed. [`AppendLog::truncate`] drops
//! everything strictly below a caller-supplied LSN. Truncation is only allowed
//! below [`AppendLog::flushed_through`] — you can never truncate bytes that are
//! not yet safe in S3. Local fragments for flushed segments are already gone;
//! truncate additionally deletes the flushed S3 segment objects and forgets
//! their manifest records.
//!
//! ## 6. Epoch / fencing for writer handoff
//!
//! Each append log carries a monotonic writer [`Epoch`] — the fencing token.
//! [`AppendLog::fence`] installs a strictly greater epoch and persists it; every
//! [`AppendLog::append`] presents its epoch and is rejected with
//! [`AppendError::Fenced`] unless it matches the current one. When a new compute
//! takes over a timeline it fences to `old + 1` and appends under the new epoch;
//! a stale former writer that is still trying to append is fenced out and can
//! never corrupt the log. This is the safekeeper's single-writer guarantee made
//! into one primitive.
//!
//! ## 7. The failure model — what an ack means
//!
//! - **Geometry.** `k` data fragments + `m` parity fragments, any `k` of the
//!   `n = k + m` reconstruct the append (Reed-Solomon, an erasure code).
//! - **Ack quorum.** An append is acked after `w` fragments are fsynced on `w`
//!   distinct live nodes, `k <= w <= n`. Until the segment flushes to S3, those
//!   fragments are the *only* copy of the acked WAL.
//! - **Loss tolerance.** From an acked append the log survives losing any
//!   `w - k` of its acked fragments and still reconstructs; once all `n`
//!   fragments are placed (stragglers included) it survives `m = n - k` losses.
//!   Losing more than the surviving-below-`k` threshold within the ack-to-flush
//!   window is data loss — the disclosed, bounded cost of the buffer, closed by
//!   flushing to S3 promptly.
//! - **Single-node degenerate geometry.** A one-node deployment cannot form a
//!   quorum. It runs `k = 1, m = 0, w = 1`: one fragment fsynced to local NVMe
//!   plus the fsynced manifest record is the ack, no origin round-trip, async S3
//!   flush behind it. Durability is then "one local disk until the segment
//!   flushes" — issue #286's shape, applied to the WAL. This is selected
//!   automatically for a genuinely single-node deployment; it is not a knob.
//! - **No configuration beyond the geometry.** `(k, m, w)` is the whole tuning
//!   surface. There are no other knobs: segment size is a fixed flush-
//!   granularity constant, not a parameter.
//!
//! ## Where Neon consumes this
//!
//! - Walproposer `START_WAL_PUSH` messages are decoded by [`protocol`] and their
//!   explicit LSN ranges become [`AppendLog::append`] calls under the elected
//!   term. `flush_lsn` advances only after the EC durability barrier.
//! - Pageserver `START_REPLICATION` reads byte-identical WAL through
//!   [`AppendLog::read`] from its requested LSN to [`AppendLog::tail`].
//! - The storage lifecycle flushes sealed WAL segments to object storage and
//!   truncates only after pageserver no longer needs the prefix.

mod broker;
mod contract;
mod log;
mod manifest;
pub mod protocol;
pub mod server;

pub use contract::{AppendError, AppendGeometry, AppendLog, Appended, Epoch, Lsn, SafekeeperState};
pub use log::{EcAppendLog, reclaim_legacy_state_descriptors};
pub use server::{SafekeeperServer, ServerError};

// Re-export the substrate pieces this crate reuses, so a consumer wires an
// append log from one crate. The erasure codec, the fragment store, the peer
// fragment transport, and the live-membership view are the *same* machinery the
// object write-back tier (#180/#286) is built on — this crate adds the ordered,
// LSN-addressed append log over them, it does not re-implement any codec, store,
// or transport.
pub use verglas_write::membership::{AgentMembership, LiveMembership, SingleNodeMembership};
pub use verglas_write::transport::{FragmentTransport, PeerFragmentTransport, TransportError};
