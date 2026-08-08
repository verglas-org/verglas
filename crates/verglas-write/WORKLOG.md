# verglas-writeback worklog

- #180: New crate. The erasure-coded NVMe write-back tier. A PUT for an opt-in
  prefix is encoded into k+m fragments and acked once w land on distinct live
  nodes (WriteCoordinator); if the live gossip view cannot support w, or fan-out
  falls short by the deadline, the write degrades to a synchronous write-through
  to the origin (never a sub-quorum ack, never a rejected write). Dirty objects
  are journaled durably, read back from fragments before propagation
  (WritebackReader), propagated to the origin in the background, repaired on node
  loss by re-encoding from the survivors, and replayed on restart. WritebackPolicy
  holds the per-prefix opt-in; WritebackMetrics counts quorum vs write-through
  acks and mode transitions. Fragment placement is abstracted behind
  FragmentTransport so the coordinator's contract is tested with in-memory fakes.
- #180: The coordinator now streams a write end to end. `put_stream` feeds the
  body into `StreamingStripeEncoder` and appends each stripe's shards to per-
  fragment streaming placements, so DRAM is one stripe regardless of object size.
  Placement excludes nodes without fragment-store headroom; if fewer than `w`
  have room the write degrades to write-through before the body is consumed. A
  mid-stream shortfall rebuilds from the committed fragments (any `k`) and writes
  through, or fails cleanly below `k` — never a sub-quorum ack. `put(Bytes)` is a
  thin wrapper over `put_stream`.
- #46: the write-back reader stamps `ServedTier::Dram` on the `ObjectGet` it
  serves for dirty objects — those bytes are reassembled from DRAM-resident
  fragments, a warm serve for the request-duration histogram.
- #220: reassembly and node-loss repair now verify each fragment's checksum
  before use — a corrupt survivor is treated as an erasure and re-encoded from
  the healthy `k`, never trusted as clean. Added a background scrubber
  (`scrub_once` + `spawn_scrub_loop`) that walks the dirty journals on the
  configured interval, verifies every stored fragment, and re-encodes any corrupt
  or missing one before the object drops below `k`; it yields between objects to
  stay polite. New counters: fragments scrubbed, corrupt fragments found (repairs
  reuse the existing fragments_repaired counter).
- #286: Single-node write-back — local-durability acks with a commit barrier.
  A one-node deployment (SingleNodeMembership, now reporting `is_single_node()`)
  no longer degrades every write to synchronous write-through. Instead the
  coordinator degenerates the coding to k=1, m=0, w=1: one object fragment is
  fsynced to local NVMe and, with the fsynced journal, is the ack — no origin
  round-trip on the write path — and background propagation to the origin runs
  exactly as the §6 quorum path does. No new configuration: write-back enabled +
  single-node membership selects this mode. The codec gained the degenerate
  identity code (m=0: k data fragments, no parity, any lost fragment is
  unrecoverable) — the byte-level expression of "durability is one local disk
  until propagation completes." Multi-node quorum geometry still requires m>=1 at
  the config layer; the identity code is internal to the single-node path, so §6
  behavior is byte-for-byte unchanged (the branch is never taken for a pod, even
  one degraded to a single live node — that keeps the safe write-through
  fallback). Full buffer with the origin down refuses the write with a clear
  error (backpressure, never a silent drop); with the origin up it backpressures
  through a synchronous write-through.
- #286: The commit barrier (barrier.rs). CommitBarrier is the durability gate a
  table commit crosses before it may publish; JournalBarrier implements it over
  the shared JournalStore. Because BOTH durability backends — the §6 EC quorum
  and the #286 single-node local fsync — record their acks and propagation in the
  same journal (Dirty until the origin write succeeds, then Clean), one barrier
  over the journal serves both; CommitBarrier is the seam a future backend that
  tracked durability differently would implement. `await_referenced` waits on
  exactly the data files a commit names; `await_all_dirty` is the conservative
  superset the loopback catalog POST path uses without parsing manifests. The
  bounded wait governs how long a COMMIT blocks, never the in-flight S3
  propagation — a timed-out commit is refused with a clear error while the
  buffered data stays put and keeps retrying (transport-level-only, no wall-clock
  abandonment). Recovery-gates-serving falls out of the shared journal:
  JournalStore::open rebuilds the dirty index from the fsynced journals at boot,
  so a commit issued right after restart is gated on the recovered segment's
  replay to the origin. Wiring into the daemon's `/catalog` commit route is a
  documented seam in `bins/verglasd/src/admin.rs::catalog_request` (the tier is
  built in the data-plane scope while that router is assembled in the admin
  scope, so the barrier reaches it through a deferred OnceLock slot like the
  other engine-dependent routes) — deferred as disproportionate to this PR, whose
  core is the fast-ack path and the tested barrier primitive.
- #286: Crash-recovery evidence in `tests/writeback-recovery/run.sh`: a real
  daemon fast-acks a PUT with MinIO stopped, is kill -9'd between the ack and the
  flush, and after MinIO returns and the daemon restarts, boot recovery replays
  the buffered segment so the object reaches S3 byte-identically. The test first
  proves the object was ABSENT from S3 at crash time, so this is genuine
  ack-then-crash-then-replay. It kills only its own daemon PID and stops MinIO by
  a unique container name.
- #3: Renamed the crate from `verglas-writeback` to `verglas-write`; write-back and synchronous write-through are internal durability modes of one logical write subsystem.
- #263: Removed the commit-barrier documentation's dependency on a loopback
  catalog proxy. Verglas does not host a catalog; the barrier remains available
  to explicit customer-invoked commit operations without implying such a route.
- #91: Updated write-back process documentation for the `verglas-server`
  rename. Durability barriers and propagation behavior are unchanged.
- #74: Added strict quorum mode for storage publications that must keep one
  acknowledgement boundary. Membership, headroom, and placement shortfalls now
  reject a strict write instead of falling back to an origin acknowledgement.
- #74: Replicated each dirty journal to its fragment holders and added ring-wide
  manifest discovery, so any cache node can serve an acknowledged object before
  origin propagation. Propagation now retries until success or shutdown instead
  of abandoning a dirty journal after eight attempts.
- #66: Softened single-node write-back durability comment (replicated block volume, not cloud-replicated product language).
