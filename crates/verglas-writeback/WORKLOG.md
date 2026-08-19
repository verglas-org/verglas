# verglas-writeback worklog

- #135: Added an injected universal-consensus commit seam to immutable
  write-back. Fragment staging now produces a hash-bound immutable header and
  durable-placement certificate, and the journal/client acknowledgement follows
  the consensus commit rather than passive fragment placement alone.

- #135: Removed the write-through shortfall paths for opt-in immutable
  write-back. A staging or consensus-quorum shortfall now refuses the PUT, so
  the universal group is the sole acknowledgement authority for dirty objects.

- #135: Updated the single-node and observability contract to match consensus
  admission. A full local fragment store refuses the PUT even if origin is up;
  a genuine one-node deployment still uses its complete-mode consensus group.

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
- #66: Softened single-node write-back durability comment (replicated block volume, not cloud-replicated product language).
- #84: Updated write-back coordination and placement keys to carry immutable storage-binding identity. Equal object names from separate providers now produce independent quorum work and ownership.
- #127: Extended the shared fragment transport with authenticated prefix
  discovery. The embedded safekeeper uses the same local-store and peer-RPC
  path to rebuild self-describing WAL after coordinator loss.
- #82: Added a compact catalog mutation log over the existing EC fragment transport. Appends persist every fragment before acknowledgment, tail reads require matching quorum copies, and readers reconstruct ordered mutations after minority loss.

- #135: Removed the standalone catalog mutation EC log. Warehouse catalog
  ordering and durability now belong only to the consensus substrate, so the
  write-back crate no longer exposes a competing authority.
- #137: Restored the verglas-writeback library name so the EC durability layer is not confused with a standalone write service. Its quorum acknowledgement and repair implementation are unchanged.
- #164: Replaced per-object propagation with a size-triggered offload stream
  (new `offload.rs`: `OffloadStream` accumulator plus `PackIndex`, the local
  resolver for flushed packs). An acked object below the configured size
  limit joins its `(storage binding, bucket)` offload stream; the stream
  flushes the whole accumulated batch into one packed S3 object, keyed under
  a reserved `_verglas/packs/` prefix, when accumulated bytes cross the limit
  or a caller invokes `WriteCoordinator::drain_offload`/`drain_all_offload`.
  An object at or above the limit bypasses accumulation and uploads directly,
  reusing the old per-object logic under new names
  (`bypass_upload_once`/`bypass_upload_with_retry`) since `propagate`,
  `propagate_locked`, and `propagate_once` are deleted outright, with no
  fallback path left to the old immediate-propagation behavior. The pack
  index (key to pack object, offset, length) commits through
  `ConsensusCommitter::commit_pack`, a new trait method mirroring the
  existing `commit`, never a sidecar file. `WritebackReader` resolves a
  flushed key by reading the exact byte range of its pack object through the
  ordinary read path, so read-your-writes holds across a flush exactly as it
  already held across the dirty window. Shaped deliberately to mirror the WAL
  segment-archive model (accumulate, flush on threshold or drain, commit,
  release local storage) so collapsing the two onto one engine (issue #164
  §6) stays a small step; that unification is not done here. Tests written
  first in `tests/offload.rs` and `offload.rs`'s own unit tests; confirmed
  failing to compile against the pre-#164 API before implementing.

- #180 (RIME perf candidate P2, negative result): investigated moving
  `finish_stream_ack`'s consensus commit off the client-ack path (defer it to
  a background task) to remove the per-object Raft round trip from write-back
  throughput. Rejected after proof by test, not by inspection.

  The write-back journal (`JournalStore`) is local, unreplicated filesystem
  state on the accepting node only — nothing else in this codebase
  reconstructs it from a peer. The synchronous `ConsensusCommitter::commit`
  call is the only mechanism that makes an object's identity, geometry, and
  fragment placements durable anywhere off that one node. Deferring it means
  the client can be acked before that replication happens; if the accepting
  node is then lost (the exact case the write-back durability contract must
  survive — `w=3` fragments living on other nodes is worthless if nothing
  durable maps them back to a key), the object is unrecoverable.

  Added `ack_never_precedes_the_consensus_commit_completing` to
  `tests/coordinator.rs` (a `BlockingCommitter` fake that stalls inside
  `commit` so the test can observe ordering directly) and confirmed it passes
  against the current synchronous implementation. Then experimentally
  rewrote `finish_stream_ack` to spawn the commit in the background and ack
  immediately after fragment quorum + local journal fsync. Both the new test
  and the pre-existing `staged_fragments_do_not_ack_before_the_consensus_commit`
  (which uses a `RejectingCommitter` to prove fragment durability alone must
  never acknowledge) failed:

  ```
  thread 'ack_never_precedes_the_consensus_commit_completing' panicked:
  the client must not be acked while the commit is still in flight

  thread 'staged_fragments_do_not_ack_before_the_consensus_commit' panicked:
  a non-committed header cannot acknowledge the PUT: PutOutcome { e_tag:
  Some("\"e7cd741eca0e9a2d61976038472228bd\""), ... }
  ```

  The second failure is the sharper proof: with a committer that will *never*
  succeed, the deferred design still returns a success `PutOutcome` to the
  client. That is exactly gate P4 in `tests/cluster-local/PERF-OBJECTIVE.md`
  ("making the commit best-effort with no ordering guarantee is a
  rejection"), demonstrated rather than merely asserted. Reverted
  `coordinator.rs` to the original synchronous commit (byte-identical to
  `f5f5104d`) and kept the new regression test, which now guards against a
  future attempt to make the same change without re-deriving this proof.
  Conclusion: the commit must stay on the ack path under the current
  architecture; the shard-count/RTT throughput ceiling needs a different
  candidate (e.g., batching many concurrent objects into fewer Raft round
  trips while still awaiting the commit before ack), not removing the
  synchronous commit.
