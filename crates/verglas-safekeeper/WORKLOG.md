# verglas-safekeeper worklog

- #372: New crate. Pinned the append-log contract (the substrate<->pageserver
  boundary for pg-engine) as the crate-root doc plus the `AppendLog` trait, and
  built the EC quorum-append WAL buffer (`EcAppendLog`) against it. Reuses the
  existing erasure machinery — the write-back codec, the cluster fragment store,
  the peer fragment transport, and the live-membership view — and adds the
  ordered, LSN-addressed append log over them: sync quorum-acked append,
  un-flushed-tail read-back from fragments, seal-and-flush segments to S3 then
  drop local fragments, truncation/GC below the flush watermark, and epoch
  fencing for writer handoff. Single-node deployments run the degenerate
  `k=1,m=0,w=1` geometry (#286's shape). Durable state is a single fsynced JSON
  manifest so a restart rebuilds the tail from fragments (within erasure
  tolerance) or from S3 after a full flush. In-process crate only; no network
  service wrapper and no verglasd wiring yet — the pageserver fork consumes it as
  a library. Tests: 9 multi-node contract/recovery tests + 3 single-node tests.
- chore: Remove docs/ cross-references after deleting the docs directory. Crate module docs are the reference now.
- #3: Updated the logical-write subsystem dependency to its `verglas-write` package name.
- #13: Renamed the crate to `verglas-safekeeper` and replaced the obsolete
  direct-pageserver seam with the Neon protocol-v3 boundary used by an embedded
  cache-node safekeeper. WAL appends now preserve caller-supplied PostgreSQL
  LSNs, validate reconnect overlap byte-for-byte, append only a new suffix, and
  reject gaps or divergent WAL without moving the durable tail.
- #13: Completed the embedded safekeeper. Added the PostgreSQL v3 listener and
  Neon's protocol-v3 `START_WAL_PUSH`, greeting/vote/elected/append exchange,
  quorum-backed append acknowledgements, `START_REPLICATION` WAL readback,
  `IDENTIFY_SYSTEM`, and `TIMELINE_STATUS`. Timeline terms, watermarks, term
  history, append descriptors, and the latest-state head are replicated across
  the EC ring, so a scheduler replacement with an empty local state directory
  recovers from surviving caches. A background loop drains committed WAL to
  object storage and only then deletes EC fragments. Tests cover pinned Neon
  wire vectors, real PostgreSQL sockets, node loss, coordinator replacement,
  asynchronous drain, and byte-identical physical replication.
- #46: Stopped the background drain from deleting WAL based only on the
  walproposer truncate watermark. A lagging or cold pageserver can now read
  drained segments until a future pageserver-confirmed retention boundary is
  added, with a socket-level regression test covering that launch race.
- #58: Allowed the eight-argument `EcAppendLog::open` (node identity plus ring plane) and replaced test `unwrap`s with `expect` so clippy stays clean after the cache-metadata fleet fixes.

- #66: Documented Neon broker advertise reachability as Postgres compute over the tenant network rather than a microVM.
- #84: Updated safekeeper cache identities to include the mandatory storage binding. Durable write tracking no longer aliases equal bucket and key names from different origins.
- #84: Matched the published Verglas Neon walproposer's 8 MiB append contract and
  made the durable storage binding an explicit safekeeper input. Large startup WAL
  batches are accepted and WAL drains target the cache node's actual backend.
- #87: Added optional foreground admission accounting to the Neon listener. Cache-node deployments now reject new safekeeper connections after a host fence and retain existing connections in the in-flight count until they close, while background WAL drain remains independent.
- #87: Count safekeeper activity per accepted protocol message instead of per TCP connection. Idle and dead Neon sessions no longer prevent a fenced cache from scaling to zero, while every WAL mutation remains fenced through its durability acknowledgement.
- #87: Release the `START_REPLICATION` command guard before entering the long-lived physical replication stream, then account only active WAL sends, feedback reads, and keepalives. An idle replication client no longer pins the cache awake, and the socket regression test now asserts the safekeeper plane returns to zero in-flight work while the connection remains open.
- Fixed persistent-fragment exhaustion on sustained Neon WAL. Replicated
  recovery manifests now alternate between two quorum-published slots instead
  of leaking one full descriptor per revision. WAL admission that cannot find
  quorum headroom synchronously streams the already-acked tail to object
  storage, publishes the compact flushed state, evicts only then, and retries
  placement. Upgrade recovery accepts legacy revision-keyed descriptors, and a
  conservative local migration deletes only stale legacy descriptors whose
  committed head names another revision. Bounded-store tests cover steady-state
  commits, pressure offload/eviction, migration GC, and legacy recovery. Every
  acknowledged append now wakes the S3 drain immediately; the one-second timer
  remains only as retry insurance after origin failures.
- #74: Made pageserver feedback advance retention only from the explicit
  `vg_durable_lsn` watermark and accepted Neon's read-replica
  `START_REPLICATION SLOT ... TIMELINE ...` command.
- #127: Parallelized WAL fragment and checkpoint placement so a four-node
  `k=2,m=2,w=3` ring acknowledges after three NVMe-durable completions without
  waiting for the fourth. WAL fragment identities now carry their recovery
  metadata, removing per-commit manifest replication while preserving
  replacement-coordinator recovery from any reconstructible contiguous suffix.
- #127: Matched Neon backup batching by draining complete 16 MiB WAL segments
  during normal operation and retaining partial tails on persistent cache
  volumes. Explicit lifecycle flush still checkpoints a partial tail to object
  storage, and pressure may force a drain before rejecting new WAL.
- #127: Made physical replication reconstruct only WAL appends that overlap the
  requested range. Tail-following pageservers no longer reread every fragment in
  the open segment for each frame, which had made sustained writes quadratic and
  left pageserver minutes behind the acknowledged safekeeper LSN.
- #135: Added the Neon-facing timeline adapter over `verglas-consensus` with
  acquire, append, fenced read, release, and archive-checkpoint operations. The
  new protocol exposes no proposer vote, donor, election, or quorum controls.
- #135: Removed the old Neon proposer/acceptor protocol, per-ingress `EcAppendLog`,
  donor recovery, broker registration, and their legacy tests. The crate now
  contains only the thin WAL data protocol over the universal consensus engine.
- #135: Replaced the Neon-facing JSON messages with a strict canonical binary
  codec suitable for the transport-only C client. External clients can no
  longer claim S3 archive progress; only the internal archive worker may submit
  that state-machine transition.
- #135: Added a linearizable WAL-status operation returning the applied index,
  committed tail, and verified archive watermark. Pageservers can now choose an
  exact committed ReadWal interval without guessing beyond the group tail.

- #135: Added an internal asynchronous WAL segment archiver. It reads a fenced
  committed interval, uploads and verifies its content-addressed object, then
  commits the archive checkpoint before exposing a checkpoint-gated release
  token; origin failure leaves local WAL retained.

- #135: Exposed the same verified archive path for an explicit final partial
  WAL tail. Lifecycle callers cannot turn an empty or uncommitted tail into an
  archive watermark because the regular range and checkpoint validation remain
  in force.
- #135: Added the transport-only OpenTimeline operation carrying an exact retry identity and initial PostgreSQL LSN. Compute can now initialize a fresh consensus timeline at its real redo position instead of assuming LSN zero.
- #135: Added verified archive-object reads and deterministic multi-segment WAL
  prefix composition. Missing, corrupt, overlapping, or incomplete archive
  identities fail closed before any retained tail is returned.
