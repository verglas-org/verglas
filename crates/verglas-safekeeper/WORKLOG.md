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
- #130: Made recovery state identify the database timeline rather than the ingress node, so another cache member can resume the same WAL stream without duplicating the EC durability quorum. Background origin drain now accumulates the two 8 MiB Neon frames in a 16 MiB PostgreSQL WAL segment, while the one-second timer and explicit flush bound low-volume and pause latency.
