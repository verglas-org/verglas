# verglas-appendlog worklog

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
