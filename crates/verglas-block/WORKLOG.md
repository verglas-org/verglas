# verglas-block worklog

Append-only log of changes to this crate, by feature. Every PR touching this
crate adds an entry (see /AGENTS.md, "Worklog discipline").

- #382: New crate. Durable, cache-served virtual block devices for microVMs.
  Workload VMs are stateless; this crate is the stateful layer, and its
  durability is the cache's backing bucket. Built the content-addressed chunk
  store (`chunk`, `store`), the per-version device manifest with the flush
  barrier that never acks a manifest before every referenced chunk is durable
  (`manifest`, `device`), and the object-backend seam over `object_store`
  (`backend`). Fixed 2 MiB chunks are SHA-256 addressed so identical chunks
  dedup across devices and versions; unwritten ranges read as zeros with no
  stored chunk. Tests: chunking math, dedup across two devices, zero ranges,
  the manifest-barrier ordering (a manifest never references a non-durable
  chunk), version-swap atomicity, and cross-box read-back after flush.

- #382: Added the ring flush write-back plane (`ring.rs`). On FLUSH a device seals
  the target manifest plus its not-yet-durable chunk bytes into a bundle,
  erasure-codes it across the cache ring (RS(n-1,1), one fragment per node, acked
  once all n are durable), and returns on that ring quorum — the R2 drain runs in
  the background and releases the ring copies only after the R2 barrier. A
  degenerate or quorum-short ring falls through to the existing synchronous R2
  barrier (topology-driven, not a knob). A replicated per-flush drain descriptor
  lets any surviving shard-holder reconstruct and finish the drain after an
  originator crash (lease-expiry takeover); the chunk PUTs and manifest commit are
  idempotent, so the committed version is exactly-once. Reuses the object
  write-back tier's codec, fragment store, peer transport, and membership verbatim
  (re-exported from verglas-cluster/verglas-writeback, mirroring verglas-appendlog)
  rather than inventing a parallel mechanism. `BlockDevice` gains a flush plane;
  `ensure`/`open` keep the synchronous barrier, `ensure_on_ring`/`open_on_ring`
  attach the plane. Tests: EC bundle round-trip with one shard lost, flush acks on
  the ring quorum not R2 (proven with a failing backend), originator-crash peer
  reconstruct-and-drain committing the version exactly once, quorum-short fallback
  to the synchronous barrier, and the device-level single-node read-back.
- #3: Updated the logical-write subsystem dependency to its `verglas-write` package name.
- #91: Updated block-tier ownership documentation for the renamed server
  process. The ring and durability contracts are unchanged.

- #66: Neutralized crate and module docs so virtual block devices describe attached NBD clients rather than microVMs or proprietary cloud placement.
- #84: Updated ring write-back cache identities to carry the mandatory storage binding. Fragment placement can no longer collide across origins with equal bucket and object names.
- #127: Updated the block-ring transport test double for the shared fragment
  prefix-discovery contract introduced by safekeeper recovery. Block durability
  behavior is unchanged.
- #137: Updated block flush integration to the renamed verglas-writeback library. The fragment transport and quorum behavior are unchanged.
