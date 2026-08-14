# verglas-cluster worklog

- #135: Classified refused consensus-peer HTTP connections as OpenRaft
  `Unreachable` instead of immediately retriable network errors. Replication
  now uses OpenRaft's bounded 500ms peer backoff after a node is killed, so a
  retained group cannot burn CPU in a connection-refusal retry storm.

- #135: Added authenticated HTTP/2 OpenRaft vote, append, and snapshot routes,
  plus a numeric-voter network factory. Requests only reach registered local
  Raft instances and unresolved or rejected peers fail closed.

- #27/#28: created the crate that houses the pod's cluster agent — `chitchat`
  gossip membership (#27) and the live weighted rendezvous ring (#28). Kept in
  its own crate so `chitchat` stays out of `verglas-core`'s dependency graph.
- #28: implemented `LiveRing` — weighted rendezvous hashing over an
  atomically-swappable (`ArcSwap`) member set, implementing the core `Ring`
  trait. `owner` scores each member `weight * (-1/ln u)` off the shared
  `rendezvous_hash`, so ownership is capacity-proportional and equal weights
  reduce to plain highest-hash (agreeing key-for-key with `RendezvousRing`).
  `update` swaps membership and bumps the epoch; a live pod reroutes without
  rebuilding the engine. Property tests cover determinism, weight
  proportionality within 5%, and minimal join/leave movement.
- #27: implemented the chitchat gossip cluster agent — `ClusterAgent` publishes
  node identity/capacity/addresses, discovers peers from the seed list, runs
  phi-accrual failure detection (tuned off the configured suspicion window), and
  a background task rebuilds the member view and updates the `LiveRing` on every
  membership change (ring first, snapshot second, so `members()` and the ring
  never disagree). `NodeMeta`/`NodeState` encode the published gossip fields.
  Transports are re-exported so consumers avoid a direct chitchat dep. Tests
  (in-process ChannelTransport) prove 3-node convergence, metadata propagation,
  and killed-node detection dropping from the ring within the window.
- #27: added in-process unit tests for the daemon-facing mapping (`AgentConfig::
  from_config` defaulting and error path, and the `NodeState` wire vocabulary),
  which the subprocess daemon tests exercise but do not cover. Raised the CI
  coverage floor to 85 (overall line coverage rose to ~85.5%).
- #29: added the peer-fetch RPC pair (`src/peer.rs`). `PeerServer` serves blocks
  from a cache-only callback over HTTP/2 (axum/hyper), matching the full
  `BlockKey` so it never serves the wrong version and never fills the backend on
  a miss; `PeerClient` implements `PeerFetch` over a pooled, multiplexed reqwest
  h2 client with a tight connect/request timeout budget, resolving an owner's
  advertised address via a `PeerResolver` (a `GossipResolver` over live
  membership in the daemon, a `StaticResolver` in tests). Wire format is a JSON
  request + raw-bytes/`204` response with a `/v0/` path marker and no version
  negotiation (prototype rules); exact-BlockKey matching subsumes ring-epoch
  `NotOwner`. Auth is a shared-secret header. Integration tests cover hit,
  clean miss, stale-ETag miss, wrong-secret reject, dead-peer fast-fail, and
  unknown-node miss.
- #30/#31: taught `LiveRing` about draining members. The snapshot now keeps
  `owners` (non-draining, scored by `owner`) and `donors` (all live, with a
  draining flag); `RingMember`/`update_states` carry the disposition and the
  agent feeds it (a `draining` gossip state excludes a node from ownership but
  keeps it a donor). Implemented `warm_donor` (rank the other live members
  against self: a member above self is a draining predecessor pulled
  unconditionally, the best below is the join predecessor) and
  `should_serve_peer` (owner plus the owner's one warm donor). Added
  `ClusterAgent::set_state` so a node can gossip itself `draining`.
- #31: added an in-process gossip test proving `set_state(Draining)` sheds a
  node's ownership on peers (their owning ring shrinks) while it stays a
  `Draining` member of the pod — the donor a successor warms from.
- #180: Added the write-back fragment plumbing. `fragments::LocalFragmentStore`
  persists erasure-coded fragments as fsynced files under the cache dir (an Ok
  store means the bytes are durable on this node, the unit the write-back ack
  counts). `peer.rs` gained three fragment endpoints (put/get/delete) served
  when the daemon binds with `FragmentHandlers`, plus a `FragmentClient` that
  places, reads, and deletes fragments on peers over the existing HTTP/2 stack
  and membership resolver. A placement failure is a real error the coordinator
  counts against quorum, not a silently-degraded miss like a block fetch.
- #180: `LocalFragmentStore` now enforces a hard byte budget: `store_fragment`
  reserves bytes before writing and refuses with `FragmentIoError::Full` over the
  ceiling, deletes release bytes, and the used count is rebuilt from disk on open.
  The budget gates new writes only — it never evicts a stored (un-propagated)
  fragment, which keeps the pod's only durable copy of acked data safe until
  propagation deletes it. Added `FragmentWriter` for streaming a fragment shard
  by shard (temp file, budget reserved per append, commit fsyncs and renames),
  a `has_headroom` check and a fragment-headroom peer RPC so placement can
  exclude a full node, and a streaming fragment PUT so a receiving node holds one
  stripe at a time.
- #220: the fragment store now writes `payload || CRC32C(payload)` on disk and
  verifies it on load. `FragmentRecord::new`/`fragment_checksum` compute the
  checksum; `load_fragment` returns a `LoadedFragment { bytes, checksum }`;
  `verify_fragment` and `list_fragment_keys` let the scrubber walk and check
  stored fragments. The streaming `FragmentWriter` folds a running CRC and writes
  the trailer at commit. The byte budget tracks payload only (the 4-byte trailer
  is fixed overhead). The fragment GET peer RPC carries the checksum in a header
  so the reader verifies end-to-end.
- #252: Added data-block geometry to the peer block request identity. A node
  with a different block size now returns a clean miss instead of ever serving
  a same-index range from the wrong byte offset.
- #223: The fragment store's byte ceiling is now dynamic. `LocalFragmentStore`
  holds a shared `Arc<AtomicU64>` the daemon's disk poll updates each tick;
  `reserve` re-reads it per attempt. A lowered ceiling only refuses new writes —
  a stored (acked) fragment is never dropped. Added `with_dynamic_ceiling`;
  `with_budget` stays as a fixed-ceiling convenience for tests.
- #223: Fragment-store docs updated for the shared-budget rework: the dynamic
  ceiling now tracks the share of the one NVMe budget the block cache is not
  using (first come, first served) rather than a fixed safety fraction. The
  ceiling mechanics (shared atomic, refuse-new-only, never drop a stored
  fragment) are unchanged.
- #61: The peer block client sends the originating request id in the
  `x-verglas-request-id` header; the block server adopts it for the handler, so
  a block one node cold-fills on a peer's behalf logs under the id the client
  saw — the cross-node trace correlation key.
- #3: Updated logical-write references for the `verglas-write` package rename.
- #91: Updated cluster process and test terminology from daemon to server for
  the `verglas-server` rename. Membership and routing behavior are unchanged.
- #84: Added storage-binding identity to peer block-fetch envelopes. Peers now distinguish identical bucket, key, ETag, and block coordinates belonging to different origins.
- Preserve the live fragment's budget charge when a larger same-key replacement
  is refused or its atomic write fails. This closes an accounting hole exposed
  by the safekeeper's bounded manifest slots: failed metadata replacement can no
  longer make occupied NVMe appear free.
- #74: Added prefix-filtered fragment discovery and authenticated owner fetch
- #127: Exposed the existing prefix-filtered fragment listing through the shared
  placement transport so WAL recovery can discover durable fragments on local
  and remote cache nodes without a separate metadata service.
  and placement RPCs for reconstructed Neon pages and ordinary cache blocks.
- #127: Split path-safe fragment identities across bounded filesystem components.
  Self-describing safekeeper WAL keys can no longer exceed Linux's per-component
  filename limit, while recursive listing still reconstructs the exact identity
  needed for replacement recovery.
- #135: Added authenticated HTTP/2 OpenRaft vote, append, and snapshot RPCs,
  keyed by both group and voter identity. One cache node can host many independent
  warehouse and timeline groups without one registration overwriting another.

- #135: Allowed the cache peer listener to merge the authenticated consensus
  router with block and fragment routes. Raft reuses the existing multiplexed
  peer port instead of introducing another deployment endpoint.
- #135: Added authenticated group provisioning on every explicit voter before
  Raft initialization. Dynamic timeline and warehouse groups create persistent
  local replicas fleet-wide instead of failing on the first peer RPC.
- #135: Added a separate authenticated bootstrap call for the deterministic
  first voter. Group creation separates fleet-wide replica opening from the
  single Raft initialization authority.
- #135: Added an authenticated opaque application-command route beside Raft
  peer RPCs. Catalog and WAL ingress adapters can forward typed requests to the
  observed group leader without duplicating agreement logic in either engine.

- #135: Added an authenticated typed membership lifecycle RPC beside ordinary
  Raft traffic. A non-leader cache ingress can forward an explicit replacement
  request to the process that owns the leader-side ring registration sequence.

- #135: Made the explicit Raft peer registry safely extensible for a prospective
  voter address. Learner admission can now bind its exact peer endpoint before
  replication starts, while unknown identities still fail closed.
- #135: Reopen a configured local Raft voter from authenticated vote, append,
  and snapshot traffic before looking up its in-memory replica. A restarted node
  now reloads retained durable groups lazily; unknown targets still return 404
  and unauthenticated traffic cannot invoke the opener.

- #135: Gave fragment recovery reads their own short availability deadline while
  preserving the longer fsync budget for fragment placement. A retained catalog
  leader now treats a blackholed certified holder as unavailable quickly, so it
  can verify the full committed prefix from surviving replicas before the
  request deadline without skipping corruption checks.
- #135: Bound the authenticated leader-command route explicitly for one
  JSON-expanded canonical 8 MiB WAL append. Non-leader ingresses can now
  forward the complete command without reopening an unbounded peer endpoint.

- #135: Moved the internal peer listener onto an owned two-worker Tokio runtime
  on its own OS thread. Raft and fragment RPCs now remain schedulable while the
  public S3/WAL/NBD runtime is saturated, and shutdown drains then joins that
  runtime so the peer listener does not outlive its cache node.
- #135: Increased the bounded fragment recovery deadline to accommodate
  multi-megabyte shards served by CPU-throttled peers. A delayed-peer regression
  keeps healthy durable fragments available while blackholed reads still fail
  before the longer placement budget.
