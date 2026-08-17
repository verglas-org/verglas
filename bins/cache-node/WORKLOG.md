# cache-node worklog

- fleet cache image: New standalone cache serving daemon `verglas-cache-node`.
  It is the CACHE-relevant subset of `verglasd`: config load (reusing
  `verglas-core`'s schema and validation verbatim), the SigV4 S3 frontend
  (`verglas-s3` `router_with_passthrough`), the foyer cache tiers over a
  single-node rendezvous ring (`verglas-cache` `HybridCacheEngine` with
  `NoopPeerFetch` + `RendezvousRing::single`), the origin backend with the
  startup probe and read-through/write-through (`verglas-backend`), the
  disk-full admission guardrail (#96), and a four-endpoint admin surface
  (`/admin/healthz`, `/admin/version`, `/admin/stats`, `/metrics`) matching the
  fleet health-check and metrics-scrape contracts. Deliberately excludes the
  cluster ring/gossip/peers, the write-back tier (the fleet cache image never
  enables it and it needs the excluded peer transport), the catalog watcher and
  table lifecycle, and every DataFusion/platform/harness/memory surface — the
  cache VM does exactly one job. Accepts the exact config the fleet cache image
  boot script renders (`fleet/images/boot/cache-boot.sh`), so the image swaps
  binaries without a boot-script change. Auth/logging/`background_fill_limit`
  helpers are copied from `bins/verglasd` (each noted in-place) rather than
  shared, to keep this crate independent of the daemon.

- #382: Added the block-device tier. New `nbd` module serves attached
  `verglas-block` devices to the Linux kernel NBD client over a single fixed
  newstyle listener on port 8335 (export name = device id; READ/WRITE/FLUSH/
  TRIM/DISC, with FLUSH and clean DISC forcing the durability barrier + manifest
  commit). New `blockdev` module holds the device registry over one chunk store
  and the `POST /blocks/ensure` control route (merged into the admin surface) the
  host agent calls before attaching. `serve::run` builds the chunk store over the
  single configured backend bucket, binds the NBD listener, and joins it with the
  admin and S3 planes. `vhost-user-blk` is noted as the extension point.

- #382: Wired the block-flush write-back ring (`ring.rs`). When VERGLAS_RING_PEERS
  names a ring (id=host:port entries) and VERGLAS_NODE_ID is this box, the node
  builds the flush plane over the device registry's chunk store, serves the
  fragment RPC endpoints peers place shards through on VERGLAS_RING_ADDR
  (default :8336, no new authz — VXLAN isolation like the NBD plane), and runs the
  drain-takeover loop. With no ring configured the block tier stays single-node and
  FLUSH is the synchronous origin barrier, unchanged. The object serve path is still a
  peerless cluster-of-one; only the block tier reaches verglas-cluster's fragment
  store and peer RPC. `DeviceRegistry` gained an optional ring plane it attaches
  once at startup; ensure/get route flushes through it when present.
- #3: Updated the logical-write subsystem dependency to its `verglas-write` package name.
- #91: Renamed the full local process from `verglasd` to `verglas-server` in
  cache-node parity documentation. The comparison now names the foreground
  server binary used by self-hosted deployments.
- #13: Embedded `verglas-safekeeper` in the cache-node process. The Neon
  PostgreSQL listener shares the existing fragment transport, membership,
  local NVMe fragment store, and peer RPC listener with block FLUSH; it is not
  another daemon or deployment. Three-node rings use `k=2,m=1,w=3`; four and
  larger nodes retain two parity fragments and acknowledge after `n-1`
  placements. Added a process-level test that launches three real cache-node
  binaries, pushes WAL through one ingress, observes the EC quorum ack, and
  reads the exact bytes back with physical replication.
- #58: Added the cache-owned Iceberg REST gateway and catalog watcher used by local query workers. Watcher refreshes and query reads share the same bounded response cache, so ephemeral query processes never own upstream credentials or catalog state.

- #58: Cache-node catalog watching uses `PollingWatcher` only. Dropped `VERGLAS_CATALOG_FEED_*` and the websocket upgrade attempt against the catalog origin.

- #58: Hardened the embedded safekeeper process test: wait for all three children to log listen readiness, capture stderr, and retry the Postgres startup handshake so CI does not flake on early connect.

- #74: Routed pageserver layer and index PUTs through the shared EC fragment
  ring. The S3 endpoint now acknowledges a ring-backed PUT after quorum fsync,
  keeps dirty fragments outside cache eviction, propagates to the origin in the
  background, and exposes write-back counters. The disk monitor gives ordinary
  cache blocks and durability fragments one physical NVMe ceiling without
  evicting acknowledged dirty data.
- #74: Made origin probing asynchronous so a cache node recovers and serves
  dirty ring data during an origin outage. The fragment server now exposes the
  journal-manifest discovery callback used by cross-node dirty reads.
- #66: Rewrote block-device and NBD docs for attached NBD clients instead of microVMs, and dropped cloud-fleet wording from the package description.
- #66: Rewrote cache-node crate and serve docs for standalone self-host (dropped fleet image / cloud product contrasts); kept scripts/cloud path references out of this binary.
- #84: Wired the cache node's built-in managed lakehouse binding explicitly
  through backend construction, block-device store lookup, and the S3 router.
- #82: Added explicit eventual polling and strong quorum-backed catalog runtimes. Strong mode requires the three-node fragment ring, verifies ordered Lakekeeper events, catches query reads up to the EC tail, and returns applied event proofs without a polling fallback.
- #84: Added the cache-node Docker target and rendered local startup contract used by the three-member OSS fragment ring. The ring exposes one selected embedded safekeeper for managed Neon while retaining erasure-coded WAL durability across all three cache volumes.
- #84: Passed the cache node's managed backend binding into its embedded
   safekeeper so completed WAL segments drain to the configured object store.
- #87: Added the authenticated host-agent quiescence API and wired one atomic admission fence across S3/catalog HTTP, NBD connections, fragment RPC operations, and embedded safekeeper connections. The fence rejects new work, reports already-accepted work until it drains, and can be reopened only with its current generation; background recovery and propagation do not create a ring-drain requirement.
- #109: Kept the stacked cache-node base compatible with the current Rust lint gate by boxing large response/catalog values and grouping the S3 server inputs in one explicit context.
- #109: Resolved DNS names in `VERGLAS_RING_PEERS` at startup. Containerized
  cache peers can now use stable Compose service names while the fragment RPC
  client retains its concrete socket-address contract.
- Reclaim stale revision-keyed safekeeper recovery descriptors when the shared
  fragment ring starts. The committed legacy head remains pinned, malformed or
  missing heads cause no deletion, and the new two-slot safekeeper protocol no
  longer grows this metadata without bound.
- #74: Exposed exact reconstructed-page GET/PUT routes on the admin listener,
  backed by the same hybrid engine and recovery gate as Iceberg data.
- #74: Extended rendezvous ownership and cache peer transport to reconstructed
  Neon pages and ordinary object blocks, allowing any ingress node to fetch from
  the owner and retain a local hot replica.
- #74: Added a four-node shared-cache regression covering cross-node page heat,
  real Neon query-after-write, safekeeper publication, and quorum writes with a
  ring member stopped.
- #133: Made the cache node the public engine container entrypoint and added optional Iceberg catalog rendering to its startup script. The engine-only Compose stack now launches only object storage and the cache role.
- #109: Made configured ring membership fail closed while peer DNS is changing.
  Startup now waits for every declared peer instead of silently forming a
  smaller EC ring that can advertise health without the safekeeper PostgreSQL
  depends on.
- #127: Replaced ring-size-derived safekeeper geometry with mandatory explicit
  `VERGLAS_SAFEKEEPER_EC_K/M/W` settings validated against the complete ring.

  Managed four-node deployments can now declare `2/2/3`, while the OSS
  three-node stack declares `2/1/3` rather than silently changing durability.
- #135: Served group-keyed Raft RPCs on the existing cache peer listener and
  derived an explicit numeric voter address map from the configured ring. Gossip
  remains outside consensus membership and cannot reconfigure a group.
- #135: Adapted consensus coded representations to the existing fsynced fragment
  RPC and store. Successful remote placement now supplies the durability proof
  used before Raft header submission, with checksummed reconstruction reads.
- #135: Added the cache-node Multi-Raft registry. Dynamic groups open persistent
  replicas on every explicit voter and initialize through one deterministic
  bootstrap voter before catalog or WAL commands can be accepted.
- #135: Added any-ingress typed command routing to the leader reported by the
  local Raft replica. Forwarded commands execute only on the actual leader and
  preserve their exact request identity across leadership changes.
- #135: Kept any-ingress routing within its existing leader-observation deadline
  when a just-observed leader dies before receiving a command. The ingress now
  retries the unchanged typed request only after re-observing Raft, so leader
  loss does not become a false client conflict or alter request identity.
- #135: Replaced the cache node's live Neon Vote/Elected safekeeper listener with
  the Verglas WAL protocol. Every ingress provisions a timeline group and routes
  writer, append, read, release, and checkpoint commands through Raft.
- #135: Added CRaft complete-entry fallback to the live WAL ingress. When coded
  staging cannot reach its intersection threshold, the same exact append is
  retried as a full copy on a regular majority without changing Raft safety.
- #135: Added the native managed-catalog ingress beside the WAL protocol. Typed
  Lakekeeper transactions and fenced namespace/table reads route to independent
  warehouse groups on the same Multi-Raft and payload substrate.
- #135: Switched the live Neon ingress to the canonical binary WAL protocol and
  removed client-submitted archive checkpoints. The cache node now decodes
  complete frames strictly and returns only consensus-applied binary results.

- #135: Removed the cache-node's legacy catalog EC log and its strong admin
  proxy. External REST catalogs are explicitly eventual; managed warehouse
  catalog mutations and reads use their native ConsensusPlane group only.

- #135: Attached each cache-node group's distributed durable payload store to
  its state machine before Raft can build snapshots. Catalog checkpoint
  compaction now reclaims only the already-checkpointed ring fragments, and a
  failed peer deletion leaves authoritative metadata available for retry.
- #135: Wired immutable S3 write-back publication to per-object groups on the
  same Multi-Raft plane. A coded fragment set is no longer acknowledged until
  its deterministic placement certificate is committed by consensus.
- #135: Added the fenced binary WAL-status response used by pageservers to read
  exact committed ranges through any ingress without safekeeper discovery.

- #135: Native catalog requests now require explicit tenant and warehouse path
  identities. The ingress first registers and resolves the tenant-root route,
  then submits only to that tenant-scoped warehouse group.
- #135: Routed OpenTimeline through the timeline Multi-Raft group before writer acquisition. Cache ingresses now establish a real PostgreSQL starting LSN as committed state.
- #135: Wired the segment archiver into successful WAL appends using an explicitly configured archive bucket. Complete segment boundaries upload and verify content-addressed objects in the background, then commit their checkpoint through the timeline group without delaying foreground acknowledgement.
- #135: Restricted complete-entry WAL fallback to failures that prove coded
  durability is unavailable. Semantic conflicts and Raft failures now fail closed
  instead of being retried under a different storage mode.

- #135: Routed typed hosted-catalog record reads and listings through warehouse
  consensus groups. The cache-node ingress now exposes one read authority for
  Lakekeeper domain state and Iceberg metadata pointers.

- #135: Added explicit prospective-voter peer-address registration at the ring
  boundary. Membership lifecycle code can resolve a learner's authenticated
  Raft endpoint without treating ordinary read-ring gossip as consensus authority.

- #135: Added leader-only voter replacement sequencing: register a precise peer
  address, provision and catch up the learner, repair committed payloads, then
  commit OpenRaft's joint and uniform voter transition.

- #135: Mounted authenticated administrative voter replacement. Host agents now
  submit an explicit remove/add/address request; the node validates its durable
  voter set and invokes the repair-first CRaft transition with no catalog path.

- #135: Made authoritative stop prove local voter relinquishment after draining.
  A host can only receive stop success once this process is absent from every
  committed hosted-group voter set; archive completion alone is insufficient.

- #135: Added route coverage for authenticated membership replacement. The test
  proves unauthenticated and malformed transitions are rejected, while a valid
  request invokes exactly one injected lifecycle operation.

- #135: Registered prospective voters in the shared payload peer map before
  learner catch-up, then publish new representation slots only after uniform
  membership commits. Candidate fragment traffic now uses the stable node ID
  carried and verified by the lifecycle request.

- #135: Refresh the open group's payload allocation after uniform membership
  commits. New WAL and catalog entries now stage against the committed voter
  ordering rather than the process's original static ring snapshot.

- #135: Forward membership replacement from a non-leader ingress to its
  observed leader, and refuse the operation while no leader is known. Lifecycle
  reconfiguration no longer has a local no-leader execution path.
- #135: Made ring representation identities include both the content hash and
  exact request identity, and persisted the hash inside the representation
  frame. Repeated empty or equal bodies can no longer overwrite unrelated
  committed allocations before a recovery fence.
- #135: Added group and configuration generation to fragment object identity.
  Membership repair stages into a separate namespace, so candidate fragments
  cannot overwrite the currently committed source allocation before Raft
  publishes the replacement certificate.
- #135: Made dynamic consensus-group provisioning open all configured voters
  concurrently with a hard per-peer deadline. It now proceeds only after a
  Raft majority opens successfully and bootstraps through the lowest successful
  voter, so one unavailable nonleader does not block the four-voter `2/2/3`
  geometry while a two-voter minority still fails closed. Once an ingress has
  an initialized local replica, normal WAL and catalog requests skip group
  provisioning and rely on the committed Raft quorum directly.
- #135: Register this cache node's Raft voter identity before serving peer RPCs.
  Authenticated replication can now lazily reopen a retained durable timeline
  or warehouse group after restart, while unknown numeric targets remain closed.
- #135: Return HTTP 409 only for CRaft catalog CAS and request-identity
  conflicts. All other catalog submission failures are HTTP 503 so another
  ingress can preserve availability during leader or quorum disruption.
- #135: Added a four-process native-catalog leader-loss regression with a
  realistic retained warehouse prefix. It proves a surviving ingress serves
  an immediate fenced read and mutation at three of four voters, while a
  two-voter catalog minority remains closed.
- #137: Mounted the semantic REST-JSON dispatcher on the existing cache-node
  S3 listener before the ordinary S3 fallback. When a catalog is configured it
  opens the customer Iceberg catalog and routes Graph requests through it; no
  query-node, write-node, or process-local graph registry remains.
- #137: Mount semantic routes only when a customer Iceberg catalog is present.
  They now reuse the configured cache credentials with the semantic SigV4
  verifier, so unsigned REST-JSON calls never reach the durable adapter.
- #135: Raised the embedded WAL/catalog router's explicit Axum request-body
  ceiling to 17 MiB. This accepts the benchmark's canonical 8 MiB WAL frames
  and one complete 16 MiB WAL segment with bounded wire headroom, while still
  rejecting larger requests before they enter consensus.
- #135: Removed deleted query and write roles from the root image build after
  their workspace consolidation into cache-node. The production cache image
  now builds only the unified binary instead of referencing absent packages.
- #135: Added a real four-process WAL regression that retains four 8 MiB
  frames, kills the exact first leader, and commits the same 8 MiB continuation
  through every survivor. It captures HTTP bodies and child diagnostics so a
  future failover rejection identifies the transport or consensus boundary.
- #135: Moved ring-local fragment filesystem work onto Tokio's blocking pool.
  Streaming uploads now relay a bounded number of body chunks to a worker that
  owns append, fsync, rename, and directory fsync, and HTTP success still waits
  until that worker reports the fragment durable.
- #135: Put each OpenRaft core and its replication timers on a cache-node-owned
  current-thread Tokio runtime. Group creation now dispatches to that runtime even
  when peer HTTP opens a retained group, and teardown stops every core before
  joining the runtime thread; a saturated-public-runtime real-vote regression
  protects that scheduling boundary.
- #135: Extended ranked election windows around one canonical 8 MiB durable
  append. The 250 ms heartbeat, 2.5–3.0 s first window, and 2.5 s rank spacing
  leave compact vote metadata time to fsync before a live follower is treated as
  failed; client submission remains bounded at 25 seconds and still fails closed.
- #137: Renamed the embedded erasure-coded durability dependency to verglas-writeback. The cache node retains its write-back and repair behavior without presenting a separate write product.
- #135: Composed checkpointed WAL reads from hash-verified archive objects and
  the retained coded tail under one consensus fence. Aborted peer uploads now
  fail without committing a partial fragment, preserving quorum availability.
- #135: Aligned the process-level catalog failover regression with the bounded
  30-second consensus recovery window. Retained-prefix verification still must
  complete before an immediate linearizable read can succeed.
- #135: Shortened the fixed, non-overlapping ranked election slots so the first
  survivor after any one-voter loss campaigns within 2.5 seconds. This leaves
  the frozen five-second WAL request deadline for the required current-term
  ReadIndex fence and exact committed-prefix reconstruction.
- #135: The cache-node image entrypoint now writes the required WAL archive configuration whenever the authoritative WAL ingress is enabled. Container deployments therefore start the four-voter durability engine with an explicit archive bucket and prefix instead of failing before health checks.
- #135: Gave checkpointed 16 MiB WAL archival its own 120-second hard submission ceiling instead of the 25-second interactive ingress ceiling. SF10 sustained writes had proven that a live four-voter ring could need longer to reconstruct, upload, verify, checkpoint, and release one segment under load; foreground requests retain their existing bound.
- #135: Split authoritative durability into typed WAL and catalog archive
  destinations. The cache node now requires both configured targets when the
  fragment ring is active, and catalog checkpoints cannot reuse the WAL store
  or prefix by accident.
- #135: Bound every Neon timeline to its database bucket through an authenticated,
  consensus-backed admin operation before accepting WAL. Archive reads, writes,
  and drains resolve that exact binding and never fall back to a global bucket.
- #135: Sharded immutable-object headers across four bounded Raft groups per
  storage binding and bucket instead of creating one Raft group per object.
  A failed voter no longer creates an unbounded heartbeat storm that starves a
  concurrently active WAL timeline after a large Iceberg ingest.

- Container-only daemon configuration: the compose stack no longer bundles
  MinIO — `[backend]` must point at a real S3-compatible origin, and
  the required VERGLAS_STORAGE_* variables fail fast when unset. The container
  entrypoint gained VERGLAS_CATALOG_BEARER_TOKEN so an authenticated Iceberg
  REST catalog (Cloudflare Data Catalog) works with env-only configuration; the daemon
  needs no host-side config file.
- #20 #96: Replaced the bundled local object-store Compose topology with one
  disposable cache process and three provider profiles: Verglas Cloud,
  Cloudflare Data Catalog, and Amazon S3 Tables. Provider catalog credentials
  are rendered into owner-only files; external catalog changes reconcile by poll
  while Cloud can additionally wake reconciliation through its event hint.
- #20 #96: Restored the local SDK Table read/append surface and `/admin/access`
  discovery in the cache process. Tables, Graphs, and Vectors open Iceberg only
  through the loopback catalog gateway, so provider bearer and SigV4 authority
  stays inside this process while all table FileIO re-enters its local S3 cache.
- #135: Invalidated a restored local committed-leader vote before reopening its
  Raft group. A restarted leader now runs a fresh election and reconstructs its
  replication workers instead of accepting commands that can never commit.
  Exact immutable WAL-binding replays also return from locally applied
  committed state without appending a redundant Raft command.
- #135: Canonicalized consensus voter and fragment-slot ordering by numeric
  voter identity. A restarted group now seals and reconstructs payloads from
  the same slots used before and after Raft restores its sorted voter set.
- Release packaging: the daemon no longer ships as raw prebuilt binaries
  (`dist = false`). Releases build the root Dockerfile into a multi-arch
  container image at ghcr.io/verglas-org/verglas-cache-node via the
  publish-docker job in the release pipeline.
- Replaced the 1 s disk poll with the event-driven space broker: fragment
  shortfall reclaims cold cache blocks (deficit plus the configured floor)
  and republishes the ceiling immediately; drain releases grow the cache
  back beyond live fragments plus the floor. No filesystem watching — the
  disk is dedicated and fixed-size, so the budget is the only bound.

- RIME ingest-perf-journal: `POST /v1/ingest/{name}?mode=append` now acks
  after durable local-disk journaling instead of waiting for the Iceberg
  commit, cutting warm NDJSON append latency (the synchronous CAS commit was
  ~20 sequential ops, 1.6-2.1s on the lite topology). `wait=true` or
  `commit=sync` keeps the old synchronous behavior and still returns the
  committed snapshot id; an `idempotency-key` header always forces the
  synchronous path (duplicate detection reads a committed snapshot's
  summary, which an uncommitted async ack does not have). `TableState` now
  opens `verglas_iceberg::AsyncIngestQueue` over `cache.dir/async-ingest`
  and replays it once, lazily, the first time an async ingest request needs
  the catalog. `TableState::new` gained the WAL directory parameter and is
  now fallible.
