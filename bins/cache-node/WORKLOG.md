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
  FLUSH is the synchronous R2 barrier, unchanged. The object serve path is still a
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
