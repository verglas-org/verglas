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

- #382: Wired the block-flush write-back ring (`ring.rs`). When VERGLAS_BLOCK_PEERS
  names a ring (id=host:port entries) and VERGLAS_NODE_ID is this box, the node
  builds the flush plane over the device registry's chunk store, serves the
  fragment RPC endpoints peers place shards through on VERGLAS_BLOCK_RING_ADDR
  (default :8336, no new authz — VXLAN isolation like the NBD plane), and runs the
  drain-takeover loop. With no ring configured the block tier stays single-node and
  FLUSH is the synchronous R2 barrier, unchanged. The object serve path is still a
  peerless cluster-of-one; only the block tier reaches verglas-cluster's fragment
  store and peer RPC. `DeviceRegistry` gained an optional ring plane it attaches
  once at startup; ensure/get route flushes through it when present.
