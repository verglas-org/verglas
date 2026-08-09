# verglas-core worklog

Append-only log of changes to this crate, by issue. Every PR touching this
crate adds an entry (see /AGENTS.md, "Worklog discipline").

- #1: Scaffolded as part of the initial cargo workspace: stub with module-level
  docs, placeholder types wiring real dependency edges, and an integration
  test directory. Toolchain pinned (1.96.1), workspace clippy lints applied.
- #5: Added shared admin API wire types (`VersionInfo`, `HealthzInfo`) and path
  constants so the CLI and daemon agree on the private control-plane JSON
  contract before individual commands land.
- #17: Added the cluster-of-one ring abstraction: `NodeId` newtype, `Ring`
  trait with a rendezvous-hash (XXH3-64) implementation, and an async
  `PeerFetch` trait (RPITIT, no async-trait dep) whose N=1 impl always
  returns `Ok(None)`. The hash + framing choice is documented in-code as a
  cross-node wire commitment (no version negotiation per prototype rules),
  with a marked extension point for #28's capacity weighting.
- #4: Added the minimal M1 `config` module: TOML schema with defaults (listen
  ports, cache dir/capacity/dram with human-size byte strings, backend.bucket
  as the only required field, optional auth keypair), `deny_unknown_fields`,
  and validation limited to bucket shape, cache-dir writability, and sane
  ports — errors name the exact field. Fields for unbuilt features are
  deliberately absent per the new AGENTS.md rule.
- #47: Added the optional `[catalog]` config section for the Iceberg REST
  catalog watcher (uri, poll_interval_secs, include/exclude filters, optional
  bearer_token and warehouse), with validation naming `catalog.uri` and
  `catalog.poll_interval_secs`, and the catalog URI in the startup summary.
  Lands with the feature it configures (verglas-tables watcher), per the
  config-fields-with-their-feature rule.
- #12: Added `BlockKey` — the cache-entry identity `(bucket, key, etag,
  block_index)` composing `CacheKey` — and rewrote `CacheKey`'s docs: it is
  the ring-ownership/invalidation granularity, deliberately ETag-free so
  ownership is stable across overwrites, while `BlockKey` carries the
  version (the foundation of #14's immutable-key scheme).
- #121 review follow-up: the data-plane traits moved here from verglas-s3 —
  `read` (ObjectRead and its range/meta/body types) and `write`
  (ObjectWrite, Invalidation, and the write wire types) — so producers
  (cache engine, passthrough) and the protocol layer share them without
  depending on each other. Pure move; verglas-s3 re-exports unchanged.
- #18: Relaxed `backend.bucket` validation to accept the three object_store
  URI schemes (`s3://`, `gs://`, `az://`) instead of hard-requiring `s3://`;
  the "not yet served" refusal for gs/az now lives at client construction in
  verglas-backend, not here. Added `backend.max_concurrent_requests` (default
  64, must be ≥1) — the per-bucket fill-concurrency ceiling that
  verglas-backend's semaphore enforces.
- #25/#26: Added the `medium` module: `CacheMedium::detect` resolves the
  filesystem type (statfs) and IO mode (O_DIRECT capability probe; buffered on
  macOS by construction) for a cache directory, and `measure_baseline` runs a
  quick sequential+random read-throughput probe. Shared by the `verglas dev`
  startup banner (#25) and `verglas bench` output (#26) so every published
  number carries its hardware/IO-mode context. Added the `libc` dependency.
- #132: Multi-bucket serving. Removed the single-bucket `backend.bucket` field
  (and its scheme validation / `BACKEND_SCHEMES`): the daemon now names no
  bucket and `[backend]` keeps only `max_concurrent_requests`, defaulting the
  whole table. `deny_unknown_fields` makes a stale `bucket` entry fail loudly.
  Added `ReadError`/`WriteError` `AccessDenied` and `WriteError::CrossBucketCopy`
  variants for the wildcard passthrough's origin-error and same-bucket-copy
  paths, and switched `summary()` to state `backend=wildcard`.
- #8: Added the `list` module: the `ObjectList` trait plus its `ListRequest`,
  `ListObject`, `ListPage`, and `ListError` types. LIST is a third data-plane
  trait (separate from `ObjectRead`) because listings must never be cached —
  they always pass through to the origin. It is an `#[async_trait]` trait so the
  front-end can hold it as `Arc<dyn ObjectList>`.
- #138: Added the `POST /cache/purge` admin wire contract to `admin`: the
  `PURGE_PATH` constant and the `PurgeReport` response type (mapping bytes
  freed, DRAM block bytes freed, disk-cleared flag) shared by the daemon
  handler and the CLI/bench callers.
- #20: Added `RetryPolicy` and `BreakerPolicy` under `[backend]` — the retry/
  backoff and per-bucket circuit-breaker tuning verglas-backend's resilience
  layer reads. Both take documented defaults (3 retries, 100 ms→3 s jittered
  backoff, 10 s budget; trip at 50% over ≥20 samples, 5 s cooldown, 3 probes)
  and validate their invariants by field name (non-zero backoff, `max ≥
  initial`, failure_rate in (0,1], non-zero cooldown/probe count).
- #141: added the `/admin/stats` wire contract (`STATS_PATH`, `StatsInfo`,
  `CacheConfigInfo`, `CountersInfo`). It carries the daemon's configured DRAM/disk
  ceilings plus the live read-path counters and DRAM-tier usage, so bench reports
  can stamp tier context on every number and the nvme/constrained profiles can
  prove disk-serving and eviction from real counters.
- #27/#28: evolved the `Ring` trait so a live gossip-backed ring can implement
  it — `members()` now returns an owned `Vec<NodeId>` snapshot (a ring whose
  membership swaps atomically has no stable slice to borrow) and a new
  `epoch()` (default 0) exposes membership-change generations for staleness
  detection. Extracted the pod-wide hash-framing wire commitment into a public
  `rendezvous_hash(key, node)` so both the unweighted `RendezvousRing` and the
  weighted live ring score identically.
- #27: added the optional `[cluster]` config section (pod_id, node_id,
  gossip_addr, advertise_addr, seeds, weight, suspicion_secs) with strict
  validation and an annotated example. Absent means single-node (cluster=off in
  the summary) — the turn-off path is unchanged; the fields land here with the
  gossip feature that reads them.
- #27: added the `/admin/members` wire contract (`MEMBERS_PATH`, `MembersInfo`,
  `MemberInfo`) — this node's identity, pod, ring epoch, and its live view of
  every pod member (ids, addresses, capacity/weight, state) for `verglas status`
  and debugging. Plain serde types with string addresses; no gossip dependency.
- #15: added the `[cache.admission]` config struct (scan-resistant admission,
  #15): `enabled` (default true, a debugging off-switch) and `frequency_threshold`
  (default 2, "admit on the second sighting"), with validation that rejects a
  zero threshold by field name. Landed with the verglas-cache feature it
  configures.
- #29: reshaped the `PeerFetch` trait so `fetch` takes a whole `BlockKey`
  (object + ETag + block index) instead of `(CacheKey, byte range)`. Carrying
  the ETag is what lets a peer serve *exactly* the version the caller resolved
  and miss on any other — wrong bytes are unrepresentable rather than guarded
  against. `NoopPeerFetch` and the core peer-fetch test moved to the new shape.
- #29: added `peer_misses`, `peer_errors`, `peer_served_blocks`, and
  `peer_served_bytes` to the admin `CountersInfo` wire type so `/admin/stats`
  exposes both sides of a peer fetch (requester and donor) and the intra-pod
  byte amplification (#46).
- #29: added the peer-fetch knobs to `[cluster]` config: `secret` (shared
  peer-auth secret, rotatable), `peer_connect_timeout_ms` (default 5), and
  `peer_request_timeout_ms` (default 50), with validation rejecting zero
  timeouts by field name. Landed with the peer RPC feature they configure.

- #50: added `cache.meta_fraction` (default 0.05), the share of the cache
  carved out for the dedicated metadata store that pins Iceberg/Parquet
  planning metadata hard-isolated from data eviction. Validated to lie in
  (0.0, 1.0) exclusive by field name. Landed with the verglas-cache meta store.

- #50: extended the admin `/admin/stats` counters wire form (`CountersInfo`)
  with the metadata-store gauges (`meta_hits`, `meta_misses`,
  `meta_bytes_served`) so operators and the bench harness can see the pinned
  store's ~100% hit rate and metadata bytes served.
- #16: extended `HealthzInfo` with a `starting` readiness state (plus
  `HEALTH_STATUS_OK`/`HEALTH_STATUS_STARTING` constants and a
  `HealthzInfo::starting()` constructor) so `/admin/healthz` can serve-gate on
  cache recovery — `starting`/503 until the disk index is rebuilt, `ok`/200
  after.
 #164: added `cache.admission.churn_admit_probability` (default `1.0`) with
  range validation `(0, 1]`. It configures the resident-biased thinning of a
  cyclic scan under cache pressure — the fraction of frequency-qualifying
  candidates admitted once the cache is full — so a disk-constrained profile
  can converge on the cyclic-scan hit ceiling instead of churning to ~0% hits.
 #14: added `ObjectRead::revalidate` — a conditional `If-None-Match` mapping
  revalidation primitive returning `Revalidation` (Unchanged / Changed / Vanished)
  — with a HEAD-based default so existing implementors need no change. Added
  `cache.mutable_mapping_ttl_secs` (default 5s), the TTL after which a
  mutable-classified key→ETag mapping is revalidated.
 #168: Added a blanket `ObjectRead for Arc<T>` impl so background producers
  (warming #168, prefetch #51) can hold and cheaply clone a shared `Arc<Engine>`
  reader without the concrete engine being `Clone`.
- #168: Added `[cache.warming]` config (enabled, concurrency, footer_read_bytes,
  byte_budget_bytes_per_sec) with validation, and a `WarmingInfo` block on the
  admin `/admin/stats` `StatsInfo` (optional, serde-default-tolerant) so warming
  progress is observable.
- #143: added `write::WriteMetadata` (system headers + user `x-amz-meta-*`) and
  threaded it through the `ObjectWrite` trait — `put`/`create_multipart` take it
  and `copy` takes `Option<WriteMetadata>` (None = MetadataDirective COPY, Some =
  REPLACE). Extended `read::ObjectMeta` with the same header set + user-metadata
  map so a GET/HEAD reports back what a PUT stored. #146: added
  `WriteError::EntityTooSmall` and `WriteError::InvalidRequest` so origin 4xx on
  the multipart-complete/copy paths keep their S3 code instead of flattening to
  500.
 #178: Reshaped the `PurgeReport` and `StatsInfo` admin wire types for the
  generation-epoch purge. `PurgeReport` now carries `generation`,
  `mapping_bytes_freed` (freed synchronously), and `reclaimable_bytes` (live an
  instant ago, resident until LRU reclaim) — dropping `dram_block_bytes_freed`
  and `disk_cleared`. `StatsInfo` gained `dram_live_bytes` and
  `dram_reclaimable_bytes` splitting the existing `dram_usage_bytes` total, so a
  repeat-cold benchmark reads the live working set as ~0 at T=0.
 #189: added `expires: Option<String>` to `WriteMetadata` and `ObjectMeta` —
  the `Expires` object header, carried as the raw HTTP-date string. `object_store`
  0.14 models no `Expires` attribute in either direction, so a PUT that sets it
  (and the GET/HEAD read-back) route through the passthrough's raw-request path;
  the field is the channel that carries it end to end.
- #30/#31: added `Ring::warm_donor` (returning the new `WarmDonor { node,
  draining }`) and `Ring::should_serve_peer`, both with fixed-ring defaults
  (`None` / plain ownership) so single-node and mock rings are unchanged. These
  are the ring-level hooks for warm-from-peers: `warm_donor` names a missed
  owned block's previous holder during a join/drain transition, and
  `should_serve_peer` widens a key's peer-serve set to that one donor so a
  transition warms from a single peer without replica fan-out.
- #31: added the `POST /admin/drain` wire contract (`DRAIN_PATH`,
  `DrainRequest { timeout_secs }`, `DrainAck { node_id, state, timeout_secs }`)
  so the CLI and daemon agree on the graceful-drain control.
- #30/#31: added `[cluster].warm_from_peers_secs` (default 300) and
  `drain_timeout_secs` (default 600) — the join warm-from-peers window and the
  drain donor window before a drained node exits. Landed with the daemon wiring
  that reads them.
- #51: Added the `[cache.prefetch]` config section (`Prefetch`): enable flag,
  fill concurrency, footer window, organic-yield `K`, queue bound, and heat-ledger
  epoch/cap/channel sizing — with validation and example.toml docs. Configures
  the snapshot-driven prefetcher and its request-path heat ledger.
- #180: Added the `[cache.writeback]` config for the erasure-coded write-back
  tier: `enabled` (off by default), the `k`/`m`/`w` geometry, `ack_deadline_ms`,
  `max_object_bytes`, and per-prefix opt-in overrides. Config validation rejects
  geometries that can never reach quorum on any pod — `w > k+m` (more fragments
  than exist) or `w < k` (the acked set cannot reconstruct) — for both the
  default and each prefix override, naming the field. Documented the section and
  its durability-contract caveats in verglas.example.toml.
- #180: Added WritebackStatsInfo to the /admin/stats wire type (quorum vs
  write-through ack counts, propagations, failures, repairs), as an optional
  field so older clients still decode.
- #180: Added `cache.writeback.fragment_fraction` (default 0.10, validated in
  (0,1)) — the share of `capacity_bytes` reserved for the fragment store, carved
  from inside the NVMe budget so total on-disk usage stays under the ceiling.
  Added `Cache::fragment_budget_bytes()` as the single source the engine and the
  fragment store both read. Removed `max_object_bytes`: the streaming encoder
  keeps one stripe resident, so the write-back size limit is NVMe headroom, not a
  DRAM cap.
 #11: Added the S3 listener's TLS and virtual-hosted addressing config to
  `[listen]`: an optional `tls` sub-table (`cert_path`/`key_path`, validated to
  exist) that turns the endpoint from plain HTTP to HTTPS, and an optional
  `domain` base for virtual-hosted-style addressing (validated as a bare host
  name). Both are absent by default — plain HTTP, path-style — which is the
  `verglas dev` local default. `summary()` now reports tls on/off and the
  addressing mode.
- #153/#154/#156: added the escape-hatch read/write surface. `ObjectRead` gained
  `get_direct`/`head_direct` (version/part/checksum reads, forwarded uncached)
  and `object_attributes`, with default impls so test readers need not
  implement them. `ObjectWrite` gained `upload_part_copy` and
  `list_multipart_uploads`. New types (`DirectReadOptions`, `DirectGet`,
  `ObjectAttributes`, `Checksums`, `WriteChecksum`, `MultipartUploadsPage`, ...)
  and the `ReadError::InvalidPart` / `WriteError::InvalidRange` codes. Checksums
  are always forwarded, never computed.
- #208: Wired checksums through the multipart lifecycle. `WriteChecksum` gained
  `checksum_type`; `CompletedPartRef` now carries per-part `Checksums`;
  `create_multipart` returns `MultipartCreation` (upload ID + echoed
  algorithm/type), `upload_part` takes a `WriteChecksum` and returns
  `PartUpload` (ETag + echoed checksums), and `complete_multipart` takes the
  object-level `WriteChecksum`. All forwarded, never computed.
- #221: added backend origin settings to `[backend]` config — `endpoint`,
  `region`, `allow_http`, `virtual_hosted_style`, `credentials_file`, and
  `credentials_profile`. All optional; absent means the AWS env chain supplies
  them, so production IAM/instance-role is unchanged. `Backend::validate` rejects
  an http endpoint without `allow_http` and a non-http(s) endpoint scheme.
  `Backend` now derives `Clone` so the backend registry can hold an owned copy.

- #216: The config.toml scaffold is now generated from the Config struct
  instead of a hand-written template. Config structs derive schemars::JsonSchema,
  and the new `config_template` module walks that schema to emit annotated TOML:
  each field's doc comment becomes the setting's help text, defaults come from the
  struct. The struct is the single source of truth, shared by rustdoc, the schema,
  and the config file, so they cannot drift. Rewrote the field doc comments as
  plain operator documentation. Changed `[auth]` to name an AWS-format credentials
  file (credentials_file/credentials_profile) rather than carry the endpoint
  keypair inline, so no secret lives in config.toml.
- #226: reverted to single-bucket serving; deleted the #132 per-bucket registry; backend.bucket is now required and gates serving. Multi-bucket is deferred to #226.
- #216: added a provider dimension to `[backend]`: a `BackendProvider` enum
  (s3/azure/gcp, serde-lowercase, default s3) and a `provider` field. s3 covers
  AWS/OCI/MinIO; azure and gcp select their own object-store clients. Surfaced
  `backend.provider` in the generated config template (defaults to s3) and made
  the endpoint validation provider-agnostic. Credentials stay out of config.toml.
- #46: added the `metrics` module — a `NodeMetrics` registry with the
  `verglas_request_duration_seconds` histogram and `verglas_requests_total`
  counter, plus a `render` that appends the snapshot-derived node families
  (bytes-by-tier, cache hit/miss/admission, cache size/capacity, fill inflight,
  backend health). Added `ServedTier`/`TierCell` on `ObjectGet` so the front-end
  learns the tier that served a read, and a `METRICS_PATH` admin constant.
- #231: exposed the `[catalog]` section in the generated `config.toml` (uri,
  include, exclude, warehouse, and a new file-based `credentials_file`), all
  commented since the section is optional. `poll_interval_secs` and the inline
  `bearer_token` stay hidden. Added `Catalog::resolve_bearer_token` (reads the
  0600 token file, trims it) and `Catalog::validate` (http/https URI, positive
  poll interval, at most one token source, credentials file must exist).
- #220: added `cache.writeback.scrub_interval_secs` (default 6 h, rejected when
  zero and the tier is enabled) — the knob for the background fragment scrubber.
  It stays an internal tuning knob: not in the config template's EXPOSED list, so
  the generated config keeps the default and never shows it. Surfaced
  `fragments_scrubbed` and `corrupt_fragments_found` on the write-back admin stats
  DTO. Rebased onto #46's metrics module: `MetricsSnapshot` gained an optional
  `writeback` field and `render` emits `verglas_writeback_fragments_scrubbed_total`,
  `_fragments_repaired_total`, and `_corrupt_fragments_found_total` when the tier
  is enabled (omitted otherwise).
- #234: Aligned the compiled-in admin defaults (`DEFAULT_ADMIN_ADDR`,
  `DEFAULT_ENDPOINT`) with the documented `[listen]` config default: 8334, not
  9090. A default CLI now reaches a default-config daemon. Added a consistency
  test tying the constants to `Listen::default().admin_port`.
- #235: Added `backend.bucket_globs` alongside the single `backend.bucket`: the
  daemon serves the union set, and at least one must be set (validation names the
  field). Added `Backend::serves_bucket` / `describe_bucket_set` and a shared
  `glob` module (moved the wildcard matcher here so bucket and table globs agree
  on `*`). Surfaced `bucket_globs` in the generated config scaffold.
- #236: Added SigV4 catalog settings to `[catalog]`: `sigv4_region` and
  `sigv4_signing_name` (both-or-neither, mutually exclusive with a bearer token),
  plus `credentials_profile` for the AWS-INI file used in SigV4 mode. Added
  `Catalog::sigv4_enabled`; `resolve_bearer_token` returns None in SigV4 mode.
  Surfaced the new fields in the generated config scaffold.
- #252: Added `cache.data_block_bytes`, a validated power-of-two geometry from
  1 MiB through 8 MiB with a measured 2 MiB default. The geometry is now part
  of `BlockKey`, so cache and peer identities cannot confuse different offsets.
- #96: Added `disk::free_bytes` (statfs available blocks) and `Config::validate`
  now refuses `cache.capacity_bytes` larger than the free space on the cache
  filesystem, with a message naming the field, the configured size, and the
  available space. An unprobeable filesystem does not gate.
- #96: Added `disk::disk_decision`, the pure runtime disk-full decision the
  daemon's background poll runs: hysteresis over free space to pause block
  admission, and a dynamic fragment ceiling from the free NVMe. Off every hot
  path — the serve/fill paths only read the atomics it publishes.
- #223: Removed `cache.writeback.fragment_fraction` and `default_fragment_fraction`;
  the fixed 10% carve is gone. Replaced `Cache::fragment_budget_bytes()` with
  `fragment_ceiling_bytes()` = `FRAGMENT_SAFETY_FRACTION` (half) of the NVMe
  budget: the dynamic fragment store's safety ceiling, still carved from inside
  `capacity_bytes` so the read cache keeps a floor and the total stays a hard
  ceiling.
- #223: Reworked to one shared NVMe budget after review rejected the fixed
  half-budget safety fraction (it permanently capped the read cache at 50%).
  Removed `FRAGMENT_SAFETY_FRACTION` and `Cache::fragment_ceiling_bytes()`; no
  sizing knob exists. `disk::disk_decision` now takes the foyer stores' physical
  growth room and grants fragments exactly the budget the block cache is not
  using, pausing block admission (with hysteresis) before it would grow into
  fragment-held bytes. Added `disk::file_growth_room` (logical minus allocated
  bytes of a sparse device file) as the accounting unit.
- #61: Added the `trace` module — a task-local per-request `RequestId`, a
  `scope`/`current` pair to set and read it without threading it through every
  signature, the `x-verglas-request-id` peer header name, and a redacted
  `key_hash` for logs (raw keys are never logged). Added the `[log]` config
  section (format json/pretty, level) and the `POST /admin/log` admin DTOs.
- #267: Updated the example config's catalog block to match the generated
  template: removed the inline bearer_token example and documented
  credentials_profile, sigv4_region, and sigv4_signing_name for AWS-hosted
  catalogs. Added a test asserting the example documents this auth surface.
- #288: Doc-comment only — `DrainRequest` references the CLI verb by its new
  name (`verglas drain`, local-only) after the `node drain` fleet verb was
  removed from the CLI. No wire-contract change.
- #287: Added the `LocalAccess` admin wire type and `ACCESS_PATH` (`/admin/access`)
  to the shared admin API. It reports the daemon's S3 endpoint, loopback catalog
  mount, region, bucket, and endpoint keypair so the agent-facing CLI verbs
  resolve a connection with zero configuration. Round-trip tests cover the shape.

- #298: The startup capacity gate now credits bytes the cache already holds:
  it refuses `cache.capacity_bytes` only when the budget exceeds free space
  plus the physical allocation under `cache.dir`. Before, a warm daemon whose
  own files had consumed the disk could never restart at the budget it booted
  with cold. Added `disk::allocated_bytes` — a best-effort recursive walk
  summing `st_blocks * 512` so sparse foyer device files count at physical
  size.

- #300: The #96 filesystem gate in `disk_decision` now applies only while the
  foyer device files can still grow the filesystem footprint. On APFS the
  files are physically preallocated to the full budget at boot, so a
  near-disk-size budget left almost no free space and the gate paused
  admission forever — the cache served but admitted nothing, refetching every
  read from S3. With zero growth room, admission recycles bytes the files
  already own and cannot fill the disk. New fragments still consume real disk
  and stay gated through the fs headroom.

- #305: CountersInfo carries the retirement figures (pending gauge,
  reclaimed bytes/files) so /admin/stats can show how much of the resident
  cache is dead and how much the sweep has physically reclaimed.

- #295: `[agent]` config section. The daemon's one config file gains
  agent-side identity and local state (agent_id, memory_namespace, spool_dir,
  model_cache, aws_profile; deny_unknown_fields, full defaults so absence
  changes nothing for the daemon). `agent_env_pairs` derives everything else
  the agent binaries need from the same file — daemon endpoint from [listen],
  warehouse from [catalog], region from [backend], dirs defaulting under the
  config directory — and `apply_agent_env` applies the pairs at process start
  without overriding existing env vars (dev overrides keep working). The
  example config documents the section.

- dashboard sink (Rill): added the shared admin wire types for the
  `GET /dashboard` probe — the `DASHBOARD_PATH` constant, `DashboardSinkInfo`
  (name, target table, project path, scaffolded flag, last-refresh snapshot,
  state), and `DashboardInfo` wrapping the list. Both the daemon and the CLI
  agree on these types.
- Security fix (/admin/access credential exposure): Dropped the
  `secret_access_key` field from `LocalAccess` entirely, so the wire shape the
  daemon serves on the unauthenticated, host-scoped loopback admin socket can
  never carry the endpoint secret. The probe now reports only the non-secret
  discovery fields (s3_endpoint, catalog_path, region, bucket, access_key_id).
  Updated the admin wire tests to assert the served JSON omits the secret key.
- Control-plane config section. Added an optional `[control_plane]` table to
  `Config` (`ControlPlane { url }`), written by `verglas login`. It has to be a
  first-class field because `Config` denies unknown fields and the daemon loads
  the same `~/.verglas/config.toml`; the daemon ignores it. Like the `agent`
  section it is not surfaced in the generated `config.toml` scaffold, so the
  template and its coverage tests are unchanged. The matching API key lives in a
  mode-0600 credentials file, never in this config.
- index registry: Added the optional `[cluster].id` field (the vector-index
  registry cluster id) plus `Config::resolve_cluster_id`: `VERGLAS_CLUSTER_ID`
  env override, else `[cluster].id`, else the machine hostname (POSIX
  `gethostname` via `machine_hostname`), else `"localhost"`. Resolved once at
  daemon start; a daemon stamps this id on the `verglas_sys.indexes` rows it
  writes and filters by on reboot. Blank values are treated as unset. Works
  without a `[cluster]` table (single node): the hostname default covers it.

- #95: Added `cache.shadow_capacity_bytes` (ByteSize, default 1GB): the hard byte
  ceiling for the cache-managed shadow store of Verglas-derived Puffin artifacts
  (vector indexes today), kept separate from the block cache's `capacity_bytes`.
  The one shadow-store budget knob; serde-defaulted so existing configs parse
  unchanged.

- windows release build (cfg audit): `machine_hostname()` used `libc::gethostname`
  unconditionally, which does not exist in `libc` on Windows and blocked every
  binary (all depend on verglas-core) from compiling for the release target. It
  is now `#[cfg(unix)]`; the `#[cfg(not(unix))]` version reads `COMPUTERNAME`
  (Windows) and otherwise returns `None`, so cluster-id resolution still falls
  back cleanly to `"localhost"`. Part of the cargo-dist release cfg audit.
- #60: Added the `telemetry` module (per-table access metrics). `tape.rs`: a
  16-byte `#[repr(C)]` `AccessEvent` and per-core double-buffered `ShardTape`s;
  the hot-path `record` is one relaxed atomic load + one `fetch_add` + a 16-byte
  store (~1.8ns, measured in `benches/telemetry_tape.rs`) — never a lock, a
  per-table atomic, or a label format; a full buffer drops and counts the event
  (sample under load). `rollup.rs`: a background fold into
  `HashMap<(table,tier,op), CounterSet>` with per-table latency EWMAs, a hard
  cardinality cap (excess tables fold into `_overflow`, unmapped into
  `_unmapped`), and derived per-table requests-avoided / latency-saved. `mod.rs`:
  the `Telemetry` hub (record + roll_up + report/metering/render_families) plus
  the `encode_table_id`/`decode_table_id` boundary that reserves id 0 for
  `_unmapped` without renumbering the mapper. Dollars are never stored — raw
  counts only. Added `TABLE_METRICS_PATH` to `admin`.
- #379: Make the validated daemon configuration cloneable so the S3 serving task can own an immutable startup snapshot. This lets loopback S3 begin serving before catalog-backed bootstrap without adding shared mutable configuration.
- chore: Remove docs/ cross-references after deleting the docs directory. Crate module docs are the reference now.
- chore: Remove the unused DashboardInfo / DASHBOARD_PATH admin surface after the daemon probe was deleted.
- #3: Added explicit query/write role configuration and validation; neither role has an embedded daemon fallback.
- chore: Drop the `[agent]` config section and `apply_agent_env` — durable agent memory is not part of this product. Keep `user_config_path()` for locating `~/.verglas/config.toml`.
- chore: Replace stale `verglas dev` references in configuration and cache-medium documentation after removing the CLI launcher. Local human-readable logging remains available as a daemon configuration choice.
- #263: Removed `catalog_path` from the local-access wire type. Catalog configuration is private daemon state and is no longer presented as a service endpoint.
- #91: Removed the obsolete vector shadow-store capacity setting. Vamana files
  are customer-invoked Iceberg statistics attachments and flow through the
  ordinary object cache.
- #91: Renamed shared admin metadata and configuration documentation from the
  daemon identity to `verglas-server`. The version response now reports the new
  executable name only.
- #3: Added the configured upstream catalog URI and warehouse to local access
  discovery. These are non-secret client coordinates and do not create a
  Verglas-hosted catalog service.
- #8: Extended local access discovery with an explicit query API URI. On-prem clients now receive separate S3, query/write, and catalog proxy coordinates.
- #16: Replaced the obsolete worklog-only dashboard sink concept with validated `[analytics.rill]` network configuration. The configuration names Rill's private runtime, browser address, and return S3 path, and requires an Iceberg catalog for table resolution.
- #29: Extended the shared NVMe accounting contract to include the durable KV log alongside acknowledged write-back fragments. Both are protected from eviction while the remaining object cache stays heat-managed with no fixed partition.

- #66: Replaced control-plane config examples and comments that referenced api.verglas.dev / verglas login with self-host-neutral wording, and switched admin test fixtures to example.test hostnames.
- #66: Documented optional analytics config as self-hosted only (removed cloud-composes-independently contrast).
- #66: Removed optional external control-plane node reporting and the [control_plane] config field from the OSS server.
- #84: Added immutable storage-binding identity to object, list, multipart, and rendezvous-placement keys. Equal bucket and object names at different origins now remain distinct throughout the data plane.
- #84: Added dynamic-runtime validation for cache processes whose backend and catalog bindings arrive after startup. Static file configuration still requires an explicit backend and keeps its existing validation contract.
- #82: Added the explicit `eventual` and `strong` catalog consistency contract. Eventual catalogs poll; strong catalogs require quorum-backed direct delivery and cannot select a third mode.
