# verglasd worklog

- #385: Added content-negotiated Arrow IPC query streaming, Arrow IPC table
  commits, and exact table-definition inspection to the daemon data plane. A
  real in-memory Iceberg route test covers definition, append, and query end to
  end without external services or result-wide query collection.

Append-only log of changes to this crate, by issue. Every PR touching this
crate adds an entry (see /AGENTS.md, "Worklog discipline").

- #1: Scaffolded as part of the initial cargo workspace: stub with module-level
  docs, placeholder types wiring real dependency edges, and an integration
  test directory. Toolchain pinned (1.96.1), workspace clippy lints applied.
- #5: Stood up the private admin HTTP listener on axum with `/admin/version` and
  `/admin/healthz`, bound by default to `127.0.0.1:9090` (override via
  `VERGLAS_ADMIN_ADDR`). The daemon now stays up to serve the CLI control API.
- #4: Wired `--config <path>`: parse and validate before binding, exit 1 with
  the field-named error, print a one-line summary on success. The admin
  listener now reads `listen.admin_port` from the config (VERGLAS_ADMIN_ADDR
  still overrides for tests), and a dev access keypair is generated and
  printed when `[auth]` is absent.
- #6: Started the S3 listener on `listen.s3_port` (all interfaces; override via
  `VERGLAS_S3_ADDR` for tests) alongside the admin listener, serving
  verglas-s3's front-end over the passthrough reader built from
  `backend.bucket`. The generated-or-configured access keypair is now wired
  into the endpoint's signature check, and boot logs one "serving S3 on ..."
  line. E2E smoke test spawns the real binary against an in-process mock
  origin and asserts bytes, range headers, and error XML over raw HTTP.
- #9: The S3 listener now serves writes too: one shared backend S3 client
  feeds both the read and the new write passthrough, wired into the router
  with the no-op invalidation hook (nothing cached yet). E2E smoke test
  drives put/copy/delete and the multipart lifecycle through the spawned
  daemon and verifies every byte is durable at the mock origin — Verglas is
  never the only copy.
- #7: E2E smoke test now SigV4-signs every client request with the configured
  engine keypair so it exercises the enforced auth path end to end, including
  put/copy/delete/multipart writes and direct origin verification with the
  origin keypair.
- #12: The S3 listener now serves reads through the hybrid cache engine
  (`HybridCacheEngine::single_node` over the read passthrough built on the
  shared backend client, budgets and directory from `[cache]`) instead of
  the bare passthrough. The smoke test now exercises cached reads end to
  end.
- #9/#120 follow-up: the cache engine is now the router's invalidator (one
  engine handle serves reads, a clone serves invalidation), replacing
  NoopInvalidation — write-through PUT/DELETE/Complete/Copy invalidate the
  cached mapping before the ack. Smoke tests get per-test scratch dirs
  because the engine owns cache.dir exclusively.
- #18: `serve_s3` now builds the backend store via
  `verglas_backend::BackendClient::connect` instead of the removed
  `s3_store_from_backend`, and logs the resolved provider/credential mode and
  bucket at startup so operators can confirm which credential path (static
  env, web identity, or IMDS instance role) came up.
- #132: Wired the daemon to the wildcard `BackendRegistry` shared by the read
  and write passthroughs (lazy per-bucket clients), replacing the single
  backend client. Startup log now states the wildcard mode and credential
  source rather than a bucket name. Updated the config-flag and s3-smoke tests:
  configs no longer carry `backend.bucket`, and the smoke test asserts a second
  bucket is served (wildcard) rather than rejected.
- #8: The daemon now builds a `PassthroughList` over the backend registry and
  passes it to `verglas_s3::router`, so LIST forwards to the request's bucket
  (never cached), same routing as GET/PUT.
- #8: Added an s3_smoke test that drives ListObjectsV2 end-to-end through the
  spawned daemon (SigV4-signed) against the mock origin: a paginated
  continuation-token walk at max-keys=2 returns every key exactly once, and a
  delimited listing rolls up the directory prefixes.
- #138: Added the `POST /cache/purge` admin endpoint. The engine is now built
  once in a shared `serve` and handed to both listeners — as an
  `Arc<dyn CachePurger>` to the loopback admin router and as the reader/
  invalidator to the S3 surface — so purge is reachable only on the admin
  port. The handler logs the freed byte counts and returns a `PurgeReport`;
  an integration test drives a configured daemon and asserts the loopback
  contract.
- #141: the engine and backend registry are now built once in `main` (via
  `build_engine`) and shared by both the S3 and admin surfaces; a below-floor
  DRAM budget fails fast there, before any listener binds. Added `GET
  /admin/stats`, which reports the configured cache budgets alongside the live
  read-path counters and DRAM usage (the source of truth for bench tier context).
- #27/#28: wired gossip membership and the live ring into the daemon. With a
  `[cluster]` config the daemon starts the chitchat cluster agent, hands the
  engine the agent's live `LiveRing` (ownership reroutes on membership change
  with no engine rebuild), logs the pod join, and serves `GET /admin/members`.
  Without `[cluster]` it runs a single-member `LiveRing` under the fixed node id
  `single` — ownership byte-identical to a pre-cluster daemon and no members
  route (the turn-off path). Integration tests cover 3-daemon convergence over
  real UDP gossip, the single-node turn-off, and a remote-owned key still
  backend-filling locally (peer FETCH is #29) with correct bytes.
- #29: wired the peer-fetch client and server into the daemon. When `[cluster]`
  is configured, the engine's peer rung is a real `PeerClient` resolving owners
  through live gossip membership (secret + tight timeout budget from config),
  and — when a peer address is advertised — a `PeerServer` binds it and serves
  this node's owned, cached blocks from the engine's cache-only `local_block`.
  Single-node daemons use a disabled peer client and start no server (the
  one-member ring owns every key), so the turn-off path is unchanged. Added the
  four new peer counters to `/admin/stats`, and an in-process two-engine + RPC
  integration test (`tests/peer_fetch.rs`) covering owner-serve/zero-backend-
  GET, pod-wide miss, dead-owner fast fallback, and stale-ETag safety.

- #50: surfaced the metadata-store counters (meta hits/misses/bytes) through the
  daemon's `/admin/stats` source. The daemon's engine uses the heuristics-only
  metadata router by default, so Iceberg/Parquet planning objects
  (metadata.json, manifest lists/manifests, Parquet footers) are pinned in the
  dedicated store with no catalog wiring required.

- #16: serve-gating on cache recovery. The admin listener now comes up before
  the cache engine finishes building, so `/admin/healthz` answers `starting`
  (503) — not connection-refused — while disk recovery runs, then `ok` (200)
  once it completes; a `Health` gate drives it and the engine-dependent routes
  (purge/stats/members) are wired through deferred `OnceLock` slots filled at
  the same moment (answering 503 until then). `serve()` builds the engine and
  admin listener concurrently, times recovery for the operator log, flips the
  gate, then serves S3. New `tests/restart.rs` drives the real binary through a
  `kill -9` and restart over the same `cache.dir`, proving the warm object
  serves from the recovered NVMe tier with zero backend block re-fills after one
  metadata round trip, and that the restarted daemon reports ready only once
  recovery is done.
- #170: added a parent-death watch so a dev daemon never outlives its
  `verglas dev` parent. When the CLI sets `VERGLAS_PARENT_PID` at spawn, a small
  background thread polls `getppid()` and exits the process the moment it
  diverges from the recorded parent pid (the kernel reparents an orphan to pid 1
  on macOS and Linux) — freeing the port instead of squatting it with stale
  keys after the parent is SIGKILLed. Chosen over an inherited-pipe watch
  because it needs no fd plumbing and is portable across both platforms; it arms
  only when the env var is present, so a production daemon under systemd/launchd
  is never killed spuriously.
#168: Wired the eager warming pipeline into the daemon. When a catalog is
  watched and warming is enabled, `spawn_warming` builds a REST catalog watcher,
  a warmer over the serving engine, and a coordinator that warms every watched
  table on startup and re-warms on each commit; the byte budget comes from
  config and the footer-footprint alert ceiling from the metadata store's DRAM
  carve. Warming progress is surfaced on `/admin/stats`.
 #178: `POST /cache/purge` now reports the generation-epoch purge (generation +
  reclaimable bytes) and `/admin/stats` exposes the new `dram_live_bytes` /
  `dram_reclaimable_bytes` split; the purge log line was reworded accordingly.
- #30/#31: added `tests/warm_from_peers.rs`, an in-process live proof wiring
  real engines + the peer RPC + one shared `LiveRing`: a joiner warms its
  newly-owned key from the incumbent (peer_hit up, backend_fills/GETs zero) and
  skips the hop once its warm window closes; a drain successor warms a shed key
  from the draining donor with zero backend fills and no client error.
- #30: at startup a clustered daemon opens the engine's warm-from-peers window
  (`begin_warming`) for `cluster.warm_from_peers_secs`, so a freshly-joined node
  pulls its newly-owned cold-miss blocks from their previous holders before the
  backend. A single-node daemon never opens it.
- #31: added the `POST /admin/drain` route + handler (present only when
  clustered): it gossips this node `draining` via the agent, schedules a clean
  `process::exit` after the request/configured timeout so the ring rebalances,
  and acks. Router/`serve_admin` gained the drain slot; unit tests cover the
  route wiring, empty-body default, absence without a cluster, and the 503
  serve-gate.
- #31: added `tests/drain.rs`, an end-to-end subprocess proof — a real 3-node
  gossip pod is drained through the resolve-then-`POST /admin/drain` flow the CLI
  uses; the node acks `draining`, exits after its window, and the survivors' ring
  rebalances to two members (the daemon drain glue that ends in `process::exit`
  is only reachable this way).
- #51: Wired the table-lifecycle pipeline. `spawn_lifecycle` now builds one
  shared REST watcher and one shared byte budget for both eager warming (#168)
  and snapshot-driven prefetch (#51). Prefetch spins up the logical-key map + its
  updater, the heat ledger + a request-path aggregator, one budgeted
  organic-yielding prefetch executor, and the prefetch coordinator that repairs
  the cache and retires orphans on each compaction commit. The serving engine is
  wrapped in the `HeatFeed` decorator (feeds the ledger, occupies an organic-yield
  slot per read); it is a no-op passthrough when prefetch is off.
- #180: Wired the erasure-coded write-back tier into the daemon. When
  [cache.writeback] is enabled the S3 write/read paths are wrapped in the tier's
  writer/reader at construction (feature-gated, so a disabled tier costs the hot
  path nothing); the peer server also serves the fragment put/get/delete
  endpoints backed by a local fragment store; the node-loss repair loop runs when
  clustered; and /admin/stats gains a writeback block (quorum vs write-through
  acks, propagation, repairs). A single-node daemon builds the tier too but every
  write degrades to write-through (the fallback proof).
- #180: The fragment store is built with a byte budget carved from
  `cache.capacity_bytes` (`Cache::fragment_budget_bytes()`), the same figure the
  engine subtracts from its block-cache disk capacity, so total NVMe usage holds
  under the ceiling. The peer fragment handlers gained a streaming store and a
  headroom callback. The write-back tier no longer takes a buffer cap — the
  coordinator streams every eligible write.
 #152: the S3 listener is now built with `router_with_passthrough`, passing the
  shared backend registry so unmodeled bucket-config operations (HeadBucket,
  GetBucketLocation) are forwarded to the origin instead of returning 501.
#11: The S3 listener now terminates TLS and speaks both addressing styles.
  `[listen.tls]` serves HTTPS from the configured cert/key via axum-server over
  rustls, reloading both on SIGHUP without dropping live connections (production
  cert rotation is zero-downtime); absent, it serves plain HTTP as before, the
  dev default. `listen.domain` is threaded into `router_with_domain` so
  virtual-hosted requests (`bucket.<domain>`) resolve alongside path-style. New
  `tls` module owns the crypto-provider install, PEM load, and SIGHUP reload.
 #198: Passed the serving cache engine to the prefetch coordinator as its
  evict-first demotion sink (`Arc<dyn BlockDemoter>`), so a compaction commit
  hard-evicts its retired files after the grace window. The demoter is an Arc
  clone of the same engine that serves reads.

- #216: Removed the obsolete `removed_bucket_field_exits_nonzero` config test.
  `backend.bucket` is an accepted informational field again (#221 wildcard
  model), so a config carrying it must parse, not exit non-zero.

- #216: `resolve_auth` now reads the endpoint keypair from the AWS-format file
  named by `[auth] credentials_file` (falling back to a generated ephemeral pair
  when `[auth]` is absent), so the endpoint secret never lives in config.toml.
  Updated the restart/s3_smoke/tls integration tests to write the keypair to a
  credentials file the config points at.
- #226: reverted to single-bucket serving; deleted the #132 per-bucket registry; backend.bucket is now required and gates serving. Multi-bucket is deferred to #226.
- #46: the daemon builds one `NodeMetrics`, hands it to the S3 front-end (which
  records per request) and to a `/metrics` render closure wired through a
  deferred `MetricsSlot` on the admin listener. The closure reads the engine
  counters and the backend breaker/retry snapshots at scrape time and renders
  the Prometheus text exposition (`text/plain; version=0.0.4`).
- #231: `spawn_lifecycle` propagates the `RestCatalogSource::from_config` error
  so a missing catalog credentials file surfaces at startup rather than as a
  silent unauthenticated poll.
- #220: the daemon spawns the background fragment scrubber
  (`spawn_scrub_loop`) whenever the write-back tier is enabled, regardless of
  cluster mode — silent bit-rot has no membership event, so even a single node
  scrubs on `cache.writeback.scrub_interval_secs`. Rebased onto #46: the
  `/metrics` render closure now also reads the write-back metrics at scrape time
  and stamps them into the snapshot, so the fragment-integrity counters appear in
  the Prometheus exposition alongside the node families.
- #235: The backend registry now serves a bucket set; the boot log names the set,
  and node metrics aggregate the breaker snapshot across served buckets. Added an
  s3_smoke test proving a glob-matched bucket is served end-to-end and a
  non-matching bucket is NoSuchBucket.
- #241: `verglasd` now installs an env-filtered `tracing` subscriber on stderr
  before it starts background work. `RUST_LOG` controls its filter and the
  default is `info`, so existing catalog watcher and warming failures reach
  `verglas logs`; a subprocess test proves an unavailable catalog is visible.
- #250: The daemon now sizes repeated aligned background cache fills to one
  quarter of the configured backend concurrency. Organic reads retain the
  other three quarters, while larger origin budgets let the cache converge fast enough for
  subsequent query waves.
- #16: Added an end-to-end kill-nine restart regression proving metadata that
  never evicted from DRAM is recovered from NVMe with no origin refill.
- #263: Added a loopback-only `/catalog` Iceberg REST surface backed by the watcher-shared catalog cache. The daemon passes catalog mutations through to the configured provider and exposes no catalog route when catalog awareness is disabled.
- #252: Updated peer, warm-from-peer, and restart integration fixtures to
  declare their 8 MiB data-block geometry explicitly. Product defaults may now
  use 2 MiB while these focused legacy-size scenarios keep their original
  expectations.
- #96/#223: Added `spawn_disk_monitor`, a once-a-second background poll of free
  NVMe under `cache.dir`. It publishes the engine's `caching_paused` flag
  (pause admission when the disk nears full, with hysteresis) and the fragment
  store's dynamic ceiling (`min(safety ceiling, free NVMe)`), keeping every
  statfs off the serve and fill paths.
- #223: The fragment store is now built with `with_dynamic_ceiling` over a shared
  atomic initialized to `Cache::fragment_ceiling_bytes()`, replacing the fixed
  `fragment_budget_bytes()` carve.
- #223: The disk poll now drives the shared-budget accounting: it reads the
  engine's physical growth room each tick and grants the fragment store exactly
  the budget the block cache is not using, pausing block admission before it
  would grow into fragment-held bytes. The fragment ceiling starts at 0 and the
  poll's first (immediate) tick raises it before serving starts. The #96
  filesystem free-space gate is unchanged.
- #61: Replaced the startup subscriber (#247) with one built from `[log]`:
  JSON (one object per line) or human-pretty, level from config with `RUST_LOG`
  and `VERGLAS_LOG_FORMAT` overrides, behind a reload handle. Added
  `POST /admin/log` to hot-reload the level at runtime without a restart.
- #233: The daemon now runs the backend startup probe before serving and exits
  with a clear error when a configured bucket cannot be reached or authenticated,
  rather than coming up healthy over an empty store. Dev/test that runs without a
  reachable origin opts out explicitly with VERGLAS_DEV_ALLOW_MISSING_ORIGIN.
- #288: Doc-comment only — the drain lifecycle test's module doc references the
  CLI verb by its new name (`verglas drain`, local-only). The daemon's
  `/admin/drain` and `/admin/members` machinery is unchanged.
- #287: The daemon now serves `GET /admin/access`, a loopback-only snapshot of
  its connection details (S3 endpoint, catalog gateway mount, region, bucket, and
  the endpoint keypair) built from config in `build_local_access`. This is what
  the zero-config `verglas table`/`verglas query` verbs read to reach this daemon.
  The route is present only when the snapshot is configured; the router grew one
  `access` parameter.
- #194: The daemon now binds ephemeral ports and reports them. Added
  `verglasd --ports-file <path>`: as each listener binds it appends a
  `<role> <ip:port>` line (s3, admin, gossip, peer) so the `verglas dev` parent
  learns the kernel-assigned ports without probing. The S3 and admin listeners
  are now bound up front in `serve` and served on the held sockets (zero-window),
  and the resolved S3 port feeds `/admin/access`. When a `[cluster]` gossip or
  advertise address is `:0`, `build_ring` resolves the real port before gossip
  advertises it (chitchat 0.11 fixes its advertised address before it binds, so
  the port must be known up front). Reporting is write-only and off every serving
  path.

- #300: The disk poll logs pause/unpause transitions with the free-space,
  growth-room, and fragment readings. A paused cache serves correctly but
  admits nothing; hit counters were previously the only way to notice.

- #305: The lifecycle pipeline persists retirement state at
  cache_dir/retire-state.json and restores it before spawning (re-demoting
  grace-pending objects and immediately reclaiming any that expired while
  the daemon was down). /admin/stats now reports retired_bytes_pending,
  retired_bytes_reclaimed, and retired_files_reclaimed.

- dashboard sink (Rill): added the `GET /dashboard` admin probe. It lists every
  dashboard-delivery sink with its target table, Rill project path, scaffolded
  flag, last-refresh snapshot, and state. Wired through a deferred `DashboardSlot`
  like the other engine-dependent routes, so the route exists before recovery and
  answers 503 until its slot fills; present only when a catalog is configured. The
  slot is filled after recovery by a source that reads `verglas_sys.sinks` through
  the daemon's own loopback catalog (new deps on verglas-agent and
  verglas-agentmem) and reports each dashboard sink's Rill project state from the
  agent dashboards dir. Opening that catalog client is a probe-only step: a
  failure leaves the route at 503 rather than failing the daemon.
- Security fix (/admin/access credential exposure): `build_local_access` now
  takes only the endpoint access key id, never the secret, so the `/admin/access`
  snapshot the daemon serves omits the secret access key. The served-route test
  asserts the response body carries the discovery fields and no
  `secret_access_key`. The exposed loopback keypair must be rotated as an
  operator follow-up (bounce the daemon; clients re-read the new creds).
- SDK table API: the admin listener now serves `POST /v1/tables/{name}/commit`
  and `GET /v1/tables/{name}/{snapshot,rows,delta}`, the routes `@verglas/sdk`
  calls. They are thin wrappers over `verglas_agent::tables_api`, gated behind a
  `TablesSlot` catalog handle filled after recovery (503 until then) exactly like
  the dashboard probe, and present only when a catalog is configured. The handle
  is an in-process `Arc<dyn Catalog>` over the daemon's own loopback gateway;
  extracted `loopback_connection` so the dashboard probe and the table routes
  share one connection builder.
- Added `POST /v1/tables/{name}`: creates a table from an explicit schema and
  partition spec (arbitrary column types, per-column nullability, identity/month
  transforms) via `tables_api::create_table`. This is the create the SDK uses when
  schema inference on first commit cannot express the columns a caller needs. Same
  `TablesSlot` catalog handle and 503-until-recovered behaviour as the other table
  routes.
- Raised the tables-route request-body ceiling from axum's 2 MiB default to a
  deliberate 32 MiB. The default was never a design choice, and it forced bulk
  ingest (a day of minute bars) into hundreds of tiny commits, each an Iceberg
  snapshot and a rate-limited catalog call. Bodies are buffered while a batch
  becomes Parquet, so the ceiling stays bounded.
- Added the `<name>_LOGS` retention housekeeping loop. Once recovery has opened
  the loopback catalog, a detached task runs `verglas_agent::retention` on
  startup and hourly, pruning every `_LOGS` table to the platform's fixed 3-day
  TTL. It is a background chore off every serving path: a pass failure is logged
  and the next tick retries, and the prune's own CAS retry tolerates concurrent
  appends. The daemon owns only the schedule; the crate owns the strict suffix
  guard and the removal.

- #320: Re-pointed at the extracted engine crate. The loopback catalog builder,
  the SDK table routes, and the retention loop now call `verglas_iceberg`
  (`Connection`, `catalog::open_catalog`, `tables_api`, `write`, retention,
  `parse_table_ident`, `AgentError`) instead of `verglas-agent`. The daemon no
  longer depends on `verglas-agent` — it never resolved a connection, it builds
  the loopback `Connection` directly.

- #322: Daemon API growth. New routes on the admin surface, all behind the
  same 503-until-ready slots as the tables routes: `POST /v1/query` (SQL
  through the embedded engine over the loopback catalog, with optional time
  travel; a bad statement is a 400); `GET`/`PUT /v1/watermark?deployment=`
  (deployment-scoped durable watermark, byte-identical wire shape to the
  cloud endpoint — locally the deployment is a query parameter because the
  daemon has no per-deployment bearer tokens; documented on the route);
  `verglas_sys` registry CRUD (`/v1/sources`, `/v1/mvs`, `/v1/sinks`:
  register, list with active/archived/all views, show, state transitions,
  source complete, MV consumed-watermark) served through a daemon-held
  `SystemCatalog` over the loopback catalog — the daemon is now the only
  local writer of `verglas_sys`; and `POST /v1/platform/memory-pipeline`
  (the installer's idempotent declare, over HTTP). The router's eleven
  positional slot arguments became one named `Slots` struct. TDD: the nine
  wire-contract tests were written first and failed to compile (no `Slots`,
  no `SysSlot`, no routes) before the implementation.

- #323: The tables router grows the routes the re-targeted CLI needs:
  `GET /v1/tables` (list, optional namespace filter), `GET
  /v1/tables/{name}/describe` and `/history` (the inspect reports), and `POST
  /v1/tables/{name}/ingest?mode=create|append&format=&partition_by=` — the
  CLI's create/append moved server-side (the daemon stages the uploaded bytes
  in a scratch file, infers the schema, writes Parquet, and commits through
  the CAS path). TDD: the three route tests were written first and failed
  (404, no routes) before the implementation.

- #328: The daemon now runs the agent-data platform. A new `platform` module
  (exposed through a new `verglasd` lib target so the router and supervisor are
  testable in-process) adds a `SysWatermarkStore` over `verglas_sys.watermarks`
  and a registry-driven `Supervisor`: each tick reads the active local
  deployments and runs due cron sources (through `verglas_source::run_source`),
  daemon-owned SQL MVs, under the `run_guarded` single-flight/backoff policy and
  the daemon's guard dir. Executors commit and read through the daemon's own
  loopback catalog handle, so pipeline I/O is cache-pathed by construction
  (§7.4); the in-process cache adapter is an extension-point comment only. The
  supervisor is spawned detached after the loopback catalog opens and before
  `mark_ready`, so it never blocks recovery. New admin routes: the platform
  queue `/v1/queues/<name>/{enqueue,poll,ack}` (backed by
  `verglas_harness::queue`, the TS SDK verb's target) and the manual-run
  (`POST /v1/sources/<name>/run`) and webhook (`POST /v1/webhooks/<name>`)
  triggers. To avoid double-running the memory pipeline (agentmem still owns it
  until Phase 4) the supervisor skips non-cron sources and non-SQL MVs. TDD:
  the queue round-trip, the cron-source executes/pause/resume acceptance, and
  the throwaway-daemon queue e2e were written first and failed before the
  routes and supervisor existed.

- #330: Proved the massive-tradebars migration capability (test only, no live
  cutover). A massive-tradebars-shaped TypeScript Source
  (`clients/sdk-typescript/src/examples/tradebars-source.ts`) runs as a
  registered `subprocess` cron source: the platform supervisor's real subprocess
  factory spawns it under the bun shim, `run_source` commits the day's sessions
  to a throwaway table, and a re-run after a reset watermark replays the range
  via the idempotency key with no duplicate rows. The live launchd job
  `com.cascadelabs.massive-tradebars-daily` and its state file are referenced
  only in the cutover runbook doc-comment — never written — so the production job
  is provably untouched. TDD: the run/resume acceptance was written first and
  failed before the capability was wired.
- #331: Repointed the platform supervisor, registry routes, and admin slot to
  `verglas-platform` for the verglas_sys registry types (SystemCatalog,
  SourceSpec/MvSpec/SinkSpec, SystemState, PlatformError). The memory-pipeline
  declare route still calls `verglas_agentmem::platform::declare_memory_pipeline`.
- #331: Added the memory read routes POST /v1/recall and POST /v1/session-context,
  backed by a RecallSlot holding the daemon-side recall engine (loaded local
  embedder over the loopback catalog). Wired lazily after recovery on the blocking
  pool so the embedder load never blocks the serving path; the routes answer 503
  until it is ready. TDD: the route tests (scores over the engine; 503 until wired)
  were written first.
- #331: Added POST /v1/remember (the deliberate memory write) alongside the recall
  routes, backed by the same recall engine. TDD: the remember-then-recall route
  test was written first.
- #331: Reworded the platform supervisor's doc comments: the memory workflow runs
  on the same harnesses but by its own triggers (hook-fed capture, boundary
  consolidation), not the daemon tick, which correctly runs only cron sources and
  SQL MVs. Dropped the stale "until Phase 4 / agentmem" framing.
- #331: Added the #262 acceptance e2e (tests/memory_continuity.rs): the real admin
  router served over a real high port; harness A captures + consolidates, harness B
  recalls A's memory over /v1/recall and sees it in the /v1/session-context block.
  Plus a recall-parity test proving the HTTP ranking matches the embedded
  MemoryStore ranking exactly. Deterministic mock embedder, hermetic catalog, no
  live-system contact.
- #262: memory_continuity e2e updated to trajectory-shaped capture input — the
  harness-A capture helper writes a CaptureRecord to the queue instead of a
  RawEvent. Still green: memory written in harness A is recalled in harness B.

- #258: Added the `graph` verb-family admin routes (`/v1/graphs/...`) backed by
  the verglas-graph engine over the same loopback catalog as the table routes.
  POST create (ensure the two backing tables), POST nodes/edges (batch insert,
  returns the new snapshot), POST index (build/refresh the Puffin adjacency
  index), POST query (neighbors/kHop/neighborhood/paths with index-or-scan
  fallback, reporting which backend served it), and GET show (backing tables,
  counts, index state). A graph shares the catalog handle with the table routes
  and answers 503 until it is wired after recovery, so the two backing tables are
  plain Iceberg tables queryable on their own. Wire-contract tests cover every
  route, the turn-off equivalence (identical results with and without the index),
  and that the node/edge tables appear in the plain table list.

- Memory redesign (graph-recall e2e): added `tests/graph_recall_e2e.rs` — the
  hybrid-recall acceptance gate. Serves the real admin router (recall engine +
  graph routes) on a throwaway high port over one hermetic in-memory catalog,
  captures + consolidates a session with a relationship-emitting extractor, and
  asserts (1) the knowledge graph landed (nodes + edges + bound Puffin index,
  visible over GET /v1/graphs/agent_memory and the traversal query route) and
  (2) POST /v1/recall surfaces a memory reachable ONLY over a typed relationship
  (vector term 0) above an unrelated one. A second test proves the turn-off: a
  relationship-free consolidation gives every recall hit graph term 0.
- index registry: Wired the vector index to the durable `verglas_sys.indexes`
  registry. VectorRuntime now holds the SystemCatalog handle and the resolved
  cluster id (hostname by default via Config::resolve_cluster_id). Declaring an
  index writes a durable row (cluster-id-keyed) in addition to the in-memory
  build; the reflected snapshot and blob ref land in the row when the build
  produces them. On boot, rehydrate_indexes reads this cluster's Running rows and
  per row loads the present shadow-store blob (serve immediately) or rebuilds a
  missing one (recording the rebuild back to the registry); it runs detached so
  it never blocks recovery. Rows for other cluster ids are visible in the
  registry but not served locally — a search for one takes the existing
  brute-force fallback (the turn-off path), not a 404 (smallest surface). New
  GET /v1/indexes serves the durable registry across tables, graphs, and
  clusters (name/target/field/metric/cluster_id/state/reflected_snapshot).
  Phase 5 hand-off: surfacing this in verglas-cloud is a later step and the
  Iceberg registry table is already cloud-readable, so verglas-cloud is untouched.
  Tests: declare_survives_reboot_via_registry (declare writes a cluster-id row;
  reboot rehydrates the present blob and serves the same NN without a rebuild;
  GET /v1/indexes lists it) — the in-process-router equivalent of the
  throwaway-daemon smoke, since the vector routes are catalog-gated.

- #95: The vector-index routes now persist their Vamana Puffin blobs to the REAL
  cache-managed shadow store (`verglas_cache::shadow::ShadowStore`, NVMe-resident
  under the cache dir, budget-bounded by `cache.shadow_capacity_bytes`), replacing
  the `LocalDirShadowStore` seam. New `shadow::CacheShadowStore` is the daemon-side
  adapter implementing `verglas_vector::ShadowBlobStore` over the cache store, so
  neither engine crate depends on the other (verglas-vector stays cache-free). The
  daemon opens the shadow store once and injects the adapter into the
  VectorRuntime; reboot rehydration reads the present blob back from it. The
  vector_routes tests (declare→build→search, and the durable reboot/rehydrate
  round-trip) now run against the cache shadow store, not LocalDir/in-memory —
  the same store production uses. A reopened store rebuilds its index from disk,
  so the reboot test proves durability end-to-end through the routes.

- ann recall + memory index: The daemon now serves recall's vector-seed from the
  ANN index. One shared `VectorService` backs both the vector-index routes and
  recall's `MemoryVectorSeeder`, so recall searches the built memory index and
  falls back to the brute-force scan when none is built. `ensure_memory_index`
  declares the memory pipeline's index over `agent_memory.memories`/`embedding`
  (uuid-keyed, `IdEncoding::UuidHash`) at vector-runtime wiring so a fresh install
  builds + maintains it; the durable `verglas_sys.indexes` row carries the id
  encoding (`index_params_json`/`config_from_row` round-trip it). New
  `POST /v1/tables/{name}/indexes/{field}/refresh` runs a maintenance pass and
  reflects the build into the registry row — how the memory index builds once the
  first consolidation writes rows. `VERGLAS_RECALL_EMBED=mock` selects the
  deterministic embedder for the hermetic real-daemon recall e2e (the embedder
  analogue of the injected deterministic extractor); unset loads the local ONNX
  encoder as before. The recall response carries `seed_source`.

- cloud-agnostic sweep: removed every Cloudflare/R2 mention and tenant-named
  fixture from code, docs, and tests. Comments now describe the constraint
  ("strict S3-compatible stores reject variable-size parts", "some managed REST
  catalogs gzip responses") instead of naming a vendor; test fixtures use
  neutral hosts/entities (storage.example.com, acme, blobstore). No behavior
  change — the daemon and SDK are wiring-agnostic over any S3 bucket + Iceberg
  REST catalog.

- windows release build (cfg audit): Gated the one Unix-only path that blocked
  the daemon from compiling on Windows. `tls::spawn_reload_on_sighup` (live TLS
  cert reload on SIGHUP) is now `#[cfg(unix)]`; a `#[cfg(not(unix))]` stub logs
  once that reload-on-signal is unavailable and the operator must restart the
  daemon to load a new cert. The SIGHUP-less path is the only difference —
  serving TLS and everything else is unchanged. (`libc::getppid` in the
  parent-death watch was already `#[cfg(unix)]`.) Part of the cargo-dist release
  pipeline: all four shipped binaries must at least compile on the Windows
  target; the Windows CI runner is the real gate.

- daemon-instance tracking (node register + heartbeat): Added `node_report`, a
  fully detached background task that registers this daemon with the tenant's
  control plane on boot and heartbeats every 5 minutes (one const, no config
  knob). The node id reuses `Config::resolve_cluster_id` — no second identity.
  The heartbeat carries a small flat `metrics` object (uptime, NVMe/DRAM
  capacity, DRAM resident, pooled hits/misses, hit rate, backend fills, bytes
  served, tables warmed) computed from the existing `/admin/stats` counters via
  the shared `stats_slot` — no new instrumentation, nothing on a hot path.
  Spawned in `serve` before any listener binds and outside the data-plane build,
  so it never gates readiness and a dead control plane never affects serving;
  failures log at debug and retry next tick. PRIVACY INVARIANT (release-blocking):
  `NodeReporter::from_config` returns None unless the machine is logged in
  (a `[control_plane].url` AND the `~/.verglas/credentials/control-plane-token`
  file) — no reporter, no task, no network — so a self-hosted daemon never phones
  home. New deps: reqwest (workspace pin, json+rustls-tls) and thiserror. Tests
  (TDD, RED first): the privacy invariant (no config → no reporter; URL without a
  token → no reporter), a positive path driving the real reporter against a local
  axum mock (register + heartbeat bodies, metrics in the heartbeat), and the
  metrics-mapping unit tests.
- #60: Wired the per-table telemetry hub through `serve`: created it in
  `spawn_prefetch` (with the mapper), spawned a ~1s rollup task, and passed it +
  the mapper into the `HeatFeed` serving decorator. Added `GET /v1/metering/tables`
  (the `TablesReportSlot`) serving the per-table report, extended `/metrics` with
  the per-table families, and extended the `node_report` heartbeat with a metering
  record: per-window per-table DELTAS (hits/misses/bytes/requests-avoided) since
  the last successful report, buffered bounded (drop-oldest, logged) when the
  control plane is unreachable, resent idempotently. The privacy invariant stands
  (no `[control_plane]` → no reporter → no metering). Metering-windowing unit
  tests added; the #358 no-config/no-network and register/heartbeat tests stay
  green.
- catalog websocket change feed: `spawn_lifecycle` now builds the catalog
  watcher as a `CatalogFeed` instead of a `PollingWatcher`. It derives a
  `WsFeedConfig` from the catalog origin and reuses the catalog bearer token,
  passing it to the feed so the daemon attempts a websocket upgrade at
  `<origin>/v1/catalog/feed` and falls back to polling automatically for
  third-party catalogs. SigV4 (AWS) catalogs pass `None` and poll directly. No
  new config field — transport selection is automatic. `spawn_warming` and
  `spawn_prefetch` take the `CatalogFeed` handle.
- workers: Replaced the source/MV/sink supervisor with the worker runtime
  (platform.rs): a cron scheduler computing logical intervals with Airflow-style
  catchup against a per-worker cursor, data_change fan-out (on_table_commit),
  and manual/webhook runs, all over harness::run_worker under the guard policy.
  Admin routes: /v1/workers register/list/show/state and /v1/hooks/{name} +
  /v1/workers/{name}/run replace /v1/sources|mvs|sinks and /v1/webhooks. Deleted
  the memory recall/remember/session-context routes, the memory-pipeline declare
  route, and ensure_memory_index. Boot translates legacy sources → workers and
  logs dropped MVs/sinks. Dropped the verglas-source/mv/sink/memory/memory-jobs
  deps; the /dashboard probe now reports empty. Removed the source-executor and
  memory verglasd tests.
- #379: Start the loopback S3 server before catalog-backed system-table bootstrap. This removes the circular startup wait that left catalog-configured daemons permanently unhealthy while opening `verglas_sys.workers` through their own cache endpoint. Keep daemon assertions tied to the package version and the current catalog-feed diagnostic so release bumps and transport cleanup do not leave CI stale.
- PR #378: Added `POST /admin/compact` as a manual, one-shot compaction trigger.
  The daemon does not register or schedule compaction; recurring policy runs as a
  container-backed worker outside this runtime.
- serving surface: The `/v1` API (`POST /v1/query` and the `/v1/tables/...`
  group) now answers on the SigV4-gated S3 data port too, not only the loopback
  admin listener. Extracted `v1_serving_router` (the tables + query sub-routers,
  shared catalog slot; `/admin/compact` stays admin-only) and reused it inside
  the admin `router`. Added `V1ServingApi`, which drives that router once per
  request via `tower`'s `oneshot` behind the crate's `ServingApi` trait, and
  wired it into `serve_s3`/`run_s3` so the S3 front-end serves `/v1` when a
  catalog is configured. The edge re-signs cache-pathed `/v1` forwards with the
  cache keypair and they land here; SigV4 gating comes from the existing
  `[auth]`/`StaticAuth` path. Added `tower` as a direct dependency (was
  dev-only). Covered by a router test committing a table end to end through
  `v1_serving_router`.
- chore: Remove boot-time legacy→workers translation, the empty /dashboard probe, and the query-worker→embedded fallback. When [query_worker] is set it is fail-closed.
- #393: Switched from in-tree `vendor/iceberg` to the pinned `verglas-org/iceberg-rust` fork (`verglas/v0.9.1` @ a40f9268) for `TableCommit::from_parts`. Same patch, maintained out of tree; drop when upstream exposes overwrite/replace commits.
- #393: Removed platform `_LOGS` run logging and day-partition retention from Verglas. Catalog-side lakekeeping owns telemetry write/TTL; this crate keeps only the compact-adjacent APIs (snapshot expiry where applicable). Harness no longer writes `verglas_logs.<name>_LOGS`; verglasd no longer runs the hourly prune loop.
- #1 (verglas-org/verglas): Removed the fleet `verglas-compact` one-shot binary and its workspace/dist membership. Compaction stays in `verglas-iceberg` + daemon `POST /admin/compact` / `verglas table compact` until the async maintenance API lands; e2e retargeted to that path.
- #3: Removed the daemon catalog/table proxy and embedded execution fallbacks. The daemon now dispatches bounded requests to isolated `verglas-query` and `verglas-write` roles.
- #263: Removed the daemon's `/catalog` Iceberg REST proxy and stopped advertising a catalog path from `/admin/access`. Internal table, graph, vector, platform, and query-worker operations now connect privately to the configured upstream catalog while continuing to route object IO through the daemon's S3 cache endpoint.
- #91: Vector routes now publish and discover Vamana indexes through the source
  table's snapshot-bound Puffin statistics metadata. Removed the shadow-store
  adapter, side registry, boot rebuild, and brute-force serving path.
- #91: Renamed the foreground container process, crate, and executable from
  `verglasd` to `verglas-server`. Runtime identity, tests, documentation, and
  build surfaces now use the server name with no legacy binary alias.
- #3: Advertise the configured upstream catalog URI and warehouse through the
  access-discovery response alongside the Verglas S3 endpoint. SDK clients use
  those coordinates directly; the server does not host or proxy the catalog.
- #3: Packaged the isolated query and write role binaries in the self-hosted
  Docker image and enabled both dispatchers in its config. The Docker
  application can now execute SQL and logical writes without embedded paths.
- #8: Reduced the server crate to process startup and listener assembly, delegating HTTP composition to `verglas-rest`. On-prem startup now advertises and internally uses local S3, query/write, and cached catalog endpoints.
- #11: Verglas now owns and serves the on-prem scheduler object queue from its existing state directory. The separate scheduler container connects through REST and no longer requires a shared filesystem mount.
- #11: Connected manual, HTTP callback, and catalog data-update ingress to the standalone scheduler event service. The server requires its scheduler URL when catalog-backed platform routes are enabled and keeps durable queue storage behind `verglas-rest`.
- #11: Removed scheduler state from the Verglas cache directory and made worker ingress optional. A missing or empty scheduler URL now leaves the storage, query, and catalog server fully operational without mounting worker-trigger routes.
- #11: Changed the on-prem catalog watcher into an Iceberg snapshot CloudEvent producer. Catalog updates now enter the generic event subscription path instead of a private data-update trigger.
- #19: Capped the self-hosted server container at 8,192 open file descriptors.
  Docker previously granted it 1,048,576, so a future socket or cache leak could
  exhaust the host file table instead of failing inside the container.
- #18: Added an explicit environment-configured startup mode for the Docker image. The Compose application now supplies cache, R2, catalog, endpoint-auth, and execution-role settings directly and no longer copies or mounts a server TOML or credentials directory.
