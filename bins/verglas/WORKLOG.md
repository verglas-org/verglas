# verglas worklog

- #376: Removed the CLI-local daemon HTTP implementation and now calls the
  reusable `verglas-sdk` transport directly. CI asserts the CLI dependency
  graph contains no Verglas Iceberg or DataFusion engine crates.

Append-only log of changes to this crate, by issue. Every PR touching this
crate adds an entry (see /AGENTS.md, "Worklog discipline").

- #55: Added `vessel add/list/get/remove/curl/query` against the authenticated local Docker runtime
  manager, with YAML/JSON manifests and a dedicated runtime endpoint.

- #1: Scaffolded as part of the initial cargo workspace: stub with module-level
  docs, placeholder types wiring real dependency edges, and an integration
  test directory. Toolchain pinned (1.96.1), workspace clippy lints applied.
- #5: Replaced the scaffold stub with a clap command tree, shared output
  helpers, and a reqwest admin client. `verglas version` is the first real
  command and calls `GET /admin/version`; the remaining subcommands print a
  simple not-yet-implemented message until their follow-up issues land.
- #25/#26: Implemented `verglas dev` and `verglas bench`. `dev` resolves a
  temp-or-`--cache-dir` cache, generates a dev keypair, spawns a loopback
  `verglasd` cluster-of-one (path-style, buffered IO), and prints DuckDB /
  Polars / aws-CLI snippets for one endpoint serving both reads and writes;
  Ctrl-C tears the daemon and temp cache down. `bench` is a thin adapter over
  the new `verglas-bench` crate. Added `verglas-bench`, `tempfile` deps.
- #132: `verglas dev` no longer takes `--bucket` — it serves any bucket the
  ambient AWS credential chain can read. The generated daemon config drops the
  bucket line, the banner states the wildcard mode, and the printed engine
  snippets use a placeholder bucket. `verglas bench --bucket` is unchanged (it
  targets one table in one bucket by design).
- #138: `verglas bench` gained a `--purge` flag. It plumbs the global
  `--endpoint` (the daemon admin API) into `BenchOptions.purge_admin_endpoint`
  so the bench resets the cache to cold before the cold leg via
  `POST /cache/purge`, making repeat-cold runs honest without restarting the
  daemon.
- #141: `verglas dev --dram SIZE` exposes the previously hardcoded 1 GB DRAM
  ceiling as a flag (default unchanged), so bench runs can pick a tier profile —
  1 GB keeps an SF1 dataset DRAM-resident, `80MB` (the engine floor) forces
  NVMe-resident serving. The value flows verbatim into the daemon config; a
  sub-floor value fails fast at daemon startup with the engine's budget error.
- #141: `verglas bench` gains `--admin-endpoint` (default: the bench endpoint's
  next port, matching `verglas dev`) and reads the daemon's cache budgets from
  /admin/stats so the report stamps DRAM/disk tier context on the numbers.
- #160: `verglas dev --nodes N` boots a local pseudo-cluster. `--nodes 1` (the
  default) is the single-node cluster-of-one, byte-for-byte unchanged (locked by
  a golden banner snapshot). `N > 1` spawns N `verglasd` children on consecutive
  4-port blocks (S3/admin/gossip/peer per node), each with its own cache dir and
  its own per-node `--dram`/`--cache-size` budgets, rendered with a `[cluster]`
  section seeded at node 0's gossip address (gossip exists now, #27 — no
  static-ring fallback). Dev keys are generated once and shared across the pod so
  a client can talk to any node; the engine snippets point at node 0. Ctrl-C
  tears every child down in reverse order and removes the ephemeral per-node
  caches; a child dying early kills the rest and exits non-zero naming the node.
- #160 (addendum): node death in a dev pod is now two-phase. During startup
  (until every node's /admin/healthz has answered once) a dying node still
  kills the pod fail-fast, naming it — a partially-booted pod is a config
  error. After a healthy boot, a dying node no longer kills the pod: a loud
  notice names it (exit status, surviving count, ring repair within the
  suspicion window) and the survivors keep serving, because a cluster must be
  resilient to single-node failures and the dev harness exists to exercise
  exactly that. The final exit after Ctrl-C is non-zero if any node died
  mid-run, so scripts still detect the degraded run.
- #164: added `verglas dev --admit-probability P`, emitting a
  `[cache.admission] churn_admit_probability = P` override into the generated
  daemon config (omitted when the flag is absent, so the daemon keeps the
  default). This lets the disk-constrained TPC-H profile engage the
  resident-biased admission thinning that cuts warm-leg backend fills.
- #170: `verglas dev` no longer orphans its `verglasd` children. A single
  shutdown handler now catches SIGINT, SIGTERM, and SIGHUP (was SIGINT only) and
  routes all three through the same reverse-order teardown and ephemeral-cache
  cleanup, so `kill`/`pkill`/terminal-close tears the cluster down like Ctrl-C.
  Each child is spawned with `VERGLAS_PARENT_PID` set to the CLI's pid, arming
  the daemon's parent-death watch so children exit even when the parent is
  SIGKILLed (no handler possible). Startup now pre-flights every S3/admin port
  and, on a collision, fails loudly naming the port and handing over
  `lsof -i :<port>` with an orphaned-prior-`verglas dev` hint. The dev_nodes
  port helper was reworked to an atomic disjoint-range allocator (plus a wider
  per-process seed and a squatter-acquire retry) so the added live tests do not
  race each other for loopback ports.
- #178: Updated the `verglas bench --purge` admin-stats fixture for the new
  `/admin/stats` live/reclaimable DRAM split; the CLI reads the split through
  the shared `verglas_core::admin::StatsInfo` type (no CLI logic change).
- #31: added `verglas node drain <node-id> [--timeout 10m]`. It resolves the
  node id to its admin address via `GET /admin/members`, then POSTs
  `POST /admin/drain` to that node with the parsed timeout and prints the ack.
  Added the admin-client `members`/`drain` calls (and a generic JSON `post`) and
  a `<N>[s|m|h]` duration parser, both unit-tested.
- #180: Added `verglas dev --writeback` (with --writeback-k/-m/-w, default
  k=2/m=1/w=3 to fit a 3-node pod). It emits an enabled [cache.writeback] section
  into every node's config so the write-back tier can be benchmarked on a local
  pod. Off by default; the section is omitted entirely when the flag is absent.
- #216: Added the host lifecycle commands `verglas init/start/stop/restart/
  status/logs/uninstall`, which install and run `verglasd` as a managed OS
  service. A small internal `ServiceManager` trait has one implementation per
  platform behind `#[cfg(target_os)]`: launchd on macOS (renders a LaunchAgent/
  LaunchDaemon plist, drives `launchctl bootstrap/bootout/kickstart/print`) and
  systemd on Linux (renders a `.service` unit, drives `systemctl --user`/system
  `daemon-reload/enable/start/stop`, reads state from `systemctl show`). `init`
  reuses the daemon config loader to write and validate the config, provisions
  the cache/state dir, generates dev keys, captures the AWS credential-chain env
  into the service definition (a boot-started service inherits no shell), and
  installs the service; it prints the endpoint and the same engine snippets
  `verglas dev` does. Both scopes are supported (per-user default, `--system`).
  `start` blocks until `/admin/healthz` reports ready. `status` merges the OS
  service state with `/admin/healthz` and `/admin/stats` (health + cache warmth).
  `logs` tails the plist log files (macOS) or `journalctl` (systemd). The systemd
  unit sets the cgroup budget knobs (MemoryMax/CPUWeight/IOWeight) from config.
  `uninstall` removes the service, removes the config only with `--purge-config`,
  and never touches cache data. plist/unit rendering, scope resolution, status
  aggregation, and the config-less paths are unit-tested; a macOS-gated
  integration test installs+starts+stops+uninstalls a real LaunchAgent. Windows
  (SCM) is deferred to a follow-up.
- #216 (addendum): Live-verification fixes. The launchd plist now raises the
  open-files limit (SoftResourceLimits/HardResourceLimits NumberOfFiles) and the
  systemd unit sets LimitNOFILE, because the cache engine opens many region files
  and a service inherits the supervisor's low default (256 under launchd) — the
  daemon otherwise fails to build its cache with "too many open files". The
  systemd unit is Type=exec, not Type=notify: readiness is enforced by the CLI
  polling /admin/healthz (the same path launchd uses), since verglasd does not
  speak sd_notify. Config and logs now always live at the well-known per-scope
  state dir (~/.verglas or /var/lib/verglas); --cache-dir moves only the cache
  data, so start/status/logs/uninstall find the config without a flag.
- #221: `verglas init` now scaffolds `~/.verglas/config.toml` — a complete,
  fully-commented config showing every daemon setting at its default, with the
  required trio (backend.bucket/endpoint/region) filled from --bucket/--endpoint/
  --region or left blank-and-commented. A coverage test reflects over the
  serialized default Config and fails if a new field is missing from the
  scaffold, so it cannot drift. Re-running init refuses to clobber an existing
  config without --force. Credentials are scaffolded under `~/.verglas/
  credentials/` (AWS credentials-file format, mode 0600); the config names the
  file/profile and secrets never go in config.toml. The installed service now
  carries no AWS_* env — the daemon reads endpoint/region from config and creds
  from the credentials file. `verglas start`/`restart` validate the required
  config before touching the service manager and fail with a message naming the
  missing field, the file, and how to set it (endpoint/region accept the AWS env
  as a fallback so production IAM is not forced to set them). The global admin
  flag was renamed `--admin-endpoint` (distinct arg id) so `init --endpoint` no
  longer collides with it. Config file moved from `verglas.toml` to `config.toml`.
- #216: `verglas init` now generates config.toml from the daemon schema via
  `config_template::render` and writes the generated endpoint keypair to a
  mode-0600 AWS-format creds file at `<credentials_dir>/endpoint`, so no secret
  is ever written into the config. `verglas dev` likewise writes an ephemeral
  endpoint creds file per node and points `[auth] credentials_file` at it
  instead of inlining keys. The old hand-written scaffold template and
  `ScaffoldValues` were deleted; the init banner now names both the backend and
  endpoint credentials files.
- #226: reverted to single-bucket serving; deleted the #132 per-bucket registry; backend.bucket is now required and gates serving. Multi-bucket is deferred to #226.
- #216: `verglas init` gained `--provider s3|azure|gcp` (default s3), setting
  `[backend] provider` and writing the matching backend credentials template
  (AWS-INI for s3, key=value for azure/gcp). Added `parse_provider` validation
  and provider-specific credential scaffolds. Secrets still never touch config.toml.

- Removed the `verglas bench` command and the `verglas-bench` crate. The
  synthetic trace-replay benchmark was misleading (reactive-only, newest-
  metadata heuristic). Real benchmarks live under `benchmarks/` with their own
  `run.sh`. The `medium` module stays (used by `verglas dev`). Note: the
  writeback benchmark's `run.sh` still calls `verglas bench --seed` and needs
  its own seeder, like the compaction/pod benchmarks have.

- #213 (CI fix): `verglas dev` installed its SIGINT/SIGTERM/SIGHUP handler only
  after waiting for the child daemons to answer their admin ports and printing
  the banner. A supervisor (or the pod teardown test) that saw a child answer
  could deliver SIGTERM inside that window, before the handler existed, so the
  kernel's default action killed the parent (exit-by-signal 15) and the pod was
  never torn down. Signal registration now happens up front, before any node is
  spawned, via a `ShutdownSignal` whose OS handlers are armed at construction
  rather than on first poll. Both the single-node and pod paths install it
  before their ready-wait.
- #235: `verglas start` preflight accepts a bucket SET — `backend.bucket` and/or
  `backend.bucket_globs` — instead of requiring a single `backend.bucket`, so a
  globs-only config (the S3 Tables case) starts. Added a test for the globs-only
  path.
- #61: `verglas dev` spawns each `verglasd` with `VERGLAS_LOG_FORMAT=pretty` so
  a local run emits human-readable logs regardless of the config's log format.
- #233: The dev-pod integration tests point at a placeholder origin, so they now
  set VERGLAS_DEV_ALLOW_MISSING_ORIGIN to skip the new backend startup probe and
  keep exercising pod lifecycle without a reachable backend.
- #288: Stripped the CLI to the commands that perform their advertised job end
  to end. Deleted the `version` subcommand and the unimplemented `analyze`,
  `deploy`, `keys`, `tables`, `warm`, and `doctor` stubs (command code, enum
  variants, help text, and the `not_implemented` helper). `-V`/`--version` (clap
  built-in) prints the CLI version; the running daemon's version moved into
  `verglas status`, which already probes the daemon. Kept `dev`, `node`, and the
  lifecycle commands (`init`/`start`/`stop`/`restart`/`status`/`logs`/`uninstall`).
  Refreshed the help-output test to pin the exact surviving command set so a
  future stub cannot creep back, and added a live happy-path test for
  `node drain`.
- #288 (review follow-up): The CLI only knows about itself — it never does
  cluster activities. Removed the `node` subcommand and the CLI-side
  `/admin/members` lookup path; added a top-level `verglas drain` that drains
  the LOCAL daemon only (POST `/admin/drain` on this machine's admin endpoint,
  no target argument). Removed `verglas uninstall` and the service layer's
  uninstall operation; removal is now a documented manual step (README,
  "Removing Verglas") so it cannot be invoked by accident. Daemon-side
  machinery (`/admin/drain`, `/admin/members`) is untouched. The help-list
  test pins the final set: dev, drain, init, start, stop, restart, status,
  logs.
- #287: Added the agent-facing verbs `verglas table` (create/append/list/show/
  history) and `verglas query`. They resolve a connection (flags > env > the local
  daemon's /admin/access), open the Iceberg REST catalog, and run the client-side
  operations in verglas-agent, rendering `--output json` or a human summary. Also
  added `verglas dev --catalog-uri`/`--catalog-token`, which turns on the dev
  daemon's loopback catalog gateway so the verbs work zero-config against it.
- #194: Fixed the `verglas dev` port TOCTOU. `--port` now defaults to `0`: every
  child binds `127.0.0.1:0` for each listener and reports the kernel-assigned
  ports back through a `--ports-file` the parent names, so nothing races to grab
  a probed-free port between probe and bind. The pod boots sequentially — node 0's
  reported gossip address seeds the rest — so two pods come up concurrently with
  no coordination. An explicit `--port` keeps the fixed base and the loud
  pre-flight collision error. Added `verglas dev --ports-file` (resolved node
  endpoints for scripts/tests). The dev-node tests now boot with `:0` and read the
  reported ports instead of probing; added a concurrent-two-pods regression test.

- #261/#285: Added `verglas skills install [--harness claude|codex|cursor|all]`.
  Writes the skill files (SKILL.md + references) per harness, the Cursor .mdc
  rule, and the capture/consolidate/session-start hook scripts, plus an env file
  the hooks source. Merges the hooks into ~/.claude/settings.json additively
  after a timestamped backup (existing entries and other skills are never
  touched; re-install is idempotent), registers the verglas-memory MCP server
  via the Claude CLI, and pre-fetches the embedding model (a fetch failure does
  not fail the install). Skill assets are embedded in the binary.

- #297: Hook scripts (capture.sh/.py, consolidate.sh, session_start.sh) exit
  immediately when `VERGLAS_CONSOLIDATION_CHILD` is set, so a Verglas-spawned
  claude child captures zero events and spawns zero consolidators (recursion
  tests written first and shown failing against the old scripts). The empty
  `VERGLAS_CONSOLIDATE_BIN` kill switch is now a documented, tested feature.
  capture.py also enforces the per-session spool size cap.

- #296/#261: `verglas memory seed` (standalone re-run verb) and default install
  seeding: install announces the seed scope, then launches it detached so first
  use is never delayed. Completed the Codex install (skill dir, marked
  AGENTS.md trigger block, `[mcp_servers]` entry appended to config.toml with a
  backup) and the Cursor install (always-apply rule, skill dir, mcp.json merge
  with a backup). Codex/Cursor have no lifecycle hooks; the skill documents
  that memory there is MCP-only.

- #285: Wired Codex and Cursor to the same capture -> consolidation -> injection
  loop as Claude Code. Both harnesses have native hook frameworks: the installer
  now writes `~/.codex/hooks.json` (Claude-format events SessionStart, PostToolUse,
  UserPromptSubmit, Stop, PreCompact) and `~/.cursor/hooks.json` (Cursor's flat,
  versioned schema: sessionStart, postToolUse, beforeSubmitPrompt, stop,
  sessionEnd), additively and idempotently with backups. The one capture pipeline
  now parses per harness: session id from session_id or conversation_id, and the
  session-start hook emits the Claude/Codex hookSpecificOutput envelope or Cursor's
  flat additional_context. The secret scrubber (ported from the seed path) now runs
  in capture.py, so no credential shape reaches the spool from any harness. All
  #297 guards hold across harnesses: one shared spool means one host-wide slot
  budget. Codex non-managed hooks need a one-time trust grant (no supported
  programmatic path, openai/codex #21615) — the install prints the one manual step.
- #285: Moved all agent-skill state out of the POC scaffolding paths. env.sh,
  hooks, spool, and seed state now live under `~/.verglas/agent/`, the embedding
  model under `~/.verglas/models/` (new siblings of the daemon's own files, which
  are untouched). Removed every "poc" name from code defaults, hook scripts, and
  docs. No migration code (prototype rule): the installer just writes the new
  layout.

- #295: `table create`/`append` take the source file as a positional argument
  and a new `--from junit|lines` flag that forces the fixed-schema ingester;
  without it the format is still inferred from the extension. Wired the flag
  through to `verglas-agent`'s ingest format override.

- #295: platform-v0 control-plane verbs. Added `verglas source|mv|sink
  list|show|pause|resume`, each with `--json`, wired to the new
  `verglas-agentmem::platform::SystemCatalog` over the same connection
  resolution the `table` verbs use (no embedder needed). Pause/resume append a
  revision that flips state; a missing declaration is a clear error. `table
  create`/`append` now also record each bounded ingest run as a `completed`
  source row in `_verglas.sources` (connector `file`, target set to the table),
  so a table's origin is a catalog query — best-effort, one extra append, never
  fails the ingest (the turn-off invariant). Updated the CLI help-surface guard
  test to include the three new verbs.

- #295: capture writes the queue layout. capture.py now appends to the
  per-source segment log (`streams/<harness>_hooks/`), same layout and bounds
  as the Rust SegmentLog (segment roll, source-size cap with dropped
  counter); scrubbing still happens before any write. Added `verglas memory
  migrate-spool` for the one-shot move of pending old-layout spool events
  into the logs. Capture adapter tests were rewritten to the new layout
  first and shown failing against the old per-session spool writer.

- #295 (phase B): `verglas mv run <name> --window|--sweep` executes the
  memory_consolidation MV through the shared executor (all #297 controls in
  Rust); consolidate.sh became a thin emitter that detaches exactly
  `$VERGLAS_BIN mv run memory_consolidation --window <id>` (kill-switch check
  kept; all guards live in the executor). The installer declares the pipeline
  rows (`declare_memory_pipeline`, idempotent, never resumes a paused MV) and
  its run() went async for the catalog call. Added `verglas source register`
  (adopt an external job into the declared state; schedule/bounded recorded
  in the config JSON) and `verglas source complete` (final watermark,
  state=completed). SKILL.md and cli-reference updated: MV consolidation,
  external-job adoption playbook, queue layout.

- #295 (phase C fix): renamed the system namespace to `verglas_sys` in the
  verbs' docs, SKILL.md, and the ingest-origin recorder — S3 Tables rejects
  `_verglas` (namespaces cannot start with an underscore).

- #295: env.sh is gone; there is one config. The installer merges an [agent]
  section into ~/.verglas/config.toml (pure text-additive: other sections
  byte-identical, backup first, idempotent — unit-tested on a config with
  unrelated sections) and stops emitting env.sh. Hook scripts source nothing:
  the installer bakes the verglas/verglas-mcp binary paths in, the scripts
  self-locate the config file (handed to children via VERGLAS_CONFIG), and
  capture.py reads the [agent] section with a minimal key="value" reader that
  works on any python3 — proven by env-cleared hook tests. MCP registrations
  drop their env blocks (the server reads the config itself). The
  VERGLAS_CONSOLIDATE_BIN kill-switch semantics and the preserve-empty logic
  are deleted; the consolidate trigger runs without the variable. The CLI
  main applies the [agent]-derived environment before the runtime starts.

- #295: removed the `--from junit|lines` CLI surface. Deleted the `FromFormat`
  value-enum and the `--from` flag; `table create`/`append` revert to the
  pre-junit signature (positional source, extension-inferred format). Dropped
  the `ingest_format` mapper; the ingest-origin recorder now labels the format
  from the file extension only. SKILL.md loses the "Track test runs" and
  "Analyze logs" playbooks and the test-runs/logs frontmatter triggers (the
  app-data source playbook and the platform content stay); cli-reference.md
  and verglas.mdc drop their junit/lines mentions.

- #295: wired the agent self-callback pipeline into the CLI. Added `mv create`
  (declare an `error_filter` MV over a log source) and `sink create` (declare a
  `spawn`/`next_turn` callback sink, with delivery and config validated before
  the row lands). `mv run <name>` now runs an `error_filter` MV locally
  (tail the source log, isolate errors into the stream) for any name other than
  `memory_consolidation`, which keeps its existing consolidation path. Added
  `sink run <name>` to fire the sink runtime — a detached investigator under the
  fork-bomb controls, or an inbox notice. SKILL.md gains a headline "instrument
  your own workload and get called back on failure" playbook with the exact
  commands, and the honest-limits section is updated (real streaming log tail,
  two MV transforms, two sink deliveries). session_start.sh now drains the
  next_turn inbox and folds the notices into the injected context.

- dashboard sink (Rill): `sink create --delivery dashboard` is now accepted (the
  validation error lists spawn, next_turn, dashboard). For a dashboard sink the
  `--input` is the target Iceberg table the dashboard follows. `sink run` on a
  dashboard sink takes a new path: it resolves the daemon's S3 and catalog
  endpoints, scaffolds the sink's Rill project under `<spool>/../dashboards/<sink>`,
  and triggers Rill's refresh — a missing `rill` binary is a printed note, not a
  failure. `sink list` already prints the delivery column, so `dashboard` flows
  through unchanged. Help text documents the new delivery with no issue numbers.
- Security fix (/admin/access credential exposure): Updated the `--secret-access-key`
  flag help and the `ConnArgs` resolution note to state that the secret comes
  from the local credentials file (`~/.verglas/credentials/endpoint`), not the
  daemon probe. No user-facing behavior change for the zero-config path against
  an `init`-ed user-scope daemon.

- Removed the `verglas memory` user-facing command entirely. Deleted the
  `Memory(MemoryCommand)` enum variant, `MemoryCommand`, `MemoryMigrateArgs`,
  and `MemorySeedArgs` from `cli.rs`, the dispatch arm in `main.rs`, and
  `commands/memory.rs` (which held the `seed`/`migrate-spool` handlers). Seeding
  stays automatic at install: `skills.rs::launch_seed()` now detaches into a new
  hidden internal subcommand `__seed` (`#[command(hide = true)]`) instead of the
  deleted public verb, preserving all its env wiring and the never-wait,
  log-to-seed.log behavior. The `__seed` handler (`commands/seed.rs`) resolves
  the live store and delegates the orchestration to
  `verglas_agentmem::seed::run_seed`. `verglas --help` no longer lists `memory`,
  `verglas memory ...` is an unknown command, and `__seed` is hidden. Stale
  comments in `skills.rs` that pointed users at `verglas memory seed` were
  updated (seeding is automatic). CLI test asserts `memory`/`seed`/`migrate-spool`
  are unknown commands and `__seed` is absent from help.
- #295: Added `verglas source archive <name>` — appends an `archived` revision
  (fields carried forward) to remove a dead source from the active list while
  keeping its record; restore with `verglas source resume`. `verglas source list`
  now hides archived sources and `verglas source list --archived` shows only
  them (new `--archived` flag on `SourceListArgs`). `source show` still displays
  an archived source. `record_ingest_origin` now records a test-namespace ingest
  as `archived` via `ingest_origin_state`, so a `table create`/`append` into
  `verglas_test.*` no longer pollutes the active source list. Help text is plain
  English with no issue numbers.
- #295: Updated the memory skill asset (`skill_assets/SKILL.md`) to drop the
  `transcript_backfill` source from the reference pipeline description. That
  connector was removed as redundant with seeding; the skill now states that
  existing transcripts are distilled into memories once at install by the
  seeding path, not declared as a source.

- #295: Wire `mv run` to the SQL MV executor. A non-memory MV whose `transform`
  is a SQL declaration runs `verglas_agentmem::sqlmv` (read the input delta, run
  the SQL in DataFusion, append the result via the write path, advance the
  watermark); anything else still falls through to the local error-filter runner.
  `mv create` validates a SQL transform before it lands in the catalog. The
  `--transform` help documents the SQL option.

- #295: Add `verglas source run <name> [--follow]` and `verglas instrument`.
  `source run` builds a connector from a declared source's config and drives it:
  one step by default, or the foreground continuous follow loop with `--follow`
  (Ctrl-C stops it cleanly; a durable watermark under `<spool>/sources/<name>`
  resumes a restart). `instrument <name> --log <path> [--parse] [--dashboard]
  [--callback] [--follow]` declares the whole observability loop over a log file
  and prints how to follow it, or starts following. `log_source_path` now also
  reads `trigger.path`, so the error-filter runner works over instrument's
  config-driven sources. Foreground run of a `rest_poll` (cron) source is a
  clear "not wired yet" error — the cadence engine supports it, but the blocking
  HTTP pump and secret resolver are deferred.
- Control-plane login and the cross-node/cloud deployments view. Added `verglas
  login [--url <url>] [<api-key>]`, which verifies the key against the control
  plane `GET /v1/me`, then stores it at `~/.verglas/credentials/control-plane-token`
  (mode 0600, never printed) and the URL in `~/.verglas/config.toml` under
  `[control_plane]`; a rejected key writes nothing, and the key is read from
  stdin when not passed so it stays out of shell history. Added a
  `controlplane` client module (typed `GET /v1/me` and `/v1/deployments`, bearer
  auth, a clear "run `verglas login`" error when no key is stored) and `verglas
  deployments [--json]`, which prints one table of every source/MV/sink across
  local nodes and the cloud. This is purely additive: the local `source`/`mv`/
  `sink` verbs still run on S3 and catalog parameters with no login, and only the
  two control-plane verbs require the stored key.
- Made the platform primitives (`source`/`mv`/`sink`) the commands and removed
  the generic `deployments` command. Its cross-node/cloud view now lives inside
  the primitives: `source|mv|sink list` fetch the tenant's deployments of that
  kind from the control plane (`GET /v1/deployments`, filtered by kind) and merge
  them with the local self-managed rows into one table with placement
  (local/cloud), node, state, target, and the kind-appropriate field. The merge
  is gated on a stored login, so not-logged-in the primitives stay local-only and
  never call the control plane. `source|mv|sink show` became a code-artifact
  viewer: for a registry artifact it resolves the name to an id and fetches the
  detail record (`GET /v1/deployments/:id`, which carries `code`); a new `--code`
  flag prints just the code. A purely-local artifact shows its declared
  config/transform. Added a `code` field to the control-plane `Deployment` type
  and a `deployment(id)` client method. The `deployments` enum variant, handler,
  and module are deleted; the run/pause/resume/create/register/archive verbs are
  unchanged. Tests: unit tests for the pure merge and code rendering (local-only
  vs local+cloud merged, JSON keying, code-only error), and CLI tests that
  `deployments` is now an unknown command, `source show --code` prints an
  artifact's code from the mock registry, and a not-logged-in `source list` makes
  zero control-plane requests.
- Scoped-lakehouse login: `verglas login` now also fetches the tenant's scoped
  lakehouse config from `GET /v1/lakehouse` after verifying the key, and writes
  the daemon config from it — `[backend]` (S3 endpoint/bucket/region +
  credentials_file), `[catalog]` (uri/warehouse + credentials_file) — plus the
  scoped S3 key pair (AWS-INI) and catalog bearer token to mode-0600 credential
  files. All writes happen only after both control-plane calls succeed, so a bad
  key or unprovisioned tenant leaves nothing on disk; re-login is idempotent.
  Secret values are written only to the 0600 files, never printed. Added a
  `Lakehouse` type + `lakehouse()` client method and backend/catalog credential
  path helpers to controlplane.rs. Tests: unit tests for the section merge and
  the backend/catalog blocks (no inlined secrets), and a CLI test that login
  writes the config sections and the 0600 credential files and prints no secret.
  Also fixed the test mock's `/v1/deployments` to return the `{deployments:[...]}`
  wrapper the client now decodes.

- Remove the config-driven source machinery so the CLI only implements the
  code-artifact model. Delete the `verglas instrument` command (and its module)
  and the `source run` follow-runner (`SourceCommand::Run`, `SourceRunArgs`,
  `drive_source`, and the `verglas-source-runtime` dependency). `verglas table
  create`/`append` no longer registers an `ingest:<table>` `file`-connector
  source in `verglas_sys.sources` (`record_ingest_origin` deleted). The
  `source`/`mv`/`sink` list/show/register/create/pause/resume/archive verbs and
  `mv run`/`sink run` are unchanged. `log_source_path` now reads only the
  top-level `path` field. Tests: dropped the instrument/`source run --follow`
  help tests and moved `instrument` to the removed-commands set; the surviving
  help and merge tests stay green.

- Add a durable cross-run watermark to the TypeScript SDK. New `client.watermark()`
  and `client.setWatermark(w)` methods hit `GET`/`PUT /v1/watermark` on the cloud
  endpoint (relative to the endpoint origin, authenticated by the caller's own
  bearer token, which identifies the deployment — no id in the path). The transport
  now accepts `PUT`. A cloud source worker is a fresh isolate each dispatch, so it
  reads the watermark at run start and writes the advanced value back after a
  successful run, letting consecutive dispatches resume with no overlap. The local
  daemon does not serve this route; only the cloud control plane does. Tests: added
  SDK client tests (null before first set, PUT-then-GET round-trip, bad-token
  rejection) and a `/v1/watermark` cell to the mock endpoint.

- #320: Re-pointed the `table`, `query`, `platform`, and `skills` verbs at the
  extracted `verglas-iceberg` engine crate (catalog, write, inspect, query,
  report, ident). Connection resolution still comes from `verglas-agent`
  (`ConnFlags`/`Connection`), now via `ConnFlags::resolve`.

- #323: Every data-plane verb now speaks the daemon HTTP API through the new
  `daemon` module (DaemonClient): table create/append stream the source file's
  bytes to `POST /v1/tables/{name}/ingest`, list/show/history read the inspect
  routes, query posts to `/v1/query`, and every source/mv/sink registry verb
  calls the `/v1/sources|mvs|sinks` routes. The connection-resolution flags
  (ConnArgs: --catalog-uri/--endpoint/keys/region/token/warehouse) are deleted
  — data verbs need only `--daemon-endpoint`. Daemon down = one clear error
  naming the endpoint and the fix; there is no fallback path and no backend
  traffic. The `mv run`/`sink run` executors keep their engines via
  verglas-agentmem's loopback resolution until Phase 3 moves them into the
  daemon. The skills installer declares the memory pipeline through
  `POST /v1/platform/memory-pipeline`. TDD: the daemon_verbs integration tests
  (mock-daemon wire assertions + the daemon-down test) were written first and
  failed against the embedded-engine verbs before the re-target.

- #329: The `source|mv|sink list` merge view and `show` now carry the unified
  deployment-record fields. `MergedRow` grew `trigger` and `schedule` (rendered
  as TRIGGER/SCHEDULE columns, identical for local and cloud rows); the cloud
  `Deployment` type grew `trigger`; `source register` sends `trigger`/`schedule`
  as first-class fields (a `--schedule` makes it a `cron` trigger) instead of
  burying the schedule in config. TDD: the render/JSON unit tests were extended
  to assert the new columns first.
- #331: Repointed the platform verbs to the new `verglas-platform` crate for the
  verglas_sys registry types; the memory executors still resolve through
  verglas-agentmem.
- #331: `mv run memory_consolidation` calls the consolidation_mv Job (was mvrun).
- #262: the __seed target seeds through the trajectory normalizer (BunNormalizer)
  over a multi-harness SeedSource list (claude-code always; codex when its
  session dir exists), replacing the hand-parsed Claude Code path.
- #262: Replaced per-event capture with session-close normalization. Deleted
  capture.py/capture.sh (the hand-rolled per-event parser). Added the hidden
  `verglas __capture` target: it normalizes a harness transcript via the bun
  trajectory CLI, scrubs, and enqueues, failing open. consolidate.sh now detaches
  capture-then-consolidate at session close (Stop/SessionEnd/PreCompact), gated on
  a transcript_path and the harness->adapter map (claude->claude-code,
  codex->codex; unsupported harnesses skip capture). Dropped the PostToolUse/
  UserPromptSubmit capture hooks from the Claude/Codex/Cursor wiring.

- #258: Added the `graph` verb family, parallel to `table` and daemon-only:
  `create`, `add-node`/`add-edge` (batch JSON from a file or stdin), `neighbors`,
  `k-hop --hops N`, `paths --max-hops N`, `index`, and `show`, plus the shared
  `--predicate`/`--min-confidence`/`--direction` traversal flags. Each verb calls
  the daemon's `/v1/graphs/...` routes and renders a human summary or `--json`;
  the CLI embeds no engine. Integration tests drive the real binary against a
  mock daemon, asserting each verb's wire call and that the traversal request is
  assembled from the flags.

- Memory redesign (per-prompt injection + install-wiring): the installer now
  wires the SECOND injection point and closes the trajectory capture gap.
  - Per-prompt injection: ships `prompt_recall.sh` (a UserPromptSubmit hook that
    reads the prompt, calls `verglas-mcp --recall --query <prompt>`, and injects
    the top memories) and registers it — UserPromptSubmit for Claude/Codex,
    beforeSubmitPrompt for Cursor. Session-start injection (session_start.sh) is
    the first point; both are now installed.
  - Install-wiring: `stage_sdk` stages the trajectory SDK CLI into
    `~/.verglas/sdk` (trajectory-cli.ts + trajectory.ts, pinned from the
    canonical SDK sources, plus a minimal package.json) and runs `bun install`
    so BunNormalizer has a runnable capture normalizer on a fresh install.
    Best-effort like the model prefetch (missing bun / offline degrades, never
    fails install). TDD: stage_sdk_produces_a_runnable_trajectory_cli stages into
    a temp HOME, installs deps, runs the CLI against a two-turn transcript, and
    asserts trajectory-v1 output (leading meta record); skips when bun is absent.
- index registry: Added the top-level `verglas index list` command (and
  IndexCommand), reading the durable index registry via GET /v1/indexes — the
  listing alongside `source|mv|sink list`, showing every declared index across
  tables, graphs, and clusters with its cluster id, state, and reflected
  snapshot (`--json` for the stable shape). Declare/search/per-table listing
  stay under `verglas table index`.

- cloud-agnostic sweep: removed every Cloudflare/R2 mention and tenant-named
  fixture from code, docs, and tests. Comments now describe the constraint
  ("strict S3-compatible stores reject variable-size parts", "some managed REST
  catalogs gzip responses") instead of naming a vendor; test fixtures use
  neutral hosts/entities (storage.example.com, acme, blobstore). No behavior
  change — the daemon and SDK are wiring-agnostic over any S3 bucket + Iceberg
  REST catalog.

- windows release build (cfg audit): Made the CLI compile on Windows for the
  cargo-dist release. `manager()` previously `compile_error!`d on any
  non-macOS/Linux target; it now returns a stub service backend
  (`service::unsupported::UnsupportedManager`) whose install/start/stop return a
  clear "managed-service install is not supported on this platform yet; run the
  daemon in the foreground: `verglasd --config <path>`" error (new
  `ServiceError::Unsupported`). The daemon itself is cross-platform; only the
  launchd/systemd install is absent on Windows. Every other Unix-only site in
  the CLI (0600 credential writes, exec perms, SIGINT/TERM/HUP watcher in
  `dev`) was already `#[cfg(unix)]`-gated with a portable fallback.

- Browser and device OAuth login. `verglas login` now has three modes. With no
  flag it runs the browser authorization-code flow with PKCE(S256): it binds a
  loopback listener, opens the browser to `/oauth/authorize`, receives the
  redirect on `127.0.0.1`, checks the `state`, exchanges the code at
  `/oauth/token`, and provisions. `--device` runs the headless device-code flow:
  it prints a user code and verification URI, then polls `/oauth/token` at the
  server's interval (backing off on `slow_down`) until authorized. `--api-key`
  keeps today's key flow verbatim (positional key or stdin) for CI; the
  positional key is only read in that mode. All three converge on
  `POST /v1/provision`, which returns the long-lived api key and the scoped
  lakehouse config; the CLI then writes the same token/backend/catalog/config
  files as before, only after provisioning succeeds. No secret (access token,
  api key, S3 secret, catalog token) is ever printed. Added POST/form support
  and the OAuth methods to the control-plane client (`anonymous`,
  `authorize_url`, `exchange_code`, `device_code`, `poll_device_token`,
  `provision`) with new error variants for the OAuth states. A test-only
  `VERGLAS_LOGIN_NO_BROWSER` env var skips the OS browser open so the end-to-end
  tests can play the browser over the loopback; its only effect is skipping that
  one call. TDD: the browser and device end-to-end tests (mock OAuth endpoints,
  loopback callback, poll counter) were written first and shown red before the
  implementation.

- verglas status shows this node's control-plane state: When the machine is
  logged in, `verglas status` now adds a line reporting whether the control
  plane currently lists THIS node as active (resolving the node id through the
  daemon's own `Config::resolve_cluster_id`, so the CLI and daemon agree). Added
  a `nodes()` method + `Node` type to the control-plane client (`GET /v1/nodes`).
  Best-effort: not logged in, an unreadable config, or an unreachable control
  plane leaves the output unchanged (no error, no extra line); the JSON form
  gains a `node` object only when the lookup succeeds. TDD: a unit test pins the
  pure `render_node_cp_status` formatter across active / registered-stale /
  not-registered.
- #60: Added `verglas tables` — reads the daemon's `/v1/metering/tables` report
  and prints one line per watched table (hit rate, cached bytes, requests
  avoided) plus a dollar-savings ESTIMATE. The estimate is presentation-only:
  `requests_avoided` times a fixed published S3 GET rate, computed at display
  time (no config knob, no stored dollars), so a rate change re-prices it. Unit
  tests pin the estimate arithmetic and the presentation-layer property.
- #319 (thin client / pure-client backend resolution): Made explicit that the
  CLI is a PURE CLIENT — never required to be co-located with a daemon. Added
  `src/backend.rs`, the one place that resolves which backend a command targets:
  `backend::daemon(endpoint)` for the data-plane/registry verbs (table, query,
  graph, source/mv/sink CRUD, index, tables) and `backend::control_plane()` for
  the login-gated cloud enrichment. Every command's client construction now
  routes through it (previously each `commands/*.rs` called `DaemonClient::new`
  / `ControlPlaneClient::from_stored` inline). The daemon endpoint may be
  localhost (default) OR a remote daemon: a `data_sidecar` host can `sink create`
  / `source register` / `mv create` against a remote daemon with
  `--daemon-endpoint <url>` and never run one locally — the declaration
  registers in THAT daemon's registry. Rewrote the daemon-unreachable error: it
  no longer says "the daemon is the only local write authority, so data verbs
  need it running" (which wrongly implied a LOCAL daemon is required) and instead
  names the endpoint and tells the operator to point at a running daemon — which
  may be remote — with `--daemon-endpoint`/`VERGLAS_ENDPOINT`, or start a local
  one; a local/edge daemon is for cache/read latency, never a requirement. The
  bounded connect timeout (already present) makes an unreachable endpoint fail
  fast, not hang. TDD: `tests/thin_client.rs` — a mock remote daemon proves the
  three declares land in it with no local daemon (shown green: the capability
  already existed via the global endpoint), and the unreachable-endpoint error
  test was shown red against the old message, then green. No daemon or
  control-plane SERVER change. Placement note: which daemon owns a declaration is
  the endpoint; a control-plane-owned ("cloud") declare would need a
  `POST /v1/deployments` route that does not exist in this repo (the control
  plane lives in verglas-cloud) — flagged, not implemented, per the no-server-
  change constraint. Thinning note: the binary still links the full vendored
  Iceberg/DataFusion engine via `verglas-sdk` (wire types), `verglas-memory-jobs`
  (the mv-run/sink-run/seed/capture executors), and `verglas-platform`; those
  executors are deferred to epic #319 Phase 3/4, so no dep could be dropped
  without breaking `mv run`/`sink run`/`__seed`/`__capture` — reported for the
  phase that moves them into the daemon.
- `verglas table compact` (PR #378): a one-shot manual compaction command that
  POSTs `/admin/compact` and renders the pass report (tables scanned, per-table
  files rewritten, snapshot committed) as JSON or a human summary. The daemon runs
  no compaction on a schedule; this is the manual trigger.
- `verglas table compact` output reflects partial passes (PR #378). The human
  summary and JSON now show groups committed, undersized files remaining, and a
  budget-bounded note (run again to continue), matching the executor's ratcheting.
- feat/cli-secrets: Add the `verglas secrets` control-plane resource group
  (list, set, delete) alongside workers/containers/tables/db. `list` shows names
  only (values are never returned by the control plane); `set` takes the value
  from --value, --file, or stdin (the default, so it stays out of shell history),
  refuses an empty value, and never echoes it; `delete` removes one by name. A
  deployment references a secret by name as `@secret:NAME` in its config and the
  value is sealed to the box at dispatch. TDD: `tests/cloud.rs` mock secrets
  routes; set tests prove the value crossed the wire yet is never printed.
- workers refocus: Removed the source/mv/sink command groups (commands/platform.rs),
  the memory __capture/__seed internal verbs, and the memory+hooks+MCP installer
  (commands/skills.rs + skill_assets). The CLI no longer depends on
  verglas-memory-jobs or verglas-platform. Memory moved out of the daemon into a
  separate container track; the data verbs (table/tables/graph/query/index/workers)
  are unchanged.

- catalog-aware containers: `verglas containers` gained `catalog` (list the curated
  apps with UI/MCP/default flags), `deploy <catalog-id>` (idempotent catalog deploy
  that prints the container id, UI hostname, and MCP endpoint), and `config
  <container-id>` (show the config schema + current mode/values, or `--set KEY=VALUE`
  / `--mode` to write; secrets read from stdin via `KEY=-`, warned on inline, never
  echoed). Added `ControlPlaneClient::put_value`. The commands speak the vgk_
  control-plane routes so the CLI never rides a browser session.
- skills: rebuilt `verglas skills install` on the tenant MCP endpoint (memory
  moved out of the daemon into a per-tenant cognee MCP container). The installer
  writes three shared lifecycle hooks under `~/.verglas/agent/hooks/`
  (session_start -> `session_context`, prompt_recall -> `recall`, consolidate ->
  `remember`), resolves the memory MCP ingress + endpoint bearer from the control
  plane's `GET /v1/mcp` discovery route (so the CLI never hardcodes the volatile
  memory-container name), writes them to `~/.verglas/credentials/`
  (`mcp-endpoint` 0644, `mcp-bearer` 0600), and wires the per-harness skill +
  hooks (Claude settings.json, Codex/Cursor hooks.json) additively with backups.
  Hooks are curl+python3 only, fail-open (exit 0, empty) on any error, and follow
  cognee's API-mode remember/recall pattern. Added `skill_assets/` (SKILL.md + the
  three hooks) and `tests/skills.rs` (asset install + mock-server hook behavior +
  fail-open). Re-added `skills` to the CLI's surviving-command set.

- volumes: Added the `verglas volumes` cloud resource group (list/get/create/
  resize/delete) mirroring the `db` group's control-plane UX — human table +
  `--json`, honest "not supported yet" mapping. `create`/`resize` take a `--size`
  that accepts a byte count or a suffixed size (10GiB, 500MB); a shrink is refused
  before the request. Added a size parser + human-size renderer with unit tests and
  end-to-end tests against the mock control plane in tests/cloud.rs.
- Typed databases: `verglas db create <name> --type postgres|mysql|clickhouse`.
  Added a `--type` flag (default postgres) that passes the engine through to the
  control plane as `type`; the CP validates it and returns the engine's connection
  endpoint (postgres :5432 / mysql :3306 / clickhouse :8123). `db list` now shows
  the engine as a column (name/type/state/created_at). Create prints the one-time
  connection credentials (now nested under `connection`) with the shown-once
  warning. Tests in tests/cloud.rs cover the type crossing the wire, the list
  column, and delete-by-name.
- Per-database deployments: every `verglas db` database is its own serverless
  deployment (own VM, own storage, scale-to-zero). `db list` now shows each db's
  own compute state as a `compute` column (name/type/state/compute/created_at);
  connection hosts are per-database (`<db>-<slug>.<engine base>`). Reworded help
  text: containers and databases are independent deployments (no stack concept).
  A free tenant over its metered db-compute allowance gets a plain 402 upgrade
  message from the control plane, surfaced verbatim.
- CLI table/index cleanup: removed the `verglas tables` (plural) group, which
  duplicated `verglas table`. Its unique per-table cache-metrics view moved to
  `verglas table metrics` (module renamed `tables.rs` -> `table_metrics.rs`); the
  cloud registry `tables list`/`tables get` were dropped as duplicates of
  `table list`/`table show`. Removed the `index` subcommand from `verglas table`;
  indexes now live only under the top-level `verglas index` group, which gained
  `add` and `search` (moved from `table index`) and a `list [table]` that lists
  either the whole registry or one table's indexes. Added `verglas table delete
  <namespace.table>`: it drops the table through the tenant's Iceberg REST catalog
  (`DELETE /v1/{prefix}/namespaces/{ns}/tables/{table}`), reading the `[catalog]`
  uri and bearer from `~/.verglas/config.toml` and resolving the route prefix from
  `/v1/config`; it requires `--yes` or an interactive `y` confirmation and prints
  exactly what will be dropped. New `commands/catalog.rs` holds the direct catalog
  client (the only table verb that does not go through the daemon).
- PR #378 merge resolution: Updated stale CLI help to describe containers and
  workers as the deployment model. No source, MV, or sink command surface is
  reintroduced by compaction.
- #281: `verglas table compact`'s human summary now shows old snapshots expired
  and orphaned files reclaimed, at both the pass total and per-table lines, when
  either is nonzero — otherwise unchanged from before. Reflects the compaction
  engine's new snapshot-expiration mechanism; no new flag, the fields ride the
  existing `CompactionReport`/`CompactReport` JSON.
- chore: Depend on verglas-sdk via the workspace path after it moved to sdks/rust.
- chore: Require the unified worker spec for cloud create (no legacy JSON passthrough). Drop dead /v1/sources mock routes from daemon_verbs.
- #393: Switched from in-tree `vendor/iceberg` to the pinned `verglas-org/iceberg-rust` fork (`verglas/v0.9.1` @ a40f9268) for `TableCommit::from_parts`. Same patch, maintained out of tree; drop when upstream exposes overwrite/replace commits.
- #1 (verglas-org/verglas): Removed the fleet `verglas-compact` one-shot binary and its workspace/dist membership. Compaction stays in `verglas-iceberg` + daemon `POST /admin/compact` / `verglas table compact` until the async maintenance API lands; e2e retargeted to that path.
- chore: Point install/docs/release at `verglas-org/verglas` (drop cascade-labs URLs and the external releases repo). macOS launchd label is now `org.verglas.verglas`.
- #3: Routed file ingestion through `verglas-write` and moved list/show/history metadata calls directly to Iceberg REST.
- chore: Remove durable agent memory from the CLI — delete `verglas skills` (cognee MCP hooks/skill assets), drop mcp/consolidate from install docs, and stop applying a memory `[agent]` env at startup.
- chore: Remove OS service daemonization (`init`/`start`/`stop`/`restart`/`logs`, launchd/systemd). Self-host is Docker; `verglas status` probes admin HTTP only. README quickstart is login@verglas.dev or compose + `VERGLAS_ENDPOINT`.
- chore: `verglas login` defaults to Verglas Cloud (`https://api.verglas.dev`); `--url` is only an override.
- chore: Remove `verglas dev` from the shipped CLI because the client release no longer bundles the sibling daemon binary it launched. Self-hosted daemons run in Docker; repository benchmarks remain separate tooling.
- #263: Corrected query command documentation to describe the daemon's private upstream catalog client instead of a loopback catalog service. Query data still travels through the Verglas S3 endpoint.
- #91: Made `verglas index list` explicitly table-scoped and removed the global
  `verglas_sys.indexes` path. Search now requires an exact-snapshot Vamana
  attachment instead of silently scanning the embedding column.
- #91: Renamed the local process and CLI terminology from `verglasd` and
  daemon to `verglas-server` and server. The endpoint flag is now
  `--server-endpoint`; no compatibility alias remains.
- #3: Corrected the execution-boundary tests after rebasing the isolated roles:
  table metadata reads now exercise the customer Iceberg REST catalog directly,
  while create, append, query, graph, and maintenance requests exercise Verglas.
- #11: Added `webhook` and `data_change` to the portable worker manifest and projected them into both the local registry and cloud trigger configuration. Operators can now create every bounded scheduler trigger through `verglas workers create --file` instead of hand-writing REST payloads.
- #11: Replaced the portable data-change manifest with a generic CloudEvent subscription over exact type and optional source and subject. Local and cloud projections now carry the same event filter contract.
- #11: Let one portable worker declaration contain cron, HTTP, and CloudEvent triggers while keeping manual dispatch implicit. Worker files can now be bundled with relative `@file:` references, and the CLI rejects the removed singular-trigger manifest instead of preserving a compatibility path.
- #16: Added `verglas dashboard create`, `list`, `show`, and `delete` as pure-client commands against the on-prem REST API. The commands print the generated Rill Explore URL and never access Rill or its project files directly.
- #18: Reorganized the worker example around a reusable `market-data-ingest` definition and kept SPY only as an input symbol. The manifest tests now use the same neutral worker and callback names shown throughout the rewritten trigger guides.
- #18: Added documentation regression tests that keep the complete Compose contract and every displayed worker file synchronized with the runnable repository examples. CLI help now points dashboard users to the Compose analytics profile instead of the removed server TOML configuration.
- #29: Added direct `verglas kv set` and `verglas kv get` commands against the server's built-in KV engine. Set accepts an optional TTL and both commands use the existing endpoint and bearer-token environment without KV configuration.
- #55: Dropped the bundled Linear Vessel example from CLI tests; the vessel commands keep a generic demo fixture.
- #66: Removed Verglas Cloud login/control-plane CLI (containers/db/volumes/secrets/login) and retargeted workers to the local server registry.
- #66: Neutralized CLI tests that asserted removed control-plane verbs (#66); renamed catalog_delete token fixture away from cloud-catalog.token.
- #84: Added local `db create` commands for managed Lakehouse, managed Neon Postgres, customer S3, and external Iceberg REST compositions. Added singular `secret create` with typed URI scopes and secret material accepted only through a hidden terminal prompt or stdin; both commands are thin clients of the local resource APIs and reject ambiguous compositions before sending a request.
- #84: Routed database and secret commands to `VERGLAS_ACCESS_ENDPOINT` (localhost port 8345 by default), keeping credential and database administration off the cache server's admin API.
- #84: Updated the runnable Compose contract test for dynamic databases: Lakekeeper, the three-member cache/safekeeper ring, and the container runtime are now mandatory services rather than singleton catalog environment values.

- #access-tokens: Added `verglas token create`, `list`, and `revoke` backed by
  the access service. Minted bearer values are stored in an owner-only local
  credentials file, and the CLI forwards the resolved scoped token to every
  data-plane, runtime, and administrative request instead of using a separate
  runtime-wide credential.

- #database-tokens: Added `verglas db token <database>` for short-lived
  Postgres connection credentials. It stores the returned Neon password token
  in the owner-only credential file by default and prints it only when the
  caller explicitly requests `--print-password` for `PGPASSWORD`.
- #84: Made `verglas query` require its target database and post SQL only to
  `/v1/databases/{database}/query`. Its existing JSON report output and
  `--at` snapshot or timestamp selection are unchanged.
- Changed `verglas token create` to mint the combined `verglas-cli` audience by default, matching the OS Developer access token contract.
- #97: Updated access-only CLI client construction after the Rust SDK removed
  process-wide catalog coordinates. Token and database administration remain
  scoped to the standalone access service and do not select a data database.
