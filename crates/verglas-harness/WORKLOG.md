# verglas-harness worklog

- #324: New crate. The shared runtime the source/mv/sink harnesses build on.
  `runlog` promotes the `<name>_LOGS` run logger out of
  `verglas-agentmem::pipelog` (generic over pipeline/kind, no memory-specific
  observability). `commit` holds the idempotent keyed batch commit (the key
  rides on the snapshot summary, matching the tables_api property names) and the
  `WatermarkStore` trait plus an in-memory store. `transport` is the connector
  transport: one async `SourceStream` with an in-process (native `Connector`)
  and a subprocess (framed stdio protocol) implementation. `cron` is a
  Vixie-semantics matcher for the source harness's cron trigger, including the
  day-of-month/day-of-week OR rule. Decision recorded in lib.rs: a dedicated
  harness-support crate rather than folding into verglas-sdk, so the SDK keeps
  no engine/catalog dependency.

- #325: `guard` module added — the runaway-worker controls (SessionLock,
  SlotLock + pending markers, RetryPolicy backoff, CHILD_MARKER_ENV/is_child)
  lifted verbatim out of verglas-agentmem into shared harness policy, with parity
  unit tests. `runlog` gains a `synthesize` step event so the memory MV keeps its
  per-transform log row. `transport` gains a `transform` method and the MV
  data-carrying control/reply frames for the subprocess MV path.

- #326: `transport` gains a `deliver` method for sink Jobs (hand the child a
  committed batch, await its Delivered ack).

- #327: `queue` module added — the platform queue output type. The per-source
  segment log (durable JSONL segments + consumer-group watermarks) promoted out
  of verglas-agentmem::stream and made generic over a `QueuePayload`. At-least-
  once with consumer-side idempotency, documented in the module. Its tests moved
  here (tests/queue.rs) over a generic payload; agentmem no longer owns them.

- cloud-agnostic sweep: removed every Cloudflare/R2 mention and tenant-named
  fixture from code, docs, and tests. Comments now describe the constraint
  ("strict S3-compatible stores reject variable-size parts", "some managed REST
  catalogs gzip responses") instead of naming a vendor; test fixtures use
  neutral hosts/entities (storage.example.com, acme, blobstore). No behavior
  change — the daemon and SDK are wiring-agnostic over any S3 bucket + Iceberg
  REST catalog.

- windows release build (cfg audit): `pid_alive()` used `libc::kill(pid, 0)` for
  its liveness probe, which is unavailable on Windows and blocked the release
  build. It is now `#[cfg(unix)]`; the `#[cfg(not(unix))]` version errs
  conservative and reports the holder alive (a lock is never stolen from a pid
  that may still be running), at the cost of not auto-reclaiming a crashed
  holder's lock on those platforms. Part of the cargo-dist release cfg audit.

- fleet: add a durable `S3WatermarkStore` (and `S3WatermarkConfig`) next to the
  in-memory one. It keeps each deployment's watermark as a small object in the
  warehouse bucket over the same S3/R2 keypair the catalog FileIO uses
  (`<prefix>/<deployment>.json`), so a fleet cron run resumes from the stored
  cursor instead of re-bootstrapping. `get` maps a NotFound object to `None`;
  `set` overwrites. Round-trip and isolation are unit-tested over an in-memory
  OpenDAL operator via the `from_operator` constructor.
- workers: Added the `worker` executor (run_worker) — runs one worker as a
  subprocess, env-in / result-file-out, mirroring the TS endpoint-run harness,
  with run logging to <name>_LOGS (new KIND_WORKER). Moved the guard policy
  (run_guarded/Guarded/Skipped) here as `policy` so the one shared home outlives
  the Source/Sink/MV crates being deleted.
- workers refocus: Updated the crate doc to the worker model (worker executor +
  policy + cron cursor); the framed-stdio transport is retained only for the
  protocol round-trip tests now that Source/Sink/MV are gone.
- chore: Delete the framed-stdio connector transport and the retired KIND_SOURCE/MV/SINK runlog constants. Worker exec accepts the unified exec array only.
- #393: Removed platform `_LOGS` run logging and day-partition retention from Verglas. Catalog-side lakekeeping owns telemetry write/TTL; harness no longer writes `verglas_logs.<name>_LOGS`.
- #91: Renamed harness process documentation and error guidance from daemon to
  server. Worker execution contracts otherwise remain unchanged.
- #11: Passed the complete serialized trigger event into every worker subprocess. HTTP callbacks and data updates now cross the harness boundary without losing their payload, while malformed events fail the run.
- #11: Reduced the subprocess event contract to one validated `VERGLAS_CLOUD_EVENT` binding. Removed the trigger discriminator and cron-specific environment variables so workers cannot consume two competing event formats.
- #11: Added declared environment variables to the subprocess execution contract. The scheduler can now run a self-contained worker bundle with its configured endpoints and arguments instead of relying on host-global state.
- chore: Deleted WatermarkStore (memory + S3), its tests, and the OpenDAL
  dependency. The harness commit path is keyed idempotent appends only; workers
  have no cross-run watermark cell.
- #66: Removed cloud queue-backing and cloud-lakehouse dual-plane docs from queue, follow, and worker harness comments.
