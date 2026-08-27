# Worklog

- #171: Added dedicated JavaScript and Python cold-chain Worker examples plus a
  production-only harness that builds the Stream, Pipeline, Sink, and Catalog
  artifacts into one aggregate gateway manifest. The harness preserves one data
  root through a full gateway/celld restart and refuses to claim success without
  explicit remote Turso credentials and the production runtime binaries. The
  aggregate route uses Stream object `events` to match Pipeline's baked source,
  and does not add the products' internal `*_DO` bindings as global routes.
  App status uses the same `SINK_A` identity as Pipeline delivery, and the
  published payload is scalar so the stock `SELECT *` Iceberg inference stays
  within its supported flat-row surface.
- #171: Switched the Python cold-chain manifest's Pipeline, Sink, and Catalog
  entries from fake Durable Object namespaces to strict direct service bindings;
  the Worker now calls each configured service with `fetch` while preserving its
  `COUNTER` Durable Object and `STREAM` Pipeline binding.
- #171: Required the operator-owned runtime host configuration for production
  cold-restart runs and pass it through celld only to Catalog children. Build-only
  verification remains credential-free, while a real run now fails before launch
  unless Turso, origin/cache, and runtime binary prerequisites are all explicit.
- #171: Corrected the cold-restart harness for the self-hosted deployment contract.
  It now requires only generic S3-compatible bucket configuration, stages run-specific
  Sink and Catalog variables, generates private host credentials without logging them,
  and relies on each runtime child's embedded Turso database across process restart.
- #171: The first factual run reproduced a Durable Object launch failure because
  the macOS temporary directory made celld's object socket exceed `SUN_LEN`.
  The harness now stages under a short configurable `/tmp` root so production
  Unix socket paths remain valid without changing the runtime protocol.
- #171: The next factual run crossed Counter, Stream, Pipeline, and Sink before
  exposing inconsistent stock Sink fences: Pipeline named `primary_sink` while
  Sink, Catalog, and the host capability named `primary`. Aligned all staged and
  baked identities so one deterministic Sink name reaches SQLite publication.
- #171: JavaScript then completed a factual cold restart, while Python trapped in
  CPython's first monotonic-clock call because its builder emitted trapping WASI
  stubs. Python components now import the runtime's locked-down Preview 2 clocks
  without receiving filesystem preopens, network access, or storage credentials.
- #171: Allowing full WASI clocks then proved componentized Python also attempted
  filesystem startup and was denied by the runtime. The final adapter keeps WASI
  trapping stubs and drives its immediate WIT-backed coroutines without asyncio,
  preserving the tenant filesystem/network prohibition while avoiding clocks.
- #171: Completed separate factual JavaScript and Python cold-restart proofs and
  independently listed each warehouse's Parquet, metadata JSON, manifest list,
  and manifest in the S3 fixture. Corrected the harness summary to name only the
  language or languages actually requested instead of always claiming both.
- #171: Added a persistent compiled-component cache directory to every factual
  artifact descriptor. Initial activation still compiles real WASM, while the
  required process restart reuses verified cwasm bytes instead of recompiling
  all six products for another ten minutes.
- #171: Updated the factual harness to launch `verglasd`, use
  `--verglasd-control`, and keep its private socket and logs under that name.
  No old executable or argument alias is accepted.
## #0 — 2026-08-26

- Added a scheduled entry point to the JavaScript cold-chain acceptance Worker.
- Each cron invocation performs the same durable increment and Stream publish as `POST /incr`, giving staging verification an observable end-to-end result.
- The JavaScript cold-chain Worker now acknowledges `/process` after durable Pipeline enqueue instead of waiting for Stream, Sink, and Catalog. The acceptance harness polls independent stage status before asserting progress and restart idempotency.
- #0: Run cold-chain component builds through the published JavaScript and Python SDK commands. The example no longer depends on SDK source trees or a private Python environment inside the runtime repository.
