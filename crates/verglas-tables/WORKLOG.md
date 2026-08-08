# verglas-tables worklog

Append-only log of changes to this crate, by issue. Every PR touching this
crate adds an entry (see /AGENTS.md, "Worklog discipline").

- #1: Scaffolded as part of the initial cargo workspace: stub with module-level
  docs, placeholder types wiring real dependency edges, and an integration
  test directory. Toolchain pinned (1.96.1), workspace clippy lints applied.
- #47: Added the catalog watcher: a `CatalogWatcher` trait (last-known table
  enumeration/state/lineage plus a tokio broadcast subscription for
  `TableChanged` events), a generic `PollingWatcher` loop (interval + jitter,
  exponential backoff on outages with last-known state preserved, bounded
  snapshot lineage, include/exclude filters applied before any load-table
  call), and a minimal direct-REST `CatalogSource` (config/namespaces/tables/
  load-table via reqwest, lenient pointer-only parsing) — iceberg-rust was
  deliberately not used for polling; see the module docs in `catalog/rest.rs`.
  The `CatalogSource` trait is what the Glue watcher (#48) implements to reuse
  the whole loop. Tests were written first against a mock axum REST catalog
  and observed failing before implementation.
- #49: Added the logical key mapper — the keystone of Iceberg-awareness. New
  modules: `fetch` (the `MetadataFetch` IO trait with an offline `object_store`
  impl and a through-cache `ObjectRead` adapter for #50), `iceberg`
  (metadata.json + manifest-list + manifest parsing to `SnapshotPlan`/
  `DataFileEntry` via apache-avro directly — see the iceberg-rust-vs-avro note),
  `parquet_meta` (footer → `ChunkSpan`s, memoized by `(path, etag)`), and
  `mapper` (`MapperState`/`TableIndex`/`FileMeta` behind an `ArcSwap`, the
  lock-free allocation-free `classify()`, a clone-on-write incremental builder
  that shares unchanged `Arc<FileMeta>` across commits, and a single-writer
  `MapUpdater` driven by #47's watcher events). Avro/ORC data files are
  first-class: format comes from the manifest, column-chunk resolution is
  Parquet-only, `classify()` returns `chunk: None` for row-oriented files and
  never invokes the footer path for them. Criterion benches cover classify()
  at 1k/100k files and the 10k-file commit. Tests derive from the acceptance
  criteria (golden classify across the fixture matrix, a counting-allocator
  no-alloc guard, and a concurrent-swap consistency/time-travel test).
- #49 (review follow-up): validated the parser against REAL pyiceberg-written
  metadata, closing the circularity of hand-built fixtures. Checked in a small
  Iceberg v2 table generated offline by pyiceberg 0.8.1
  (`tests/fixtures/pyiceberg-v2/`: partitioned, two append snapshots, full
  embedded Avro schemas, field-id partition records, int status, sequence
  numbers, real `s3://` URIs via an s3-scheme-to-local-dir shim in
  `regenerate.py`). `tests/real_metadata.rs` walks it through the offline
  `MetadataFetch`, asserts classify() on the real data-file paths and
  range→chunk on the real footers, and proves the assertions are live by
  corrupting a copy (manifest re-encoded without `file_format`; metadata.json
  without `location`) and asserting the walk fails naming the field. No parser
  fixes were needed — the lenient by-name reader handled the real metadata
  (partition records via the record path, int status via the long/int
  unwrapper) on the first run.

- #168: Added `warming::budget::TokenBucket`, a reusable byte-rate token
  bucket. Warming (#168) and the snapshot prefetcher (#51) both flood the
  backend with speculative fills; this bounds the byte rate they may pull so
  they stay polite tenants. Boring by design: tokens (bytes) refill at a steady
  rate up to a burst capacity, `acquire` waits for the deficit to refill.

- #168: Added the eager warming pipeline (`warming` module). `Warmer::warm_table`
  walks a watched snapshot through the cache — reading metadata.json, the
  manifest list, and manifests as whole-object `Full` reads (pinning the block
  entries a planning engine hits) and every Parquet footer via a speculative
  suffix read (≤2 GETs: one 64 KiB read, a second only if the footer is larger).
  Non-Parquet files are never suffix-read (Avro rule). Concurrency is capped by a
  semaphore and the byte rate by the shared TokenBucket; a table whose footer
  footprint exceeds the metadata budget alerts and caps instead of thrashing.
  `WarmProgress` exposes counters for the admin API. Made `iceberg::parse_manifest`
  / `parse_manifest_list` and a new `parquet_meta::parse_footer_from_tail` public
  so warming parses block metadata it read as `Full` reads without re-fetching.

- #168: Added `WarmingCoordinator`, which binds the `Warmer` to a
  `CatalogWatcher`: it warms every watched table on startup and re-warms a
  table's new pointer whenever a commit fires a `TableChanged` (a lagged
  subscription triggers a full resync). Reworked the `Warmer` to read through a
  boxed `WarmSource` trait (blanket-impl'd for any `ObjectRead`) and to run
  footer warms as owned-Arc futures, so the whole job is `Send` and can be
  spawned on a background task for a generic cache reader.
- #168: fixed startup warming of pre-existing tables (found live by the
  benchmarks/warming demo). The coordinator's initial `warm_all` raced the
  watcher's first poll: it enumerated a still-empty watched set, and the
  seeding poll deliberately emits no events, so a table that existed before
  the daemon started was never warmed until its next commit. `CatalogWatcher`
  now exposes a `seeded()` watch signal that `PollingWatcher` flips after its
  first successful poll, and the coordinator awaits it before the startup
  pass — race-free regardless of poll latency, with the silent-seed event
  semantics unchanged for other consumers.
 #143: updated the test's `ObjectMeta` literals to spread `..Default::default()`
  now that the read metadata carries the extra system-header / user-metadata
  fields; no behaviour change (the synthetic reader sets no metadata).

- #51: Added the `lifecycle` module — snapshot-driven prefetch and orphan
  retirement. `diff.rs` classifies a commit from the snapshot `summary.operation`
  (`replace` = compaction) and diffs added/removed files from the *changed
  manifests only* (those whose manifest-list `added_snapshot_id` equals the new
  snapshot), reading manifest-entry statuses rather than whole file listings.
  `heat.rs` is the heat ledger: a sharded, bounded, EWMA-over-epochs map keyed on
  the rewrite-stable logical identity `(table, partition, column)`, fed from the
  request path by a `HeatFeed` `ObjectRead` decorator through a drop-on-full
  channel and folded by a background aggregator that runs the mapper's lock-free
  `classify()` off the hot path (Avro/whole-file accesses key on the
  `ColumnId::WHOLE_FILE` sentinel). `prefetch.rs` plans a compaction-repair
  (footers of added files, then their hot column chunks scored against the
  ledger, then hot Avro whole files) and drains it through one shared, budgeted
  four-level priority executor (organic > compaction-repair > onboarding >
  prewarm) that shares warming's `TokenBucket` and yields to organic traffic via
  a small starvable semaphore. `retire.rs` computes the demotion schedule and
  grace window from table properties (`history.expire.*`, default 7 days).
  Extended the iceberg parser with `parse_manifest_list_refs` /
  `parse_manifest_entries` and carried `operation` / `parent_snapshot_id` on
  `SnapshotRef`; added the `ColumnId::WHOLE_FILE` sentinel.
- #198: Wired the retirement schedule to the cache engine's evict-first demotion.
  The prefetch coordinator now holds an optional `Arc<dyn BlockDemoter>` sink: on
  a compaction commit it demotes the removed files in the engine (their cached
  blocks become unreachable misses, so they age out before live data), and a new
  `sweep_expired` drains the scheduler's expired demotions and hard-evicts them
  once the grace window closes. The spawned loop runs the sweep on a 5-minute
  timer. Time-travel reads within grace still resolve (the mapper retains old
  snapshots) and serve correct bytes, just cold. Added verglas-cache as a
  (non-dev) dependency for the `BlockDemoter` trait.
- #226: reverted to single-bucket serving; deleted the #132 per-bucket registry; backend.bucket is now required and gates serving. Multi-bucket is deferred to #226.
- #46: updated the test fetch reader to construct `ObjectGet` with the new
  `served_from` tier cell. No behaviour change.
- #231: `RestCatalogSource::from_config` now resolves the catalog bearer token
  via `Catalog::resolve_bearer_token`, so the token can come from a 0600 file
  (`catalog.credentials_file`) instead of an inline value. It returns a Result
  because reading the file or a both-sources conflict can fail.
- #235: TableFilter now matches via the shared `verglas_core::glob` matcher
  instead of a private copy, so a bucket glob and a table-name glob interpret `*`
  identically. No behavior change to filtering.
- #236: The REST catalog client now signs requests with AWS SigV4 when the
  catalog is configured for it (S3 Tables / Glue), resolving credentials from a
  named AWS-INI file or the ambient chain and signing each GET directly with
  aws-sigv4 (no proxy). The /v1/config warehouse-prefix bootstrap is signed too.
  The bearer path is unchanged when SigV4 is off.
- #236: Stopped recursive `parent` namespace requests for the `s3tables` SigV4
  service because AWS S3 Tables supports one namespace level and rejects those
  requests with HTTP 400. Other Iceberg REST catalogs keep recursive namespace
  discovery.
- #263: Shared complete successful Iceberg REST GET responses between the catalog watcher and a byte-bounded loopback gateway. Catalog mutations remain authenticated write-through operations and clear cached responses so on-demand ingestion observes its commits, while transient provider failures leave the last validated responses available.

- #305: Retirement is now durable and physical. The scheduler keeps each
  demotion's ETag/size/generation (never downgrading a known ETag), persists
  the schedule plus per-table replay watermarks to retire-state.json
  (atomic rename, lenient load), and the coordinator: replays every commit
  since the watermark on each catalog event (commits made while the daemon
  was down are diffed and retired too), retires the superseded metadata.json
  and expired snapshots' manifest lists, restores state at startup
  (re-demoting in the engine — no restart amnesty), and hard-evicts
  physically via the engine's new receipt API. Prefetch plans submit only
  for the newest commit, so a replay cannot storm the executor.

- #307/#310: The loopback catalog gateway now decompresses gzip upstream
  responses and forwards plain JSON. Cloudflare R2 Data Catalog returns
  `Content-Encoding: gzip`; reqwest is built without the gzip feature, so the
  proxy was passing raw gzip bytes to clients that expect JSON (the CLI and the
  sidecar iceberg-rust client), which rejected them and fell back to the vendor
  origin. `send_upstream` now strips the client Accept-Encoding, requests gzip
  itself, and decodes the body per request (fresh MultiGzDecoder, no shared
  decode state) while removing the Content-Encoding header. Identity responses
  pass through unchanged. This also resolves the concurrent-garble race, since
  no decode buffer is shared across requests.

- #95: Test-only. Added the new `cache.shadow_capacity_bytes` field to the two
  fully-enumerated `Cache` config literals in warming.rs and lifecycle.rs so they
  keep compiling; no library change.

- #336: Gunzip gzip-compressed table metadata objects before parsing. A catalog with `write.metadata.compression-codec=gzip` (R2 Data Catalog) stores `metadata.json` gzip-compressed as `NNNNN-<uuid>.gz.metadata.json`; warming and mapper rebuild were feeding the raw gzip bytes to the JSON parser, failing thousands of times a day with "malformed metadata ... expected value at line 1 column 1". The shared `iceberg::parse_metadata_json` helper now decompresses when the object carries the gzip magic bytes (authoritative) or the `.gz.metadata.json` suffix; a decompression failure surfaces as the same warn-and-retry `Malformed` error, never a panic. Plain `.metadata.json` parses unchanged.

- cloud-agnostic sweep: removed every Cloudflare/R2 mention and tenant-named
  fixture from code, docs, and tests. Comments now describe the constraint
  ("strict S3-compatible stores reject variable-size parts", "some managed REST
  catalogs gzip responses") instead of naming a vendor; test fixtures use
  neutral hosts/entities (storage.example.com, acme, blobstore). No behavior
  change — the daemon and SDK are wiring-agnostic over any S3 bucket + Iceberg
  REST catalog.
- #60: `HeatFeed` (the serving decorator) now optionally records per-table
  telemetry. When a telemetry hub + mapper are wired it classifies each served
  GET once (the mapper's existing lock-free, allocation-free `classify()`) to get
  the `TableId`, and a body wrapper records one `AccessEvent` when the stream
  drains (so the serving tier and latency are known). This is the single classify
  on the serve path; unmapped reads record under `_unmapped`. A `tests/lifecycle`
  integration test asserts a served read attributes to the right table's family.
- catalog websocket change feed: added a websocket transport as the second
  change-feed implementation behind the existing `CatalogWatcher` seam, next to
  polling. New `feed` module carries the JSON protocol (hello/subscribe/change/
  resync) and a socket-independent `FeedState` (cursor + resync decisions);
  `websocket` module runs the connection driver and transport selection. The
  polling loop's per-table diff was factored into `apply_one`, reused by a new
  `refresh_table` that services a `change` frame with a single targeted pointer
  read then the same emit the poller drives. A new `CatalogFeed` type implements
  `CatalogWatcher` over the shared state and picks the transport: with a
  `WsFeedConfig` it attempts the upgrade and falls back to polling, without one
  (SigV4 catalogs) it polls only. rustls-only `tokio-tungstenite`, matching the
  workspace TLS choice. Unit tests cover the protocol and state machine; an
  integration test spins a local ws server and asserts upgrade→hello→subscribe→
  change drives a refresh, drop→reconnect resumes from the cursor, and a non-101
  upgrade falls back to polling.
- analyzer removal: the verglas-analyzer crate was deleted from the workspace
  (operator ruling: it never worked). Updated the `fetch` module and
  `tests/parsing.rs` doc comments that named the offline analyzer (#53) as a
  consumer of `MetadataFetch`/`ObjectStoreFetch`/`walk_snapshot`; the offline
  read path stays, its consumers are now tests and benches.
- #263: Removed the loopback catalog gateway, its response cache, and mutation-forwarding surface. The REST transport now exists only as the daemon's private, lenient catalog watcher client, including bearer and SigV4 authentication and gzip decoding.
- #91: Renamed active table lifecycle documentation from daemon terminology to
  the `verglas-server` process. Catalog watching and cache warming are unchanged.
- #8: Moved shallow Iceberg REST transport ownership into `verglas-catalog` and re-exported its polling contracts. Catalog polling now shares the same bounded response cache as the on-prem proxy.

- #58: Removed the Verglas-owned catalog websocket change feed (`/v1/catalog/feed` client, `CatalogFeed` transport selection, and `catalog_ws_feed` tests). Catalog watching is Iceberg REST polling only; hosted-catalog push notify stays a cloud concern.
- #66: Clarified catalog watching is Iceberg REST polling only; hosted-catalog push notify is out of band, not a cloud product integration.
