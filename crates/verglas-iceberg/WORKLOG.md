# verglas-iceberg worklog

- #376: Moved serde-only report and table-definition wire ownership to
  `verglas-sdk`; this crate now supplies only Iceberg-to-wire conversion and
  engine execution, breaking the SDK-to-engine dependency that fattened CLI
  clients.

- #385: Exposed DataFusion query execution as an incremental record-batch
  stream while retaining the JSON query report adapter. Added exact SDK table
  definition inspection and an Arrow-batch entry into the existing idempotent
  CAS commit path.

- #320: New crate. Extracted the pure-client Iceberg engine out of
  `verglas-agent`: catalog open, the fixed-part S3 storage factory, the CAS write
  path, CSV/JSONL/Parquet ingest, the DataFusion query path, `_LOGS` retention,
  table inspection, the commit/snapshot/rows/delta table API, ident parsing, the
  shared error enum, and the `--output json` report data shapes. The crate has
  zero dependency on `verglas-cache` or any daemon internals (and does not even
  link `verglas-core`) — it reads and writes wherever its endpoint points
  (WHITEPAPER §7.4). The resolved `Connection` data record moved here too; the
  flags/env/daemon-probe resolution that produces one stays in `verglas-agent`.
  All engine integration tests (tables_api, commit conflicts, retention, schema
  coercion, table verbs) moved here and stay green.

- #323: The report types derive `Deserialize` alongside `Serialize` — they are
  now the wire shapes the daemon's table and query routes serve and the CLI
  reads back. The `Connection` module doc no longer names the deleted
  `verglas-agent` crate as the resolver.

- rest-catalog partition ids: `build_partition_spec` assigns explicit partition
  field ids from 1000 so SDK-created partitioned tables (e.g. the `_LOGS` tables)
  carry valid field ids over a strict Iceberg REST catalog rather than null ones.

- cloud-agnostic sweep: removed every Cloudflare/R2 mention and tenant-named
  fixture from code, docs, and tests. Comments now describe the constraint
  ("strict S3-compatible stores reject variable-size parts", "some managed REST
  catalogs gzip responses") instead of naming a vendor; test fixtures use
  neutral hosts/entities (storage.example.com, acme, blobstore). No behavior
  change — the daemon and SDK are wiring-agnostic over any S3 bucket + Iceberg
  REST catalog.

- compaction executor: new `compaction` module rewrites many small data files
  into fewer target-sized files and commits a REPLACE snapshot (remove the small
  files, add the compacted ones) via the vendored `TableCommit::from_parts`, the
  same replace-commit primitive `retention` uses. Files are grouped by partition
  and bin-packed under a 128 MiB target; when the table has a sort order the
  merged rows are sort-compacted. Rows are read through the engine's Arrow reader
  and written back unchanged, so `count(*)` and content are identical and time
  travel to the pre-compaction snapshot still works. A REPLACE that conflicts with
  an interleaved append (stale `assert-ref-snapshot-id`) is retried against the
  reloaded table, never overwriting the append. `cleanup_orphans` deletes a
  candidate file only once no retained snapshot references it (conservative). Full
  hermetic test suite over MemoryCatalog covers file-count drop, byte-identical
  content, REPLACE, time travel, the append-conflict case, sort-compaction, and
  orphan-cleanup conservatism.

- compaction table selection: `run_compaction` now only spends a pass on a table
  with more than a small-file floor (`DEFAULT_MIN_SMALL_FILES`, 64) of undersized
  data files, so a healthy table is skipped cheaply and a table that has piled up
  thousands of single-record files is picked up. The count reads per-file sizes
  from the manifests (`undersized_file_count`), never a snapshot summary total,
  which is per-commit and not a table-wide count. `compact_table` itself stays
  threshold-free — selection is the scan's job, the rewrite compacts whatever is
  mergeable. Added selection/threshold unit tests (undersized count, over/under
  the floor, strict-floor boundary).
- compaction report is now a wire type (PR #378): derived `Deserialize` on
  `CompactionReport` and `CompactReport` (already `Serialize`) so the CLI can
  deserialize the `POST /admin/compact` response and render it.
- compaction progress now ratchets and the pass is time-bounded (PR #378). A pass
  used to read and plan the whole table and commit a single REPLACE at the very
  end, so a run killed before that commit made zero progress — the cascadelabs
  ~8,000-file backlog ran the fleet host-agent's full 900s ACK ceiling and lost
  everything. `run_compaction`/`compact_table` now commit one REPLACE per bin-pack
  group (each bounded by `max_files_per_group`, default 512), so a killed run keeps
  every committed group and the next run continues from the new table state. A
  hardcoded 10-minute wall budget (`DEFAULT_PASS_BUDGET`, no env/config knob) bounds
  the whole pass; it is checked only between group commits and between tables, never
  against an in-flight read/write/commit, and at least one group always commits
  before a stop. Per-group commits keep the existing CAS retry (reload + re-plan on
  conflict). The report now honestly reflects partial passes: groups committed,
  files rewritten, undersized files still mergeable, and whether the pass was
  budget-bounded. Added tests for the ratchet (one group then stop, re-runnable and
  convergent, content byte-identical) and per-group conflict retry.
- PR #378: Aligned compaction with Apache Iceberg's size and scan semantics. The
  default target is 512 MiB (or `write.target-file-size-bytes`), only files below
  the 75% healthy-size floor are selected, best-fit groups use Iceberg's 100 GiB
  ceiling, and partition spec ids remain in grouping. Planned scan tasks apply row
  deletes, unaffected and delete manifests survive REPLACE, only affected
  manifests are rewritten, and rejected CAS output is cleaned before retry.
- #281: Compaction had no snapshot expiration, so it made the reported table's
  metadata problem worse — a REPLACE per bin-pack group with no history pruning
  meant repeated passes only ever grew `metadata.json` further. Added
  `retention::expire_snapshots`, a general (not `_LOGS`-gated) metadata-only
  commit that drops old snapshot history down to a keep-last count through the
  same vendored `TableCommit::from_parts` escape hatch `prune_logs_table` already
  uses (`TableUpdate::RemoveSnapshots` is otherwise unreachable through Iceberg
  0.9.1's public transaction API too). `compaction::compact_table_bounded` now
  calls it at the end of every pass (`DEFAULT_KEEP_LAST_SNAPSHOTS` = 10, a fixed
  platform constant, not a new knob), then reclaims through the existing
  `cleanup_orphans` whichever of this pass's own removed files expiry left
  unreachable — bounded work, since expiry itself never rewrites data and the
  reachability check only walks the snapshots still retained afterward, not the
  table's full history. `CompactReport`/`CompactionReport` gained
  `snapshots_expired`/`orphan_files_deleted` fields. Files a still-retained
  snapshot's own point-in-time view needs (a mid-chain REPLACE genuinely can
  still need an as-of-then file even after a later pass merges it further) are
  correctly left in place, never deleted out from under a valid time-travel
  target — full reclamation of everything a table ever compacted away happens
  gradually as the table keeps being written to and the keep-last window
  advances past the old lineage, not in one pass. Added tests: history shrinks
  rather than grows across a pass, time travel to a snapshot the keep-last
  window retains still resolves, and an already-expired snapshot's files are
  reclaimed while a still-needed one is not.
- Moved serializable table and compaction report shapes into the dependency-leaf
  `verglas-api` crate while landing the thin client on current main. The engine
  owns construction and aggregation behavior without depending on the client
  SDK.

- #393: Switched from in-tree `vendor/iceberg` to the pinned `verglas-org/iceberg-rust` fork (`verglas/v0.9.1` @ a40f9268) for `TableCommit::from_parts`. Same patch, maintained out of tree; drop when upstream exposes overwrite/replace commits.

- #393: Removed platform `_LOGS` run logging and day-partition retention from Verglas. Catalog-side lakekeeping owns telemetry write/TTL; this crate keeps only the compact-adjacent APIs (snapshot expiry where applicable). Harness no longer writes `verglas_logs.<name>_LOGS`; verglasd no longer runs the hourly prune loop.

- #1 (verglas-org/verglas): Removed the fleet `verglas-compact` one-shot binary and its workspace/dist membership. Compaction stays in `verglas-iceberg` + daemon `POST /admin/compact` / `verglas table compact` until the async maintenance API lands; e2e retargeted to that path.
- chore: Update the no-connection diagnostic to direct self-hosted users to the Docker daemon after removing the CLI's local launcher.
- #263: Clarified that production catalog handles connect directly to the configured upstream Iceberg REST service. Verglas supplies only the S3 cache endpoint and does not provide a catalog gateway.
- #91: Updated engine integration and test documentation for the renamed
  `verglas-server` process. The engine continues to connect directly to the
  customer's catalog and use the server only as its S3 endpoint.
- #42: Enabled DataFusion information-schema discovery and typed positional parameter binding for live OLAP clients. Each execution still constructs its table providers from the catalog's current Iceberg state.
