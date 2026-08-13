# compaction — snapshot-driven prefetch recovery (issue #51)

Proves the #51 headline: after an Iceberg **compaction** (`rewrite_data_files`)
rewrites a partition's small files into fewer large ones, a format-blind cache
craters — every cached byte is orphaned and the "same" data now lives in objects
the cache has never seen — while a Verglas cache with **snapshot-driven
prefetch** carries warmth across the rewrite by *logical identity* (partition +
column) and recovers its hit rate before queries re-earn it from the origin.

The trigger is the production path: the `[catalog]` REST watcher (#47) observes
the compaction commit, the diff classifies it as `replace`/`overwrite`, and the
prefetch coordinator (#51) plans the rewritten files' hot chunks — scored against
the heat ledger fed by the preceding steady query load — and pulls them into
cache under the shared, organic-yielding executor. No benchmark shortcut.

## What it runs

Same seeded, partitioned Iceberg table, a fresh cache dir per config:

| Config | Prefetch | What it measures |
|--------|----------|------------------|
| **A** (baseline) | OFF (`[cache.prefetch] enabled=false`) | first post-compaction query wave cold-misses; hit rate re-earned query by query |
| **B** (prefetch) | ON | first post-compaction query wave already warm — hit rate recovers to ≥90% within the watcher interval |

Both the **Parquet** fixture and (per the Avro addendum) an **Avro-format**
fixture are run. Each config:

1. seeds the table on Polaris (data on the origin S3), if not present;
2. starts verglas-cache-node watching Polaris, warms the metadata;
3. drives a **steady read load** through the Verglas endpoint — this is what
   makes the queried columns *hot* in the heat ledger;
4. runs `rewrite_data_files` (compaction) through pyiceberg on Polaris;
5. waits one watcher interval for the coordinator to observe the commit;
6. drives the **first post-compaction query wave** and records the server's
   `/admin/stats` counter delta (`dram_hits`/`disk_hits` vs `backend_fills`) —
   the hit-rate-recovery number.

The `/admin/stats` counters are the mechanism evidence, exactly as in
`benchmarks/warming`.

## Inputs

Origin credentials come from a repo-root `.env` (never logged or committed):
`AWS_ENDPOINT`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`.

Everything else is env- or flag-overridable (defaults in `run.sh`):
`COMPACTION_BUCKET`, `COMPACTION_PREFIX`, `COMPACTION_NAMESPACE`,
`COMPACTION_CATALOG`, `COMPACTION_PARTITIONS`, `COMPACTION_FILES_PER_PARTITION`,
`COMPACTION_TARGET_FILE_SIZE`, `COMPACTION_VG_PORT`, `COMPACTION_VG_DRAM`,
`COMPACTION_VG_DISK`, `COMPACTION_POLL_SECS`, `COMPACTION_FORMAT`
(`parquet`|`avro`|`both`).

Pinned Polaris image digest matches `benchmarks/polaris` and
`benchmarks/warming`.

## Running

```bash
# from the repo root, with a live S3-compatible origin in .env
cargo build --release -p verglas-cache-node
benchmarks/compaction/run.sh
```

Nothing here runs in PR CI (it needs Docker, a live origin, and pyiceberg). The
hermetic, CI-checked version of this headline number lives in
`crates/verglas-tables/tests/lifecycle.rs`
(`benchmark_hit_rate_recovery_prefetch_on_vs_off`), which drives a real cache
engine over an in-memory origin and asserts prefetch recovers ≥90% while the
baseline cold-misses the rewritten files.

## Reuse

`compaction_demo.py` imports the Polaris/verglas-cache-node/counter helpers from
`benchmarks/warming/warming_demo.py` (same catalog bring-up, same
`/admin/stats` reading), so this demo shares one Polaris machinery with warming.
