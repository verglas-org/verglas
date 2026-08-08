# verglas-pgcdc (binary) worklog

- feat/pg-cdc-iceberg: New thin binary. Implements the env-in / result-file-out
  worker contract shared by `verglas_harness::worker::run_worker` and the TS
  SDK's endpoint-run: resolves the run environment (`VERGLAS_ENDPOINT`,
  `VERGLAS_TOKEN`, `DEPLOYMENT`, `TARGET`, `PG_DSN` or discrete `PG_*`, the cache
  `VERGLAS_CATALOG_S3_ENDPOINT`, and the catalog URI/token/warehouse) into a
  `CdcEnv`, connects the Postgres pool, opens the Iceberg catalog through the
  cache endpoint, runs exactly one `verglas_pgcdc::runner::drain_tick`, and writes
  a `verglas_sdk::worker::RunResult` JSON to `RESULT_PATH` (exit 0/1). Env/DSN
  resolution is unit-tested; the LSN watermark is durable in the replication slot
  (a daemon `/v1/watermark` mirror is a documented TODO).

- pg-cdc: locked the launch-env contract to the control plane's `VERGLAS_CDC_*`
  names (pg_cdc.ts) — discrete PG parts via `PgConnectOptions` (no DSN splicing,
  so an unsealed password never needs URL escaping), slot/publication from env,
  and the cache S3 endpoint made REQUIRED so a run without the cache refuses to
  start (no direct-R2 branch). Dropped the `PG_DSN`/`VERGLAS_CATALOG_URI`
  alternates per the no-fallbacks rule.
- #91: Updated the CDC runner's local endpoint documentation for the renamed
  `verglas-server` process. No legacy executable or endpoint spelling remains.
- #11: Made the CDC worker reject malformed scheduler event payloads instead of silently running with a fabricated trigger. The binary now shares the strict SDK subprocess contract used by the scheduler harness.
- #11: Switched the CDC worker subprocess boundary to the shared CloudEvent envelope. The worker now validates the same single event binding as every other scheduled worker before draining WAL.
- chore: Dropped the documented TODO that would mirror the CDC LSN to /v1/watermark. The replication slot remains the sole durable cursor.
