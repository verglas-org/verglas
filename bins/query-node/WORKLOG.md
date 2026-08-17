# query-node worklog

- RIME query-node candidate A: new standalone binary `verglas-query-node`
  serving `POST /v1/query` (and the `/v0` `{"q": ...}` request alias) over
  `crates/verglas-iceberg`'s existing `PreparedCatalog`/`query_stream`
  (DataFusion). Config is env-only (`VERGLAS_QUERY_LISTEN`,
  `VERGLAS_CATALOG_URI`, `VERGLAS_WAREHOUSE`, `VERGLAS_CATALOG_TOKEN`,
  `VERGLAS_BACKEND_S3_ENDPOINT`/`_REGION`/`_ACCESS_KEY_ID`/`_SECRET_ACCESS_KEY`
  matching `images/cache/boot.sh`'s S3 endpoint/keypair names,
  `VERGLAS_QUERY_MEMORY_LIMIT_BYTES` default 1 GiB,
  `VERGLAS_QUERY_TIMEOUT_SECS` default 300). No auth on the query route
  itself in v1 — org-network-only, the same trust model as the cache node's
  admin port. Responses answer the `/v0` result shape (`{meta, data, rows,
  statistics}`), streamed batch-by-batch through the existing
  `batch_to_json_rows_fragment` rather than collected in memory. Every
  failure (bad JSON, a SQL syntax/unknown-table error, an over-cap
  `memory_limit_bytes` request, a wall-clock timeout) answers a clean JSON
  4xx — never a hang, never a raw 5xx.
  A fresh DataFusion catalog session opens per query rather than once for the
  process lifetime: `iceberg-datafusion` 0.10.1's `IcebergCatalogProvider`
  snapshots the namespace/table list once at construction (its own source
  says "tables might become stale"), so a session opened at boot would never
  see a table created afterward — exactly what the frozen local-lite
  protocol does (the query node boots before `check` creates
  `main.pairing_events`). No new engine features were added to
  `verglas-iceberg` to avoid this; `PreparedCatalog`/`query_stream`/
  `batch_to_json_rows_fragment` were already public on `rime/ingest-perf`.
