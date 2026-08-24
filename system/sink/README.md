# Sink system project

This is a literal Cloudflare-style Worker plus Durable Object deployment. Build
it with the JavaScript SDK:

```sh
node sdks/worker-js/bin/build.mjs system/sink --out /tmp/verglas-sink-build
```

The artifact has one Sink Durable Object class and one ordinary named Catalog
binding. The Sink never reads a Stream, owns a Pipeline cursor, schedules a
roll, writes an object, constructs Parquet, or performs an Iceberg commit. The
Catalog binding is the only Iceberg authority.

## Immutable configuration

The Durable Object requires these `vars` when it is first opened:

- `SINK_ID` — the Sink resource identity.
- `SINK_TYPE` — exactly `iceberg`.
- `SINK_CATALOG_BINDING` and `SINK_CATALOG_OBJECT` — the binding and named
  Catalog object used for commits.
- `SINK_BUCKET`, `SINK_NAMESPACE`, and `SINK_TABLE` — the destination table.
- `SINK_COMPRESSION` — explicitly `zstd`, `snappy`, `gzip`, `lz4`, or `uncompressed`.
- `SINK_ROLL_INTERVAL_SECONDS` — explicit interval, at least 60 seconds.
- `SINK_ROLL_SIZE_BYTES` — explicit positive file-size policy.

The object stores a canonical configuration JSON and SHA-256 digest in Turso.
A later activation with a different digest is a hard error: delete and
recreate the object. The Sink validates the 60-second minimum but never uses
the policy to flush; Pipeline decides when to send a batch.

The 60-second minimum follows Cloudflare's R2 Data Catalog sink contract:
[batching and rolling policy](https://developers.cloudflare.com/pipelines/sinks/available-sinks/r2-data-catalog/#batching-and-rolling-policy).
Cloudflare's sink creates a namespace/table when absent and does not bind an
existing Iceberg table. That creation/rejection policy belongs to Catalog.

## Pipeline request protocol

Only `POST https://verglas.internal/sink/batch` is accepted by the Sink object.
The Worker exposes only that internal control and
`GET https://verglas.internal/sink/status`; every other route is 404.

The request must be `application/json` and contain these headers:

```text
x-verglas-pipeline-id: <pipeline id>
x-verglas-sql-digest: <64 lowercase SHA-256 hex characters>
x-verglas-batch-id: ["<pipeline id>","<sql digest>",<first sequence>,<last sequence>,"<sink id>"]
```

The body is the exact Pipeline envelope:

```json
{
  "batch_id": "[\"orders\",\"<sql digest>\",1,10,\"primary\"]",
  "pipeline_id": "orders",
  "sql_digest": "<sql digest>",
  "source": "events",
  "sink": "primary",
  "first_sequence": 1,
  "last_sequence": 10,
  "records": [{"id": 1}]
}
```

The Sink checks header/body identity, the deterministic tuple, positive safe
sequence bounds, non-empty object rows, at most 10,000 rows, and an 8 MiB body
ceiling. A valid batch may have fewer rows than its sequence range because a
Pipeline filter can reject input records.

## Catalog commit protocol

For every unconfirmed batch, the Sink sends one request to the configured
Catalog binding/object:

```text
POST https://verglas.internal/catalog/commit
content-type: application/json
x-verglas-sink-id: <sink id>
x-verglas-batch-id: <batch id>
x-verglas-file-id: verglas/<sink id>/batch-<sha256(batch id)>.parquet
x-verglas-pipeline-id: <pipeline id>
x-verglas-sql-digest: <sql digest>
```

The JSON request is:

```json
{
  "batch_id": "<deterministic batch id>",
  "file_id": "verglas/primary/batch-<sha256(batch id)>.parquet",
  "sink_id": "primary",
  "pipeline_id": "orders",
  "sql_digest": "<sql digest>",
  "source": "events",
  "first_sequence": 1,
  "last_sequence": 10,
  "bucket": "lake",
  "namespace": "analytics",
  "table": "events",
  "format": "parquet",
  "compression": "zstd",
  "roll_interval_seconds": 60,
  "roll_size_bytes": 5242880,
  "records": [{"id": 1}]
}
```

Catalog must make `batch_id` its idempotency key. A successful Catalog response
is HTTP 2xx JSON with the exact identity and committed row count:

```json
{
  "committed": true,
  "batch_id": "<same batch id>",
  "file_id": "<same file id>",
  "snapshot_id": "<opaque non-empty snapshot id>",
  "rows_committed": 1
}
```

The Sink writes no ledger row until that response validates. It then stores
and returns this receipt:

```json
{
  "accepted": 1,
  "batch_id": "<same batch id>",
  "file_id": "<same file id>",
  "snapshot_id": "<same snapshot id>"
}
```

A confirmed retry reads that exact receipt from Turso and does not call
Catalog. If the process dies after Catalog commits but before the ledger
insert, the retry sends the same `batch_id` and `file_id`; Catalog returns its
idempotent result and the Sink records the receipt. A Catalog failure or an
invalid response never records a ledger row.

The parent Rust Catalog integration must expose this internal endpoint and
translate the `records` rows into its Iceberg append path using `batch_id` as
its snapshot idempotency key. It must return the response fields above for both
new and replayed commits.
