# Pipeline system project

This is a literal Worker plus Durable Object deployment. Build it with the
JavaScript SDK:

```sh
node sdks/worker-js/bin/build.mjs system/pipeline --out /tmp/verglas-pipeline-build
```

## Immutable configuration

All of these `vars` are required and are read when the Durable Object is first
opened:

- `PIPELINE_ID` — the resource identity.
- `PIPELINE_SQL` — one or more semicolon-separated statements.
- `PIPELINE_SOURCE_BINDING` and `PIPELINE_SOURCE_NAME` — the one input binding
  and Stream object name.
- `PIPELINE_SINK_BINDINGS` — an object mapping every SQL sink name to a named
  Sink binding, for example `{ "purchases": "SINK_PURCHASES" }`.
- `PIPELINE_BATCH_MAX_ROWS`, `PIPELINE_BATCH_MAX_BYTES`, and
  `PIPELINE_BATCH_MAX_SECONDS` — explicit rolling limits. The hard ceilings are
  10,000 rows, 8 MiB, and 24 hours.

The object stores the complete configuration and SHA-256 SQL digest in
`ctx.storage.sql`. A later activation with a different digest or any different
configuration fails. Changing SQL is delete-and-create; there is no update
path.

## SQL target and gap

The implemented target is intentionally a small stateless SQL evaluator, not a
claim of full DataFusion or Cloudflare function parity. It accepts:

```sql
INSERT INTO sink_name
SELECT * FROM stream_name;

INSERT INTO sink_name
SELECT field, alias.field AS renamed,
       UPPER(kind) AS kind, amount * 1.1 AS gross
FROM stream_name alias
WHERE amount > 10 AND kind = 'purchase';
```

Projection expressions support field paths, numeric and string literals,
`NULL`, `TRUE`, `FALSE`, parentheses, `+ - * / %`, comparisons, `AND`, `OR`,
`NOT`, `IS [NOT] NULL`, `LIKE`, `||`, and the scalar functions `UPPER`,
`LOWER`, `LENGTH`, `TRIM`, `ABS`, `ROUND`, `COALESCE`, `NULLIF`, and `CONCAT`.
A projection alias may use `AS` or the SQL implicit alias form. A source alias
may qualify fields. Multiple statements may read the same configured Stream and
fan out to different named Sinks.

The gap is deliberate: `WITH`, subqueries, `UNNEST`, joins, comma joins,
aggregates, windows, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, DDL,
`UPDATE`, `DELETE`, qualified functions, double-quoted identifiers, and unknown
functions are rejected before serving. Records are JSON values; projections
produce JSON object rows. The evaluator uses JavaScript numeric and boolean
semantics for this small target.

## Binding protocol

The source binding is a normal DO namespace or direct service-style binding.
Pipeline sends:

```text
GET https://verglas.internal/stream/read?after=<u64>&limit=<u32>
```

The Stream response is JSON `{ "records": [{ "sequence": <u64>,
"record": <json> }], "next_after": <u64> }`. The response must be contiguous
from the exclusive `after` value. The Pipeline has no Stream cursor; each
Pipeline object owns its own cursor.

For each targeted sink, the named binding receives:

```text
POST https://verglas.internal/sink/batch
content-type: application/json
x-verglas-pipeline-id: <pipeline id>
x-verglas-sql-digest: <sha256 hex>
x-verglas-batch-id: <JSON tuple>
```

The JSON body is:

```json
{
  "batch_id": "[\"pipeline\",\"sql-digest\",1,10,\"sink_name\"]",
  "pipeline_id": "pipeline",
  "sql_digest": "sha256 hex",
  "source": "stream_name",
  "sink": "sink_name",
  "first_sequence": 1,
  "last_sequence": 10,
  "records": [{"...":"transformed row"}]
}
```

`batch_id` is the deterministic tuple `(pipeline id, SQL digest, first
sequence, last sequence, sink)`, encoded as JSON. A Sink confirms delivery with
any 2xx response and must deduplicate this identity. The Pipeline resends the
same body and identity after a crash or failure, and updates its cursor only
after every targeted Sink confirms.

The only Worker controls are `POST /pipeline/process-now` and
`GET /pipeline/status`; all other paths return 404. A process-now call reads at
most one bounded batch and persists its rolling alarm before delivery. A due
alarm flushes the same durable pending batch. There is no public tenant route.

The target follows Cloudflare's current [SELECT reference](https://developers.cloudflare.com/pipelines/sql-reference/select-statements/), [Pipeline management](https://developers.cloudflare.com/pipelines/pipelines/manage-pipelines/), and [Sink management](https://developers.cloudflare.com/pipelines/sinks/manage-sinks/) shape. Those pages define the external product surface; this README labels the smaller evaluator and binding protocol implemented here.
