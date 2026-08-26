# @verglas/sdk

The thin TypeScript client for the Verglas Catalog and Worker management APIs. It
runs in fetch-capable edge/serverless runtimes and in Node 18+. Worker authoring
lives in `sdks/worker-js` and `sdks/worker-py`; this package does not contain a
Worker runtime, scheduler, or job runner.

The SDK speaks HTTP contracts. It does not read Parquet or implement Iceberg
storage in JavaScript.

## Catalog and tables

Connect to a Verglas endpoint with a bearer token:

```ts
import { connect } from "@verglas/sdk";

const client = connect({
  endpoint: process.env.VERGLAS_ENDPOINT!,
  token: process.env.VERGLAS_TOKEN!,
});
```

`client.table(name)` exposes the catalog-backed table surface:

- `snapshot()` reads current snapshot metadata;
- `scan({ limit, cursor })` reads a paged current snapshot;
- `delta(watermark, { limit })` reads rows after an opaque read cursor;
- `append(rows, { idempotencyKey })` writes one JSONL batch;
- `client.createTable(name, definition)` and `client.ensureTable(name, definition)`
  create exact Iceberg schemas and partition specs.

```ts
const table = client.table<{ id: string; value: number }>("app.points");
const page = await table.scan({ limit: 1000 });
const result = await table.append([{ id: "a", value: 1 }], {
  idempotencyKey: "batch-a",
});
console.log(result.snapshotId, result.rowsCommitted, page.watermark);
```

An append posts JSONL rows to the endpoint's ingest route and returns the
snapshot ID, committed row count, watermark, and idempotency result. The SDK
passes watermarks through as opaque cursors; it does not interpret them as
scheduler state.

## Logging

The `@verglas/sdk/logging` subpath contains framework-neutral log rows, a
buffering `RunLogger`, and safe error/field helpers:

```ts
import { RunLogger } from "@verglas/sdk/logging";

const logger = new RunLogger({
  pipeline: "app.points",
  kind: "worker",
  placement: "remote",
  runId: "run-1",
});
logger.log("run_start", { message: "started" });
await logger.flush(client);
```

`RunLogger` buffers standard rows and flushes them through a connected Catalog
client. Log writes remain best-effort and use the normal table append contract.

## Worker script management

`WorkersManagementClient` remains available from the package root for verglasd's
account-prefix-free Cloudflare-shaped script management API. It supports module-
syntax script upload, list, get, and delete operations, multipart form helpers,
and typed API errors. Script metadata retains Cloudflare namespace binding
records, including `type: "durable_object_namespace"` and its `class_name`; Worker
source builds belong to `sdks/worker-js` or `sdks/worker-py`.

```ts
import { WorkersManagementClient } from "@verglas/sdk";

const management = new WorkersManagementClient(process.env.VERGLAS_ENDPOINT!);
await management.listScripts();
```

The TypeScript SDK has no scheduler client, worker-definition contract, trigger
registration API, local job harness, or reference job-worker templates.

## Public API

The root package exports Catalog/table clients, `VerglasHttpError`, and Worker
script-management clients and metadata types. The supported subpath is
`@verglas/sdk/logging`.

## Development

```sh
pnpm test
pnpm typecheck
pnpm build
```

Tests use fake tokens and local mock endpoints; no secrets belong in code or
fixtures.
