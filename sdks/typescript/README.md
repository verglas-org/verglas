# @verglas/sdk

The thin TypeScript client for Verglas catalog, cache, and semantic services. It
runs in fetch-capable edge/serverless runtimes and in Node 18+. Worker authoring
lives in `sdks/worker-js` and `sdks/worker-py`; this package does not contain a
Worker runtime, scheduler, or job runner.

The SDK speaks HTTP and SigV4 service contracts. It does not read Parquet or
implement an Iceberg engine in JavaScript.

## Catalog and tables

Connect to a Verglas endpoint with a bearer token:

```ts
import { connect } from "@verglas/sdk";

const client = connect({
  endpoint: "http://127.0.0.1:8334",
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

### Catalog change feed

`client.follow` subscribes to table-commit notifications over the catalog feed.
`client.followRows` uses those notifications to perform bounded delta reads:

```ts
const subscription = client.followRows("app.points", (rows, watermark) => {
  console.log(`received ${rows.length} rows at ${watermark}`);
});

// Later:
subscription.close();
await subscription.closed;
```

The feed uses one authenticated WebSocket per client and reconnects with capped
backoff. It delivers notifications, not an alternate table-storage protocol.

### Reflected integrations

An Integration can publish a reflection manifest with JSON Schema inputs and
outputs. The connected client discovers those namespaces and invokes bounded or
streaming methods through `client.namespace`:

```ts
const manifest = await client.reflect("crm");
const contact = await client.namespace.crm.contacts.get({ id: "c-1" });
console.log(manifest, contact);
```

Generated TypeScript registries can be supplied as `connect<Namespaces>(...)` to
add static input and output checking without changing runtime reflection.

## `/v0` data client

`createDataClient` provides the independent append-ingest and SQL API. Table,
vector, and log rows use the same NDJSON endpoint; SQL uses the `/v0/sql` route.
Every request requires an explicit workspace bearer token.

```ts
import { createDataClient } from "@verglas/sdk";

const data = createDataClient({
  baseUrl: "https://api.verglas.dev",
  token: process.env.VERGLAS_WORKSPACE_TOKEN!,
});

await data.appendEvents("app_logs", [{ level: "info", message: "boot" }]);
await data.tableWrite("analytics.events", [{ path: "/" }]);
await data.vectorWrite("embeddings", [{ id: "a", vector: [0.1, 0.2] }]);
const rows = await data.sql("SELECT count() FROM analytics_events");
```

`tableWrite` and `vectorWrite` are aliases for `appendEvents`.

## S3 Vectors and Graphs

The cache listener's semantic APIs use AWS SigV4. The SDK includes the complete
checked-in S3 Vectors and Verglas Graph REST-JSON models:

```ts
import {
  S3VectorsClient,
  VerglasGraphsClient,
  graphFromEnv,
} from "@verglas/sdk";

const credentials = {
  accessKeyId: process.env.VERGLAS_ACCESS_KEY_ID!,
  secretAccessKey: process.env.VERGLAS_SECRET_ACCESS_KEY!,
};
const vectors = new S3VectorsClient("http://127.0.0.1:8333", credentials);
const graphs = new VerglasGraphsClient("http://127.0.0.1:8333", credentials);
const graph = graphFromEnv("rime-evidence");

await graphs.listGraphs({});
await vectors.listVectorBuckets({});
await graph.insertNodes([{ id: "run-1", labels: ["Run"] }]);
```

`Graph` is a small handle for graph creation and node/edge writes. The direct
clients expose the service-model operations, including vector queries and graph
neighborhood/path/precedent searches.

## Logging and observability

The `@verglas/sdk/logging` subpath contains framework-neutral log rows, a
buffering `RunLogger`, safe error/field helpers, and chart declarations:

```ts
import {
  logsCharting,
  logsTableName,
  observabilityFor,
} from "@verglas/sdk/logging";

const logs = logsTableName("app.points");
const chart = logsCharting("app.points");
const observability = observabilityFor("app.points");
console.log(logs, chart, observability);
```

`RunLogger` can buffer standard rows and flush them through a connected catalog
client. Charting functions return declarations for a renderer; they do not draw
charts or schedule jobs. Log writes remain best-effort and use the normal table
append contract.

## Worker script management

`WorkersManagementClient` remains available from the package root for celld's
account-prefix-free script management API. It supports module-syntax script
upload, list, get, and delete operations, multipart form helpers, and typed API
errors. Script metadata retains Cloudflare namespace binding records, including
`type: "durable_object_namespace"` and its `class_name`; Worker source builds
belong to `sdks/worker-js` or `sdks/worker-py`.

```ts
import { WorkersManagementClient } from "@verglas/sdk";

const management = new WorkersManagementClient("http://127.0.0.1:8334");
await management.listScripts();
```

The TypeScript SDK has no scheduler client, worker-definition contract, trigger
registration API, local job harness, or reference job-worker templates.

## Public API

The root package exports:

- catalog/table clients and feed/reflection types;
- `/v0` data client types and `createDataClient`;
- S3 Vectors, Graph clients, Graph handles, and semantic DTOs;
- `VerglasHttpError`;
- Worker script-management clients and metadata types.

The supported subpaths are `@verglas/sdk/data` and
`@verglas/sdk/logging`. There is no scheduler or examples subpath.

## Development

```sh
npm install
npm run typecheck
npm test
npm run build
```

Tests use fake tokens and local mock endpoints; no secrets belong in code or
fixtures.
