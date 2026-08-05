# @verglas/sdk

The thin TypeScript client that agent-generated worker code imports to read and
write Verglas Iceberg tables. It runs unchanged in any **fetch-only edge/serverless
runtime** (no Node APIs) and in **Node** locally.

A worker is a small code artifact that runs in an isolated sandbox and uses this
SDK to move data. The SDK is deliberately thin: it never reads Parquet or runs an
Iceberg commit in JavaScript. It speaks a small HTTP contract to a Verglas
**endpoint** — the local server or a cloud Verglas endpoint — which owns the
catalog, the snapshots, and the content-addressed write path.

## The worker model

The whole programming surface is one primitive: a **worker**. A worker is a
handler `worker(ctx)` invoked **once per trigger**. It reads and writes tables
through the connected client and is a **pure function of its inputs** — the
trigger that invoked it and the data currently committed. A worker holds nothing
between runs: there is no durable per-job watermark in the programming model. The
platform owns scheduling, backfill, and replay; a worker only computes the run it
was handed.

You build one with `defineWorker` and default-export it from a module:

```ts
import { defineWorker, type WorkerContext } from "@verglas/sdk";

export default defineWorker({
  name: "app.points",
  triggers: [{ type: "cron", schedule: "*/5 * * * *" }],
  secrets: ["FEED_KEY"],
  async handler(ctx: WorkerContext) {
    const rows = await fetchUpstream(ctx.env.FEED_KEY, ctx.signal);
    const result = await ctx.client.table(ctx.output).append(rows);
    return { rowsWritten: result.rowsCommitted };
  },
});
```

`defineWorker` also accepts a bare handler for the trivial case:

```ts
export default defineWorker(async (ctx) => {
  await ctx.client.table(ctx.output).append([{ ok: true }]);
});
```

### `ctx: WorkerContext`

The handler receives everything the run needs and nothing it doesn't:

- `ctx.client` — a connected `VerglasClient` for read/write via the table verbs.
- `ctx.trigger` — the CloudEvent that invoked this run (see below).
- `ctx.output` / `ctx.outputs` — the **deployment-configured** output table(s).
  Output is deployment config, never hardcoded in worker code — the platform
  passes it in (the fleet harness maps the `TARGET` binding here). `outputs` lists
  every configured output when a deployment declares more than one; `output` is
  `outputs[0]`.
- `ctx.env` — the deployment environment: declared secret bindings and config
  values.
- `ctx.log(message, meta?)` — a structured log sink. Baseline run logging is
  automatic; call this only to add your own steps. Only the standard `meta` fields
  (`level`, `rows`, `duration_ms`, `message`, `error`) are recorded, so a stray
  value carrying a secret can never land in the logs table. Never write secrets
  here.
- `ctx.signal` — an `AbortSignal` to stop long-running work (a paged fetch, a
  stream drain).

There is **no watermark on `ctx`**. Progress for a cron worker is the trigger's
**logical time** (the Airflow model: `logicalDate` / `intervalStart` /
`intervalEnd`), not durable job state. A worker doing an incremental pull ranges
its query over the interval it was handed; the platform, not the worker, decides
which interval to run next.

A handler may return a `WorkerResult` (an optional summary the runner folds into
the run log and the host's JSON response) or nothing at all.

## Triggers

Triggers are **deployment config, not code**. The SDK *types* them so a definition
can declare what it expects (`TriggerSpec`), but the platform's deploy path
registers them; the worker receives one CloudEvents 1.0 envelope on
`ctx.trigger`. There are three deployment-trigger shapes, plus manual dispatch:

| Trigger | Spec (deploy config) | Event (`ctx.trigger`) |
|---|---|---|
| `cron` | `{ type, schedule, startDate?, catchup? }` | `org.verglas.schedule.tick` with interval `data` |
| `webhook` | `{ type, path? }` | `org.verglas.http.request` with request `data` |
| `event` | `{ type, eventType, source?, subject? }` | The matching CloudEvent unchanged |

`ctx.trigger.type` identifies the event contract; event-specific fields live in
`ctx.trigger.data`.

### Cron and backfill

A cron trigger event carries the run's **logical time**: the nominal instant the
platform scheduled it for and the half-open interval `[intervalStart, intervalEnd)`
it is responsible for. That interval is the only progress signal a worker sees.

Backfill is first-class on the spec. With `startDate` in the past the platform
replays every scheduled interval from `startDate` up to live, running each as its
own dispatch with its own logical interval. `catchup` chooses how the backlog
runs:

- `"sequential"` — one interval at a time, oldest first (ordered backfill);
- `"parallel"` — intervals fanned out concurrently (fast, unordered);
- `"none"` — skip the backlog, start at the next live interval.

```ts
triggers: [{ type: "cron", schedule: "0 * * * *", startDate: "2026-01-01T00:00:00Z", catchup: "sequential" }]
```

## The endpoint model: local vs cloud

`connect` takes an endpoint and a token. The interface is identical; only the
endpoint differs.

- **Local** — point at the server's base URL (e.g. `http://127.0.0.1:8334`) for
  Verglas APIs. Iceberg clients continue to use the customer's catalog endpoint
  directly; the server never serves or proxies that catalog.
- **Cloud** — point at the tenant's Verglas endpoint. The cloud commit service
  implements the same contract against the tenant's managed Iceberg REST catalog.

```ts
import { connect } from "@verglas/sdk";

const client = connect({
  endpoint: "http://127.0.0.1:8334", // or the cloud endpoint
  token: process.env.VERGLAS_TOKEN!, // never hard-code
});
```

Inside a worker you never call `connect` yourself — the runner hands you a
connected `ctx.client`. You call `connect` only when driving the SDK directly
(tests, a script, the fleet entry).

## Reading and writing

```ts
const table = client.table<{ id: string; value: number }>("app.points");

// Current snapshot, paged.
const page = await table.scan({ limit: 1000 });

// Only rows committed after a watermark.
const delta = await table.delta(page.watermark);

// Append a batch. Commits synchronously as its own snapshot.
const result = await table.append([{ id: "a", value: 1 }]);
console.log(result.snapshotId, result.rowsCommitted, result.watermark);
```

### Watermarks in reads

A watermark is an **opaque cursor** marking a position in a table's history. The
SDK never parses it — it hands the value straight back to the endpoint on the next
`delta`. A table advances its watermark when a new snapshot commits. It is a *read
cursor* you carry within a run (e.g. paging a `delta`), not a durable per-worker
position: a worker's cross-run progress is its cron interval, not a stored
watermark.

## A cron worker: ranging by the logical interval

A cron worker doing an incremental pull ranges its query over
`[intervalStart, intervalEnd)`, so each scheduled run — including a backfill run
for a past interval — pulls exactly its own slice, with no watermark to persist:

```ts
import { defineWorker, type Row, type WorkerContext } from "@verglas/sdk";

export default defineWorker({
  name: "http-poll",
  triggers: [{ type: "cron", schedule: "*/5 * * * *" }],
  secrets: ["POLL_KEY"],
  async handler(ctx: WorkerContext<{ POLL_URL: string; POLL_KEY?: string; POLL_TIME_FIELD?: string }>) {
    const headers = ctx.env.POLL_KEY ? { authorization: `Bearer ${ctx.env.POLL_KEY}` } : undefined;
    const resp = await fetch(ctx.env.POLL_URL, { headers, signal: ctx.signal });
    if (!resp.ok) throw new Error(`poll: HTTP ${resp.status}`);
    const items = (await resp.json()) as Row[];

    // Keep only rows whose timestamp falls in this run's logical interval.
    const tf = ctx.env.POLL_TIME_FIELD;
    const cron = ctx.trigger.type === "cron" ? ctx.trigger : undefined;
    const fresh = items.filter((r) => {
      if (!tf || !cron) return true;
      const t = String(r[tf] ?? "");
      if (cron.intervalStart && t < cron.intervalStart) return false;
      if (cron.intervalEnd && t >= cron.intervalEnd) return false;
      return true;
    });
    if (fresh.length === 0) return { rowsWritten: 0 };

    const result = await ctx.client.table(ctx.output).append(fresh);
    ctx.log("appended", { rows: result.rowsCommitted });
    return { rowsWritten: result.rowsCommitted };
  },
});
```

### A webhook worker

A webhook worker records each inbound request as a row. The platform holds the
listener; the worker is invoked per request and returns whatever it writes back:

```ts
export default defineWorker({
  name: "webhook-ingest",
  triggers: [{ type: "webhook", path: "/ingest" }],
  async handler(ctx) {
    if (ctx.trigger.type !== "webhook") throw new Error("expected a webhook trigger");
    const body = (await ctx.trigger.request.json()) as Row;
    const result = await ctx.client.table(ctx.output).append([{ ...body, received_at: new Date().toISOString() }]);
    return { rowsWritten: result.rowsCommitted };
  },
});
```

### An Iceberg event worker

An event worker can react to Iceberg snapshot commits. The CloudEvent carries
the snapshot data; the worker delta-reads the newly committed rows, transforms them,
and appends the results. Keying the output commit by the input snapshot makes a
replayed commit a free replay, never a double write:

```ts
export default defineWorker({
  name: "change-fanout",
  triggers: [{ type: "event", eventType: "org.apache.iceberg.snapshot.committed", subject: "app.input" }],
  async handler(ctx) {
    if (ctx.trigger.type !== "org.apache.iceberg.snapshot.committed") throw new Error("expected an Iceberg event");
    const changed = ctx.trigger.data as { snapshotId: string };
    const rows = (await ctx.client.table("app.input").scan()).rows;
    const out = rows.map((r) => ({ v: Number(r.v) * 2, from_snapshot: changed.snapshotId }));
    if (out.length === 0) return { rowsWritten: 0 };
    const result = await ctx.client
      .table(ctx.output)
      .append(out, { idempotencyKey: `app.input@${changed.snapshotId}` });
    return { rowsWritten: result.rowsCommitted };
  },
});
```

## Following commits over the edge change feed

`client.follow` subscribes to table-commit *notifications* over the platform's
edge change feed — a single websocket per client, held by the edge Durable Object
while the backend sleeps. The SDK never opens a long-lived connection to a
backend, so a tenant stack that **scales to zero** stays asleep until a commit
lands.

```ts
const sub = client.follow("app.points", (change) => {
  // change: { seq, table, snapshotId, committedAt } — a notification, not rows.
  // Read what changed with a short (backend-waking) delta:
  // const d = await client.table("app.points").delta(lastWatermark);
  console.log(`commit #${change.seq} → ${change.snapshotId} at ${change.committedAt}`);
});
// ... later
sub.close();
await sub.closed;
```

- Pass one table or an array; several follows on one client multiplex over the
  single socket and are filtered to their table(s) client-side.
- `opts.cursor` — omit or `null` for live-only (changes after you attach); pass a
  feed `seq` to replay changes after it (as far as the edge retains) then go live.
- `opts.onResync` — called `{ reason: "cursor-too-old" }` when the edge has
  dropped the replay you asked for; the feed re-subscribes live automatically and
  this is your cue to reconcile (e.g. a full `scan`). The SDK never polls for you.
- `opts.signal` — abort to end the follow (same as `sub.close()`).
- The socket reconnects on drop with capped exponential backoff (to ~60s),
  resuming from the last seq seen so no commit is missed or repeated.

### Change-driven row follow: `client.followRows`

`client.followRows(table, handler, opts?)` rides the change feed to deliver
**rows** instead of notifications. On each commit it delta-reads the newly
committed rows and invokes `handler(newRows, watermark)` — a commit notification
wakes a bounded `delta`, so an idle table costs nothing (the edge holds the socket
while the backend sleeps). This replaces the old interval-polling row follow.

```ts
const sub = client.followRows("app.points", async (rows, watermark) => {
  console.log(`got ${rows.length} new rows at ${watermark}`);
});
// ... later
sub.close();
await sub.closed;
```

- Starts from `opts.fromWatermark`, or the table's current snapshot when omitted
  (only rows committed from the follow's start on are delivered).
- Batches are delivered in commit order and `handler` is awaited before the next
  drain, so a slow handler applies natural backpressure.
- Pass `opts.onError` to keep following through handler/read errors; without it an
  error closes the subscription and rejects `sub.closed`. `opts.batchSize`,
  `opts.cursor`, `opts.onResync`, and `opts.signal` are also honored.

**Transport.** A websocket to `<endpoint origin, https→wss>/v1/catalog/feed`,
authenticated with the same `Authorization: Bearer {token}` as the HTTP routes.
It uses the runtime's global `WebSocket` (Bun, Node ≥ 22, and Workers all provide
one). The wire protocol: the server sends `{"type":"hello","cursor":<int>}` on
attach; the client replies `{"type":"subscribe","cursor":<int|null>}`; the server
pushes `{"type":"change","seq":<int>,"table":"ns.t","snapshot_id":"…","committed_at":"…"}`
and `{"type":"resync","reason":"cursor-too-old"}`.

## The `append` → commit-endpoint contract

`append` does **not** build Parquet or run an Iceberg commit in JS. It POSTs the
batch to the endpoint's commit service. Each `append` commits its own batch
synchronously.

**Request**

```
POST {endpoint}/v1/tables/{name}/commit
Authorization: Bearer {token}
Content-Type: application/json
Idempotency-Key: {optional}          # mirrors the body field

{
  "rows": [ { ... }, ... ],
  "idempotencyKey": "optional-string"
}
```

**Response** `200`

```json
{
  "snapshotId": "snap-42",
  "rowsCommitted": 128,
  "watermark": "10240",
  "idempotent": false
}
```

Retrying a commit with the same `idempotencyKey` returns the original result
rather than writing twice, and sets `"idempotent": true`. Non-2xx responses raise
`VerglasHttpError` carrying the status and body.

## The read contract

The same endpoint serves three read routes, all `Authorization: Bearer {token}`:

| Route | Returns |
|---|---|
| `GET {endpoint}/v1/tables/{name}/snapshot` | `{ snapshotId, watermark, recordCount? }` — a cheap metadata poll, reads no rows |
| `GET {endpoint}/v1/tables/{name}/rows?limit&cursor` | `{ rows, watermark, snapshotId, nextCursor? }` — a page of the current snapshot |
| `GET {endpoint}/v1/tables/{name}/delta?since={watermark}&limit` | `{ rows, watermark, snapshotId }` — rows committed after the watermark |

`client.followRows` is built from `snapshot` + `delta`, driven by the edge change
feed: a commit notification wakes a bounded delta-read.

## Data-plane verbs

Beyond `Table`, a client hands a worker three more handles, each a thin wrapper
over the endpoint's routes.

### Tables

`client.table<T>(name)` → `Table`: `snapshot()`, `scan(opts?)`,
`delta(since, opts?)`, `append(rows, opts?)`, plus the vector-index verbs below.
`client.createTable(name, def)` builds a table from an explicit schema and
partition spec when first-commit inference cannot express the exact column types
or partitioning you need.

### Queues

A worker may target a **queue** instead of a table. `client.queue(name)` returns a
`Queue` with `enqueue(rows)`, `poll(group, { max })`, and `ack(group, position)`.
Delivery is **at-least-once with consumer-side idempotency**: a record is durable
before any consumer sees it, and a group's watermark advances only on an explicit
`ack` after the consumer's downstream commit. A crash between `poll` and `ack`
re-serves the same records, so a consumer that must not act twice dedupes on
`QueueRecord.position`. `ack` is monotone — a regressing position is ignored.
Locally the queue is a durable segment log; the cloud backs the same verb with a
managed queue.

### Graphs

`client.graph(namespace)` returns a `Graph` — the graph equivalent of `Table`. A
graph is not a new storage primitive: it is a namespace holding two plain Iceberg
tables (`nodes` and `edges`) plus a snapshot-bound adjacency index. The handle:
`create()`, `insertNodes()`, `insertEdges()`, `buildIndex()`, `show()`, and the
traversals `neighbors()`, `kHop()`, `paths()`. Reads prefer the index and fall
back to a table scan when none is built, returning the same answer either way —
the SDK does no graph work in JS, it POSTs to the endpoint which owns the engine.

### Vector search

`Table` also carries real-time-maintained vector (ANN) search:
`addIndex(field, opts?)` declares a streaming Vamana (DiskANN) index on an
embedding field and runs the initial build; `listIndexes()` lists them;
`searchIndex(field, vector, opts?)` returns the `k` nearest neighbors from the
index attached to the table's exact current snapshot. A missing attachment is
an error. The durable `verglas-vamana-v1` Puffin file is published through the
table's Iceberg statistics metadata and cached locally for serving.

## Automatic run logging + observability

Every worker run — local or remote — emits standardized structured logs with **no
logging code in the worker**. The runner (`runWorker`) does it. Because the SDK
runs the same way against the local server and a cloud endpoint, this works
everywhere automatically.

### The `<name>_LOGS` standard table

A worker named `<name>` logs to a table `<name>_LOGS` — same namespace, `_LOGS`
suffix (`app.points` → `app.points_LOGS`). The name defaults to the worker's
`name`, or the configured output table; override with `RunWorkerOptions.name`. The
table is **auto-created on the first log commit** and is partitioned by the `day`
column.

Every worker's logs share one fixed shape, so a single dashboard works over any
`<name>_LOGS`:

| column | type | notes |
|---|---|---|
| `ts` | string | event time, epoch **nanoseconds** as a decimal string |
| `pipeline` | string | the worker/deployment name |
| `kind` | string | `worker` |
| `placement` | string | `local` \| `remote` (loopback endpoint ⇒ local) |
| `run_id` | string | unique per run; also the log-commit idempotency key |
| `event` | string | `run_start` \| `commit` \| `run_end` \| `error` \| your step |
| `level` | string | `info` \| `warn` \| `error` |
| `rows` | int | rows committed for the event |
| `duration_ms` | int | wall time for the event |
| `watermark` | string? | watermark at the event, when known |
| `message` | string | short human message |
| `error` | string? | error text on failure, else null |
| `day` | string | `YYYY-MM-DD` partition column, derived from `ts` |

### What the runner logs automatically

- `run_start` — with a generated `run_id`, tagging the trigger type.
- `commit` — one per append the run makes (the runner wraps the client so each
  `table(...).append(...)` also buffers a commit row with rows + duration +
  watermark).
- `error` — on failure, with the error message (never a token/key/credential).
- `run_end` — total rows, total duration, `info` on success / `error` on failure.

A worker can add its own steps via `ctx.log(event, fields)`; only the standard
fields (`level`, `rows`, `duration_ms`, `watermark`, `message`, `error`) are
recorded — any other key is dropped, so a stray value (e.g. a URL carrying an API
key) can never land in the logs table.

### Batching + idempotency

Log rows are **buffered and committed once per run** (not a commit per step), and
the log commit is keyed by `run_id`, so retrying a run under the same `run_id`
does not double-log. Writing logs is **best-effort**: any failure is swallowed and
reported with `console.warn` — logging can never fail a worker run.

```ts
import { runWorker } from "@verglas/sdk";

await runWorker(worker, ctx);                       // auto-logs to `<output>_LOGS`
await runWorker(worker, ctx, { name: "app.points", runId }); // reuse runId across a retry
await runWorker(worker, ctx, { logging: false });   // opt out entirely
```

### Retention (3-day TTL)

`<name>_LOGS` retains a standard 3-day TTL, but the SDK does not own it. The
serving runtime enforces retention: the server's housekeeping (and the cloud
committer's daily control-plane tick) drops `day` partitions past the cutoff. The
SDK only writes the standard `day`-partitioned rows; growth control lives with
whatever serves the lakehouse.

### Charting

Observability helpers live at the `@verglas/sdk/logging` subpath. `logsCharting(name)`
returns a charting declaration whose `chart` spec is the standard logs dashboard:
grouped by `event` and `kind` over `ts`, with measures `runs` (count), `errors`
(error rate), `rows` (sum), and `duration_{p50,p95,p99}`. The deploy path calls
`observabilityFor(name)` when a worker is registered to learn its logs table and
default charting declaration, so every worker gets observability with no per-worker
wiring:

```ts
import { observabilityFor } from "@verglas/sdk/logging";

const obs = observabilityFor("app.points");
// obs.charting  → the charting declaration to attach alongside the worker
// obs.logsTable → the `<name>_LOGS` table the runtime prunes
```

**Renderer (flagged):** this SDK defines the chart *declaration*, not the chart
*renderer*. The renderer is the missing consumer that reads `<name>_LOGS` per the
`chart` spec and draws run rates, error counts, rows, and latency percentiles.

## The fleet / edge host entry

`src/subprocess/endpoint-run.ts` is the bun entry a fleet microVM execs to run a
tenant worker against the live commit service, mirroring exactly what the
platform's cron dispatch does. **One invocation = one bounded worker run for one
scheduled interval.**

It maps the process environment onto a `WorkerContext`: it builds the cron trigger
from the logical-time bindings `VERGLAS_LOGICAL_DATE` / `VERGLAS_INTERVAL_START` /
`VERGLAS_INTERVAL_END` (a dispatch that carries none leaves the interval fields
undefined), maps the
`TARGET` binding to `ctx.output`, and runs the worker's handler once through
`runWorker` (so run logging to `<TARGET>_LOGS` is identical to the platform path).
There is **no durable watermark** here — the control plane owns scheduling,
backfill, and replay; this entry just runs the interval it was handed.

Required env: `VERGLAS_ENDPOINT`, `VERGLAS_TOKEN`, `DEPLOYMENT`, `TARGET`. Worker
secrets are already in the environment; the worker reads them off `ctx.env`. The
result file (`RESULT_PATH`, default `/run/result.json`) is always written:
`{"rows": n, "error": null}` on success, `{"rows": 0, "error": "<message>"}` on
failure, and the process exits 0/1 to match. Usage:
`bun endpoint-run.ts <module>` where `<module>` default-exports a worker.

## Reference workers

Domain-neutral templates live under the `@verglas/sdk/examples` subpath, one per
trigger shape:

- `httpPollWorker` — a cron worker: poll a JSON HTTP endpoint and append the rows
  in the trigger's logical interval.
- `webhookWorker` — a webhook worker: land each inbound request body as a row.
- `changeFanoutWorker` — an Iceberg-event worker: transform each committed batch on
  a watched table and append the results under a snapshot-keyed idempotency key.

## Public API

The root exports exactly two layers; internals live behind subpaths.

- Data-plane verbs:
  - `connect(opts)` → `VerglasClient`; `client.table<T>(name)`,
    `client.queue<T>(name)`, `client.graph(namespace)`, `client.createTable()`,
    `client.follow(table, handler, opts?)`,
    `client.followRows(table, handler, opts?)`
  - `Table`: `snapshot()`, `scan(opts?)`, `delta(since, opts?)`,
    `append(rows, opts?)`, `addIndex(field, opts?)`, `listIndexes()`,
    `searchIndex(field, vector, opts?)`
  - `Queue`: `enqueue(rows)`, `poll(group, opts?)`, `ack(group, position)`
  - `Graph`: `create()`, `insertNodes()`, `insertEdges()`, `buildIndex()`,
    `show()`, `neighbors()`, `kHop()`, `paths()`
- The worker contract: `defineWorker(def)`, `runWorker(worker, ctx, opts?)`; types
  `WorkerContext`, `WorkerHandler`, `WorkerDefinition`, `WorkerResult`,
  `RunWorkerOptions`; runtime event `CloudEvent`;
  trigger specs `TriggerSpec`, `CronTriggerSpec`, `WebhookTriggerSpec`,
  `EventTriggerSpec`
- Errors: `VerglasHttpError`
- Types: `Row`, `Watermark`, `ConnectOptions`, `ScanOptions`, `ScanResult`,
  `DeltaResult`, `Snapshot`, `FollowRowsOptions`, `FollowHandler`, `ChangeEvent`,
  `ChangeHandler`, `FollowFeedOptions`, `FeedSubscription`, `CommitOptions`,
  `CommitResult`, plus the table/queue/graph/index result types

Subpaths (internals the runner uses on the author's behalf — import only when
building deploy tooling or tests): `@verglas/sdk/logging` (run logging +
observability: `logsCharting`, `observabilityFor`), `@verglas/sdk/examples`
(reference workers).

## Development

```
npm install
npm run typecheck   # tsc --noEmit
npm test            # vitest
```

No secrets in code or tests — the tests use fake tokens against a local mock
endpoint.
</content>
</invoke>
