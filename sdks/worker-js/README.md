# Verglas JavaScript Cloudflare Workers

This private package builds an ordinary Cloudflare-style Worker project into a
WebAssembly component. Tenant code imports `DurableObject` from
`cloudflare:workers`; it does not import a Verglas shim.

## Build a project

The supported `wrangler.jsonc` subset is:

```jsonc
{
  "name": "counter",
  "main": "worker.js",
  "compatibility_date": "2025-01-01",
  "compatibility_flags": [],
  "durable_objects": {
    "bindings": [
      { "name": "COUNTER", "class_name": "Counter" }
    ]
  },
  "migrations": [
    { "tag": "v1", "new_sqlite_classes": ["Counter"] }
  ],
  "vars": { "GREETING": "hello" }
}
```

Unknown keys are errors. Migration entries accept `tag`, `new_classes`, and
`new_sqlite_classes`; other migration kinds are errors. The build command is:

```sh
node sdks/worker-js/bin/build.mjs ./my-worker --out ./build [--gateway <gateway.json>]
```

The builder bundles the project, aliases the `cloudflare:workers` module, and
componentizes it against the `service` WIT world. It writes a digest-named
`<sha256>.wasm` artifact and a Wrangler-shaped `manifest.out.json` whose
`artifacts.worker` descriptor (and `artifacts.durable_object` when Durable
Objects are declared) names the exact digest and output directory. If the
selected gateway manifest exists, it must already use those nested descriptors;
the builder updates them and preserves a final newline. Retired top-level
`component_digest` and `component_dir` fields are rejected.

## Cloudflare authoring surface

A project uses the documented Cloudflare shape:

```js
import { DurableObject } from "cloudflare:workers";

export class Counter extends DurableObject {
  async fetch(request) {
    const row = this.ctx.storage.sql.exec("SELECT count FROM counter").one();
    return Response.json(row);
  }
}

export default {
  async fetch(request, env, ctx) {
    const id = env.COUNTER.idFromName("global");
    return env.COUNTER.get(id).fetch(request);
  },
};
```

`env` contains `vars` and Durable Object namespace bindings. A namespace
implements `idFromName`, `idFromString`, `newUniqueId`, and `get`. Named IDs
are lowercase hexadecimal SHA-256 digests of their names. A stub's `fetch`
method is routed through the WIT `bindings.do-fetch` host capability.

`ctx.waitUntil(promise)` is awaited before a Worker event completes. This is a
known v0 divergence from Cloudflare's ability to continue work after sending a
response. `passThroughOnException` is accepted by the context but has no
pass-through host route.

### Pipeline Stream bindings

The manifest accepts the exact Cloudflare-shaped binding form:

```jsonc
"pipelines": [
  { "binding": "STREAM", "stream": "stream-id" }
]
```

Unknown keys are hard errors. `env.STREAM` is a dedicated binding with only
`send(records)`. It requires an array whose values are JSON-serializable,
encodes the array as compact UTF-8 JSON, and rejects encoded requests above
5 MiB before calling `verglas:do-worker/bindings@0.1.0` `do-fetch`. The call is
`(binding, stream, { method: "POST", uri: "https://verglas.internal/stream/append",
headers: [["content-type", "application/json"]], body, ws: undefined })`.
Only a 2xx response resolves the Promise; host errors and every other status
reject, with no fallback path. Structured Stream schema validation is not part
of this SDK surface yet; Pipeline processing must perform that validation.

## Durable Object storage and events

`DurableObject` receives the Cloudflare `ctx` and `env` constructor arguments.
`ctx.storage.get`, `put`, `delete`, and `list` send structured-clone bytes over
the WIT storage verbs. The guest does not create a KV table and does not submit
canonical transaction envelopes; the sandboxed host owns event transactions,
commit ordering, and durability. Missing storage capabilities fail clearly.

`ctx.storage.sql.exec(query, ...bindings)` sends one SQL statement to
`storage.sql-rows`. The returned cursor has `toArray()`, `one()`, `raw()`,
`columnNames`, `rowsRead`, and `rowsWritten`. Positional bindings are rendered
as SQL literals before crossing WIT because the v2 SQL verb carries one
statement string. SQL rows are JSON objects supplied by the host.

`ctx.storage.setAlarm`, `getAlarm`, and `deleteAlarm` call the alarm WIT verbs.
The handler export maps `alarm()` to the user's Durable Object alarm method.

The guest uses StarlingMonkey's real WHATWG `Request`, `Response`, `Headers`,
`WebSocketPair`, and WebSocket globals. Only request and response records with
byte bodies and ordered header tuples cross WIT. A pending upgrade request is
accepted by `ctx.acceptWebSocket(server)` and a `Response` with status 101 and
the client WebSocket; the handler response carries the accepted WIT socket ID.
Socket messages and closes are delivered as Durable Object methods.

## Component imports and audit

The component imports only:

- `verglas:do-worker/storage@0.1.0`
- `verglas:do-worker/sockets@0.1.0`
- `verglas:do-worker/bindings@0.1.0`

It exports the Worker `fetch` interface and the Durable Object `handler`
interface. ComponentizeJS is invoked with `--disable=all`; the build requests no
WASI imports. The storage adapter is a sandboxed guest adapter, unlike the
trusted TypeScript SDK transport that can encode canonical engine envelopes.

## Local checks

```sh
npm install
npm test
node sdks/worker-js/bin/build.mjs examples/do-workers/js-counter --out /tmp/js-build
```

The tests cover the SHA-256 namespace IDs, structured-clone storage, SQL
cursors, alarms, concurrency and waitUntil, HTTP boundary conversions, strict
Wrangler manifest parsing, digest/gateway synchronization, component WIT
conformance, and the no-WASI import audit.
