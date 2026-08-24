# Verglas JavaScript Durable Object Workers

This private package turns a small JavaScript Worker project into a WebAssembly
Component for the Verglas Durable Object runtime. It is not published to npm.
The build output targets the `durable-object` world in
`crates/verglas-do-wasm/wit`.

## Build a project

A project contains `wrangler.jsonc` and the JavaScript module named by its
`main` field. The supported manifest is deliberately small:

```jsonc
{
  "name": "counter",
  "main": "worker.js",
  "durable_objects": {
    "bindings": [
      { "name": "COUNTER", "class_name": "Counter" }
    ]
  }
}
```

Unknown top-level manifest keys are errors. `name`, `main`, and every binding's
`name` and `class_name` are required. The build command bundles the module and
shim with esbuild, invokes ComponentizeJS through jco, and writes the component
as `<sha256>.wasm`:

```sh
node sdks/worker-js/bin/build.mjs ./my-worker --out ./build
```

The output directory also contains `manifest.out.json` with the project name,
component digest, and binding list. The digest is the lowercase SHA-256 of the
component bytes.

## Worker module contract

The module has one default export. It is an object with a required `fetch`
hook and optional lifecycle hooks:

```js
export default {
  async init(env) {},
  async fetch(request, env) {
    return { status: 200, headers: { "content-type": "text/plain" }, body: "ok" };
  },
  async alarm(scheduledEpochMillis, env) {},
  async webSocketMessage(socketId, message, env) {},
  async webSocketClose(socketId, code, reason, env) {},
};
```

`request.method`, `request.url`, and `request.headers` are available. The
headers object has `get`, `has`, `set`, `append`, `entries`, and iteration.
`request.text()` and `request.json()` read the WIT body bytes. The message passed
to `webSocketMessage` is a `Uint8Array`; socket IDs are `bigint` values because
the WIT type is `u64`.

A fetch hook returns a response-like object with an integer `status`, headers
as an object, `Headers`-like value, or tuple array, and a body that is a string,
`Uint8Array`, `ArrayBuffer`, or byte array. A response body `ReadableStream` is
not supported. Exceptions from any hook become the WIT `handler-error` result.

## Environment capabilities

Every hook receives the same `env` object. Calls are synchronous host calls, so
tenant code may use them directly or await them.

- `env.storage.get(key, representation)` reads bytes and returns `null` when
  absent. `representation` is `"bytes"` (the default), `"string"`, `"json"`,
  or `{ type: ... }`. Convenience methods `getBytes`, `getString`, and `getJson`
  are also available.
- `env.storage.put(key, value)` accepts a string or byte value. Use
  `putBytes`, `putString`, `putJson`, or `{ type: "json" }` for explicit helpers.
- `env.storage.delete(key)` returns whether a key was removed.
- `env.storage.list(prefix, limit)` returns the bounded key list. The default
  limit is 1000.
- `env.sql(statement)` executes the statement through WIT `sql-rows` and
  returns the decoded JSON array of row objects. SQL errors and malformed row
  JSON throw; there is no alternate SQL path.
- `env.setAlarm(epochMilliseconds)`, `env.getAlarm()`, and
  `env.deleteAlarm()` operate on the DO's one durable alarm. `getAlarm()`
  returns an epoch-millisecond number or `null`.
- `env.sockets.send(socketId, data)` sends bytes or text after the event
  commits. `close(socketId, code, reason)`, `setAttachment`,
  `getAttachment`, `getAttachmentString`, and `attached` expose the remaining
  socket imports. Attachments use the same byte/string representation helpers.

The shim imports only `verglas:do-worker/storage@0.1.0` and
`verglas:do-worker/sockets@0.1.0`. The build disables optional StarlingMonkey
WASI features (`--disable=all`); the Rust host supplies WASI Preview 2 when a
component needs it, but this pipeline's output does not request a WASI
interface.

## Local checks

Install the pinned dependencies and run:

```sh
npm install
npm test
```

The tests reject unsupported manifest fields, check required fields, exercise
the request/response and byte helpers, build a component, inspect its WIT with
`jco wit`, and build unchanged source twice to compare digests. ComponentizeJS
0.22.0 currently emits a nondeterministic StarlingMonkey snapshot: the test
records both SHA-256 values when unchanged source produces different bytes.
The build still hashes the exact component bytes and never substitutes a source
hash. These checks do not execute a component under `verglasd`; runtime
assembly and persistence are host-side integration work.
