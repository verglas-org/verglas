# Cloudflare Python Worker SDK

This SDK injects a `workers` module while componentizing a Cloudflare-style
Python Worker into a `verglas:do-worker@0.1.0` component. A project imports the
same names as Cloudflare's Python Workers runtime; it does not import a
Verglas-specific module.

The public shape follows Cloudflare's [Python Workers documentation](https://developers.cloudflare.com/workers/languages/python/):

```python
from workers import DurableObject, Response, WorkerEntrypoint

class Counter(DurableObject):
    def __init__(self, ctx, env):
        super().__init__(ctx, env)

class Default(WorkerEntrypoint):
    async def fetch(self, request):
        return Response("hello")
```

The module-level Cloudflare form is also accepted:

```python
from workers import Response

async def on_fetch(request, env):
    return Response("hello")
```

## Toolchain and build

Use the repository-local virtual environment:

```sh
python3 -m venv sdks/worker-py/.venv
. sdks/worker-py/.venv/bin/activate
python -m pip install -r sdks/worker-py/requirements.txt
componentize-py --version
# componentize-py 0.25.0
```

Build one Wrangler project with:

```sh
python sdks/worker-py/build.py <project-dir> --out <output-dir> [--gateway <gateway.json>]
```

The builder invokes `componentize-py` against the `service` world in
`crates/verglas-do-wasm/wit`. It injects `workers._component`, imports the
project's `main` module, and exports both the Worker `fetch` surface and the
Durable Object handler surface. The output directory receives
`<sha256-of-bytes>.wasm` and `manifest.out.json`; an existing `gateway.json`
gets the same digest and output-directory update used by the JavaScript builder.

The accepted Wrangler JSON/JSONC subset is:

- `name` and `main`;
- `compatibility_date` and `compatibility_flags`;
- `durable_objects.bindings` with `name` and `class_name`;
- `migrations` entries with `tag` and optional `new_sqlite_classes` or `new_classes`;
- `vars`;
- `pipelines` entries with exactly `binding` and `stream`.

Unknown keys are hard errors, including unknown nested migration keys and
pipeline entries. Other Cloudflare bindings are not silently ignored; they are
outside this milestone.

## Cloudflare Python surface

`Default(WorkerEntrypoint)` is instantiated for Worker-tier requests. The
alternative `on_fetch(request, env)` or `on_fetch(request, env, ctx)` function
is dispatched directly. Durable Object classes extend `DurableObject` and are
constructed with `(ctx, env)` from the declared Wrangler binding.

The binding object uses the Python spelling `id_from_name(name)`, `get(id)`, and
`stub.fetch(request)`. `id_from_name` computes lowercase `hex(sha256(name))`
locally and retains the original name for the flattened `bindings.do-fetch`
host call. `get_by_name(name)` is provided as the Python spelling of the same
named-stub operation for projects that use that Cloudflare helper.

### Pipeline Stream bindings

The manifest form follows Cloudflare's current [Stream binding
configuration](https://developers.cloudflare.com/pipelines/streams/writing-to-streams/):

```jsonc
"pipelines": [
  { "binding": "STREAM", "stream": "stream-id" }
]
```

The current Cloudflare Stream page documents `send(records)` but does not
specify a Python-specific binding class or alternate casing. This SDK therefore
exposes the same operation as an asynchronous `env.STREAM.send(records)` and
uses `PipelineBinding` for the concrete runtime object. The binding is separate
from Durable Object namespaces and has no namespace methods.

`send` requires a Python list of strict JSON values, encodes the compact array as
UTF-8, and accepts an encoded body of exactly 5 MiB (5 * 1024 * 1024 bytes).
It calls the WIT `bindings.do-fetch` capability with the binding name, stream ID,
`POST https://verglas.internal/stream/append`, and
`content-type: application/json`. Only a 2xx response resolves. Host failures
and non-2xx responses propagate, with no HTTP or other fallback path.

`Request` exposes `method`, `url`, case-insensitive `headers`, and a byte body;
`await request.text()`, `await request.bytes()`, and `await request.json()`
consume the body. `Response` accepts text or bytes and supports
`Response.json(value, status=...)`. The response `web_socket` option is paired
with `ctx.accept_websocket(server)` and becomes WIT `accept-ws`.

`ctx.storage.get`, `put`, `delete`, and `list` are asynchronous as in the
Cloudflare Python API. `get`/`delete` accept a key list and `put` accepts a
mapping as well as the single-key form. `list` returns a deterministic mapping
of keys to values.
`ctx.storage.sql.exec(statement, *bindings)` is synchronous and returns a
cursor with `one()`, `to_array()`, iteration, and `raw()`; rows support both
mapping and attribute access, following Cloudflare's [SQLite storage API](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/).
`set_alarm`, `get_alarm`, and `delete_alarm` use Python snake case. `ctx.get_websockets()`
returns attached WebSocket objects, and `webSocketMessage`/`webSocketClose`
retain Cloudflare's documented handler casing. `accept_websocket` is snake case
because the Python documentation exposes the state API through the Python
runtime's idiomatic layer; its wire result is the exact `accept-ws` field from
the [WebSocket hibernation API](https://developers.cloudflare.com/durable-objects/api/state/).

## Deterministic storage encoding

The WIT storage import carries bytes, while Cloudflare Durable Object storage
uses JavaScript structured clone. This implementation uses canonical UTF-8 JSON
with tagged values:

- `None`, booleans, integers, finite floats, strings, bytes, lists/tuples, and
  string-keyed dictionaries are supported;
- dictionary keys are sorted, integers and floats use canonical text, and bytes
  use base64;
- non-finite floats, non-string object keys, JavaScript `undefined`, `Date`,
  `Map`, object identity graphs, and other JavaScript-only values fail loudly.

This is a deliberate **Divergence** from Cloudflare's exact structured-clone
value model. It is deterministic and preserves the Python values used by the
supported examples; it is not presented as a byte-for-byte Cloudflare encoding.
WebSocket attachments use the same tagged encoding and remain bounded to 16 KiB.

The v0 `ctx.wait_until` implementation awaits queued work before event completion,
which is the documented compatibility-page divergence. SQL parameter bindings
are converted to SQLite literals before the one-string `sql-rows` import because
WIT v2 does not yet carry a parameter list; unsupported binding types fail rather
than being stringified.

## Verification

```sh
PYTHONPATH=sdks/worker-py python3 -m unittest discover -s sdks/worker-py/tests -v
python sdks/worker-py/build.py examples/do-workers/py-counter --out /tmp/py-counter-build
wasm-tools component wit /tmp/py-counter-build/<digest>.wasm | tail -80
wasm-tools validate /tmp/py-counter-build/<digest>.wasm
wc -c /tmp/py-counter-build/<digest>.wasm
```

The artifact must expose `worker`, `handler`, and imports for `storage`,
`sockets`, and `bindings`; its digest filename and manifest digest must equal
the SHA-256 of the emitted bytes.
