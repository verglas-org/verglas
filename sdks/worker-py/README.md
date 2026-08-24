# Verglas Python Worker SDK

This SDK builds a Python Durable Object Worker into the
`verglas:do-worker@0.1.0` WebAssembly component. It is a prototype authoring
surface. The builder validates the small Wrangler manifest used by this
repository and does not run a Worker under a Verglas runtime.

## Toolchain

Use the local virtual environment. Do not install the toolchain globally:

```sh
python3 -m venv sdks/worker-py/.venv
. sdks/worker-py/.venv/bin/activate
python -m pip install -r sdks/worker-py/requirements.txt
componentize-py --version
# componentize-py 0.25.0
```

The pinned CLI's binding inspection command is:

```sh
componentize-py -d crates/verglas-do-wasm/wit -w durable-object \
  bindings /tmp/verglas-worker-py-bindings
```

The build entry point is:

```sh
python sdks/worker-py/build.py <project-dir> --out <output-dir> [--gateway <gateway.json>]
```

It invokes the pinned executable as:

```text
componentize-py -d <repository>/crates/verglas-do-wasm/wit -w durable-object componentize verglas_worker_entry -p <temporary-entry-dir> -p <sdk-dir> -p <project-dir> --stub-wasi -o <temporary-entry-dir>/worker.wasm
```

The temporary entry imports the project's `.py` main module and exposes the
shim's generated `Handler`. `--stub-wasi` removes component imports for the
Python runtime's ambient WASI facilities; the v0 host grants only the Verglas
storage and sockets interfaces. The output directory receives
`<lowercase-sha256>.wasm` and `manifest.out.json`. If the project directory
contains `gateway.json`, the builder updates its `component_digest` and
`component_dir`; use `--gateway` to select another manifest:

```json
{
  "name": "py-counter",
  "component_digest": "...",
  "bindings": [{ "name": "COUNTER", "class_name": "Counter" }]
}
```

Only `name`, `main`, and `durable_objects.bindings` are accepted. Unknown
top-level keys are hard errors. `main` must be an existing Python file inside
the project and must end in `.py`.

## Authoring contract

The main module exports a required synchronous callback:

```python
def fetch(request: Request, env: Environment) -> Response: ...
```

It may also export these optional callbacks:

```python
def init(env: Environment) -> None: ...
def alarm(scheduled_epoch_millis: int, env: Environment) -> None: ...
def websocket_message(socket: int, message: bytes, env: Environment) -> None: ...
def websocket_close(socket: int, code: int, reason: str, env: Environment) -> None: ...
```

The WIT world contains synchronous functions, so componentize-py generates a
synchronous `Handler` export. The adapter keeps that generated convention and
requires synchronous callbacks; it does not create an event loop inside a WIT
call. A callback exception, invalid response, or host `handler-error` is
represented as `WorkerError` at the public surface and as the WIT
`handler-error` result at the component boundary.

`Request` and `Response` mirror the WIT records. `headers` is a list of
`(name, value)` pairs, `body` is `bytes`, and `Response.status` is an integer
validated as WIT `u16` by the shim and generated component bindings. WebSocket
IDs and alarm deadlines are checked as non-negative WIT `u64` values, and
close codes are checked as `u16` values.

`env.storage` is transactional and exposes:

- `get(key)` / `get_bytes(key)` → `bytes | None`;
- `get_text(key, encoding="utf-8")`;
- `put(key, value)` and explicit `put_bytes` / `put_text` helpers, where text is
  UTF-8 and bytes-like values are copied;
- `delete(key)` → `bool`;
- `list(prefix="", limit=1000)` → key strings, with the WIT `u32` limit
  checked before the host call.

The WIT `%list` verb is generated as the Python `storage.list` method. The
shim does not translate this into a legacy name.

`env.sql(statement)` calls the versioned `storage.sql-rows` import and decodes
its JSON string with `json.loads`, returning `list[dict[str, Any]]`. It does
not fall back to the Arrow `storage.sql` bytes verb. Alarm methods are
`env.set_alarm(epoch_millis)`, `env.get_alarm()`, and `env.delete_alarm()`.
`env.sockets` provides `send`, `close`, `set_attachment`,
`get_attachment`, and `attached`; `send` accepts text or bytes-like values and
encodes text as UTF-8.

`list<u8>` values become Python `bytes`, WIT `u16`/`u64` values become Python
`int`, and generated result errors are converted to `WorkerError`. These are
componentize-py's generated mappings, not runtime emulation.

## Tests and standalone checks

Run the unit suite from the repository root:

```sh
python3 -m unittest discover -s sdks/worker-py/tests -v
```

The manifest tests cover JSONC comments/trailing commas, the accepted subset,
and rejection of unknown top-level fields and malformed Python projects. The
shim tests cover records, bytes/text storage helpers, SQL-row decoding, socket
encoding, and `WorkerError`.

A built file can be checked without a Rust runtime:

```sh
wasm-tools component wit <output-dir>/<digest>.wasm | head -80
wasm-tools component wit <output-dir>/<digest>.wasm | grep 'import wasi:'
wc -c <output-dir>/<digest>.wasm
python sdks/worker-py/build.py <project-dir> --out /tmp/build-a
python sdks/worker-py/build.py <project-dir> --out /tmp/build-b
```

Each output is content-addressed: its filename and `component_digest` must equal
the SHA-256 of that output's own bytes. `componentize-py` may emit different
bytes for unchanged source, so the two-build command is diagnostic only and is
not a reproducibility proof. This is not an end-to-end `verglasd` execution test.
