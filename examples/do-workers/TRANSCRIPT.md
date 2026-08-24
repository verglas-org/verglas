# AC5 acceptance transcript

Captured 2026-08-24 on macOS from `/Users/jfbrown/code/verglas`.
The component files are intentionally built into `/tmp/verglas-do-poc`; the two
checked-in `gateway.json` files point at those content-addressed artifacts.
Output below is trimmed to protocol and acceptance evidence; the long Wasmtime
cold-start progress emitted by `curl` is omitted.

## Environment and artifacts

```text
$ pwd
/Users/jfbrown/code/verglas
$ node --version && npm --version && python3 --version && cargo --version && rustc --version
v25.5.0
11.8.0
Python 3.14.6
cargo 1.96.1 (356927216 2026-06-26)
rustc 1.96.1 (31fca3adb 2026-06-26)
$ sdks/worker-py/.venv/bin/componentize-py --version
componentize-py 0.25.0
```

Build commands and digests:

```text
$ rm -rf /tmp/verglas-do-poc && mkdir -p /tmp/verglas-do-poc/js-build /tmp/verglas-do-poc/py-build
$ node sdks/worker-js/bin/build.mjs examples/do-workers/js-counter --out /tmp/verglas-do-poc/js-build
2eff935af3a65f4e4e0e69d0c643943af5b49bdeacb363f5f7973439d958f791
$ sdks/worker-py/.venv/bin/python sdks/worker-py/build.py examples/do-workers/py-counter --out /tmp/verglas-do-poc/py-build
5a072659ed7a2805765b729a1ff3dfc66f4bc322ddedaf13a27489fbc86d8860
$ sha256sum /tmp/verglas-do-poc/js-build/*.wasm /tmp/verglas-do-poc/py-build/*.wasm
2eff935af3a65f4e4e0e69d0c643943af5b49bdeacb363f5f7973439d958f791  .../js-build/2eff935af3a65f4e4e0e69d0c643943af5b49bdeacb363f5f7973439d958f791.wasm
5a072659ed7a2805765b729a1ff3dfc66f4bc322ddedaf13a27489fbc86d8860  .../py-build/5a072659ed7a2805765b729a1ff3dfc66f4bc322ddedaf13a27489fbc86d8860.wasm
```

The gateway manifests are `examples/do-workers/js-counter/gateway.json` and
`examples/do-workers/py-counter/gateway.json`. Each contains the Wrangler
`name`, `main`, and binding fields plus `component_digest`, `component_dir`, and
`data_root`; the source `wrangler.jsonc` files remain the SDK input manifests.

Binaries:

```text
$ cargo build -p verglas-runtime -p verglas-gateway
Finished `dev` profile [unoptimized + debuginfo]
```

## Integration diagnostics and fixes encountered

These were real failures before the final run, not suppressed:

```text
$ curl -iS -X POST http://127.0.0.1:18080/do/COUNTER/global/incr
HTTP/1.1 502 Bad Gateway
celld rejected Durable Object spawn: Durable Object COUNTER--global-replica did not become socket-ready
```

The worker component takes roughly 67–74 seconds to instantiate cold; the old
2-second celld readiness fence was too short. The readiness regression test was
first run against the old code and failed with:

```text
ReadinessTimeout("slow-worker")
```

After the readiness budget was extended, the real replica reached its socket.
The next real commit exposed the identity seam:

```text
Error: Host(Backend { message: "commit authority failed: replica endpoint rejected persistence: ERR transaction belongs to DO COUNTER--global, expected COUNTER--global-replica" })
```

The gateway now spawns the follower under a host-local pager key while passing
the exact logical DO identity to both `verglasd` processes. The control test for
`SPAWN` followed by `SPAWN_WORKER` was written first and failed with:

```text
ERR Durable Object agent-pair is already supervised on this host
```

The native SQL DDL bridge was also test-driven. Before it, the first fetch after
`init` failed with:

```text
DataFusion operation failed: Error during planning: table 'datafusion.public.counter' not found
```

The bridge registers supported DataFusion `CREATE TABLE` schemas through the
engine's native `create_table` API. The original examples then reached the
engine's explicitly unsupported UPDATE path:

```text
UPDATE operation on table 'counter'
This feature is not implemented: UPDATE not supported for Base table
```

Both examples now use the immutable create/insert/select surface: each increment
appends one `global` row and reads `COUNT(*)`. No SQL path or error was faked.
Finally, the gateway reads the replica `STATUS` applied fence before launching a
Worker, so a restart does not falsely require sequence zero.

## JS counter: `js-counter`

Digest: `2eff935af3a65f4e4e0e69d0c643943af5b49bdeacb363f5f7973439d958f791`

Start the whole stack with one persistent root:

```sh
rm -rf /tmp/verglas-do-poc/js-data /tmp/verglas-do-poc/js-gateway.log /tmp/verglas-do-poc/js-celld.log
mkdir -p /tmp/verglas-do-poc/js-data
target/debug/celld-host \
  --host-id cell-js \
  --root /tmp/verglas-do-poc/js-data \
  --child "$PWD/target/debug/verglasd" \
  --control /tmp/verglas-do-poc/js-data/celld.sock \
  >/tmp/verglas-do-poc/js-celld.log 2>&1 & CELLD_PID=$!
while [ ! -S /tmp/verglas-do-poc/js-data/celld.sock ]; do sleep .02; done
target/debug/verglas-gateway \
  --manifest examples/do-workers/js-counter/gateway.json \
  --listen 127.0.0.1:18080 \
  --celld-control /tmp/verglas-do-poc/js-data/celld.sock \
  --data-root /tmp/verglas-do-poc/js-data \
  >/tmp/verglas-do-poc/js-gateway.log 2>&1 & GATEWAY_PID=$!
```

HTTP increments and read:

```text
$ time curl -iS --max-time 180 -X POST http://127.0.0.1:18080/do/COUNTER/global/incr
HTTP/1.1 200 OK
content-type: application/json
{"count":1}

$ curl -iS -X POST http://127.0.0.1:18080/do/COUNTER/global/incr
HTTP/1.1 200 OK
{"count":2}
$ curl -iS -X POST http://127.0.0.1:18080/do/COUNTER/global/incr
HTTP/1.1 200 OK
{"count":3}
$ curl -iS http://127.0.0.1:18080/do/COUNTER/global
HTTP/1.1 200 OK
{"count":3}
```

WebSocket echo plus SQL-backed count:

```text
$ printf 'hello\n' | websocat -q -t -n --max-messages 1 --max-messages-rev 2 ws://127.0.0.1:18080/do/COUNTER/global/ws
hello
{"count":3}
```

Stop, preserving the same root:

```sh
kill -INT "$CELLD_PID"
kill -TERM "$GATEWAY_PID"
wait "$CELLD_PID" "$GATEWAY_PID" 2>/dev/null || true
# /tmp/verglas-do-poc/js-data/COUNTER--global/1/replica.sqlite remains present
# /tmp/verglas-do-poc/js-data/COUNTER--global-replica/1/replica.sqlite remains present
```

Restart with the same root and query again:

```sh
target/debug/celld-host --host-id cell-js --root /tmp/verglas-do-poc/js-data \
  --child "$PWD/target/debug/verglasd" \
  --control /tmp/verglas-do-poc/js-data/celld.sock \
  >/tmp/verglas-do-poc/js-celld-restart.log 2>&1 & CELLD_PID=$!
while [ ! -S /tmp/verglas-do-poc/js-data/celld.sock ]; do sleep .02; done
target/debug/verglas-gateway --manifest examples/do-workers/js-counter/gateway.json \
  --listen 127.0.0.1:18080 \
  --celld-control /tmp/verglas-do-poc/js-data/celld.sock \
  --data-root /tmp/verglas-do-poc/js-data \
  >/tmp/verglas-do-poc/js-gateway-restart.log 2>&1 & GATEWAY_PID=$!
time curl -iS --max-time 180 http://127.0.0.1:18080/do/COUNTER/global
```

Proof:

```text
HTTP/1.1 200 OK
content-type: application/json
{"count":3}
```

The replica status observed after the restart was `OK replica 10 0 0`; the
Worker command carried the applied fence read from the replica (`--start-sequence
8` in the process listing before subsequent restart GET events advanced the
replica). The count is recovered from committed rows, not process memory.

Stop the JS restart stack:

```sh
kill -INT "$CELLD_PID"; kill -TERM "$GATEWAY_PID"
wait "$CELLD_PID" "$GATEWAY_PID" 2>/dev/null || true
```

## Python counter: `py-counter`

Digest: `5a072659ed7a2805765b729a1ff3dfc66f4bc322ddedaf13a27489fbc86d8860`

Start commands (same gateway port, new persistent root):

```sh
rm -rf /tmp/verglas-do-poc/py-data /tmp/verglas-do-poc/py-gateway.log /tmp/verglas-do-poc/py-celld.log
mkdir -p /tmp/verglas-do-poc/py-data
target/debug/celld-host --host-id cell-py --root /tmp/verglas-do-poc/py-data \
  --child "$PWD/target/debug/verglasd" \
  --control /tmp/verglas-do-poc/py-data/celld.sock \
  >/tmp/verglas-do-poc/py-celld.log 2>&1 & CELLD_PID=$!
while [ ! -S /tmp/verglas-do-poc/py-data/celld.sock ]; do sleep .02; done
target/debug/verglas-gateway --manifest examples/do-workers/py-counter/gateway.json \
  --listen 127.0.0.1:18080 \
  --celld-control /tmp/verglas-do-poc/py-data/celld.sock \
  --data-root /tmp/verglas-do-poc/py-data \
  >/tmp/verglas-do-poc/py-gateway.log 2>&1 & GATEWAY_PID=$!
```

HTTP proof:

```text
$ time curl -iS --max-time 240 -X POST http://127.0.0.1:18080/do/COUNTER/global/incr
HTTP/1.1 200 OK
{"count":1}
$ curl -iS -X POST http://127.0.0.1:18080/do/COUNTER/global/incr
HTTP/1.1 200 OK
{"count":2}
$ curl -iS -X POST http://127.0.0.1:18080/do/COUNTER/global/incr
HTTP/1.1 200 OK
{"count":3}
$ curl -iS http://127.0.0.1:18080/do/COUNTER/global
HTTP/1.1 200 OK
{"count":3}
```

WebSocket proof:

```text
$ printf 'hello\n' | websocat -q -t -n --max-messages 1 --max-messages-rev 2 ws://127.0.0.1:18080/do/COUNTER/global/ws
hello
{"count":3}
```

Stop and restart on exactly the same Python root:

```sh
kill -INT "$CELLD_PID"; kill -TERM "$GATEWAY_PID"
wait "$CELLD_PID" "$GATEWAY_PID" 2>/dev/null || true
# the worker and replica SQLite pagers remain under /tmp/verglas-do-poc/py-data

target/debug/celld-host --host-id cell-py --root /tmp/verglas-do-poc/py-data \
  --child "$PWD/target/debug/verglasd" \
  --control /tmp/verglas-do-poc/py-data/celld.sock \
  >/tmp/verglas-do-poc/py-celld-restart.log 2>&1 & CELLD_PID=$!
while [ ! -S /tmp/verglas-do-poc/py-data/celld.sock ]; do sleep .02; done
target/debug/verglas-gateway --manifest examples/do-workers/py-counter/gateway.json \
  --listen 127.0.0.1:18080 \
  --celld-control /tmp/verglas-do-poc/py-data/celld.sock \
  --data-root /tmp/verglas-do-poc/py-data \
  >/tmp/verglas-do-poc/py-gateway-restart.log 2>&1 & GATEWAY_PID=$!
time curl -iS --max-time 240 http://127.0.0.1:18080/do/COUNTER/global
```

Proof:

```text
HTTP/1.1 200 OK
content-type: application/json
{"count":3}
```

The replica status observed after the Python restart was `OK replica 9 0 0`.
The state was replayed from the committed SQLite/replica log in the same root.

```sh
kill -INT "$CELLD_PID"; kill -TERM "$GATEWAY_PID"
wait "$CELLD_PID" "$GATEWAY_PID" 2>/dev/null || true
```

## Verification commands

All touched Rust crates were checked without weakening existing assertions:

```text
$ cargo fmt --all --check
$ cargo build -p verglas-celld -p verglas-gateway -p verglas-do-engine -p verglas-runtime
Finished `dev` profile [unoptimized + debuginfo]
$ cargo test -p verglas-celld
6/5/3/3/3/6 integration tests passed (including the slow-start and paired-spawn regressions)
$ cargo test -p verglas-gateway
5 gateway tests and 6 manifest tests passed
$ cargo test -p verglas-do-engine -- --test-threads=1
All engine targets passed, including `sql_create_table_registers_native_schema`
$ cargo test -p verglas-runtime -- --test-threads=1
All runtime targets passed, including real celld acceptance and event tests
$ cargo clippy -p verglas-celld --all-targets -- -D warnings
Finished
$ cargo clippy -p verglas-gateway --all-targets -- -D warnings
Finished
$ cargo clippy -p verglas-do-engine --all-targets -- -D warnings
Finished
$ cargo clippy -p verglas-runtime --all-targets -- -D warnings
Finished
$ npm test                         # from sdks/worker-js
9 passed
$ python3 -m unittest discover -s sdks/worker-py/tests -v
9 tests, OK
```

The first parallel/default DO-engine run hit the command's 300-second harness
limit after compiling and starting the suite; the serial rerun above completed
all targets successfully. No acceptance criterion remained unsatisfied in the
final run.
