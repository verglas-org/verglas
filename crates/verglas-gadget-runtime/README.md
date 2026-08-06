# Verglas Gadget runtime

`verglas-gadget-runtime` executes Verglas OS Gadget server modules outside
Cloudflare Workers. The same image supports two deployment shapes:

- The local Compose service registers and supervises multiple Gadgets, with one
  Bun child process per Gadget revision.
- A cloud microVM sets `VERGLAS_GADGET_ID`. The runtime then accepts only that
  Gadget identity and enforces a capacity of one.

The OS remains the source of truth for Gadget code. Registration is deliberately
in memory: after a runtime restart, the OS registers each desired immutable
revision again. Re-registering the same ID, version, and bytes is idempotent;
reusing a version with different bytes is rejected.

## Run with Docker Compose

The repository's default Compose stack exposes the runtime at
`http://127.0.0.1:8350`:

```bash
export VERGLAS_GADGET_RUNTIME_TOKEN="$(openssl rand -hex 32)"
docker compose up -d --build
curl -fsS http://127.0.0.1:8350/healthz
```

The token protects code registration, client-module reads, and RPC upgrades.
The health endpoint is intentionally unauthenticated.

## Register and call Gadgets

Register an immutable source bundle:

```bash
curl --fail-with-body \
  -X PUT http://127.0.0.1:8350/v1/gadgets/hello \
  -H "Authorization: Bearer $VERGLAS_GADGET_RUNTIME_TOKEN" \
  -H 'Content-Type: application/json' \
  --data @- <<'JSON'
{
  "version": "01JEXAMPLE",
  "serverModule": "import { DurableObject } from 'cloudflare:workers'; export class Gadget extends DurableObject { hello(name) { return `Hello, ${name}`; } }",
  "clientModule": "export const title = 'Hello';",
  "files": {}
}
JSON
```

The OS loads the browser module from
`GET /v1/gadgets/hello/client.js` and opens Cap'n Web at
`ws://127.0.0.1:8350/v1/gadgets/hello/rpc`, using the same bearer token on both
requests. `GET /v1/gadgets` lists selected revisions, and
`DELETE /v1/gadgets/{id}` removes a revision and stops its child process.

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `VERGLAS_GADGET_RUNTIME_LISTEN` | `0.0.0.0:8350` | Registry and RPC listener |
| `VERGLAS_GADGET_RUNTIME_TOKEN` | none | Required bearer token |
| `VERGLAS_GADGET_MAX_GADGETS` | `64` | Local registration ceiling |
| `VERGLAS_GADGET_ID` | unset | Constrain the runtime to one cloud Gadget |
| `VERGLAS_GADGET_HOST_COMMAND` | `/usr/local/bin/bun` | Child JavaScript executable |
| `VERGLAS_GADGET_HOST_SCRIPT` | `/opt/verglas-gadget-runtime/host.mjs` | Cap'n Web host module |
| `VERGLAS_GADGET_STARTUP_SECS` | `15` | Child startup deadline |
| `VERGLAS_GADGET_DATA_ENDPOINT` | none | Verglas REST base used by the trusted capability proxy |
| `VERGLAS_GADGET_DATA_TOKEN` | none | Upstream bearer token held only by the runtime control process |

The data endpoint and token are required. The runtime control process retains
the upstream token and gives each child a derived, Gadget-scoped capability.
The child removes that capability configuration before loading generated code.
Both `ctx.storage` and the repository's `@verglas/sdk` client use this capability
after ambient network access is disabled; the client is injected as
`this.env.VERGLAS`. Generated server code can use the complete table, SQL,
queue, graph, vector-index, and KV handle surface without receiving a raw token.
Method return values travel back to the Gadget UI over Cap'n Web; durable data
changes must use the SDK client.

## Security boundary

Each Gadget gets a separate source directory, module instance, loopback port,
and Bun process. The host removes ambient `fetch`, `WebSocket`, and `EventSource`
from Gadget globals. This process boundary is suitable for trusted local code;
it is not a hostile multi-tenant sandbox. Cloud deployments must place the
single-target runtime inside the platform's microVM/container sandbox and issue
deployment-scoped runtime and data credentials.

Persistent capability restoration and the full Durable Object storage surface
are not implemented by this crate yet. `ctx.restore()` fails explicitly rather
than silently weakening capability semantics.
