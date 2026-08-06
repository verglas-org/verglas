# Verglas Gadget runtime

`verglas-gadget-runtime` executes Verglas OS Gadget server modules outside
Cloudflare Workers. The same image supports two deployment shapes:

- A local managed container sets `VERGLAS_GADGET_ID` and runs one Gadget
  revision. The container runtime manager owns its Docker lifecycle.
- A cloud microVM sets `VERGLAS_GADGET_ID`. The runtime then accepts only that
  Gadget identity and enforces a capacity of one.

The OS remains the source of truth for Gadget code. Registration is deliberately
in memory: after a runtime restart, the OS registers each desired immutable
revision again. Re-registering the same ID, version, and bytes is idempotent;
reusing a version with different bytes is rejected.

## Use the managed runtime image

The default Compose stack does not start a shared Gadget supervisor. Its runtime
manager image already carries the Gadget executable and Bun host. Submit one
single-target declaration per Gadget with:

```json
{
  "deployment_id": "gadget-example",
  "image": "verglas/verglas-container-runtime:local",
  "entrypoint": ["verglas-gadget-runtime"],
  "environment": {
    "VERGLAS_GADGET_ID": "example",
    "VERGLAS_GADGET_RUNTIME_LISTEN": "0.0.0.0:8350",
    "VERGLAS_GADGET_RUNTIME_TOKEN": "scoped-secret"
  },
  "published_ports": [
    {"container_port": 8350, "host_port": 18350}
  ]
}
```

The declaration sets `VERGLAS_GADGET_ID`, its scoped runtime and KV
credentials, and a unique published host port. The token protects code
registration, client-module reads, and RPC upgrades. The health endpoint is
intentionally unauthenticated.

## Register and call Gadgets

Register an immutable source bundle:

```bash
curl --fail-with-body \
  -X PUT http://127.0.0.1:18350/v1/gadgets/hello \
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
`ws://127.0.0.1:18350/v1/gadgets/hello/rpc`, using the same bearer token on both
requests. `GET /v1/gadgets` lists selected revisions, and
`DELETE /v1/gadgets/{id}` removes a revision and stops its child process.

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `VERGLAS_GADGET_RUNTIME_LISTEN` | `0.0.0.0:8350` | Registry and RPC listener |
| `VERGLAS_GADGET_RUNTIME_TOKEN` | none | Required bearer token |
| `VERGLAS_GADGET_MAX_GADGETS` | `64` | Local registration ceiling |
| `VERGLAS_GADGET_ID` | unset | Constrain the runtime to one cloud Gadget |
| `VERGLAS_GADGET_HOST_COMMAND` | `bun` | Child JavaScript executable |
| `VERGLAS_GADGET_HOST_SCRIPT` | `/opt/verglas-gadget-runtime/host.mjs` | Cap'n Web host module |
| `VERGLAS_GADGET_STARTUP_SECS` | `15` | Child startup deadline |
| `VERGLAS_GADGET_KV_ENDPOINT` | unset | Verglas KV base URL |
| `VERGLAS_GADGET_KV_TOKEN` | unset | Scoped KV bearer token |

The KV endpoint and token must be set together. They are captured by the host
before ambient network globals are disabled and are exposed to Gadget code only
through `ctx.storage`.

## Security boundary

Each Gadget gets a separate source directory, module instance, loopback port,
and Bun process. The host removes ambient `fetch`, `WebSocket`, and `EventSource`
from Gadget globals. This process boundary is suitable for trusted local code;
it is not a hostile multi-tenant sandbox. Cloud deployments must place the
single-target runtime inside the platform's microVM/container sandbox and issue
deployment-scoped runtime and KV credentials.

Persistent capability restoration and the full Durable Object storage surface
are not implemented by this crate yet. `ctx.restore()` fails explicitly rather
than silently weakening capability semantics.
