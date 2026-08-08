# Verglas Docker container runtime

This crate places open-source Verglas workloads on the operator's Docker Engine. It is the local
placement adapter for Verglas container deployments; Verglas Cloud continues to use its
Firecracker adapter.

The runtime connects to the host engine through Docker's API and defaults to the local Docker
socket. Only this trusted runtime process receives Docker authority. Managed workloads never
receive the Docker socket or Docker client credentials.

Lifecycle reconciliation is label-owned and idempotent. Verglas refuses to mutate a same-named
container without its ownership labels, and replaces an owned container when its immutable
specification digest changes.

## Runtime manager

The `verglas-container-runtime` binary exposes an authenticated local desired-state API on port
`8360`. Desired declarations are stored in its runtime metadata volume and reconciled every five
seconds. Compose supplies the existing `verglas-runtime` Docker network, so managed services
resolve each other and the bootstrap server by stable container name.

```bash
curl --fail-with-body \
  -X PUT http://127.0.0.1:8360/v1/containers/example \
  -H "Authorization: Bearer $VERGLAS_CONTAINER_RUNTIME_TOKEN" \
  -H "Content-Type: application/json" \
  --data '{
    "deployment_id": "example",
    "image": "alpine:3.22",
    "command": ["sh", "-c", "while true; do sleep 3600; done"],
    "published_ports": [],
    "environment": {},
    "bind_mounts": []
  }'
```

Use `POST /v1/containers/{id}/stop`, `POST /v1/containers/{id}/resume`, and
`DELETE /v1/containers/{id}` for lifecycle changes. Deletion removes only the container whose
Verglas ownership and deployment labels match.

Compose does not statically start Postgres, Rill, or a scheduler. The locally built
`verglas/verglas-container-runtime:local` image also carries `verglas-scheduler`. A
declaration selects that executable with its `entrypoint`; the child receives neither the
Docker socket nor Docker environment. Database declarations consume the separately published
`ghcr.io/verglas-org/verglas-neon-storage:latest` and
`ghcr.io/verglas-org/verglas-neon-compute-v16:latest` images. Neon remains multiple declared
services (broker, pageserver, and compute), not a hidden single Postgres container.
