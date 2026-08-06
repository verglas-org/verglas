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
