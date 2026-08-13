# Worklog
- #84: Pinned the complete stack to immutable published Verglas Neon
  storage/proxy and compute images. The runtime consumes only its isolated
  Neon policy-token volume; access mounts runtime state read-only solely to
  copy the manager-generated proxy TLS identity into an authenticated database
  proxy.

- #63: Added an authenticated, read-only Vessel manifest validation endpoint backed by the shared
  compositional contract; invalid compositions cannot partially mutate local desired state.
- #63: Added atomic compositional Vessel apply. The runtime builds independently versioned
  Integration and Interface projects, registers version-owned Workers, carries Integration setup
  schemas without credential values, rolls back failed reconciliation, and publishes one release
  view with its local Application preview.

- #61: Added bounded, content-addressed TypeScript Vessel project builds with declared npm
  dependencies, a platform-owned Bun Dockerfile, authenticated project deployment API, and
  reconciliation into standalone Application or Integration images.

- #43: Added manifest discovery and streaming namespace invocation for private Integration Vessels, with stable Vessel-name identity validation and no public workload ports.
- #49: Added the open-source Docker Engine placement crate with label-owned, idempotent container
  lifecycle reconciliation and fake-engine contract tests.
- #50: Added the authenticated persistent desired-state manager, shared-network and host-port
  placement, and the two-service Compose bootstrap.
- #50: Kept the self-host guide's tested Compose example synchronized with the two-service
  bootstrap after rebasing onto the merged runtime crate.
- #50: Built the runtime-manager image from the shared runtime base with scheduler only.
  It no longer layers on the Gadget runtime image or documents Gadgets as managed products.
- #55: Added persisted Vessel declarations, one-container mapping, and authenticated private-network
  HTTP proxying without publishing workload ports to the host.
- #55: Exposed GET-only localhost preview routes for Application Vessels while keeping Integration
  HTTP surfaces behind runtime authentication.
- #55: Removed the in-tree Linear integration and dashboard Application crates. Vessel persistence,
  proxying, and Application previews stay; product examples are no longer bundled.

- #65/#66: Strengthened Docker packaging assertions against gadget-runtime/gadget-host leftovers and required Bun from oven/bun; README now describes Docker-only local placement without Firecracker or Verglas Cloud.
- #75: Documented the default OSS Compose topology now that it starts the scheduler and its durable Postgres queue alongside the runtime manager. Dynamically added Vessels and database components remain owned by the runtime API.

- #52: Added persisted Vessel stop intent and authenticated stop/resume routes. Reconciliation now leaves an operator-stopped Application or Integration Vessel down across manager restarts and composition updates, while the existing desired-state file shape remains readable.
- #84: Added an explicit OCI platform to persistent container declarations and forwarded it through image pulls and container creation. Published amd64-only Neon images can now run under Docker Desktop emulation on arm64 hosts without an implicit architecture fallback.
- #84: Updated the complete-stack packaging contract to require Lakekeeper and the three cache-ring members used by managed Neon. The trusted container runtime remains the only service with the Docker socket and starts database-local Neon components on the shared private network.
- #84: Added a Compose health gate for the runtime manager so access-node startup recovery cannot race the manager's own desired-state recovery.
- #84: Reuse an already-present cross-architecture image before attempting a registry pull, while
  still applying the declared platform to pulls and container creation. This lets locally
  authenticated pulls of the published Neon images be consumed by the socket-backed runtime.
- #84: Added source-file delivery into stopped containers, content-sensitive reconciliation, and
  Docker-assigned loopback ports. Desired state retains only trusted source paths, so rotating a
  workload bearer replaces the consumer without persisting the bearer in deployments JSON.
- #84: Added a stable local self-signed PostgreSQL proxy identity under runtime state. The runtime
  creates it once before recovery and exposes only its file paths to managed proxy declarations.
- #107: Updated the complete-stack packaging contract for the one-shot Verglas Neon bootstrap and locally built independently provisioned queue-service image, removing the vanilla Postgres bootstrap services.
- #109: Added content-addressed locked worker builds for Python and Bun plus explicit Dockerfile projects. Added bounded one-shot execution with hard CPU, memory, PID, and timeout limits, runtime environment injection, result capture, bounded logs, and deterministic cleanup.
- #109: Unified Vessel and worker packaging behind one deterministic `ProjectBuildContext`. The runtime now returns the explicit SHA-256 build-context digest with every immutable worker image.
- #109: Added operator-owned per-worker scratch mounts for large bounded jobs. Worker declarations select only the container target; the runtime selects and validates the host root.
- #109: Added a separate named runtime network to the Compose application and attached the control plane and cache nodes explicitly. Runtime-created workers and database containers remain reachable when a deployment platform replaces the Compose default network.
- #109: Applied the shared runtime-network declaration to cache peers as well as
  control-plane services. This keeps deployment-platform Compose transforms
  consistent and preserves DNS reachability from runtime-created containers.
- #111: Updated the OSS packaging contract to require exactly one cache-node
  service instead of a three-process write-back topology.
