# Worklog

- #0: Added the prototype `verglas-gateway` manifest parser, celld spawn seam,
  resident NDJSON event actors, and axum HTTP/WebSocket routes. Integration tests
  exercise fake celld and verglasd sockets so protocol ordering and commit-gated
  effects remain visible at the gateway boundary.

- #0: Replaced the provisional spawn seam with the real two-step celld sequence:
  a replica `SPAWN`, then component-bearing `SPAWN_WORKER`; the gateway supplies
  `<data-root>/<do-id>/events.sock` and waits with bounded backoff for its bind.
- #0: Spawned the replica under the exact Worker DO identity instead of a `-replica` suffix. The replica endpoint rejects committed envelopes whose identity differs, so this seam fix is required for real SQL commits and restart replay.
- #0: Read the replica `STATUS` fence before `SPAWN_WORKER` and pass its applied sequence as both Worker recovery fields. A gateway restart can now reuse the same local pager and replay authority without falsely requiring sequence zero.
- #0: Added the real-stack AC1 integration proof. It builds a JavaScript component, launches celld-host and verglasd, verifies committed storage and WebSocket effects after terminal ordering, and proves handler errors deliver no staged send while the session survives.
- #0: Added strict `managed_cas` manifest parameters and the exact `SPAWN_CAS_WORKER` gateway command. CAS deployments now skip the local replica spawn, carry the held ETag/version fence, and still wait for the component event socket before serving traffic.
- #0: Accepted Wrangler compatibility date and flags, class-introduction migrations, and arbitrary `vars` values in the gateway manifest. Migration kinds outside `new_classes` and `new_sqlite_classes` remain hard errors, while vars are retained without pretending the current WIT supplies an environment import.
- #0: Inverted ingress so public routes execute a component WorkerPool before resolving DO bindings, while `/do/...` remains an internal/debug path and managed-CAS spawning is unchanged. Added DO-originated do-call servicing with a typed serialized-gate self-call rejection, guest-driven WebSocket acceptance, and fake plus real-stack worker-first coverage.
- #171: Migrated gateway spawning to the single Turso control grammar and removed managed-CAS/replica arguments. Wrangler `pipelines` bindings now remain separate from durable-object namespaces, resolve the declared Stream object identity, and use the same typed do-fetch route. Tests were written first and failed on missing Turso/pipeline APIs, then passed with strict manifest/deployment validation, exact forwarding, and fail-closed credential checks. AC1 uses a compile-time-only helper binary that keeps the real WorkerRuntime/EventEndpoint/celld chain while calling TursoStore::open_for_test; production targets cannot enable it.
