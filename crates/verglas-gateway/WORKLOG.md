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
