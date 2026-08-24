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
