# Worklog

- #8: Added the on-prem REST composition layer that mounts the query API and shallow catalog proxy on one service. Cloud roles continue to use their domain crates directly.
- #11: Added the standalone scheduler HTTP ingress and inspection surface. The REST layer accepts trigger invocations and exposes durable jobs while the scheduler crate owns queue semantics, keeping transport separate from scheduling state.
- #11: Added the on-prem scheduler queue transport and pushed-event ingress wiring. HTTP worker routes now preserve method, path, end-to-end headers, and body while lease renewal, completion, attempt inspection, and exact deadline APIs remain storage-only REST operations.
- #11: Removed the scheduler persistence facade after moving the queue to Postgres in the standalone service. REST now only validates deployments and forwards bounded manual, HTTP callback, and catalog-update events when scheduling is configured.
