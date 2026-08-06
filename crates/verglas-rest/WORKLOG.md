# Worklog

- #8: Added the on-prem REST composition layer that mounts the query API and shallow catalog proxy on one service. Cloud roles continue to use their domain crates directly.
- #11: Added the standalone scheduler HTTP ingress and inspection surface. The REST layer accepts trigger invocations and exposes durable jobs while the scheduler crate owns queue semantics, keeping transport separate from scheduling state.
- #11: Added the on-prem scheduler queue transport and pushed-event ingress wiring. HTTP worker routes now preserve method, path, end-to-end headers, and body while lease renewal, completion, attempt inspection, and exact deadline APIs remain storage-only REST operations.
- #11: Removed the scheduler persistence facade after moving the queue to Postgres in the standalone service. REST now only validates deployments and forwards bounded manual, HTTP callback, and catalog-update events when scheduling is configured.
- #11: Unified manual, HTTP, and catalog ingress around CloudEvents. Generic event fan-out now matches exact subscription attributes and forwards the unchanged envelope to the scheduler.
- #11: Exposed structured CloudEvents through the on-prem REST service and preserved their envelopes during exact worker-subscription fan-out. The write ingress also forwards idempotency keys, allowing broker redelivery to remain safe at the Iceberg commit boundary.
- #16: Added the optional on-prem Rill dashboard API. It resolves Iceberg table locations through the configured catalog, manages owned Rill connector/model/metrics/Explore files over Rill's runtime API, and refuses to overwrite unowned resources without a filesystem fallback.
- #29: Added authenticated raw-byte KV get, put, delete, and deterministic prefix-list routes. The routes enforce tenant, namespace, and verb scopes before storage and keep keys, values, tokens, and metadata out of logs.
- #42: Forwarded typed query arguments through the isolated query role and removed the dashboard resource API. Verglas now serves Rill as a query engine instead of managing Rill resources.
