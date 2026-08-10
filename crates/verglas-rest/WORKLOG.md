# Worklog

- #84: Added database-scoped Iceberg REST proxy routes backed by the live catalog registry, replacing process-global routing for multi-database tenant runtimes.

## 2026-08-08 — Issue #81

- Added `/v1/access/*` principal, resource, grant, and decision routes over the backend-neutral authorizer.
- Added route-level coverage for inherited access decisions.
- Added `/v1/access/delegations` and `/v1/access/revocations` so user-facing approval paths cannot
  turn the access service's trusted service credential into an account-wide grant bypass.

- #43: Added the local reflected Integration namespace gateway. The primary Verglas endpoint now relays discovery and bounded or streaming method calls through the authenticated Docker runtime manager.
- #8: Added the on-prem REST composition layer that mounts the query API and shallow catalog proxy on one service. Cloud roles continue to use their domain crates directly.
- #11: Added the standalone scheduler HTTP ingress and inspection surface. The REST layer accepts trigger invocations and exposes durable jobs while the scheduler crate owns queue semantics, keeping transport separate from scheduling state.
- #11: Added the on-prem scheduler queue transport and pushed-event ingress wiring. HTTP worker routes now preserve method, path, end-to-end headers, and body while lease renewal, completion, attempt inspection, and exact deadline APIs remain storage-only REST operations.
- #11: Removed the scheduler persistence facade after moving the queue to Postgres in the standalone service. REST now only validates deployments and forwards bounded manual, HTTP callback, and catalog-update events when scheduling is configured.
- #11: Unified manual, HTTP, and catalog ingress around CloudEvents. Generic event fan-out now matches exact subscription attributes and forwards the unchanged envelope to the scheduler.
- #11: Exposed structured CloudEvents through the on-prem REST service and preserved their envelopes during exact worker-subscription fan-out. The write ingress also forwards idempotency keys, allowing broker redelivery to remain safe at the Iceberg commit boundary.
- #16: Added the optional on-prem Rill dashboard API. It resolves Iceberg table locations through the configured catalog, manages owned Rill connector/model/metrics/Explore files over Rill's runtime API, and refuses to overwrite unowned resources without a filesystem fallback.
- #29: Added authenticated raw-byte KV get, put, delete, and deterministic prefix-list routes. The routes enforce tenant, namespace, and verb scopes before storage and keep keys, values, tokens, and metadata out of logs.
- chore: Removed GET/PUT /v1/watermark and its wire-shape tests. Sys routes are worker registry only.
- #66: Switched LocalAccess admin test fixtures from *.verglas.dev catalog hostnames to catalog.example.test.
- #84: Added access-service routes for creating, rotating, inspecting, and resolving typed scoped
  secrets. List and get return metadata only; the explicit resolution route requires `use_secret`
  authorization before returning material to a trusted runtime.
- #84: Added tenant-scoped database collection, item, and delete routes over the dynamic database repository. Database responses now use the public managed-or-scoped declaration and never serialize internal records or secret resource IDs.
- #84: Mapped managed database provisioning failures to a bounded gateway error; the API never reports an inactive managed runtime as created.
- #84: Replaced singleton SQL ingress with `POST /v1/databases/{database}/query`. Each turn renders an isolated query-worker config targeting that database's live catalog mount, and unknown or Postgres-only databases fail closed before process launch.
- #81: Added the fail-closed data-plane authorization boundary that forwards opaque bearer credentials to the access service and derives tenant identity only from its verified response. Database, catalog, table, graph, vector, KV, and queue routes now map to stable resources and least-privilege actions; static KV token grants and singleton graph/vector routes were removed.
- #81: Propagated the already-authorized caller bearer into each ephemeral query/write role through child-only environment state, allowing internal database-catalog checks without serializing authority into configs, arguments, or durable job state.
- #RBAC: Replaced actor-supplied access administration with signed, revocable bearer identity and current-policy checks. Added OS identity session exchange, scoped token CRUD, private policy synchronization, filtered resource discovery, and short-lived database target JWT exchange with public JWKS.
- #81: Forwarded the exact bearer verified by the data-plane boundary through database-scoped catalog routes. Requests without verified identity fail closed, and authenticated Lakekeeper responses never use the cross-request catalog cache.
- #81: Coupled database creation and deletion to the authorization registry.
  Creation records `database/{name}` only after provisioning succeeds, grants
  the creator ownership, and rolls the database back if authorization cannot
  be installed. Lakehouses grant only the Lakekeeper service its required
  child-management actions; Postgres grants only the Neon service `connect`.
  Deletion removes the database first and then cleans its authorization subtree
  child-first, so retries safely repair interrupted cleanup.
