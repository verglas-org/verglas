# Worklog

- feat: Moved worker list/get/register/state/run methods from the data-server admin client to the
  scheduler control client, matching the scheduler-owned Postgres registry and authenticated
  execution control plane.

- Database queries now require an explicit database name and use `POST /v1/databases/{database}/query`; the TypeScript SDK and admin control client both validate and URL-encode that scope.
- #81: Added typed JSON POST and DELETE helpers to the official admin control client so
  catalog-backed management surfaces can mutate Iceberg namespaces and tables without
  reimplementing authenticated HTTP transport.
- #43: Runtime callbacks now receive the connected namespace-aware SDK as both `ctx.verglas` and `this.verglas`; the legacy `ctx.client` property remains the same instance during migration.
- #91: Updated SDK endpoint documentation to call the local process
  `verglas-server`. Client wire behavior is unchanged.
- #11: Taught the endpoint runner to reconstruct manual, HTTP callback, cron, and data-update events from the scheduler harness environment. Removed WebSocket worker trigger types while leaving the catalog change-feed transport unchanged.
- #11: Replaced the TypeScript runtime trigger union with a CloudEvents 1.0 contract and generic event subscriptions. The endpoint runner validates one structured CloudEvent, and the reference workers consume event-specific data from its payload.
- #29: Added a namespace-scoped raw-byte KV client with TTL, metadata, conditional writes, idempotency, delete, and bounded prefix listing. The transport leaves opaque versions and continuation cursors under server control.
- #43: Added reflected Integration namespaces to the TypeScript SDK. Generated Applications and Workers can compose arbitrary Integration methods through `client.namespace`, using awaitable bounded calls or incremental NDJSON streams, while optional generated types preserve compile-time input and output checks.
- chore: Dropped client.watermark()/setWatermark() and the mock /v1/watermark cell. Table/queue read cursors named watermark are unchanged.
- #67: Realigned ensure/create to Iceberg REST (catalog discovery via `/admin/access` or `catalogUri`) and `table.append` to `POST /v1/ingest/{name}` JSONL. Shared surface inventory now lives in `contracts/api-surface.json`; follow/feed is documented as not live on-prem.
- #66: Rewrote endpoint-run and README host-entry docs for the local worker harness against http://127.0.0.1:8334; kept VERGLAS_CLOUD_EVENT as the CloudEvents env binding.
- #66: Removed local-vs-cloud endpoint product wording from README and SDK comments; renamed test fixtures `t.verglas.cloud` → `t.example.test` and `cloud.job_runs` → `demo.job_runs`.
- #75: Added typed clients for worker administration, scheduler secrets and job history, and local Vessel runtime operations. Verglas OS can now use the canonical TypeScript SDK from the monorepo instead of carrying a generated private copy.

- #52: Added typed runtime Vessel stop and resume controls. The SDK calls the runtime manager's persisted lifecycle routes instead of exposing generic Docker operations to product UIs.
- #84: Added a typed access-service client with create, list, get, and delete methods for dynamic tenant databases. The public database union matches the managed and scoped create declarations and omits internal tenant and secret resource identifiers.
- #84: Required every SDK SQL query to name its tenant database and routed it through `/v1/databases/{database}/query`. The typed client and control client also pass the optional table time-travel pin without retaining the removed singleton query route.
- RBAC tokens now use the mandatory control-plane bearer credential. The SDK can mint, list, revoke, and explain scoped child-principal tokens through the access service, and every control connector rejects an empty token instead of silently sending an administrator fallback.
- Added the one-time direct Postgres database-credential call. It is separately scoped to one database and returns only its bearer value and expiry, so SDK users do not need a reusable tenant database password.
