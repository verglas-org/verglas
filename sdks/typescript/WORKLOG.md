# Worklog

- #1: Moved the TypeScript data-plane and worker SDK into the public client repository and added publishable repository metadata. Its runtime and API behavior are unchanged by the extraction.

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
- #67: Realigned ensure/create to Iceberg REST (catalog discovery via `/admin/access` or `catalogUri`) and `table.append` to `POST /v1/ingest/{name}` JSONL. Follow/feed remains documented as not live on-prem.
- #66: Rewrote endpoint-run and README host-entry docs for the local worker harness against http://127.0.0.1:8334; kept VERGLAS_CLOUD_EVENT as the CloudEvents env binding.
- #66: Removed local-vs-cloud endpoint product wording from README and SDK comments; renamed test fixtures `t.verglas.cloud` → `t.example.test` and `cloud.job_runs` → `demo.job_runs`.
- #75: Added typed clients for worker administration, scheduler secrets and job history, and local Vessel runtime operations. Verglas OS can now use the canonical TypeScript SDK from the monorepo instead of carrying a generated private copy.

- #52: Added typed runtime Vessel stop and resume controls. The SDK calls the runtime manager's persisted lifecycle routes instead of exposing generic Docker operations to product UIs.
- #84: Added a typed access-service client with create, list, get, and delete methods for dynamic tenant databases. The public database union matches the managed and scoped create declarations and omits internal tenant and secret resource identifiers.
- #84: Required every SDK SQL query to name its tenant database and routed it through `/v1/databases/{database}/query`. The typed client and control client also pass the optional table time-travel pin without retaining the removed singleton query route.
- RBAC tokens now use the mandatory control-plane bearer credential. The SDK can mint, list, revoke, and explain scoped child-principal tokens through the access service, and every control connector rejects an empty token instead of silently sending an administrator fallback.
- Added the one-time direct Postgres database-credential call. It is separately scoped to one database and returns only its bearer value and expiry, so SDK users do not need a reusable tenant database password.
- #107: Replaced the local watermark queue API with PostgreSQL-backed exclusive leases and fenced receipts. Queue handles use the access endpoint and cannot implicitly create queue storage.
- #20: Added topic-aware queue messages and an async push subscription generator with reconnect. Queue delivery types now preserve the matching topic and polling callers must select exact topics.
- Repository consolidation: Moved the TypeScript SDK back beside the engine and CLI and pointed its package metadata at the unified repository.
- #67: Added browser-compatible SigV4 clients for every S3 Vectors and Graph operation. The SDK sends service-model paths to the cache listener rather than a retired platform router.
- #67: Removed the legacy client query and graph handles, table ANN-index methods, DTOs, tests, and mock routes. Callers use the typed S3 Vectors and Verglas Graph SigV4 clients for those semantic services while catalog/table support remains separate.
- #137: Pointed generated API documentation at the checked-in S3 listener contract after deleting the retired generic platform OpenAPI inventory.
- #137: Canonicalized decoded query fields with AWS percent encoding before SigV4 sorting, including repeated tag keys.
- Test-slop audit: deleted `test/retired-surface.test.ts` (negatives for
  endpoints removed in the MVP prune) and a duplicated control-plane test.
  The compiler and the surviving contract tests cover the live surface.
- Published to npm as `@verglas/sdk` by the release pipeline's publish-sdk
  job. The package now ships compiled ESM + type declarations (tsup build to
  `dist/`) instead of raw TypeScript sources, so plain Node consumers can
  import it; tests and typecheck still run against `src/`. License field
  corrected to FSL-1.1-ALv2 and the repository LICENSE ships in the tarball.
- Removed the Kv and Queue client surface (classes, verbs, types, exports,
  tests, README section) until further notice, before the first npm publish
  freezes the package surface. Subscriptions and change feeds are unaffected.
- Added `src/data.ts`: the /v0 data client (`createDataClient`) speaking the
  /v0 data surface natively — one append-ingest shape (NDJSON to
  `POST /v0/events?name=<datasource>`) for table writes, vector writes, and
  log shipping, plus SQL through `POST /v0/sql`. Moved `control.ts`'s worker
  methods off the retired `/v1/workers` route onto `/v0/workers`. Deleted
  `VerglasAccessClient` (`/v1/databases` CRUD, `/v1/access/tokens`,
  `/v1/access/database-tokens`, `/v1/access/authorize`), `VerglasAdminClient`
  (its one method queried the same dead `/v1/databases` surface), and
  `VerglasRuntimeClient` (`/v1/vessels`) — the retired tenant-local
  access-service/vessel era. `listWorkerJobs`/`getJob`/`JobSummary` were
  dropped with them: the old per-worker job history route has no /v0
  equivalent, and guessing at one was out of scope. `databasePathSegment`
  (only used by the deleted admin query) came out of `http.ts` too.
- #137: Regenerated the TypeScript Graph DTO and client operation names to match the conventional Graph REST-JSON vocabulary.

