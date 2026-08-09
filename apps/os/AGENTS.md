This project is building an agentic data-lake application that acts as a company's knowledge
brain. It connects company data and knowledge, builds repeatable workflows over that information,
uses graph relationships to connect entities and evidence, and produces Rill dashboards for
analytics. Application Vessels and Jobs are the generated control surfaces for those workflows;
they are not the primary system of record or analytics engine. Legacy Dynamic Worker Workspaces
(LOADER / facets / Workspace editor) are removed.

Keep integrations generic. Product SDKs may contribute ingestion and workflow definitions,
semantic metadata, graph mappings, Workspace blueprints, and Rill dashboard templates, but no
individual downstream application receives privileged or hard-coded integration in the OS.

The following files are commonly important to reference:

* packages/workshop-shared/node_modules/capnweb/README.md: Explains how to use Cap'n Web RPC, which is used extensively for client-server communications.
* packages/workshop-shared/src/api.ts: Defines the RPC API used between the frontend and backend.
* docs/architecture.md: Product shape and the shift from Dynamic Workers/Facets to Verglas workers, vessels, and containers.
* docs/verglas-backend-migration.md: Target topology (Postgres, Verglas KV, scheduler-owned worker containers).

Runtime direction: prefer Verglas worker deployments, Application/Integration
Vessels, and lakehouse tables over Cloudflare Dynamic Workers, Facets, or
Durable Object SQLite as the long-term execution and data plane. Local Wrangler
remains the Workshop gateway host during migration; generated Sources and
vessels already talk to Verglas admin/scheduler/container APIs.

The project structure is:

* packages/workshop-frontend: The Verglas OS Workshop UI (Jobs, Applications, Integrations, Lakehouse, workspace chat).
 * This is a pure single-page app, running entirely client-side.
 * It speaks to the backend using an RPC API over a persistent WebSocket connection.
 * Uses React, Kumo UI (https://kumo-ui.com/api/component-registry), Phosphor icons, and Vite.
 * Brand colors follow `../verglas-cloud/cloud/brand/tokens.css` (cool dark surfaces, `#3d9cf0` primary, IBM Plex).
* packages/workshop-backend: The Workshop server / kernel.
 * Still boots as Cloudflare Workers + Durable Objects under Wrangler for the local PoC gateway.
 * Owns Cap'n Web and adapters to Verglas (`verglas-*-runtime.ts`, `verglas-catalog.ts`, `model-runtimes.ts`). Target execution is containerized Verglas workers — not new Dynamic Worker / Facet machinery. External integrations go through Integration Vessels, not gatekeeper Workers.
 * This is the **kernel**: it defines the architecture and is held to a higher bar than UI code. Reviewers read *every line* of `workshop-backend` and of API changes in `workshop-shared`, so keep diffs here small and elegant. Concretely: doc-comment **every** exported member of the `workshop-shared` public API (types, consts, and functions — not just interfaces); never introduce a hand-written interface that mirrors an RPC interface plus an `as unknown as` cast (derive from the real type instead, or rethink the design); and prefer reusing existing mechanisms over adding parallel ones. When a change to this package is large, split it by concern into separate PRs (and at minimum group commits so `workshop-backend`/`workshop-shared` can be reviewed apart from UI), since fewer kernel lines = easier review.
    * `format-blueprints/` holds the **output format** blueprints the deployment ships with, committed as data: a `<name>.blueprint` archive plus a `<name>.json` sidecar giving its `blueprintId`, prose, and `output` presentation. `scripts/build-format-blueprints.mjs` globs that directory (override with `FORMAT_BLUEPRINTS_DIR`, which lets a fork ship its own set without touching this submodule) into the gitignored `src/generated/format-blueprints.ts`, so `build`, `types:check` and `test` all run the generator first. Replace one with `pnpm import:format-blueprint <.blueprint> <blueprintId>`, or add one with `pnpm import:format-blueprint <.blueprint> --new <name>`; never edit a `blueprintId` after deploy, since the install and promotion are keyed on it and a rename orphans the old entry. See `format-blueprints/README.md`.
* packages/workshop-shared: Shared API definitions between client and server.
    * This defines the application's RPC interface.
    * The RPC protocol is Cap'n Web, which has similar semantics to Cloudflare's Worker-to-Worker RPC system, while being able to run in a browser over WebSocket. Read the readme for details.

* packages/router: The public origin of a deployed instance. Serves the workshop-frontend assets and routes by path prefix: `/api/*` and screenshot prefixes to the workshop backend. The same worker doubles as the dev router (`pnpm dev-server`): with no `ASSETS` binding it proxies frontend requests to the Vite dev server instead.

Deployment admin settings (the `/admin` panel) follow a few conventions worth knowing when extending them:

* `packages/workshop-backend/src/admin-config.ts` defines `AdminConfig` — the deployment's "soft" customizations: agent instructions, banners/theme, and related offers. **Authentication/authorization config (password login via `DISABLE_PASSWORD_AUTH`) is deliberately NOT here** — it stays env-var driven (`auth/config.ts`) so it can't be changed by a compromised admin session.
* The `AdminSettings` durable object owns the authoritative `AdminConfig` and mirrors it to a single reserved KV key (`.adminConfig`, see `isReservedBlueprintKey()`), so hot-path code (connect/agent) reads it with one cheap KV get via `readAdminConfig(env)`. The DO is the only writer (`updateAdminConfig(patch)`).
* Admin operations are exposed as an `AdminApi` capability obtained via `AuthenticatedApi.getAdminApi()` (returns null for non-admins). The `#isAdmin()` check happens once when the capability is minted, so the individual methods don't re-check.

Release pipeline (`scripts/release/`) — how customer instances get deployed:

* `build-release.mjs` bundles every deployable worker byte-identically (wrangler dry-run with the pinned wrangler), builds the Access-mode frontend asset build, and generates the release manifest — the contract between this repo's CI and the deploy service, produced by `manifest-lib.mjs` from each package's wrangler.jsonc with account-specific values replaced by placeholders (`$ACCOUNT_ID`, `$WORKER_NAME(...)`, `$SECRET(...)`, `$PUBLIC_BASE_URL`, ...).
* `upload-release.mjs` mirrors the release to R2 content-addressed, manifest last; with `--candidate` the manifest lands under `candidates/<id>/` (invisible to the deploy service) so e2e can verify it, and `promote-release.mjs` then copies it to `releases/<id>/` — publishing is that single all-or-nothing manifest copy. The copy is not isolated against concurrent promotions, so CI serializes promote runs (a GitLab resource group) and the script's newer-release guard skips candidates that a later release has already superseded.
* The manifest is covered by a golden-file test; after an intentional manifest change, regenerate with `UPDATE_GOLDEN=1 node --test scripts/release-manifest.test.js` and review the golden diff.
* Running the flow by hand (upload and promote need `R2_ENDPOINT`, `R2_BUCKET`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`):
    * `node scripts/release/build-release.mjs --out release-out` — build everything into `release-out/` (id defaults to `r<CI_PIPELINE_IID>-<sha7>` in CI, `dev-<timestamp>` locally; override with `--release-id <id>`).
    * `node scripts/release/upload-release.mjs --release release-out --candidate` — mirror to R2; omit `--candidate` to publish directly (bypasses the gate — CI never does this).
    * `node scripts/release/promote-release.mjs --release-id <id>` — copy the verified candidate's manifest into `releases/<id>/`.
* Deploy-wizard configuration: a per-package `deploy-inputs.json` declares user-supplied inputs. Backend instance-state vars (`ADMINS`, `DEPLOY_URL`, ...) are injected by the deploy service at PUT time, never manifest-templated.

To test changes:
- Run `pnpm build` (optionally narrowed to a particular package) to run TypeScript type checks.
- Run `pnpm test` to run unit tests, though as of this writing most packages don't have tests yet.

Linting (oxlint):
- `pnpm lint` runs what CI currently enforces: `lint:check` (oxlint) and `types:check` (recursive `tsc --noEmit`). Run this before pushing.
- Individual scripts:
    * `pnpm lint:check` / `pnpm lint:fix` — oxlint (config in `.oxlintrc.json`; `correctness` + `suspicious` as errors).
    * `pnpm types:check` — recursive `tsc --noEmit`.
- Unused function parameters and caught errors are not lint-enforced; unused imports and local variables are still errors.
- Some rules are kept as warnings (e.g. `no-shadow`) for incremental cleanup; warnings don't block CI.
- Type-aware oxlint rules are intentionally not enabled. The type-aware engine (tsgo) requires an explicit `rootDir` under declaration emit and drops `baseUrl`, which is incompatible with this monorepo's cross-package source imports. Among other things this means `no-floating-promises` is not enforced — which is just as well, since RPC promise pipelining (below) intentionally leaves promises unawaited. Type safety is still enforced by `tsc` through `pnpm types:check` and `pnpm build`.

IMPORTANT: This repository uses pnpm, not npm. Always use pnpm.

IMPORTANT: Remember when using RPC to use promise pipelining whenever possible. Cap'n Web implements promise pipelining (similar to Cap'n Proto). This means that if an RPC returns a stub, it's not necessary to await the RPC -- the promise itself can be used in place of the stub. Also, Cap'n Web lets you use the promise for a future result (even if it isn't a stub) in the arguments for another call; the promise will be replaced with its resolution on the server side before delivering the arguments. See the Cap'n Web README.md for more details.

IMPORTANT: When using React's useState(), the state value cannot be an RPC stub. At runtime, all stubs appear to be callable (because the system doesn't actually know if the stub points to a function on the server side or not). But the setter returned by useState() has different behavior if passed a function (including any callable object): it calls the function in order to get the state. In order to avoid this problem, whenever a useState() state will contain an RpcStub, it's important to wrap the stub in an object, and set the state to that object instead.

IMPORTANT: RPC stubs must be disposed to prevent resource leaks on the server side. Call `stub[Symbol.dispose]()` when the stub is no longer needed (or use a `using` declaration where possible). In particular, when a React component obtains a stub in a useEffect, the cleanup function should dispose the stub.

IMPORTANT: Server-side logging uses `@verglas/backend-utils/logger` (frontend browser `console.*` is out of scope):
- Define a package-owned field type and module-scoped logger with a stable dot-separated `component`
  and, for gatekeepers, `vendorId`:
  `const logger = createLogger<GitHubLogFields>({ component: "gatekeeper.github", vendorId: VENDOR_ID });`.
- Emit concrete event names and relevant typed fields, for example:
  `logger.warn("failed to notify credential expiry", { event: "credentials.expiry.notify.failed", error: err });`.
  Each call emits one indexed object; module/child fields such as `vendorId` are inherited.
- Use immutable `logger.with(fields)` for object-owned or nearby context. Prefer module/object loggers
  over logger parameters, and do not replace a shallow child logger with ambient context just to
  remove a local variable.
- For bounded operation context needed by deep helpers, independent loggers, or other observability
  consumers, use `createObservabilityContext` from `@verglas/backend-utils/observability-context`.
  Re-establish it per operation;
  it does not cross RPC, hibernation, or restart, and requires `nodejs_als` or `nodejs_compat`.
- Pass caught values as `error`. The helper stringifies `Error` instances and primitives, uses an
  own string `message` for plain objects, omits `undefined`, and adds stacks to all `Error` logs.
  Keep this normalization deliberately small; do not traverse causes or copy arbitrary properties.
- Extend field vocabularies locally. Levels: `error` needs attention, `warn` continues best-effort,
  `info` is notable lifecycle, and `debug` is noisy breadcrumbs. Never log secrets, prompts, headers,
  tokens, or request/response bodies.
- To also dispatch a failure to the optional external issue Reporter (in addition to logging it),
  call `reportIssue(failureSite, caught, options?)` from
  `@verglas/backend-utils/error-reporting`. Attach ambient fields from the package's observability
  context and augment them with capture-site fields:
  `reportIssue("overseer.catalog-fallback", err, { handled: true, attributes: { ...obsContext.get(), gatekeeperId } });`.
  It is a no-op when the `ERROR_REPORTER` binding is absent (local dev / deployments without an issue
  destination). Only bounded scalars are retained as attributes; reported context obeys the same
  no-secrets rules as log fields.

IMPORTANT: Frontend error reporting is a separate, opt-in path:
- `@verglas/error-reporting` owns the vendor-neutral browser/Worker event contract and tolerant,
  bounded normalization. `VITE_FRONTEND_ERROR_REPORTING=true` enables trusted frontend producers
  and their hidden source maps at build time; deployments without reporting should leave it unset.
- The Workshop browser sends best-effort reports to the same-origin `POST /api/client-errors`
  endpoint. The backend dispatches only when both `FRONTEND_ERROR_REPORTER` and
  `FRONTEND_ERROR_RATE_LIMITER` are bound; otherwise the endpoint is an intentional no-op.
- Frontend reports and frame metadata are diagnostic only and never convey identity or authority.
  Install automatic capture only in trusted first-party surfaces, never workspace/user-authored code.
  Exception messages and stacks reach the external Reporter, so never intentionally put secrets,
  prompts, tokens, headers, or request/response bodies in thrown errors or report metadata.
