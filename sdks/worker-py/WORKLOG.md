# Worklog

- #0: Added the pinned componentize-py Python Worker builder, generated-binding
  adapter, transactional storage and socket authoring helpers, tests, and
  documentation. The build writes content-addressed components and a binding
  manifest for the supported Wrangler subset.
- #0: Matched componentize-py's synchronous WIT export convention and enabled
  its WASI stubs so generated components import only Verglas capabilities. SQL
  remains explicitly wired to the sibling `sql-rows` WIT verb without an Arrow
  or legacy fallback.
- #0: Regenerated the bindings after `storage.sql-rows` landed and rebuilt the
  py-counter component. The final component validates structurally and imports
  the versioned storage capability with the JSON-row verb present.
- #0: Added explicit checks for the WIT unsigned integer widths before storage,
  alarm, socket, and response calls. This keeps invalid Python integers from
  reaching the component boundary as truncated values.
- #0: Replaced the invented `verglas_worker` authoring surface with an injected
  Cloudflare-shaped `workers` module supporting WorkerEntrypoint/on_fetch,
  DurableObject bindings, deterministic storage values, SQL cursors, alarms,
  and guest-driven WebSocket acceptance over WIT v2. Extended the Wrangler
  subset with compatibility fields, migrations, and vars; the old surface was
  removed rather than retained as a fallback, and tests were rewritten first
  to fail on the old manifest/API before the implementation was added.
- #0: Matched the JavaScript builder's deployment artifact contract by accepting
  both wrangler.json and wrangler.jsonc, preserving compatibility/migration/vars
  fields in manifest.out.json, and verifying the emitted v2 component with
  wasm-tools validation and import/export inspection.
- #0: Matched the documented bulk storage overloads and public DurableObjectId
  helpers (`id_from_string`/`equals`) while keeping the host boundary strictly
  byte-based and deterministic.
- #0: Hardened the binding and storage shims to match Cloudflare's bulk key
  overloads and stable DurableObjectId behavior; the exact canonical value bytes
  remain covered by the mock-host tests.
- #0: Added strict Wrangler Pipeline Stream bindings to the Python SDK. `env`
  now exposes a separate asynchronous `send(records)` binding that validates
  JSON, enforces the inclusive 5 MiB UTF-8 request ceiling, and routes only the
  exact WIT Stream append request without fallback; manifest, mock-host, and
  component verification coverage documents the protocol judgment.
- #0: Added the PP4 Durable Object Stream path: handler environments route `send(records)` through WIT `storage.stream-send` for transactional staging, while Worker environments retain direct Stream ingestion. The Python transport exposes only JSON-row SQL and has no Arrow IPC compatibility surface.
- #171: Updated the Python builder to emit and update strict nested `artifacts.worker` and `artifacts.durable_object` descriptors using the exact SHA-256 component bytes. Retired top-level `component_digest` and `component_dir` gateway fields now fail closed, with tests covering nested manifests, digest paths, and rejection of the removed shape.
- #171: Added strict Wrangler `services` parsing, manifest preservation, and direct Python `env` service bindings over `bindings.do-fetch`; each binding routes its configured service target and exposes only asynchronous `fetch`. Updated the cold-chain Python Worker to keep `COUNTER` and `STREAM` as their existing bindings while using direct service calls for Pipeline, Sink, and Catalog.
- #171: The factual Python Worker run trapped in CPython's first `time.monotonic()` call because `--stub-wasi` replaced clocks with trapping component stubs. Removed that builder flag so Python components import standard WASI Preview 2 clocks from the runtime's locked-down context; filesystem preopens and network access remain absent.
- #171: The follow-up factual run showed that unrestricted componentize-py startup also requires filesystem access and correctly failed against the runtime's no-preopen policy. Restored WASI stubbing and replaced `asyncio.run` with a deterministic immediate-coroutine driver, which fits the synchronous WIT host calls and preserves the no-filesystem/no-network capability boundary.
