# verglas-sdk worklog

- #81: Added a separately addressable access-service endpoint and typed principal, resource, grant, and policy-check APIs using scoped bearer credentials.
- #43: Added the Rust half of the reflected Integration namespace contract. Clients can discover manifests, create a namespace handle, invoke bounded methods, and incrementally decode streaming methods through the same authenticated routes as the TypeScript SDK.

- #376: Moved the generic daemon HTTP/file-ingest transport and all table/query
  report wire shapes into the pure SDK. The SDK no longer depends on the
  Iceberg engine, so CLI consumers link only client/runtime dependencies.

- #385: Added the first-class authenticated daemon client: exact `ensure_table`,
  bounded idempotent Arrow `append_stream`, incrementally decoded Arrow
  `query_stream`, and resumable websocket `follow` with duplicate suppression
  and a distinct cursor-expired error. Contract tests exercise arbitrary IPC
  chunks and reconnect/resume behavior. Added the Rust half of the checked
  TypeScript/Rust semantic capability manifest.

- #321: New crate: the Rust SDK for the agent-data platform. `job` holds the
  Source/Sink/MV traits and the execution Context, mirroring the TypeScript
  SDK's contracts.ts field for field (the mapping table is in the module doc).
  The connector-core crate folded in wholesale: `connector` (the in-process
  Connector trait), `protocol` (NDJSON control frames + Arrow IPC data frames),
  and `conformance` (the three-property harness), with its protocol round-trip
  test moved to tests/. The tables_api request/response wire types stay defined
  in verglas-iceberg and are re-exported here — one definition, no duplication.

- #323: Re-exports `verglas_iceberg::report` alongside `tables_api` — the
  report shapes are now the wire contract the daemon's table/query routes
  serve and the CLI deserializes and renders.

- #324: `protocol` gains the child-to-parent `ReplyFrame` half of the connector
  wire protocol (`schema`/`batch`/`eos`/`acked`/`error`) plus `encode_reply`/
  `decode_reply`. A data-carrying reply names its Arrow IPC byte length; the
  harness transport reads exactly that many bytes after the NDJSON line. The
  subprocess source transport and the bun shim bind to these shapes.

- #325: `protocol` gains the MV transform frames — `ControlFrame::Transform`
  (parent hands the child an Arrow input batch) and `ReplyFrame::Transformed`
  (the child returns the Arrow output batch). The bun mv-shim binds to them.

- #326: `protocol` gains the sink delivery frames — `ControlFrame::Deliver`
  (parent hands the child a committed batch + watermark) and
  `ReplyFrame::Delivered`. The bun sink-shim binds to them.

- #258: Added the `graph` module: the wire types for the `graph` verb family
  (node/edge insert inputs, index-build request, traversal query request, and
  the create/insert/index/show/query result reports). Standalone camelCase serde
  shapes that mirror the `verglas-graph` engine model without depending on it, so
  the CLI and TypeScript SDK share one contract with the daemon and the CLI stays
  engine-free.
- index registry: Added the RegistryIndexInfo / RegistryIndexListResponse wire
  types for GET /v1/indexes (the durable index registry): name, targetKind,
  target, field, metric, clusterId, state, reflectedSnapshot. Distinct from the
  per-table IndexInfo (the in-memory serving projection) — this is the
  cross-table/graph/cluster registry read.
- Re-exported the pure-serde table and report contracts from the dependency-leaf
  `verglas-api` crate while landing the thin CLI on current main. The CLI keeps
  the same JSON surface, but neither the SDK nor the Iceberg engine depends on
  the other.
- workers: Added the `worker` module — the Rust worker contract (code + triggers)
  mirroring the TS `contracts.ts` `defineWorker`/`runWorker` model. Defines
  TriggerSpec (cron/webhook/websocket/data_change) with the cron catchup enum,
  TriggerEvent + CronInterval, ChangeEvent, WorkerContext/WorkerResult, the
  Worker trait, the subprocess RunResult file shape, and the VERGLAS_* env-var
  constants with `TriggerEvent::from_env`. This is the single deployment
  primitive that replaces the Source/Sink/MV trio.
- chore: Move the Rust SDK from crates/verglas-sdk to sdks/rust so both language SDKs live under sdks/.
- chore: Delete the connector/protocol/conformance stack and the Source/Sink/Mv Job traits. Keep Row/Logger/JobError for the worker contract.
- #393: Dropped `_LOGS` run-logging references from the worker contract; catalog-side lakekeeping owns telemetry.
- #3: Kept the SDK thin: direct Iceberg REST metadata, Arrow query/write role transport, and no embedded Iceberg, DataFusion, or Parquet writer.
- chore: Daemon unreachable errors point at a configured remote, cloud, or Docker daemon. The CLI no longer suggests host lifecycle commands.
- #91: Updated vector wire documentation for snapshot-bound Puffin attachments
  and removed the obsolete global cluster index-registry response types.
- #91: Renamed the local SDK process helpers from `daemon` to `server` and
  updated endpoint terminology for `verglas-server`. The retired module and
  names have no compatibility re-exports.
- #3: Let authenticated clients discover the configured upstream catalog URI
  and warehouse alongside the Verglas S3 endpoint. Clients communicate with
  the catalog directly; the server advertises coordinates but never hosts or
  proxies it.
- #3: Made generated partition field names Avro-safe by separating the source
  and transform with an underscore. Cloudflare can now emit manifests that the
  Iceberg reader accepts for SDK-created partitioned tables. Extended the
  default response-header deadline to cover cold worker startup and remote
  catalog planning.
- #8: Separated the bootstrap endpoint from the discovered query/write endpoint. The SDK now uses the three server-advertised on-prem destinations instead of assuming execution shares the bootstrap URL.
- #11: Added complete HTTP callback and manual worker events plus strict subprocess event decoding. Removed WebSocket from the worker scheduling trigger contract; catalog change-feed WebSockets remain a separate client transport.
- #11: Replaced runtime trigger variants with a validated CloudEvents 1.0 envelope and exact event subscription filters. Subprocess workers now read one `VERGLAS_CLOUD_EVENT` value with no legacy environment fallback.
- #16: Added generic JSON DELETE support to the Rust server transport so pure clients can remove REST resources while preserving server status and response errors.
- #29: Added a namespace-scoped raw-byte KV client with TTL, metadata, conditional writes, idempotency, delete, and bounded prefix listing. The client preserves opaque versions and cursors and reports whether reads came from RAM or NVMe.
- #67: Added first-class `Client::queue`, `Client::graph`, and `Client::table` handles matching the TypeScript SDK surface for queue enqueue/poll/ack, graph lifecycle and traversal, and table vector-index declare/list/search. Queue wire types live in `queue.rs`; graph and vector routes reuse the existing wire modules.
- #66: Neutralized MemoryGrantHost docs (no Firecracker) and replaced *.verglas.dev catalog fixtures in client_data_plane tests with example.test hostnames.
- #66: Rewrote unreachable-server and follow-trigger docs for self-hosted servers only (no cloud node / cloud lakehouse contrast).
- #84: Made `Client::query_stream` require a database name and send Arrow SQL
  requests only to that database's `/v1/databases/{database}/query` route. The
  SDK no longer exposes the removed singleton query endpoint.

- #access-tokens: Added typed create, list, and revoke calls for scoped access
  tokens. The SDK sends the caller's bearer credential to every lifecycle route
  and returns the plaintext value only from the creation response.

- #database-tokens: Added the typed Postgres connection-token exchange. A
  caller presents its normal scoped bearer to request a short-lived Neon JWT
  for one database, which the database proxy accepts as `PGPASSWORD`.

- #97: Added `Client::database` as the required boundary for Rust catalog,
  query, write, graph, and vector operations. Removed singleton catalog
  discovery and the dead catalog WebSocket client so every supported data
  operation names its database in the route.
- #107: Replaced watermark queue calls with PostgreSQL-backed fenced delivery receipts and routed queue handles through the access endpoint. Enqueue, poll, and ack now speak only the standalone queue-container contract.
- #20: Added topic-aware queue messages and reconnecting push subscriptions. Database handles now expose one table subscription stream plus fenced acknowledgement, while the SDK owns NDJSON framing and transport reconnection without a polling fallback.
- #20: Added an opt-in deployment test that proves an acknowledged Iceberg append wakes one durable table subscription and remains queryable afterward.
