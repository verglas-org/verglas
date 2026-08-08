# Worklog

- #43: Runtime callbacks now receive the connected namespace-aware SDK as both `ctx.verglas` and `this.verglas`; the legacy `ctx.client` property remains the same instance during migration.
- #91: Updated SDK endpoint documentation to call the local process
  `verglas-server`. Client wire behavior is unchanged.
- #11: Taught the endpoint runner to reconstruct manual, HTTP callback, cron, and data-update events from the scheduler harness environment. Removed WebSocket worker trigger types while leaving the catalog change-feed transport unchanged.
- #11: Replaced the TypeScript runtime trigger union with a CloudEvents 1.0 contract and generic event subscriptions. The endpoint runner validates one structured CloudEvent, and the reference workers consume event-specific data from its payload.
- #29: Added a namespace-scoped raw-byte KV client with TTL, metadata, conditional writes, idempotency, delete, and bounded prefix listing. The transport leaves opaque versions and continuation cursors under server control.
- #43: Added reflected Integration namespaces to the TypeScript SDK. Generated Applications and Workers can compose arbitrary Integration methods through `client.namespace`, using awaitable bounded calls or incremental NDJSON streams, while optional generated types preserve compile-time input and output checks.
- chore: Dropped client.watermark()/setWatermark() and the mock /v1/watermark cell. Table/queue read cursors named watermark are unchanged.
- #67: Realigned ensure/create to Iceberg REST (catalog discovery via `/admin/access` or `catalogUri`) and `table.append` to `POST /v1/ingest/{name}` JSONL. Shared surface inventory now lives in `contracts/api-surface.json`; follow/feed is documented as not live on-prem.
