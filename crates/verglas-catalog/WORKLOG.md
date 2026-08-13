# Worklog

- #8: Added the independent shallow Iceberg REST transport used by on-prem proxying and catalog polling. Reads share a bounded cache and successful mutations invalidate it.
- #58: The cache-owned catalog gateway now supplies its configured warehouse during local `/v1/config` discovery. Stateless query workers therefore reuse the exact prepared response held by the watcher without carrying upstream catalog coordinates.
- #84: Added a fail-closed, database-scoped catalog registry. Multiple lakehouse databases can share one tenant Lakekeeper deployment while retaining distinct warehouses and storage bindings; external catalogs remain independently bound by stable resource IDs.
- #82: Added ordered catalog mutations and a strong applied-sequence fence to the shared gateway. Replay applies the transactionally committed pointer directly, invalidates prepared responses, and leaves the next fenced read to load the catalog's current response instead of requesting unavailable historical versions.
- #135: Added the native managed-catalog client used by the Lakekeeper REST/domain
  adapter. Typed transaction batches and fenced reads now route through any
  Verglas consensus ingress instead of a PostgreSQL-backed catalog transaction.
- #135: Bound every managed catalog request to both tenant and warehouse in the ingress URL. This makes the client follow the durable tenant-root routing hierarchy instead of addressing an ambiguous warehouse name directly.

- #135: Added typed record reads and collection listings to the managed catalog
  client. Lakekeeper domain objects now have a CRaft transport path alongside
  table pointers, without a SQL read fallback.
- #135: Exposed the immutable warehouse route bound to a managed catalog
  client. The hosted Iceberg config service uses this exact route as the
  standard REST prefix instead of trusting a caller-provided warehouse string.
- #135: Made managed-catalog HTTP 409 a typed final conflict instead of an
  ingress retry signal. HTTP 503 remains retryable across explicit ingresses,
  preserving leader-availability failover without retrying failed CAS writes.
