# Worklog

- #8: Added the independent shallow Iceberg REST transport used by on-prem proxying and catalog polling. Reads share a bounded cache and successful mutations invalidate it.
- #58: The cache-owned catalog gateway now supplies its configured warehouse during local `/v1/config` discovery. Stateless query workers therefore reuse the exact prepared response held by the watcher without carrying upstream catalog coordinates.
- #84: Added a fail-closed, database-scoped catalog registry. Multiple lakehouse databases can share one tenant Lakekeeper deployment while retaining distinct warehouses and storage bindings; external catalogs remain independently bound by stable resource IDs.
- #82: Added ordered catalog mutations and a strong applied-sequence fence to the shared gateway. Replay applies the transactionally committed pointer directly, invalidates prepared responses, and leaves the next fenced read to load the catalog's current response instead of requesting unavailable historical versions.
