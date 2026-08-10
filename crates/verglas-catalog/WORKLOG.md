# Worklog

- #8: Added the independent shallow Iceberg REST transport used by on-prem proxying and catalog polling. Reads share a bounded cache and successful mutations invalidate it.
- #58: The cache-owned catalog gateway now supplies its configured warehouse during local `/v1/config` discovery. Stateless query workers therefore reuse the exact prepared response held by the watcher without carrying upstream catalog coordinates.
- #84: Added a fail-closed, database-scoped catalog registry. Multiple lakehouse databases can share one tenant Lakekeeper deployment while retaining distinct warehouses and storage bindings; external catalogs remain independently bound by stable resource IDs.
- #84: Added atomic replacement and bounded inspection for the live database catalog gateway registry so deleted databases disappear and newly provisioned Lakekeeper warehouses become routable as one snapshot.
- #81: Bound each live catalog gateway to its registry-selected database and added a verified-bearer transport for Lakekeeper. Authenticated reads bypass the shared watcher cache, caller-supplied database headers are discarded, and only the trusted database identity is injected upstream.
