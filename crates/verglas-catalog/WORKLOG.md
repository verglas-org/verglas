# Worklog

- #8: Added the independent shallow Iceberg REST transport used by on-prem proxying and catalog polling. Reads share a bounded cache and successful mutations invalidate it.
- #58: The cache-owned catalog gateway now supplies its configured warehouse during local `/v1/config` discovery. Stateless query workers therefore reuse the exact prepared response held by the watcher without carrying upstream catalog coordinates.
