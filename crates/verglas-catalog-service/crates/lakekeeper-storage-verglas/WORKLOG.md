# Worklog

- #135: Consolidated the authoritative CRaft catalog adapter, transaction
  buffering, immutable metadata publication, and hosted table and view services
  into the Verglas repository.
- #135: Advertised the complete canonical Iceberg REST capability set from the
  CRaft catalog config response. Standard clients can now issue namespace,
  table, transaction, and view operations without disabling capability checks.
- #135: Added the CRaft-bound warehouse as the standard Iceberg REST request
  prefix. Clients now target the mounted tenant warehouse route directly rather
  than issuing prefixless requests that cannot reach catalog handlers.
- #135: Bound catalog-managed table roots into the canonical Lakekeeper create
  request before metadata validation. Standard Iceberg clients may omit an
  explicit location while immutable metadata still receives the exact durable root.
- #135: Marked the Verglas-authored CRaft storage adapter as FSL-1.1-ALv2.
  The imported Lakekeeper crates retain their existing Apache 2.0 license.
