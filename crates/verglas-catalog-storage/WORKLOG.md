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
- #135: Bound catalog-managed table roots into the canonical Catalog create
  request before metadata validation. Standard Iceberg clients may omit an
  explicit location while immutable metadata still receives the exact durable root.
- #135: Marked the Verglas-authored CRaft storage adapter as FSL-1.1-ALv2.
  The imported Catalog crates retain their existing Apache 2.0 license.
- #135: Added a durable idempotency record to the same CRaft batch as hosted
  table and transaction commits. The record binds the operation and canonical
  input fingerprint to the original result, so restart or alternate-ingress
  retries replay exactly while mismatched key reuse returns a conflict.
- #135: Return the conventional Iceberg `CommitFailedException` for optimistic
  catalog conflicts and a distinct 409 for mismatched idempotency-key reuse.
  Added direct receipt-matching and public error-boundary regression tests.
- #135: Moved the CRaft-backed Catalog adapter into `verglas-cloud` while
  retaining its dependency on the public Verglas catalog and consensus engine.
  Cloud builds now provide that engine as an explicitly pinned checkout.
- #135: Routed every managed Iceberg warehouse prefix through its registered
  bucket binding before metadata publication or authorization. Client-selected
  locations outside that root now fail before object IO, so databases cannot
  share a fallback bucket or reveal each other's catalog resources.
- #137: Replaced the pinned git dependencies on `verglas-catalog` and
  `verglas-consensus` with path dependencies, as part of moving the Catalog
  fork into the `verglas` repository under `catalog/`. Engine changes no
  longer need a re-pin commit here; they are picked up from the checkout, and
  `.github/workflows/catalog.yml` builds this crate when those engine crates
  change so the coupling cannot break silently.
- #164: Dropped the "ships two ways" framing from `hosted_deployment` and the
  `serve-craft` references in `authorized`. The standalone catalog binary is
  deleted; the catalog runs inside the ring node, and
  `ManagedCatalogTransport` remains the seam a different deployment shape
  would vary.
