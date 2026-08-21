# Worklog

## 2026-08-09

- Added the Verglas `Authorizer` implementation for direct Iceberg catalog access.
- Added stable multi-database resource and least-privilege action mappings.
- Added caller-bearer authorization and policy credential-file rotation.
- Added `verglas` backend selection to the Catalog server and OpenAPI command.
- Added contract tests and operator documentation.
- Added fail-closed Catalog lifecycle synchronization for catalog resources.
- Added the Verglas fork GHCR publication workflow with full-SHA and `latest` tags.
- Replaced the static database root with trusted per-request `X-Verglas-Database-ID` warehouse parenting for multi-database catalogs.
- Changed catalog decisions to forward the caller's opaque bearer to public `/v1/access/authorize`; the policy-engine credential now performs resource lifecycle synchronization only.
- Removed caller-selected principals and Catalog principal synchronization from the Verglas path.

- #135: Consolidated the Verglas authorization adapter used by the hosted
  CRaft-backed Iceberg routes into the Verglas repository.
- #135: Marked the Verglas-authored authorization adapter as FSL-1.1-ALv2.
  The catalog licensing document now distinguishes it from upstream Catalog.
- #0: Replaced the tenant-local authorization client with Cloudflare JWKS
  verification in Catalog. Short-lived Worker credentials now carry the
  tenant and resource/action scope, so catalog checks do not call a tenant
  policy service or synchronize catalog resources into one.
