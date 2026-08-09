# Worklog

## 2026-08-08 — Issue #81

- Added the mandatory standalone `verglas-access` process for Docker and microVM deployments.
- Added fail-closed PostgreSQL/OpenFGA startup, public health, and authenticated access APIs.

- #84: Wired encrypted secret storage into the standalone access service using the same
  `verglas_permissions` Postgres repository and OpenFGA authorizer. Startup now requires an exact
  AES-256 key, so secret persistence cannot silently run unencrypted.
- #84: Mounted the tenant-local database API. Database creation resolves authorized scoped secret
  IDs through the access service and persists immutable bindings in the same Postgres database.
- #84: Added managed database runtime reconciliation to the access node. Managed lakehouses bootstrap the tenant Lakekeeper service and receive database-specific warehouses and object prefixes; failed creates compensate the durable declaration, deletes remove runtime state first, and startup recovery reasserts all declarations.
- #84: Added the managed Neon provisioner backed by the published Verglas storage and PostgreSQL 16 images. It reconciles a tenant broker plus database-local pageserver and compute containers, recovers stable timeline identity and credentials from the durable database identity, and requires an authenticated SQL probe before creation succeeds.
- #84: Isolated startup recovery failures per database after the mandatory catalog bootstrap and inventory read. An unavailable managed runtime is reported without taking healthy lakehouses or the tenant access API offline; new database creation remains transactional and fail-closed.
