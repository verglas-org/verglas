# Worklog

## 2026-08-08 — Issue #81

- Added the mandatory standalone `verglas-access` process for Docker and microVM deployments.
- Added fail-closed PostgreSQL/OpenFGA startup, public health, and authenticated access APIs.

- #84: Wired encrypted secret storage into the standalone access service using the same
  `verglas_permissions` Postgres repository and OpenFGA authorizer. Startup now requires an exact
  AES-256 key, so secret persistence cannot silently run unencrypted.
- #84: Mounted the tenant-local database API. Database creation resolves authorized scoped secret
  IDs through the access service and persists immutable bindings in the same Postgres database.
