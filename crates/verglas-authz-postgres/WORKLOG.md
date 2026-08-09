# Worklog

## 2026-08-08 — Issue #81

- Added the durable authorization registry for the `verglas_permissions` logical database.
- Added transactional policy revisions and recursive resource-grant resolution.
- Added restart-persistence integration coverage for a configured test Postgres instance.

- #84: Added the `verglas_secrets` schema inside the existing `verglas_permissions` database.
  Secret metadata and encrypted value versions commit atomically, while metadata reads never join
  or return ciphertext.
