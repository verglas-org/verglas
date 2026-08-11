# Worklog

- cloud acceptance: bounded the access authorization repository to two
  Postgres connections so metadata traffic cannot exhaust a tenant's client
  connection budget.

## 2026-08-08 — Issue #81

- Added the durable authorization registry for the `verglas_permissions` logical database.
- Added transactional policy revisions and recursive resource-grant resolution.
- Added restart-persistence integration coverage for a configured test Postgres instance.

- #84: Added the `verglas_secrets` schema inside the existing `verglas_permissions` database.
  Secret metadata and encrypted value versions commit atomically, while metadata reads never join
  or return ciphertext.

- #RBAC: Added the tenant-local `access_tokens` registry and its durable Postgres implementation.
  It stores inventory metadata, expiry, last use, and revocation only; token plaintext, signatures,
  and signing keys never enter the permissions database.
