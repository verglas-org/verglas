# Worklog

## 2026-08-08 — Issue #81

- Added universal principal, resource, action, grant, decision, and scoped-token contracts.
- Added the policy/repository composition that fails closed on registry and evaluator drift.
- Added inheritance, tenant-boundary, and token-claim tests.
- Added actor-bound delegation and revocation contracts. Delegation now requires both
  `pass_grants` and every action being passed; revocation requires `manage_grants`.

- #84: Added typed S3 and Iceberg REST secret contracts, authenticated encryption, and
  authorization-gated longest-scope resolution. Replacement preserves the stable secret resource
  identity while advancing its encrypted value version, and ambiguous or unauthorized matches fail closed.
