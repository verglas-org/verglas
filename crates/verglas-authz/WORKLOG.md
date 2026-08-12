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

- #RBAC: Added compact HMAC-signed access-token contracts for tenant-scoped child principals.
  Tokens carry only identity, audience, policy revision, and validity claims; bearer material is
  returned once, while the registry stores only public metadata, use time, and revocation state.
  Added the distinct `connect` action for database connection authorization.

- #RBAC: Added universal `project`, `generic_table`, `role`, and `tag` resource categories and
  a role principal kind. Lakekeeper and other catalogs can now synchronize their generic hierarchy
  without treating role assumptions as service-account identities.

- #RBAC: Added a dedicated Ed25519 target-JWT issuer and public JWKS contract for database
  credential exchange. Target JWTs carry audience, subject, token ID, tenant, database, and
  lifetime claims, and use a separate asymmetric key from internal access-token signing. The
  default constructor derives each public JWKS key ID from the Ed25519 public-key fingerprint.
