# Worklog

- #84: Defined tenant database resources and fail-closed creation plans for managed Neon Postgres, managed Lakekeeper lakehouses, BYO S3 storage, and external Iceberg REST catalogs.
- #84: Added a database service that resolves only immutable scoped-secret resource IDs before persisting a composition. Added the durable `verglas_databases` PostgreSQL repository in the shared permissions database, with tenant/name uniqueness, binding-shape checks, secret foreign keys, and database-specific warehouses.
- #84: Added tenant-scoped list, get, and delete operations to the database repository and service. Public projections preserve the resolved storage and catalog declaration while withholding tenant IDs, internal resource IDs, and secret resource IDs.
