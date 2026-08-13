# Worklog

- #135: Consolidated the `serve-craft` catalog entry point into Verglas. The
  entry point uses ordered Verglas ingresses and immutable object metadata
  storage rather than PostgreSQL catalog persistence.
- #135: Removed the upstream PostgreSQL migration, serving, maintenance, and
  fallback commands. The binary now has one production mode and constructs only
  the CRaft-backed catalog service.
- #135: Constructed hosted immutable metadata IO with Lakekeeper's explicit
  system-identity credential mode. Scoped deployment credentials now sign S3
  publication and verification requests instead of producing anonymous access.
- #135: Declared the assembled catalog binary as containing Apache 2.0 and
  FSL-1.1-ALv2 code. The boundary preserves Lakekeeper attribution while
  protecting the Verglas-specific service integration.
