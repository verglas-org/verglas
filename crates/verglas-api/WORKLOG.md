# Worklog

- Added the dependency-leaf API contract crate. Table definitions, table
  request/response shapes, CLI reports, and compaction reports now live below
  both the Rust SDK client and the Iceberg engine, preventing either layer from
  depending on the other.
- Preserved PR #387's `QueryMemoryEstimate` as a shared API response while
  rebasing the SDK/engine split, instead of leaving that wire type owned by the
  Iceberg implementation.
- #91: Updated API ownership documentation to call the foreground process the
  server. Wire behavior is unchanged by the executable rename.
