# verglas-iceberg-ext worklog

Append-only log of changes to this crate, by issue. Every PR touching this crate adds an entry.

- #171: Gated the axum-only structured error tracing helpers so the dependency-free extension build has no unused code or imports. The axum-enabled response behavior remains compiled and covered while the default catalog crate passes strict clippy.
- #171: Audited the shared Iceberg committer and retained the extension as a catalog adapter only; the commit path uses Iceberg CAS snapshots, verifies readback, and enforces publication boundaries in the engine while this crate's default build remains warning-free.
