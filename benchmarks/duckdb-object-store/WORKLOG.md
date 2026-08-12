# Worklog

- #126: Replaced the RAM-only protocol comparison with a real DuckDB/Quack
  object-store benchmark. It requires a dataset larger than operator memory,
  observed spill and origin traffic, cold/warm/shared-cache reads, bounded
  containers, result equivalence, and durable origin readback for both writes.
- #126: Moved MinIO data, Verglas cache, and DuckDB spill onto the external
  benchmark disk when available. Spill high-water sampling now reads the host
  bind mount, so transient Docker control-plane failures cannot abort a query.
- #126: Require provenance and a one-CPU cgroup record for all three Quack
  servers, including the shared-warm reader. This prevents a report from
  presenting a shared-cache result without proving that the third engine was
  held to the same CPU ceiling.
- #126: Increased the generated dataset to 40 million rows after the measured
  20-million-row Parquet footprint failed the one-GiB eligibility gate. Quack
  now has a fixed two-GiB process ceiling while DuckDB remains capped at 256
  MiB, separating server overhead from the operator limit that forces spill.
- #126: Added an explicit byte-valued cache-capacity input and made the report
  carry the running server's cache configuration and counters. This lets
  constrained and NVMe-resident results be distinguished using observed
  evidence rather than a handwritten profile label.
- #126: Added the live Cloudflare R2 profile with TLS, mounted credentials,
  external-origin provenance, and request counts from R2's GraphQL operations
  dataset. The same bounded Quack topology now runs against durable R2 directly
  and through Verglas, and both write paths are hashed after direct R2 readback.
- #126: Extended the live R2 benchmark to compare QuackStore's real persistent
  block cache with Verglas under the same 256 MiB logical cache budget. Each of
  five isolated repetitions records direct, cold, warm, and fresh-process cache
  legs, plus cache occupancy, immutable-data configuration, cgroups, spill,
  R2 operations, result hashes, and direct-origin write readback.
- #126: Corrected the QuackStore comparison to use the extension's global
  cache settings and a persistent cache file rather than a directory. Every
  workload now stops its primary QuackStore process before a fresh process
  reopens that same file, and the report records each handoff with file and
  process evidence.
- #126: Retained each complete live-R2 repetition as a numbered JSON artifact
  and embedded its raw evidence in the final summary. The contract now rejects
  missing raw reports, cache-file paths, overlapping QuackStore processes, and
  missing logical or physical cache-size evidence.
