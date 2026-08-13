# Worklog

- #126: Added failing contract tests for cgroup normalization and measurement
  summaries before implementing the benchmark harness. The initial test run
  failed because `benchmark.py` did not exist.
- #126: Implemented a Docker-cgroup benchmark harness pinned to DuckDB 1.5.5.
  It measures local DuckDB and raw HTTP Arrow with equivalent rows, while the
  real extension and Quack paths preserve measured unavailability instead of
  inventing fallback results.
- #126: Made all four protocol legs mandatory and comparable at 1, 10,000, and
  100,000 rows. The benchmark now builds the Linux release extension in a
  separate Docker stage, uses real Quack serve/query over its isolated network,
  and refuses any failed or non-equivalent result.
- #126: Moved query measurements into reusable prepared client sessions so
  imports, connections, and extension loading are excluded before warm-up.
  Results now stream bounded Arrow batches and record first-batch latency and
  received network bytes without materializing an unbounded result table.
- #126: Removed host configure artifacts before compiling the benchmark image.
  This keeps local extension build state from affecting the measured artifact.
- #126: Added the failing evidence contract for the real Iceberg full-stack
  comparison. It requires catalog-committed Iceberg data, the three requested
  product paths, equal cgroup budgets, and same-engine cold/warm cache controls.
- #126: Added the Iceberg full-stack orchestrator. It controls only explicitly
  declared Docker services, reads effective cgroup-v2 limits from measured
  containers, and rejects any missing catalog, cache, origin-byte, snapshot, or
  result-identity proof instead of manufacturing a benchmark result.
- #126: Added failing tests for the repository-owned managed runner. They
  distinguish direct-R2 Quack from Quack loading the real Verglas extension and
  require every product path to execute the same out-of-core SQL semantics.
- #126: Added the managed-Iceberg runner used by the full-stack benchmark. It creates a unique Iceberg v2 table through Lakekeeper, keeps Quack's direct R2 path separate from the Query worker and extension path, and streams typed Arrow evidence from real clients.
