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
