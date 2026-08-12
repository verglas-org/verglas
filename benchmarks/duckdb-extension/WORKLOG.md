# Worklog

- #126: Added failing contract tests for cgroup normalization and measurement
  summaries before implementing the benchmark harness. The initial test run
  failed because `benchmark.py` did not exist.
- #126: Implemented a Docker-cgroup benchmark harness pinned to DuckDB 1.5.5.
  It measures local DuckDB and raw HTTP Arrow with equivalent rows, while the
  real extension and Quack paths preserve measured unavailability instead of
  inventing fallback results.
