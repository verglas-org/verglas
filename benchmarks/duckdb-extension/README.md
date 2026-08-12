# DuckDB extension benchmark

`./run.sh --profile smoke --output report.json` measures one warm-up plus five
raw samples of the same 10,000-row, two-column result through local DuckDB and
raw HTTP Arrow. The client is a Docker cgroup constrained to exactly one CPU and
512 MiB. Arrow, Quack, and Verglas-extension service containers are independently
limited to 0.5 CPU and 256 MiB; each effective cgroup-v2 value is read from inside
its container and retained in the JSON report.

The extension and Quack legs make actual load/query attempts. In an unconfigured
checkout they report their measured error as unavailable; they are never replaced
with local DuckDB data and are excluded from equivalence ratios. Configure or
mount their real artifacts/endpoints before treating them as full-stack results.

Every sample records total time, time-to-first-response, CPU seconds, peak RSS,
row count, schema, and a SHA-256 digest of canonical result rows. The report also
retains hardware, limits, warmup count, and all raw samples.
