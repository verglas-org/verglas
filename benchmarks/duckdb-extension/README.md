# DuckDB extension benchmark

`./run.sh --profile smoke --output report.json` measures one warm-up plus five
raw samples of 1, 10,000, and 100,000-row two-column results through local
DuckDB, raw HTTP Arrow, the Linux release build of `verglas_query`, and Quack.
The client is a Docker cgroup constrained to exactly one CPU and
512 MiB. Arrow, Quack, and Verglas-extension service containers are independently
limited to 0.5 CPU and 256 MiB; each effective cgroup-v2 value is read from inside
its container and retained in the JSON report.

The image is a multi-stage build: it compiles the pinned Linux DuckDB 1.5.5
Verglas extension and copies only the release artifact into the measured client.
Quack uses `quack_serve` and `quack_query` over the isolated network with plain
HTTP explicitly selected. All four legs are mandatory: an error or a different
schema, row count, or digest aborts the run instead of producing an unavailable
or partial comparison.

Every sample records total time, time-to-first-response, CPU seconds, peak RSS,
row count, schema, and a SHA-256 digest of canonical result rows. The report also
retains hardware, limits, warmup count, and all raw samples.
