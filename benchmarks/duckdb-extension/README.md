# DuckDB extension benchmark

`./run.sh --profile smoke --output report.json` measures one excluded warm-up
plus five prepared-session samples of 1, 100,000, and 1,000,000-row two-column
results through local
DuckDB, raw HTTP Arrow, the Linux release build of `verglas_query`, and Quack.
The client is a Docker cgroup constrained to exactly one CPU and
512 MiB. Arrow, Quack, and Verglas-extension service containers are independently
limited to 0.5 CPU and 256 MiB; each effective cgroup-v2 value is read from inside
its container and retained in the JSON report.

The image is a multi-stage build: it compiles the pinned Linux DuckDB 1.5.5
Verglas extension and copies only the release artifact into the measured client.
Quack is installed into the image before measurement, then uses `quack_serve`
and `quack_query` over the isolated network with plain HTTP explicitly selected.
Client imports, connections, and extension loading are also completed before
the warm-up. All four legs are mandatory: an error or a different
schema, row count, or digest aborts the run instead of producing an unavailable
or partial comparison.

Every sample streams bounded Arrow batches and records total time, time to the
first decoded non-empty batch, CPU seconds, peak process RSS, received network
bytes, row count, schema, and a SHA-256 digest of canonical result rows. The
report also retains excluded setup time, hardware, limits, warm-up count, and
all raw samples. The `full` profile extends transfer sizes through 10 million
rows and takes longer; neither profile claims full Verglas cache behavior.

The `full_stack` report section is deliberately marked `protocol-only`. A
Verglas Query deployment, catalog, object store, and cache topology must be
supplied before cold, warm, and shared-warm cache results can be measured. The
harness never substitutes the synthetic Arrow endpoint for that evidence.
