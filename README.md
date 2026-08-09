# Verglas

Engine-neutral, Iceberg-aware S3 read-through cache. Point your query engine's
S3 endpoint at Verglas and hot reads come back in about a millisecond from local
NVMe instead of a round trip to the origin bucket.

[![ci](https://github.com/verglas-org/verglas/actions/workflows/ci.yml/badge.svg)](https://github.com/verglas-org/verglas/actions/workflows/ci.yml)

> Prototype — pre-release. On-disk layouts, wire formats, and config keys may
> change between commits.

## Why

Verglas caches at the S3 protocol level, so any engine that speaks S3 — DuckDB,
Trino, Spark, Polars, PyArrow, the `aws` CLI — uses it with a single
`s3.endpoint` change and no per-engine plugin. It reads through to the origin on
a miss, so turning it off just makes reads slower, never wrong. Because it
understands Iceberg metadata, it caches at column-chunk granularity, pins table
statistics for planning, and transfers cache heat across compaction by snapshot
rather than by path — so a rewrite does not crater the hit rate the way a
path-keyed cache does.

See the [architecture overview](docs/architecture/overview.mdx) for the full
design.

## Install

Install the `verglas` CLI from source (macOS, Linux, Windows). The cache server
(`verglas-server`) ships as a Docker image for self-hosting.

### Build the CLI from source

```sh
git clone https://github.com/verglas-org/verglas.git
cd verglas
cargo install --path bins/verglas --locked --force
```

Or `just install` for both the CLI and a local `verglas-server` binary (developers and
repository test tooling).

## Quickstart

### Self-host (Docker)

The Docker application boots without a process-global object store or catalog.
Provider credentials and database bindings are created through the access API,
so adding a database never requires editing Compose or restarting the cache.

```sh
docker compose up -d --build
```

Point the CLI at the container's API:

```sh
export VERGLAS_ENDPOINT=http://127.0.0.1:8334
export VERGLAS_ACCESS_ENDPOINT=http://127.0.0.1:8345
export VERGLAS_TOKEN=verglas-local-access
verglas status
```

Verglas OS is available at `http://127.0.0.1:8787`. It is a community and
development application in the Compose stack, not a dependency of the
`verglas` server or CLI binaries and not a new CLI command.

The [self-hosted guide](docs/get-started/self-host.mdx) covers bringing up the
stack and adding managed or customer-owned database resources.

### Prometheus metrics

`verglas-server` exposes `GET /metrics` on the admin listener (port `8334` by
default). Metric names and labels are in `crates/verglas-core/src/metrics.rs`.

### Tenant databases and scoped secrets

One tenant cache serves every database through explicit storage and catalog
bindings. Managed lakehouses share the tenant's Lakekeeper deployment but each
gets its own warehouse; managed Postgres databases use Verglas Neon.

```sh
# Managed storage + managed Lakekeeper warehouse
verglas db create analytics --type lakehouse

# Independent managed Neon Postgres
verglas db create my_test_db --type postgres

# Secret material is prompted or read from stdin, never passed in argv
verglas secret create customer_s3 \
  --type s3 \
  --scope s3://customer-bucket/team

# Customer storage + managed Lakekeeper
verglas db create customer_lake \
  --type lakehouse \
  --data-path s3://customer-bucket/team

# Customer storage + customer Iceberg REST catalog
verglas secret create customer_catalog \
  --type iceberg-rest \
  --scope https://catalog.customer.com

verglas db create external_lake \
  --type lakehouse \
  --data-path s3://customer-bucket/team \
  --catalog https://catalog.customer.com \
  --warehouse customer_warehouse
```

Database creation resolves the most-specific authorized secret scope once and
stores its stable resource ID. Rotating that secret updates the same resource;
creating a later overlapping secret cannot silently rebind the database.

## Iceberg tables and catalog watchers

Verglas works as a plain cache with no catalog. Point it at your Iceberg REST
catalog and it also watches for table commits, so it can pre-warm table metadata
and hot data and carry cache heat across compaction automatically — planning
reads hit warm statistics, and a rewrite does not force queries to re-earn the
cache from the origin.

Each lakehouse database owns its catalog binding. Managed databases use the
tenant Lakekeeper deployment; external databases resolve an authorized
`iceberg-rest` secret. The corresponding watcher is attached to that database,
not to process-global environment variables.

## Workers and containers

Compose bootstraps `verglas-server`, the local container runtime, the durable
worker scheduler and its Postgres queue, and Verglas OS. The runtime manager
owns dynamically added Vessels, database components, external brokers, and
other optional applications. A portable worker contains its bounded command,
bundled files, target table, and cron, HTTP, or CloudEvent triggers.

```sh
docker compose up -d --build
verglas workers create \
  --file examples/workers/market-data-ingest/worker.toml
```

The [Workers guide](docs/workers/overview.mdx) shows the complete program and
manifest before running it manually, through an HTTP callback, on cron, and
from a RabbitMQ CloudEvent.

## Verglas OS

The local agentic lakehouse application lives in [`apps/os`](apps/os). It uses
the TypeScript SDK in this repository directly and connects to the local Verglas
admin, scheduler, and container-runtime APIs.

```sh
cd apps/os
pnpm run-local
```

Open `http://127.0.0.1:8787`. See the
[`apps/os` README](apps/os/README.md) for its runtime variables and development
commands.

## Documentation

- [Architecture](docs/architecture/overview.mdx) — cache tiers, Iceberg
  awareness, the ring, write-back, and execution roles.
- `crates/verglas-core/src/metrics.rs` — Prometheus metric names/labels on
  `/metrics` (stability contract).
- Each crate under `crates/` and `bins/` carries a `WORKLOG.md` recording what
  changed and why.

## Contributing

Contributor and agent guidance lives in [AGENTS.md](AGENTS.md) (`CLAUDE.md` is a
symlink to it). The short version:

```sh
just build   # cargo build --workspace
just test    # cargo test --workspace
just lint    # cargo fmt --all --check + clippy -D warnings
```

- Branch per change; open a PR against `main` and reference the issue it closes.
- CI runs fmt, clippy (`-D warnings`), build, and the test suite; all must be
  green before review. See [`.github/workflows/ci.yml`](.github/workflows/ci.yml).
- Tests are written test-first from the issue's acceptance criteria. A nightly
  coverage job enforces a line-coverage floor and ratchets it upward; coverage
  must not decrease in a PR.
- Every crate touched by a change gets an entry appended to its `WORKLOG.md`.

## License

The Rust workspace is not yet licensed or published to crates.io. Until a
root license is added, treat it as all-rights-reserved. The separately licensed
Verglas OS application retains its Apache 2.0 license in
[`apps/os/LICENSE`](apps/os/LICENSE).
