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

Install the `verglas` CLI from the Verglas website (macOS, Linux, Windows — no
Rust toolchain required). The cache server (`verglas-server`) ships as a Docker
image for self-hosting; most users run against Verglas Cloud instead.

### Build the CLI from source

```sh
git clone https://github.com/verglas-org/verglas.git
cd verglas
cargo install --path bins/verglas --locked --force
```

Or `just install` for both the CLI and a local `verglas-server` binary (developers and
repository test tooling).

## Quickstart

### Verglas Cloud

1. Create an account at [verglas.dev](https://verglas.dev).
2. Log the CLI into your tenant (browser PKCE by default; or pipe an API key):

```sh
verglas login
# or: echo "$VERGLAS_API_KEY" | verglas login --api-key
```

Login verifies the key, then writes:

- `~/.verglas/credentials/control-plane-token` (mode 0600)
- `~/.verglas/config.toml` — `[control_plane]` plus lakehouse `[backend]` /
  `[catalog]` when the tenant has a warehouse
- scoped backend/catalog credential files under `~/.verglas/credentials/`

After login, cloud verbs work (`verglas workers`, `containers`, `db`, …) and
data verbs talk to your cloud endpoint. Re-run `login` any time to refresh.

### Self-host (Docker)

The Docker application is configured entirely by Compose environment values.
Export your R2 bucket, S3 credentials, catalog URI, warehouse, catalog token,
and a client-facing S3 secret, then start it:

```sh
docker compose up -d --build
```

Point the CLI at the container's API:

```sh
export VERGLAS_ENDPOINT=http://127.0.0.1:8334
verglas status
```

The [self-hosted guide](docs/get-started/self-host.mdx) covers R2 and Data
Catalog setup, every required environment variable, the complete Compose file,
and creating the first table.

### Prometheus metrics

`verglas-server` exposes `GET /metrics` on the admin listener (port `8334` by
default). Metric names and labels are in `crates/verglas-core/src/metrics.rs`.

## Iceberg tables: add the catalog watcher

Verglas works as a plain cache with no catalog. Point it at your Iceberg REST
catalog and it also watches for table commits, so it can pre-warm table metadata
and hot data and carry cache heat across compaction automatically — planning
reads hit warm statistics, and a rewrite does not force queries to re-earn the
cache from the origin.

The Docker application takes the catalog URI, warehouse, and bearer token from
`VERGLAS_CATALOG_URI`, `VERGLAS_CATALOG_WAREHOUSE`, and
`VERGLAS_CATALOG_BEARER_TOKEN` in `docker-compose.yml`. It watches the catalog
and warms changed metadata through the cache path.

## Workers and containers

Compose bootstraps only `verglas-server` and `verglas-container-runtime`. The
runtime manager owns every optional local service through Docker: scheduler,
Neon database components, external brokers, and optional applications. A portable
worker contains its bounded command, bundled files, target table, and cron, HTTP,
or CloudEvent triggers.

```sh
export VERGLAS_CONTAINER_RUNTIME_TOKEN="$(openssl rand -hex 32)"
docker compose up -d --build
verglas workers create \
  --local \
  --file examples/workers/market-data-ingest/worker.toml
```

The [Workers guide](docs/workers/overview.mdx) shows the complete program and
manifest before running it manually, through an HTTP callback, on cron, and
from a RabbitMQ CloudEvent.

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

Not yet declared. This repository has no license file and is not published to
crates.io (`publish = false`). Until a license is added, treat it as
all-rights-reserved and open an issue if you need clarity on use.
