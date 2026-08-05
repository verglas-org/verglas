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

See the [architecture whitepaper](docs/architecture/whitepaper.mdx) for the
full design.

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

Run the cache server in Docker and point the CLI at its admin port.

1. Copy and edit the sample config (bucket / region / origin credentials):

```sh
cp deploy/docker/verglas.toml deploy/docker/verglas.toml.local
# edit deploy/docker/verglas.toml.local — set [backend]
cp deploy/docker/credentials/backend.example deploy/docker/credentials/backend
# fill aws_access_key_id / aws_secret_access_key (mode 0600)
```

Point `docker-compose.yml` at your edited config (or edit the mounted paths),
then:

```sh
docker compose up -d --build
```

2. Point the CLI at the container’s admin API (S3 is on `8333`, admin on `8334`):

```sh
export VERGLAS_ENDPOINT=http://127.0.0.1:8334
# optional: persist in the shell profile, or pass --server-endpoint on each command
verglas status
```

`VERGLAS_ENDPOINT` / `--server-endpoint` is how every data-plane verb reaches
the server. There is no OS service install (no launchd / systemd): the container
is the process manager. Logs: `docker compose logs -f verglas-server`. Stop:
`docker compose down`.

3. Point an S3 client at the cache:

```sh
aws s3 cp s3://my-bucket/some/object.parquet /tmp/o.parquet \
  --endpoint-url http://127.0.0.1:8333
```

```sql
-- DuckDB
SET s3_endpoint = '127.0.0.1:8333';
SET s3_use_ssl = false;
SET s3_url_style = 'path';
SELECT count(*) FROM 's3://my-bucket/some/table/*.parquet';
```

The annotated reference for every server config key is
[`verglas.example.toml`](verglas.example.toml). For a multi-node pod,
`verglas drain` drains cache ownership over the admin API before you take a
container down.

### Prometheus metrics

`verglas-server` exposes `GET /metrics` on the admin listener (port `8334` by
default). Metric names and labels are in `crates/verglas-core/src/metrics.rs`.

## Iceberg tables: add the catalog watcher

Verglas works as a plain cache with no catalog. Point it at your Iceberg REST
catalog and it also watches for table commits, so it can pre-warm table metadata
and hot data and carry cache heat across compaction automatically — planning
reads hit warm statistics, and a rewrite does not force queries to re-earn the
cache from the origin.

Add a `[catalog]` table to `~/.verglas/config.toml`:

```toml
[catalog]
# Base URI of the Iceberg REST catalog, before /v1/... . Required to turn
# Iceberg awareness on.
uri = "http://localhost:8181"

# Tables to watch, as namespace.table globs. Empty watches everything.
include = ["analytics.*"]
# Tables to skip. A match here always wins over include.
exclude = ["analytics.tmp_*"]

# Optional: path to a file (mode 0600) holding the catalog bearer token.
# The token is never stored in this config.
# credentials_file = "~/.verglas/credentials/catalog-token"

# Optional: warehouse identifier, for multi-warehouse catalogs.
# warehouse = "lake"
```

With the `[catalog]` table absent, the Iceberg-awareness layer stays off and
Verglas behaves as a byte cache. `poll_interval_secs` (default `30`) tunes the
poll cadence. Prefer `credentials_file` over an inline `bearer_token` — setting
both is a startup error.

## Platform workers (local and cloud)

The deployment primitive is a **worker**: code plus triggers, registered with
`verglas workers`. The architecture whitepaper §7 is the architecture of
record — one deployment record executed locally by the server or as an isolated
tenant worker in the cloud, with pipeline I/O routed through the cache.

Without a login, workers talk to the local server (the same endpoint and
`~/.verglas/config.toml` the other local commands use). `verglas login` adds
cloud visibility so `workers list` can merge local and cloud deployments.

```sh
# Prefer `verglas login` (browser). Or pipe a key:
echo "$VERGLAS_API_KEY" | verglas login --api-key

# Register from a portable worker spec (local server):
verglas workers create ./worker.json --local

# List and inspect:
verglas workers list
verglas workers show orders_ingest
```

`login` verifies the key against the control plane before saving anything, then
prints the account it logged in as. The key is stored at
`~/.verglas/credentials/control-plane-token` (mode 0600, never printed) and the
control plane URL in `~/.verglas/config.toml` under `[control_plane]`. Re-running
`login` overwrites both.

## Documentation

- [Architecture whitepaper](docs/architecture/whitepaper.mdx) — the architecture
  and its reasoning: the cache
  tiers, Iceberg awareness, the ring, and the write-back path.
- [`verglas.example.toml`](verglas.example.toml) — annotated reference config
  covering every server setting.
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
