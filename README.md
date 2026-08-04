# Verglas

Engine-neutral, Iceberg-aware S3 read-through cache. Point your query engine's
S3 endpoint at Verglas and hot reads come back in about a millisecond from local
NVMe instead of a round trip to the origin bucket.

[![ci](https://github.com/cascade-labs/verglas/actions/workflows/ci.yml/badge.svg)](https://github.com/cascade-labs/verglas/actions/workflows/ci.yml)

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

See [WHITEPAPER.md](WHITEPAPER.md) for the full design.

## Install

Verglas runs on your own machine — self-hosted, no account, no cloud login. The
installers below fetch prebuilt binaries from the latest GitHub Release, so **no
Rust toolchain is required**. Each release ships four binaries for macOS
(arm64/x64), Linux (x64/arm64, glibc), and Windows (x64):

- `verglasd` — the cache daemon. Speaks the S3 protocol, serves hot reads from
  cache, reads through to the origin on a miss.
- `verglas` — the operator CLI (`verglas dev`, `verglas init`, `verglas
  status`, ...).
- `verglas-mcp` — the stdio MCP server exposing Verglas memory to MCP-capable
  agents.
- `verglas-consolidate` — the background memory-consolidation runner.

### One line (all four binaries)

macOS / Linux:

```sh
curl -fsSL https://raw.githubusercontent.com/cascade-labs/verglas/main/install.sh | sh
```

Windows (PowerShell):

```powershell
irm https://raw.githubusercontent.com/cascade-labs/verglas/main/install.ps1 | iex
```

Both wrappers just run the per-binary installers that
[cargo-dist](https://opensource.axo.dev/cargo-dist/) generates into every
release; each installs into `~/.cargo/bin` (or `%USERPROFILE%\.cargo\bin`) and
verifies a SHA-256 checksum. To install a single tool instead, run its own
installer directly, e.g. the CLI:

```sh
curl -fsSL https://github.com/cascade-labs/verglas-releases/releases/latest/download/verglas-installer.sh | sh
```

> **Windows note.** All four binaries run on Windows, but installing `verglasd`
> as a managed OS service (`verglas init` / `verglas start`) is not supported
> there yet — those commands print a clear message telling you to run the daemon
> in the foreground (`verglasd --config <path>`) instead. macOS (launchd) and
> Linux (systemd) get the managed service.

### Homebrew (coming soon)

A Homebrew formula is generated into each release. The public tap is not
published yet; once `cascade-labs/homebrew-tap` is live, install with:

```sh
brew install cascade-labs/tap/verglas
```

### Build from source (fallback)

Verglas is a Rust workspace; the toolchain is pinned in
[`rust-toolchain.toml`](rust-toolchain.toml) (Rust 1.96.1, Edition 2024), and
`rustup` installs it automatically on first build.

```sh
git clone https://github.com/cascade-labs/verglas.git
cd verglas
cargo build --workspace --release
```

To put the binaries on your `PATH` (installs into `~/.cargo/bin`):

```sh
cargo install --path bins/verglas  --locked --force
cargo install --path bins/verglasd --locked --force
```

Or use the [`justfile`](justfile) target that does the same: `just install`.

## Quickstart: a local cache in front of an S3 bucket

`verglas init` installs the daemon as an OS service (per-user by default —
launchd on macOS, systemd on Linux; `--system` for a root-owned service) and
scaffolds an annotated `~/.verglas/config.toml` plus credential-file templates
under `~/.verglas/credentials/`:

```sh
verglas init --bucket my-bucket --region us-east-1
```

The daemon serves the bucket you name. It can serve a whole set: set a single
`backend.bucket` and/or `backend.bucket_globs` — glob patterns like
`*--table-s3` that serve a dynamic family of buckets (for example every AWS S3
Tables underlying bucket) under one credential set. A request for a bucket
outside the set returns `NoSuchBucket`. Fill your origin keys into the
scaffolded backend credentials file (standard AWS credentials-file format, mode
`0600`) — on AWS you can skip this and let the ambient credential chain (SSO,
instance profile, environment) authenticate instead. Secrets never go in
`config.toml`; the config only names which file to read. For a non-AWS origin
(OCI, MinIO), pass `--endpoint` with the origin URL.

Start it and check health:

```sh
verglas start
verglas status    # service state, daemon health, version, cache warmth
```

The S3 endpoint listens on `127.0.0.1:8333`; the admin API binds the next port
(`8334`). Clients authenticate to the endpoint with Verglas-issued keys: `verglas
init` generates them, prints them in its banner, and writes them to
`~/.verglas/credentials/endpoint`. Point any S3 client at it. With the `aws` CLI:

```sh
# The keypair from the `verglas init` banner (also in ~/.verglas/credentials/endpoint).
export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...
aws s3 cp s3://my-bucket/some/object.parquet /tmp/o.parquet \
  --endpoint-url http://127.0.0.1:8333
```

The first read fills from the origin; read the same object again and it comes
straight from cache. With DuckDB, set the endpoint the same way:

```sql
SET s3_endpoint = '127.0.0.1:8333';
SET s3_use_ssl = false;
SET s3_url_style = 'path';
SELECT count(*) FROM 's3://my-bucket/some/table/*.parquet';
```

`verglas logs` tails the daemon and `verglas stop` stops the service (removal
is manual — see [Removing Verglas](#removing-verglas)). For real performance
point `--cache-dir` at an NVMe path and size the tiers with `--cache-size` /
`--dram` (init flags, or edit `~/.verglas/config.toml` and `verglas restart`).
The annotated reference for every config key is
[`verglas.example.toml`](verglas.example.toml).

When a node runs as part of a pod, `verglas drain` gracefully drains the local
daemon before decommissioning the machine: it stops taking new cache ownership,
donates its warmth to peers, then exits.

### Removing Verglas

There is deliberately no `verglas uninstall` — removal is a manual step so it
cannot be invoked by accident. Stop the service, then remove the service
definition for your OS. Config, credentials, and cache data are never removed
by these steps; delete `~/.verglas` (and your `--cache-dir` path, if set)
yourself if you want them gone.

macOS (per-user LaunchAgent, the default):

```sh
verglas stop
launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/com.cascade-labs.verglas.plist
rm ~/Library/LaunchAgents/com.cascade-labs.verglas.plist
```

macOS (system LaunchDaemon, installed with `--system`):

```sh
verglas stop --system
sudo launchctl bootout system /Library/LaunchDaemons/com.cascade-labs.verglas.plist
sudo rm /Library/LaunchDaemons/com.cascade-labs.verglas.plist
```

Linux (per-user systemd unit, the default):

```sh
verglas stop
systemctl --user disable verglasd.service
rm ~/.config/systemd/user/verglasd.service
systemctl --user daemon-reload
```

Linux (system systemd unit, installed with `--system`):

```sh
verglas stop --system
sudo systemctl disable verglasd.service
sudo rm /etc/systemd/system/verglasd.service
sudo systemctl daemon-reload
```

### Prometheus metrics

`verglasd` exposes `GET /metrics` on the loopback admin listener at port `8334`
by default (the same private surface as `/admin/stats` and `/admin/healthz`).
Metric names and labels are defined in `crates/verglas-core/src/metrics.rs`.

### For developers: `verglas dev`

`verglas dev` runs a throwaway single-process cache in the foreground — no
service install, cache in a temp dir removed on Ctrl-C. It is for hacking on
Verglas itself: `--nodes` spins up a multi-node pod on one machine, `--writeback`
enables the erasure-coded write-back tier, `--allow-http` permits a plain-HTTP
origin (MinIO). `verglas dev --help` lists every flag. If you just want a cache
in front of a bucket, use `verglas init` above.

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
`verglas workers`. WHITEPAPER.md §7 is the architecture of record — one
deployment record executed locally by the daemon or as an isolated tenant worker
in the cloud, with pipeline I/O routed through the cache.

Without a login, workers talk to the local daemon (the same endpoint and
`~/.verglas/config.toml` the other local commands use). `verglas login` adds
cloud visibility so `workers list` can merge local and cloud deployments.

```sh
# Reads the key from stdin, so it never lands in your shell history.
echo "$VERGLAS_API_KEY" | verglas login --url https://api.verglas.cloud

# Register from a portable worker spec (local daemon):
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

- [WHITEPAPER.md](WHITEPAPER.md) — the architecture and its reasoning: the cache
  tiers, Iceberg awareness, the ring, and the write-back path.
- [`verglas.example.toml`](verglas.example.toml) — annotated reference config
  covering every daemon setting.
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
