# Verglas

Verglas is the lakehouse runtime — an Iceberg-native statekeeper that makes
data on object storage live. Point an S3-compatible query engine at Verglas:
hot reads serve from local DRAM or NVMe, writes acknowledge at NVMe latency
under quorum durability, and your object store stays the system of record.

[![ci](https://github.com/verglas-org/verglas/actions/workflows/ci.yml/badge.svg)](https://github.com/verglas-org/verglas/actions/workflows/ci.yml)
[![coverage](https://img.shields.io/badge/coverage-77%25_measured%2C_ratcheting-green)](https://github.com/verglas-org/verglas/actions/workflows/ci.yml)


## What lives here

This repository contains the public data engine and its client surfaces:

- `verglas-cache-node`: S3 read/write-through, Iceberg catalog watching,
  warming, cache tiers, ring routing, block storage, and WAL ingress.
- `cli`: the `verglas` command-line client.
- `sdks`: the public Rust and TypeScript SDKs.
- `rime`: the RIME package installed by the CLI for supported agent hosts.
- `catalog`: the Verglas fork of Catalog, the Apache Iceberg REST
  catalog. A nested cargo workspace with its own lockfile — the root
  `cargo --workspace` commands do not reach it. See [AGENTS.md](AGENTS.md).
- The reusable Rust crates that implement the storage and client roles.

The remaining product boundaries are deliberate:

- `verglas-cloud` owns hosted access, scheduling, workers, integrations,
  databases, applications, and agent runtime services.
- `verglas-app` is the private cloud console and workspace client.

CI rejects copies of those hosted products in this repository.

## License

Verglas is available under the Functional Source License 1.1 with an Apache
2.0 future license (`FSL-1.1-ALv2`). You may self-host, modify, and redistribute
Verglas for permitted purposes, but you may not offer it as a competing
commercial product or service. Each version becomes available under Apache 2.0
two years after that version is first made available. See [LICENSE](LICENSE).

`catalog/` is the exception. It is Catalog-derived code and is mostly
Apache 2.0, with only the Verglas-authored adapters under FSL-1.1-ALv2.
[catalog/LICENSING.md](catalog/LICENSING.md) governs that subtree and
states which crates fall under which license.

## Install

Install the CLI with one command:

```sh
curl -fsSL https://verglas.dev/install.sh | sh
```

The SDKs install from their package managers:

```sh
npm install @verglas/sdk    # TypeScript
cargo add verglas-sdk       # Rust
```

The daemon is distributed as a container image:

```sh
docker pull ghcr.io/verglas-org/verglas-cache-node:latest
```

## Run the engine locally

The open-source Compose stack starts exactly one disposable `verglas-cache-node`.
It contains no catalog, object store, scheduler, or hosted control plane. Choose
one provider profile in [the self-hosting guide](docs/get-started/self-host.mdx),
then start it with the provider's credentials:

```sh
docker compose up --build verglas
```

The node exposes its S3 surface at `http://127.0.0.1:8333` and its health,
catalog gateway, and metrics endpoints at `http://127.0.0.1:8334`. Tables use
the local Iceberg REST gateway at `http://127.0.0.1:8334/catalog`; Graphs and
Vectors use the same local S3 listener. All three therefore keep provider
credentials inside the node process and route data files through it.

The supported profiles are Verglas Cloud, Cloudflare Data Catalog, and
Amazon S3 Tables. The Cloud profile accepts event hints at
`/admin/catalog/events` and always reconciles by polling. Cloudflare and AWS
are polling-only upstreams. Stop the disposable node and remove its local
state with:

```sh
docker compose down
rm -rf ./.verglas
```

## Build and test

```sh
just build   # both workspaces
just test    # engine only
just lint    # both workspaces
```

The catalog is a separate cargo workspace, so it has its own recipes:
`just catalog-build`, `just catalog-lint`, and `just catalog-test`.
Its suite needs a reachable Postgres in `DATABASE_URL`, which is why it is not
part of `just test`.

Install the runtime node and CLI from source with `just install`. The Rust and
TypeScript SDKs live under `sdks/`; RIME lives under `rime/`.

The [architecture overview](docs/architecture/overview.mdx) explains the
runtime's cache tiers, Iceberg awareness, routing, and write path. Every crate and binary keeps
an append-only `WORKLOG.md` describing why it changed.

## Contributing

Contributor and agent guidance lives in [AGENTS.md](AGENTS.md). Changes use a
branch and issue, write failing acceptance tests first, preserve or raise
coverage, and update every touched crate's worklog.
