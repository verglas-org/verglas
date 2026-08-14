# Verglas

Verglas is an engine-neutral, Iceberg-aware S3 cache and storage engine. Point
an S3-compatible query engine at Verglas and hot reads are served from local
DRAM or NVMe instead of repeatedly crossing the object-store boundary.

[![ci](https://github.com/verglas-org/verglas/actions/workflows/ci.yml/badge.svg)](https://github.com/verglas-org/verglas/actions/workflows/ci.yml)

> Prototype — pre-release. On-disk layouts, wire formats, and config keys may
> change between commits.

## What lives here

This repository contains the public data engine and its client surfaces:

- `verglas-cache-node`: S3 read/write-through, Iceberg catalog watching,
  warming, cache tiers, ring routing, block storage, and WAL ingress.
- `cli`: the `verglas` command-line client.
- `sdks`: the public Rust and TypeScript SDKs.
- `rime`: the RIME package installed by the CLI for supported agent hosts.
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

## Run the engine locally

The open-source Compose stack starts exactly one disposable `verglas-cache-node`.
It contains no catalog, object store, scheduler, or hosted control plane. Choose
one provider profile in [the self-hosting guide](docs/get-started/self-host.mdx),
then start it with the provider's credentials:

```sh
docker compose up --build verglas
```

The node exposes its local S3 cache at `http://127.0.0.1:8333` and its health,
catalog gateway, and metrics endpoints at `http://127.0.0.1:8334`. Tables use
the local Iceberg REST gateway at `http://127.0.0.1:8334/catalog`; Graphs and
Vectors use the same local S3 listener. All three therefore keep provider
credentials inside the cache process and route data files through the cache.

The supported profiles are Verglas Cloud, Cloudflare R2 Data Catalog, and
Amazon S3 Tables. The Cloud profile accepts event hints at
`/admin/catalog/events` and always reconciles by polling. Cloudflare and AWS
are polling-only upstreams. Stop the disposable cache and remove its local
state with:

```sh
docker compose down
rm -rf ./.verglas
```

## Build and test

```sh
just build
just test
just lint
```

Install the cache node and CLI from source with `just install`. The Rust and
TypeScript SDKs live under `sdks/`; RIME lives under `rime/`.

The [architecture overview](docs/architecture/overview.mdx) explains the cache
tiers, Iceberg awareness, routing, and write path. Every crate and binary keeps
an append-only `WORKLOG.md` describing why it changed.

## Contributing

Contributor and agent guidance lives in [AGENTS.md](AGENTS.md). Changes use a
branch and issue, write failing acceptance tests first, preserve or raise
coverage, and update every touched crate's worklog.
