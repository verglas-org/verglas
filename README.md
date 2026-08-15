# Verglas

Verglas is the lakehouse runtime — an Iceberg-native statekeeper that makes
data on object storage live. Point an S3-compatible query engine at Verglas:
hot reads serve from local DRAM or NVMe, writes acknowledge at NVMe latency
under quorum durability, and your object store stays the system of record.

[![ci](https://github.com/verglas-org/verglas/actions/workflows/ci.yml/badge.svg)](https://github.com/verglas-org/verglas/actions/workflows/ci.yml)
[![coverage](https://img.shields.io/badge/coverage-%E2%89%A588%25_enforced-brightgreen)](https://github.com/verglas-org/verglas/actions/workflows/ci.yml)


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

The local Compose stack runs one runtime node (`verglas-cache-node`) against a
real S3-compatible origin such as Cloudflare R2 (set the `VERGLAS_STORAGE_*`
variables). It contains no hosted control-plane, scheduler, authentication
service, or application runtime.

```sh
docker compose up --build
```

The S3 surface is available at `http://127.0.0.1:8333` with the development
credentials `verglas-local` / `verglas-local-secret`. The node health and
metrics endpoints are on `http://127.0.0.1:8334`.

To enable Iceberg-aware watching and warming, point the node at an existing
Iceberg REST catalog before starting it:

```sh
export VERGLAS_CATALOG_URI=https://catalog.example.com
export VERGLAS_CATALOG_WAREHOUSE=warehouse_name
docker compose up --build
```

Without a catalog, Verglas remains a correct S3 pass-through. It does not claim
Iceberg-aware acceleration is active.

## Build and test

```sh
just build
just test
just lint
```

Install the runtime node and CLI from source with `just install`. The Rust and
TypeScript SDKs live under `sdks/`; RIME lives under `rime/`.

The [architecture overview](docs/architecture/overview.mdx) explains the
runtime's cache tiers, Iceberg awareness, routing, and write path. Every crate and binary keeps
an append-only `WORKLOG.md` describing why it changed.

## Contributing

Contributor and agent guidance lives in [AGENTS.md](AGENTS.md). Changes use a
branch and issue, write failing acceptance tests first, preserve or raise
coverage, and update every touched crate's worklog.
