# Verglas

Verglas has exactly six products, all composed from two primitives:

- **Worker** — stateless, pool-executed ingress with authority only from bindings.
- **Durable Object** — a serialized stateful Worker class backed by Turso
  `0.7.2`, with one remote Turso database per object.
- **Stream** — a prebuilt Durable Object for durable ordered JSON records.
- **Pipeline** — a prebuilt Worker/DO with a Stream cursor, stateless SQL
  transforms, batching, and retry.
- **Sink** — a prebuilt Worker/DO with idempotent delivery; Iceberg is the first
  adapter.
- **Catalog** — a prebuilt Worker/DO exposing Iceberg REST through the existing
  Iceberg and catalog libraries.

This repository also contains independent infrastructure used by those
products and by self-hosted deployments: the S3/cache and routing layer,
Iceberg and catalog libraries, and semantic graph/vector services. Those
infrastructure components are not additional products. The TypeScript SDK and
RIME package are client/tooling surfaces.

[![ci](https://github.com/verglas-org/verglas/actions/workflows/ci.yml/badge.svg)](https://github.com/verglas-org/verglas/actions/workflows/ci.yml)
[![coverage](https://img.shields.io/badge/coverage-77%25_measured%2C_ratcheting-green)](https://github.com/verglas-org/verglas/actions/workflows/ci.yml)

## Repository contents

- `verglas-cache-node`: S3 read/write-through, cache tiers, ring routing, block
  storage, WAL ingress, and independent Iceberg/catalog integration.
- `crates/verglas-catalog-*` and `crates/verglas-iceberg-ext`: the Verglas
  Iceberg REST catalog and its extensions.
- `sdks/typescript`: the public TypeScript SDK.
- `rime`: the RIME package for supported agent hosts.
- The reusable Rust crates that implement the storage, cache, catalog, and
  server roles.

Hosted access, product provisioning, and the private cloud console remain
outside this repository. They do not add a seventh product.

## License

Verglas is available under the Functional Source License 1.1 with an Apache
2.0 future license (`FSL-1.1-ALv2`). You may self-host, modify, and redistribute
Verglas for permitted purposes, but you may not offer it as a competing
commercial product or service. Each version becomes available under Apache 2.0
two years after that version is first made available. See [LICENSE](LICENSE).

The catalog-derived crates have their own licensing boundary. See
[LICENSING.md](LICENSING.md) for the applicable terms.

## Install

Install the TypeScript SDK from npm:

```sh
npm install @verglas/sdk
```

The cache node is distributed as a container image:

```sh
docker pull ghcr.io/verglas-org/verglas-cache-node:latest
```

## Run the cache node locally

The open-source Compose stack starts one disposable `verglas-cache-node`. It
contains no hosted control plane. Choose one provider profile in [the
self-hosting guide](docs/get-started/self-host.mdx), then start it with the
provider's credentials:

```sh
docker compose up --build verglas
```

The node exposes its S3 surface at `http://127.0.0.1:8333` and its health,
Iceberg REST gateway, and metrics endpoints at `http://127.0.0.1:8334`. Existing
semantic graph/vector endpoints use the same S3 listener; they remain
independent infrastructure rather than products in addition to the six above.

The supported profiles are Verglas Cloud, Cloudflare Data Catalog, and Amazon S3
Tables. The Cloud profile accepts event hints at `/admin/catalog/events` and
always reconciles by polling. Cloudflare and AWS are polling-only upstreams.
Stop the disposable node and remove its local state with:

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

Install the runtime node from source with `just install`. The TypeScript SDK
lives under `sdks/typescript`; RIME lives under `rime/`.

The [architecture overview](docs/architecture/overview.mdx) explains the six
products and the independent cache, Iceberg, catalog, and semantic layers.
Every crate and binary keeps an append-only `WORKLOG.md` describing why it
changed.

## Contributing

Contributor and agent guidance lives in [AGENTS.md](AGENTS.md). Changes use a
branch and issue, write failing acceptance tests first, preserve or raise
coverage, and update every touched crate's worklog.
