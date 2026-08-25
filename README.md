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

This repository also contains the host runtime, Foyer cache, origin adapter,
Iceberg commit library, and Catalog libraries used by those products. These are
implementation layers, not additional products. The TypeScript SDK and RIME
package are client/tooling surfaces.

[![ci](https://github.com/verglas-org/verglas/actions/workflows/ci.yml/badge.svg)](https://github.com/verglas-org/verglas/actions/workflows/ci.yml)
[![coverage](https://img.shields.io/badge/coverage-77%25_measured%2C_ratcheting-green)](https://github.com/verglas-org/verglas/actions/workflows/ci.yml)

## Repository contents

- `crates/verglas-runtime`: the Wasmtime Worker/Durable Object host with Turso,
  Foyer tiered caching, origin access, and narrow host capabilities.
- `system/catalog`: the Turso-backed Catalog Worker/Durable Object product.
- `crates/verglas-iceberg`: the runtime's narrow deterministic commit capability.
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

## Install

Install the TypeScript SDK from npm:

```sh
npm install @verglas/sdk
```

The Worker/Durable Object runtime is built from this workspace. Product
components are authored with the JavaScript and Python SDKs and run through the
Wasmtime host; hosted provisioning and public control-plane APIs live outside
this repository.

## Build and test

```sh
just build
just test
just lint
```

The TypeScript SDK lives under `sdks/typescript`; RIME lives under `rime/`.

The [architecture overview](docs/architecture/overview.mdx) explains the six
products and their shared Wasmtime runtime, Foyer cache, origin adapter, and
Iceberg Catalog capability.
Every crate and binary keeps an append-only `WORKLOG.md` describing why it
changed.

## Contributing

Contributor and agent guidance lives in [AGENTS.md](AGENTS.md). Changes use a
branch and issue, write failing acceptance tests first, preserve or raise
coverage, and update every touched crate's worklog.
