# Verglas

Verglas has exactly nine products, all composed from two primitives:

- **Worker** — stateless, pool-executed ingress with authority only from bindings.
- **Durable Object** — a serialized stateful Worker class backed by one
  embedded Turso `0.7.2` database in the self-hosted cell data root.
- **Stream** — a prebuilt Durable Object for durable ordered JSON records.
- **Pipeline** — a prebuilt Worker/DO with a Stream cursor, stateless SQL
  transforms, batching, and retry.
- **Sink** — a prebuilt Worker/DO with idempotent delivery; Iceberg is the first
  adapter.
- **Catalog** — a prebuilt Worker/DO exposing Iceberg REST through the existing
  Iceberg and catalog libraries.
- **Vectorize** — a prebuilt Worker/DO exposing Cloudflare-shaped vector CRUD
  and search over native Turso vectors.
- **Graph** — a prebuilt Worker/DO exposing bounded property-graph CRUD and
  traversal over Turso adjacency indexes.
- **Query** — a prebuilt Worker/DO that directly consumes Pipeline batches and
  maintains bounded, declared aggregate views and query endpoints in Turso.
`system/dashboard` is a stateless Worker specialization, not an additional
product. It queries declared Query bindings on demand and owns no durable state.

This repository also contains the host runtime, Foyer cache, origin adapter,
Iceberg commit library, and Catalog libraries used by those products. These are
implementation layers, not additional products. The JavaScript and Python
Worker SDKs, CLI, and public documentation live in
[`verglas-org/verglas-sdk`](https://github.com/verglas-org/verglas-sdk). RIME
remains the repository-local agent integration.

[![ci](https://github.com/verglas-org/verglas/actions/workflows/ci.yml/badge.svg)](https://github.com/verglas-org/verglas/actions/workflows/ci.yml)
[![coverage](https://img.shields.io/badge/coverage-77%25_measured%2C_ratcheting-green)](https://github.com/verglas-org/verglas/actions/workflows/ci.yml)

## Repository contents

- `crates/verglas-gateway`: public ingress and binding routing.
- `crates/verglasd`: the self-hosted process supervisor and lifecycle daemon.
- `crates/verglas-runtime`: the Wasmtime Worker/Durable Object host with Turso,
  Foyer tiered caching, origin access, and narrow host capabilities.
- `system/catalog`: the Turso-backed Catalog Worker/Durable Object product.
- `crates/verglas-iceberg`: the runtime's narrow deterministic commit capability.
- `rime`: the RIME package for supported agent hosts.
- The reusable Rust crates that implement the storage, cache, catalog, and
  server roles.

Hosted access, product provisioning, and the private cloud console remain
outside this repository. They do not add another product.

## License

Verglas is available under the Functional Source License 1.1 with an Apache
2.0 future license (`FSL-1.1-ALv2`). You may self-host, modify, and redistribute
Verglas for permitted purposes, but you may not offer it as a competing
commercial product or service. Each version becomes available under Apache 2.0
two years after that version is first made available. See [LICENSE](LICENSE).

The Worker/Durable Object runtime is self-hosted from this workspace. Product
components are authored with the JavaScript and Python SDKs and run through the
Wasmtime host. Durable Object state stays in embedded Turso databases under the
cell data root; the configured S3 origin stores immutable Iceberg objects.

## Build and test

```sh
just build
just test
just lint
```

Install the Worker authoring SDKs from
[`verglas-org/verglas-sdk`](https://github.com/verglas-org/verglas-sdk); RIME
lives under `rime/`. The public
[architecture documentation](https://github.com/verglas-org/verglas-sdk/tree/main/docs/architecture)
explains the nine products and their shared runtime.
Every crate and binary keeps an append-only `WORKLOG.md` describing why it
changed.

## Contributing

Contributor and agent guidance lives in [AGENTS.md](AGENTS.md). Changes use a
branch and issue, write failing acceptance tests first, preserve or raise
coverage, and update every touched crate's worklog.
