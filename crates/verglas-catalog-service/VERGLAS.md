# Verglas catalog service

This directory is the Lakekeeper-derived catalog subworkspace owned by the
Verglas repository. It is kept as a subworkspace because its pinned Iceberg and
cloud-provider dependency graph differs from the cache/query engine workspace.

The production catalog entry point is `lakekeeper serve-craft`. It constructs
the Verglas CRaft storage adapter and immutable object metadata store; it does
not construct the PostgreSQL catalog backend. Build and test it from this
directory, or pass `--manifest-path crates/verglas-catalog-service/Cargo.toml`.

The former sibling repository was consolidated here at commit
`27c2db59d8339ad5cb4bd8f9517825743123a49f`.

The upstream Lakekeeper crates remain under Apache 2.0. Verglas-authored
storage and authorization adapters are under FSL-1.1-ALv2, and the assembled
binary contains code under both licenses. See [LICENSING.md](LICENSING.md) for
the exact boundary.
