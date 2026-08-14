# Verglas patch inventory

This directory is Apache Iceberg Rust 0.9.1 source with one intentional change:

- `src/catalog/mod.rs` adds the public `TableCommit::from_parts` constructor.

Verglas compaction and snapshot expiry need to submit replace and
`RemoveSnapshots` updates through `Catalog::update_table`. Iceberg 0.9.1 keeps
the generated `TableCommit` builder private and its public transaction actions
cannot express those commits.

The remaining source is byte-for-byte identical to the published 0.9.1 crate,
excluding crates.io packaging metadata and testdata. Remove this patch once an
upstream release exposes an equivalent safe API.
