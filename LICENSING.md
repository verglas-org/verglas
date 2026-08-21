# Catalog licensing boundary

This repository combines Lakekeeper-derived code with components authored for
Verglas. The licenses apply at the crate boundary below.

## Apache 2.0 crates

The following crates are derived from Lakekeeper and remain licensed under
Apache 2.0:

- `verglas-catalog-core`
- `verglas-catalog-io`
- `verglas-iceberg-ext`

Each declares `license = { workspace = true }`, which resolves to Apache 2.0.
The license follows the crate, not its directory: a crate keeps its own
`license` field if it is ever relocated, and must never be switched to the
workspace `license-file` (the Verglas license) — that would relicense
upstream-derived code.

Their source and modifications retain the permissions and notices in
[LICENSE](LICENSE) and [NOTICE](NOTICE). Nothing in the Verglas license removes
or restricts the Apache 2.0 rights granted for that code.

## FSL crates

Copyright 2026 Verglas LLC. The following Verglas-authored crates are licensed
under FSL-1.1-ALv2:

- `verglas-catalog-storage`
- `verglas-catalog-authz`

Each version converts to Apache 2.0 two years after that version is first made
available, as specified by those terms.

## Combined binary

`verglas-cache-node`, which embeds the hosted catalog, assembles
Apache-licensed Catalog code with the FSL-licensed Verglas adapters. The
applicable license for a source file or component remains the license of that
source or component as described above.

Third-party dependencies retain their own licenses.
