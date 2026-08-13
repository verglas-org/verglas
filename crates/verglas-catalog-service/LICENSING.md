# Catalog licensing boundary

This subworkspace combines upstream Lakekeeper-derived code with components
authored for Verglas. The licenses apply at the crate boundary below.

## Apache 2.0 crates

The following crates remain licensed under Apache 2.0:

- `lakekeeper`
- `lakekeeper-io`
- `iceberg-ext`

Their source and modifications retain the permissions and notices in
[LICENSE](LICENSE) and [NOTICE](NOTICE). Nothing in the Verglas license removes
or restricts the Apache 2.0 rights granted for that code.

## FSL crates

Copyright 2026 Verglas LLC. The following Verglas-authored crates are licensed
under FSL-1.1-ALv2:

- `lakekeeper-storage-verglas`
- `lakekeeper-authz-verglas`

The FSL terms are in the repository [LICENSE](../../LICENSE). Each version
converts to Apache 2.0 two years after that version is first made available, as
specified by those terms.

## Combined binary

The `lakekeeper-bin` package assembles Apache-licensed Lakekeeper code with the
FSL-licensed Verglas adapters. Its Cargo metadata therefore declares
`Apache-2.0 AND FSL-1.1-ALv2`. The applicable license for a source file or
component remains the license of that source or component as described above.

Third-party dependencies retain their own licenses.
