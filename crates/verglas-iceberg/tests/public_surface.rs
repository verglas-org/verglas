//! Static contract for the narrow Iceberg Sink/Catalog commit crate.
//!
//! This test is intentionally written before deleting the legacy engine surfaces.
//! It prevents custom CAS, query, async-ingest, and maintenance APIs from being
//! reintroduced while this prototype is slimmed.

use std::fs;
use std::path::Path;

/// Reads one crate file as UTF-8.
fn read(relative: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(relative);
    fs::read_to_string(path).unwrap_or_else(|error| panic!("read {relative}: {error}"))
}

/// The crate exposes only the Sink/Catalog commit modules and dependencies.
#[test]
fn public_surface_contains_no_retired_engine() {
    let manifest = read("Cargo.toml");
    for forbidden in [
        "verglas-api",
        "iceberg-datafusion",
        "datafusion",
        "arrow-csv",
        "arrow-json",
        "arrow-cast",
        "arrow-ipc",
        "async_ingest",
        "compaction",
        "estimate",
        "ingest",
        "inspect",
        "query",
        "report",
        "retention",
    ] {
        assert!(
            !manifest.contains(forbidden),
            "Cargo.toml still contains retired surface `{forbidden}`"
        );
    }

    let lib = read("src/lib.rs");
    for forbidden in [
        "pub mod async_ingest",
        "pub mod compaction",
        "pub mod estimate",
        "pub mod ingest",
        "pub mod inspect",
        "pub mod query",
        "pub mod report",
        "pub mod retention",
        "AsyncIngestQueue",
        "PreparedCatalog",
        "compact_table",
        "query_stream",
        "expire_snapshots",
    ] {
        assert!(
            !lib.contains(forbidden),
            "src/lib.rs still exports retired surface `{forbidden}`"
        );
    }
}

/// Legacy implementation files are deleted rather than left as dead modules.
#[test]
fn retired_modules_are_deleted() {
    for module in [
        "async_ingest.rs",
        "compaction.rs",
        "estimate.rs",
        "ingest.rs",
        "inspect.rs",
        "query.rs",
        "report.rs",
        "retention.rs",
    ] {
        assert!(
            !Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("src")
                .join(module)
                .exists(),
            "retired module {module} still exists"
        );
    }
}
