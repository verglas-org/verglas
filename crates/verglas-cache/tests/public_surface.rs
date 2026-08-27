//! Public-surface guard for the single-node Foyer origin cache.
//!
//! This test is intentionally written before the cleanup. It prevents the
//! deleted peer, write-back, metadata, materialized-page, and erasure APIs from
//! returning while the cache implementation is reduced to one direct cache.

use std::fs;
use std::path::Path;

/// Reads one file from this crate's source tree.
fn read(relative: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(relative);
    fs::read_to_string(path).expect("cache source file exists")
}

/// The cache exposes only the byte-cache engine and its counters.
#[test]
fn cache_public_surface_is_single_node_foyer_only() {
    let lib = read("src/lib.rs");
    let manifest = read("Cargo.toml");
    for forbidden in [
        "pub mod classify",
        "pub mod writeback_codec",
        "materialized",
        "MetaRouter",
        "CachePurger",
        "BlockDemoter",
        "Geometry",
        "StripeEncoder",
    ] {
        assert!(!lib.contains(forbidden), "lib.rs retains {forbidden}");
    }
    for forbidden in ["reed-solomon-simd", "reed-solomon-erasure", "crc32c"] {
        assert!(
            !manifest.contains(forbidden),
            "Cargo.toml retains {forbidden}"
        );
    }
}

/// The engine has no peer/ring/write-back or generic tier seam.
#[test]
fn cache_engine_has_no_cluster_or_writeback_surface() {
    let engine = read("src/engine.rs");
    for forbidden in [
        "HybridCacheEngine<",
        "PeerFetch",
        "RendezvousRing",
        "Ring",
        "NoopPeerFetch",
        "reclaim_disk_for_writeback",
        "restore_disk",
        "populate_on_write",
        "meta_store",
        "MaterializedPage",
        "demot",
        "writeback",
        "erasure",
    ] {
        assert!(!engine.contains(forbidden), "engine.rs retains {forbidden}");
    }
}
