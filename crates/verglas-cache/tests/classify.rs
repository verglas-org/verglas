//! Routing determinism (#50): the metadata/data store decision is a pure,
//! deterministic function of `(key, range)`. This test pins the routing table
//! from the issue — metadata.json, manifest-list Avro, manifest Avro, a Parquet
//! footer suffix read, a plain Parquet data range, and the critical negative:
//! an Avro *data* file is never suffix-probed or meta-routed.

use std::ops::Range;
use std::sync::Arc;

use verglas_cache::{MetaClass, MetaClassifier, MetaRouter};
use verglas_core::CacheKey;
use verglas_core::read::ReadRange;

/// A key in the warehouse bucket.
fn key(k: &str) -> CacheKey {
    CacheKey {
        storage_binding_id: "default".to_owned(),
        bucket: "warehouse".to_owned(),
        key: k.to_owned(),
    }
}

/// The heuristics-only router (no mapper hook) routes exactly the issue's
/// table, and routes identically on a repeat call (determinism).
#[test]
fn heuristics_route_the_documented_table_deterministically() {
    let router = MetaRouter::heuristics_only();
    // (key, range, expected store).
    let table: &[(&str, ReadRange, MetaClass)] = &[
        // metadata.json — whole-object read.
        (
            "db/t/metadata/v7.metadata.json",
            ReadRange::Full,
            MetaClass::Meta,
        ),
        // manifest list (snap-*.avro) under metadata/.
        (
            "db/t/metadata/snap-8123-1-abc.avro",
            ReadRange::Full,
            MetaClass::Meta,
        ),
        // manifest list beside the table root (some catalogs).
        ("db/t/snap-9-1-def.avro", ReadRange::Full, MetaClass::Meta),
        // manifest (Avro) under metadata/.
        (
            "db/t/metadata/abc-m0.avro",
            ReadRange::Full,
            MetaClass::Meta,
        ),
        // Parquet footer read: a suffix range against a .parquet key.
        (
            "db/t/data/part-0.parquet",
            ReadRange::Suffix(64 * 1024),
            MetaClass::Meta,
        ),
        // Plain Parquet data range → data store.
        (
            "db/t/data/part-0.parquet",
            ReadRange::Bounded(0, 8_388_607),
            MetaClass::Data,
        ),
        // Whole Parquet read → data store.
        ("db/t/data/part-0.parquet", ReadRange::Full, MetaClass::Data),
        // Avro DATA file: a suffix read must NOT be meta-routed (no footer).
        (
            "db/t/data/part-0.avro",
            ReadRange::Suffix(64 * 1024),
            MetaClass::Data,
        ),
        // Avro DATA file whole read → data store.
        ("db/t/data/part-0.avro", ReadRange::Full, MetaClass::Data),
        // Unknown object → data store.
        ("db/t/data/part-0.orc", ReadRange::Full, MetaClass::Data),
    ];

    for (k, range, expected) in table {
        let got = router.route(&key(k), *range);
        assert_eq!(got, *expected, "routing {k} {range:?}");
        // Determinism: a second identical call routes the same way.
        assert_eq!(router.route(&key(k), *range), got, "non-deterministic {k}");
    }
}

/// A classifier that says "metadata" only for one exact key — stands in for the
/// mapper (#49) reporting `TableMetadata`.
struct OnlyKey(&'static str);

impl MetaClassifier for OnlyKey {
    fn is_table_metadata(&self, key: &CacheKey, _range: Option<Range<u64>>) -> bool {
        key.key == self.0
    }
}

/// With a mapper hook wired in, a catalogued metadata object the heuristics
/// would miss (an oddly-named statistics/puffin object with no `.json`/`.avro`
/// suffix) still routes to the metadata store.
#[test]
fn mapper_hook_routes_catalogued_metadata_the_heuristics_miss() {
    let plain = "db/t/metadata/stats-0000";
    // Heuristics alone: not under a recognized pattern → data.
    assert_eq!(
        MetaRouter::heuristics_only().route(&key(plain), ReadRange::Full),
        MetaClass::Data
    );
    // With the mapper hook: the mapper says TableMetadata → meta.
    let router = MetaRouter::with_classifier(Arc::new(OnlyKey(plain)));
    assert_eq!(router.route(&key(plain), ReadRange::Full), MetaClass::Meta);
    // A different object the mapper does not claim still routes to data.
    assert_eq!(
        router.route(&key("db/t/data/other.parquet"), ReadRange::Full),
        MetaClass::Data
    );
}
