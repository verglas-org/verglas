//! Read routing: decide whether an object read is *table metadata* (pin it in
//! the dedicated metadata store, #50) or ordinary *data* (the big hybrid
//! cache). The decision is a deterministic, pure function of the object key and
//! the request's range shape — the same `(key, range)` always routes to the
//! same store, so metadata is never duplicated across the two stores.
//!
//! Two signals feed the decision, in order:
//! 1. **Pre-mapper heuristics** (this module, no table knowledge required):
//!    path- and shape-based patterns that hold for every Iceberg table before
//!    it is onboarded — `**/metadata/*.json`, `**/metadata/*.avro`,
//!    `*/snap-*.avro`, and a *suffix*-range GET against a `*.parquet` key (a
//!    Parquet footer read: engines read the last N bytes, a distinctive shape).
//! 2. **The mapper** (#49), consulted through the [`MetaClassifier`] trait when
//!    the daemon wires one in: it says `TableMetadata` for catalogued
//!    manifests, manifest lists, and resolved footer ranges of watched tables.
//!
//! Avro *data* files never route here: they carry no footer, so the suffix
//! heuristic fires only on `*.parquet` keys, and the mapper reports Avro data
//! reads as `TableData`. A suffix read of an Avro data file is therefore never
//! meta-routed and never suffix-probed as if it had a footer.
//!
//! # Mapping mutability classification (issue #14)
//!
//! Separate from *store* routing above, this module also classifies a key's
//! **key→ETag mapping mutability** — the one cache state that can go stale. The
//! ETag-in-key scheme (#121) makes every cached block immutable (an overwrite
//! gets a new ETag and so a different block key); the *mapping* from a key to
//! its current ETag is the sole exception, and [`classify_mutability`] decides
//! how long the engine may trust one:
//!
//! - **Immutable-classified** ([`Mutability::Immutable`]): Iceberg data and
//!   metadata files, whose bytes never change under a fixed key —
//!   `*.parquet`/`*.orc`/`*.avro` data files (all three formats, per the #14
//!   Avro/ORC addendum — immutability is format-blind) and `metadata/*.json`
//!   metadata files (`metadata.json`, and `.avro` manifest lists/manifests are
//!   already covered by the data-file suffixes). Their mappings are pinned
//!   indefinitely and generate **zero** revalidation traffic.
//! - **Mutable-classified** ([`Mutability::Mutable`]): everything else — the
//!   catalog pointer and arbitrary non-Iceberg bucket objects. Their mappings
//!   carry a short TTL (`cache.mutable_mapping_ttl_secs`, default 5s) and are
//!   revalidated with a conditional `If-None-Match` request after expiry.
//!
//! ## Failure modes (v1 heuristic)
//!
//! This is a v1 path heuristic; M3 replaces it with real catalog knowledge.
//! Two ways it can misjudge, both bounded and both fail-safe *for reads through
//! Verglas* (write-through invalidation, #9/#21, drops the mapping regardless
//! of class):
//!
//! - **False immutable.** A non-Iceberg object that happens to end in
//!   `.parquet`/`.orc`/`.avro` (or a `.json` under a `metadata/` dir) but is
//!   *overwritten in place behind Verglas's back* is pinned indefinitely, so
//!   the stale version is served until the mapping is evicted or an explicit
//!   purge/refresh runs. This is the deliberate cost of the immutability bet:
//!   in a lakehouse those extensions *are* immutable Iceberg files. It never
//!   serves *wrong* bytes for a read that resolved the new version — blocks are
//!   ETag-verified on fill — it only serves an older consistent snapshot.
//! - **False mutable.** An Iceberg data file under an unusual extension is
//!   treated as mutable and pays a cheap conditional revalidation every TTL
//!   window. Correct, just slightly chattier than necessary.
//!
//! Note the heuristic deliberately does *not* pin `metadata/version-hint.text`
//! (the Hadoop-tables catalog pointer): it is neither a data-file suffix nor a
//! `.json`, so it classifies mutable and is revalidated — exactly right for the
//! one genuinely-mutable object under `metadata/`.

use std::ops::Range;

use verglas_core::CacheKey;
use verglas_core::read::ReadRange;

/// Which store a read is routed to. `Copy` so it threads through the read path
/// (block body → miss ladder) without allocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetaClass {
    /// Table metadata: pin in the dedicated, hard-isolated metadata store.
    Meta,
    /// Ordinary data: the big hybrid cache under scan-resistant admission.
    Data,
}

/// The mapper-backed classification trait (#49 → #50). The daemon wires an
/// adapter over the logical-key mapper; the cache crate depends only on this
/// trait, never on `verglas-tables`, so the layering stays acyclic.
///
/// Implementations must be lock-free and allocation-free on the hot path — the
/// mapper's `classify()` already is (`ArcSwap` load + binary search).
pub trait MetaClassifier: Send + Sync {
    /// Whether `(key, range)` names table metadata (a `metadata.json`, manifest
    /// list, manifest, or a resolved Parquet footer range of a watched table).
    /// `range` is the absolute byte range when the request carries one, else
    /// `None` (a whole-object or open-ended read — the mapper falls back to
    /// path-based classification).
    fn is_table_metadata(&self, key: &CacheKey, range: Option<Range<u64>>) -> bool;
}

/// Routes reads to the metadata store or the data store. Holds the optional
/// mapper hook; the pre-mapper heuristics need no state. Cheap to clone-share
/// behind an `Arc` in the engine.
#[derive(Clone, Default)]
pub struct MetaRouter {
    /// The mapper hook, when the daemon has wired one in. Absent means
    /// heuristics-only routing — still catches every un-onboarded table's
    /// planning objects, which is what the cold-start value depends on.
    classifier: Option<std::sync::Arc<dyn MetaClassifier>>,
}

impl MetaRouter {
    /// A router with no mapper hook: routes purely on the pre-mapper
    /// heuristics. This is what a daemon without a live catalog watcher uses.
    pub fn heuristics_only() -> MetaRouter {
        MetaRouter { classifier: None }
    }

    /// A router that consults `classifier` (the mapper) after the heuristics.
    pub fn with_classifier(classifier: std::sync::Arc<dyn MetaClassifier>) -> MetaRouter {
        MetaRouter {
            classifier: Some(classifier),
        }
    }

    /// Routes one read. Deterministic: a pure function of `(key.key, range)`
    /// plus the (immutable-per-request) mapper state. Heuristics win first (they
    /// need no table state), then the mapper hook; anything unmatched is data.
    pub fn route(&self, key: &CacheKey, range: ReadRange) -> MetaClass {
        if heuristic_is_metadata(&key.key, range) {
            return MetaClass::Meta;
        }
        if let Some(classifier) = &self.classifier
            && classifier.is_table_metadata(key, absolute_range(range))
        {
            return MetaClass::Meta;
        }
        MetaClass::Data
    }
}

/// Whether a key's key→ETag *mapping* may be trusted indefinitely or must be
/// revalidated on a short TTL (issue #14). Distinct from [`MetaClass`], which
/// picks a *store*; a key can be `Data`-routed yet `Immutable`-classified (a
/// bulk `*.parquet` data read: cached in the big hybrid cache, but its mapping
/// never goes stale). `Copy` so it threads through the read path allocation-free.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mutability {
    /// An Iceberg data/metadata file: bytes never change under a fixed key, so
    /// the mapping is pinned indefinitely with no revalidation traffic.
    Immutable,
    /// Anything else (catalog pointer, arbitrary bucket objects): the mapping
    /// carries a short TTL and is revalidated with a conditional GET on expiry.
    Mutable,
}

/// Classifies a key's mapping mutability from its path alone (issue #14, v1
/// heuristic). Pure and total. See the module comment for the pattern set and
/// its failure modes.
///
/// - `*.parquet`/`*.orc`/`*.avro` anywhere ⇒ immutable Iceberg data file (the
///   Avro/ORC addendum: all three formats, immutability is format-blind).
/// - `*.json` under a `metadata/` directory ⇒ immutable Iceberg metadata file.
/// - everything else ⇒ mutable.
pub(crate) fn classify_mutability(key: &str) -> Mutability {
    let filename = key.rsplit('/').next().unwrap_or(key);
    if filename.ends_with(".parquet") || filename.ends_with(".orc") || filename.ends_with(".avro") {
        return Mutability::Immutable;
    }
    if in_metadata_dir(key) && filename.ends_with(".json") {
        return Mutability::Immutable;
    }
    Mutability::Mutable
}

/// The pre-mapper metadata heuristics. Pure and total over `(key, range)`.
///
/// - Under a `metadata/` directory and ending `.json` or `.avro`
///   (`metadata.json`, manifest lists, manifests).
/// - A `snap-*.avro` filename anywhere (manifest lists — some catalogs write
///   them beside the table root rather than under `metadata/`).
/// - A **suffix**-range GET against a `*.parquet` key (a footer read). Only
///   suffix reads: a plain bounded/whole read of a Parquet file is data.
pub(crate) fn heuristic_is_metadata(key: &str, range: ReadRange) -> bool {
    let filename = key.rsplit('/').next().unwrap_or(key);
    if in_metadata_dir(key) && (filename.ends_with(".json") || filename.ends_with(".avro")) {
        return true;
    }
    if filename.starts_with("snap-") && filename.ends_with(".avro") {
        return true;
    }
    if matches!(range, ReadRange::Suffix(_)) && filename.ends_with(".parquet") {
        return true;
    }
    false
}

/// Whether `key` sits in a `metadata/` directory (at the root or nested under
/// the table prefix).
fn in_metadata_dir(key: &str) -> bool {
    key.starts_with("metadata/") || key.contains("/metadata/")
}

/// The absolute byte range a request carries, for the mapper hook — `Some` only
/// when the range is known without the object size. Whole-object, open-ended,
/// and suffix reads yield `None` (the mapper then classifies by path alone; the
/// suffix/footer case is already covered by the heuristic).
fn absolute_range(range: ReadRange) -> Option<Range<u64>> {
    match range {
        ReadRange::Bounded(first, last) => Some(first..last.saturating_add(1)),
        ReadRange::Full | ReadRange::From(_) | ReadRange::Suffix(_) => None,
    }
}

#[cfg(test)]
mod mutability_tests {
    //! Unit coverage for the #14 mapping-mutability heuristic across every
    //! pattern and the documented false-mutable/false-immutable boundaries.

    use super::{Mutability, classify_mutability};

    #[test]
    fn iceberg_data_files_are_immutable_in_every_format() {
        for k in [
            "warehouse/db/t/data/00000-0.parquet",
            "warehouse/db/t/data/00000-0.orc",
            "warehouse/db/t/data/00000-0.avro",
            "some/deep/path/file.parquet",
        ] {
            assert_eq!(classify_mutability(k), Mutability::Immutable, "{k}");
        }
    }

    #[test]
    fn metadata_json_under_metadata_dir_is_immutable() {
        assert_eq!(
            classify_mutability("warehouse/db/t/metadata/v3.metadata.json"),
            Mutability::Immutable
        );
        // A manifest list at the table root (snap-*.avro) is caught by the
        // .avro data-file suffix.
        assert_eq!(
            classify_mutability("warehouse/db/t/snap-8342.avro"),
            Mutability::Immutable
        );
    }

    #[test]
    fn catalog_pointer_and_arbitrary_objects_are_mutable() {
        for k in [
            // The Hadoop-tables catalog pointer under metadata/ is neither a
            // data-file suffix nor a .json — correctly mutable.
            "warehouse/db/t/metadata/version-hint.text",
            "logs/app.log",
            "config.yaml",
            "a.json", // .json not under a metadata/ dir
            "no-extension",
        ] {
            assert_eq!(classify_mutability(k), Mutability::Mutable, "{k}");
        }
    }
}
