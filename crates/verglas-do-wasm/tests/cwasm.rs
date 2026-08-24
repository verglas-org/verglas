//! Acceptance tests for digest- and engine-keyed compiled component caching.

use std::fs;

use verglas_do_wasm::{ComponentDigest, CwasmCache};
use wasmtime::{Config, Engine, OptLevel};

/// Compiling on a cache miss writes one entry and a hit reuses its mtime.
#[test]
fn cwasm_cache_compiles_on_miss_and_reuses_hit() {
    let root = tempfile::tempdir().expect("cache root");
    let bytes = wat::parse_str("(component)").expect("empty component WAT");
    let digest = ComponentDigest::compute(&bytes);
    let cache = CwasmCache::new(root.path());
    let engine = Engine::new(&Config::new()).expect("engine");

    cache
        .load_or_compile(&engine, digest, &bytes)
        .expect("compile cache miss");
    let entries = fs::read_dir(root.path())
        .expect("read cache")
        .collect::<Result<Vec<_>, _>>()
        .expect("cache entries");
    assert_eq!(entries.len(), 1);
    let entry = entries.first().expect("cache entry");
    let before = entry
        .metadata()
        .expect("entry metadata")
        .modified()
        .expect("mtime");

    cache
        .load_or_compile(&engine, digest, &bytes)
        .expect("deserialize cache hit");
    let after = entry
        .metadata()
        .expect("entry metadata")
        .modified()
        .expect("mtime");
    assert_eq!(before, after, "cache hit rewrote the compiled artifact");
}

/// A different Wasmtime compatibility key gets a separate trusted entry.
#[test]
fn cwasm_cache_ignores_entry_for_different_engine_key() {
    let root = tempfile::tempdir().expect("cache root");
    let bytes = wat::parse_str("(component)").expect("empty component WAT");
    let digest = ComponentDigest::compute(&bytes);
    let cache = CwasmCache::new(root.path());
    let engine = Engine::new(&Config::new()).expect("engine");
    cache
        .load_or_compile(&engine, digest, &bytes)
        .expect("compile default engine");

    let mut alternate_config = Config::new();
    alternate_config.cranelift_opt_level(OptLevel::None);
    let alternate_engine = Engine::new(&alternate_config).expect("alternate engine");
    cache
        .load_or_compile(&alternate_engine, digest, &bytes)
        .expect("compile alternate engine");

    let entries = fs::read_dir(root.path())
        .expect("read cache")
        .collect::<Result<Vec<_>, _>>()
        .expect("cache entries");
    assert_eq!(
        entries.len(),
        2,
        "alternate key reused the incompatible entry"
    );
}
