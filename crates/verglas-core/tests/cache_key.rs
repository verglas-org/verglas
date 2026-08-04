//! Integration tests for the public `verglas-core` API.

use verglas_core::CacheKey;

#[test]
fn cache_key_equality_is_by_value() {
    let a = CacheKey {
        bucket: "lake".to_owned(),
        key: "warehouse/db/table/data/f1.parquet".to_owned(),
    };
    let b = a.clone();
    assert_eq!(a, b);
}

#[test]
fn cache_keys_differ_across_buckets() {
    let a = CacheKey {
        bucket: "lake-a".to_owned(),
        key: "same/key".to_owned(),
    };
    let b = CacheKey {
        bucket: "lake-b".to_owned(),
        key: "same/key".to_owned(),
    };
    assert_ne!(a, b);
}
