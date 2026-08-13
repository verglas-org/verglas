//! Integration tests for the public `verglas-s3` API.

use verglas_s3::cache_key_for;

#[test]
fn request_maps_to_logical_key() {
    let key = cache_key_for("default", "lake", "warehouse/db/t/data/f1.parquet");
    assert_eq!(key.bucket, "lake");
    assert_eq!(key.key, "warehouse/db/t/data/f1.parquet");
}
