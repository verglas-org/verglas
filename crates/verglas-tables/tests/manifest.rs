//! Integration tests for the public `verglas-tables` API.

use verglas_core::CacheKey;
use verglas_tables::ManifestSummary;

#[test]
fn manifest_summary_holds_data_file_keys() {
    let summary = ManifestSummary {
        data_files: vec![CacheKey {
            bucket: "lake".to_owned(),
            key: "warehouse/db/t/data/f1.parquet".to_owned(),
        }],
    };
    assert_eq!(summary.data_files.len(), 1);
}
