//! Contract tests for stable, balanced object-to-ingress assignment.

use std::collections::HashMap;

use verglas_ring_proxy::EndpointPool;

#[test]
fn every_operation_for_one_iceberg_object_uses_one_ingress() {
    let pool = EndpointPool::new([
        "http://cache-0:8333",
        "http://cache-1:8333",
        "http://cache-2:8333",
        "http://cache-3:8333",
    ])
    .expect("pool");
    let data = pool.endpoint_for_path("/warehouse/table/data/a.parquet?partNumber=1&uploadId=u");
    let later_part =
        pool.endpoint_for_path("/warehouse/table/data/a.parquet?partNumber=7&uploadId=u");
    let complete = pool.endpoint_for_path("/warehouse/table/data/a.parquet?uploadId=u");
    assert_eq!(data, later_part);
    assert_eq!(data, complete);
}

#[test]
fn iceberg_object_writes_balance_across_all_ring_members() {
    let pool = EndpointPool::new([
        "http://cache-0:8333",
        "http://cache-1:8333",
        "http://cache-2:8333",
        "http://cache-3:8333",
    ])
    .expect("pool");
    let mut counts = HashMap::new();
    for object in 0..10_000 {
        let kind = match object % 4 {
            0 => "data",
            1 => "delete",
            2 => "manifest",
            _ => "metadata",
        };
        let endpoint = pool.endpoint_for_path(&format!(
            "/warehouse/table/{kind}/object-{object:05}.bin?uploadId=ignored"
        ));
        *counts.entry(endpoint.to_owned()).or_insert(0usize) += 1;
    }
    assert_eq!(counts.len(), 4);
    assert!(
        counts.values().all(|count| *count <= 3_500),
        "skewed endpoint counts: {counts:?}"
    );
}

#[test]
fn an_empty_or_duplicate_pool_is_rejected() {
    assert!(EndpointPool::new(Vec::<String>::new()).is_err());
    assert!(EndpointPool::new(["http://cache-0", "http://cache-0"]).is_err());
}
