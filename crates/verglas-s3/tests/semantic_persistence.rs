//! Durable S3 Vectors semantic tests over a fresh Iceberg adapter instance.

use std::{collections::HashMap, sync::Arc};

use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::{Catalog, CatalogBuilder};
use serde_json::json;
use verglas_s3::semantic::{IcebergCatalogSemanticStore, SemanticApi, SemanticOperation};

/// Opens a customer-owned memory catalog whose state survives adapter recreation.
async fn catalog() -> Arc<dyn Catalog> {
    let warehouse = tempfile::tempdir().expect("warehouse").keep();
    Arc::new(
        MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_owned(),
                    format!("file://{}", warehouse.display()),
                )]),
            )
            .await
            .expect("catalog"),
    )
}

/// Calls one vector operation through the durable semantic boundary.
async fn call(
    store: &IcebergCatalogSemanticStore,
    operation: &'static str,
    input: serde_json::Value,
) -> serde_json::Value {
    store
        .call(SemanticOperation::S3Vectors(operation), input)
        .await
        .expect(operation)
}

/// Bucket/index metadata, tags, policies, and exact vector keys survive reopen.
#[tokio::test]
async fn vector_state_survives_a_fresh_semantic_adapter() {
    let catalog = catalog().await;
    let store = IcebergCatalogSemanticStore::new(catalog.clone());
    call(
        &store,
        "CreateVectorBucket",
        json!({"vectorBucketName":"bucket-one","encryptionConfiguration":{"sseType":"AES256"},"tags":{"team":"search"}}),
    )
    .await;
    call(
        &store,
        "CreateIndex",
        json!({"vectorBucketName":"bucket-one","indexName":"index-one","dataType":"float32","dimension":2,"distanceMetric":"euclidean","tags":{"kind":"primary"}}),
    )
    .await;
    call(
        &store,
        "PutVectorBucketPolicy",
        json!({"vectorBucketName":"bucket-one","policy":"{\"allow\":true}"}),
    )
    .await;
    call(
        &store,
        "PutVectors",
        json!({"vectorBucketName":"bucket-one","indexName":"index-one","vectors":[{"key":"exact/key Ω","data":{"float32":[1.0,2.0]},"metadata":{"state":"live"}}]}),
    )
    .await;

    let reopened = IcebergCatalogSemanticStore::new(catalog);
    let bucket = call(
        &reopened,
        "GetVectorBucket",
        json!({"vectorBucketName":"bucket-one"}),
    )
    .await;
    assert!(bucket["vectorBucket"]["creationTime"].is_i64());
    assert_eq!(
        bucket["vectorBucket"]["encryptionConfiguration"]["sseType"],
        "AES256"
    );
    let tags = call(
        &reopened,
        "ListTagsForResource",
        json!({"resourceArn":"arn:aws:s3vectors:us-east-1:000000000000:bucket/bucket-one"}),
    )
    .await;
    assert_eq!(tags["tags"]["team"], "search");
    let policy = call(
        &reopened,
        "GetVectorBucketPolicy",
        json!({"vectorBucketName":"bucket-one"}),
    )
    .await;
    assert_eq!(policy["policy"], "{\"allow\":true}");
    let vector = call(&reopened, "GetVectors", json!({"vectorBucketName":"bucket-one","indexName":"index-one","keys":["exact/key Ω"],"returnData":true,"returnMetadata":true})).await;
    assert_eq!(vector["vectors"][0]["key"], "exact/key Ω");
    assert_eq!(vector["vectors"][0]["data"]["float32"], json!([1.0, 2.0]));
    let query = call(&reopened, "QueryVectors", json!({"vectorBucketName":"bucket-one","indexName":"index-one","queryVector":{"float32":[1.0,2.0]},"topK":1,"returnDistance":true,"returnMetadata":true})).await;
    assert_eq!(query["vectors"][0]["key"], "exact/key Ω");
    assert!(query.get("nextToken").is_none());
}

/// Appended updates and tombstones resolve by committed order, including a write race.
#[tokio::test]
async fn vector_update_delete_order_remains_correct_after_concurrent_calls() {
    let catalog = catalog().await;
    let store = Arc::new(IcebergCatalogSemanticStore::new(catalog));
    call(
        &store,
        "CreateVectorBucket",
        json!({"vectorBucketName":"bucket-two"}),
    )
    .await;
    call(&store, "CreateIndex", json!({"vectorBucketName":"bucket-two","indexName":"index-two","dataType":"float32","dimension":2,"distanceMetric":"euclidean"})).await;
    call(&store, "PutVectors", json!({"vectorBucketName":"bucket-two","indexName":"index-two","vectors":[{"key":"same","data":{"float32":[1.0,1.0]}}]})).await;
    call(
        &store,
        "DeleteVectors",
        json!({"vectorBucketName":"bucket-two","indexName":"index-two","keys":["same"]}),
    )
    .await;
    let deleted = call(
        &store,
        "GetVectors",
        json!({"vectorBucketName":"bucket-two","indexName":"index-two","keys":["same"]}),
    )
    .await;
    assert!(deleted["vectors"].as_array().expect("array").is_empty());

    let put_store = store.clone();
    let delete_store = store.clone();
    let (put, delete) = tokio::join!(
        call(
            &put_store,
            "PutVectors",
            json!({"vectorBucketName":"bucket-two","indexName":"index-two","vectors":[{"key":"same","data":{"float32":[2.0,2.0]}}]})
        ),
        call(
            &delete_store,
            "DeleteVectors",
            json!({"vectorBucketName":"bucket-two","indexName":"index-two","keys":["same"]})
        ),
    );
    let _ = (put, delete);
    call(&store, "PutVectors", json!({"vectorBucketName":"bucket-two","indexName":"index-two","vectors":[{"key":"same","data":{"float32":[3.0,3.0]}}]})).await;
    let final_row = call(&store, "GetVectors", json!({"vectorBucketName":"bucket-two","indexName":"index-two","keys":["same"],"returnData":true})).await;
    assert_eq!(
        final_row["vectors"][0]["data"]["float32"],
        json!([3.0, 3.0])
    );
}
