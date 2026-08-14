//! Durable arbitrary-string key coverage for the Vamana bridge.

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use verglas_vector::attachment::{attach_string_key_index, load_string_key_index_for_snapshot};
use verglas_vector::{MaintenanceConfig, Metric, StringKeyIndex, VamanaParams};

/// Opens a catalog whose table metadata and Puffin files survive service reopen.
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

/// The key bridge preserves exact arbitrary UTF-8 keys across a Puffin reload.
#[tokio::test]
async fn persisted_bridge_round_trips_exact_keys_updates_and_deletes() {
    let catalog = catalog().await;
    let namespace = NamespaceIdent::new("vectors".into());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("namespace");
    let ident = TableIdent::from_strs(["vectors", "items"]).expect("ident");
    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "commit_marker",
        arrow_schema::DataType::Int64,
        false,
    )]));
    verglas_iceberg::write::create_table_from_schema(catalog.as_ref(), &ident, &schema, None)
        .await
        .expect("table");
    verglas_iceberg::write::append_batches(
        catalog.as_ref(),
        &ident,
        vec![
            arrow_array::RecordBatch::try_new(
                schema,
                vec![Arc::new(arrow_array::Int64Array::from(vec![1]))],
            )
            .expect("batch"),
        ],
        HashMap::new(),
    )
    .await
    .expect("snapshot");
    let table = catalog.load_table(&ident).await.expect("load");
    let snapshot = table.metadata().current_snapshot_id().expect("snapshot id");
    let mut bridge =
        StringKeyIndex::new(Metric::L2, 2, VamanaParams::default(), 7).expect("bridge");
    bridge.put("plain", &[0.0, 0.0]).expect("put");
    bridge.put("emoji-🧊\0exact", &[1.0, 0.0]).expect("put");
    bridge.put("plain", &[0.9, 0.0]).expect("update");
    assert!(bridge.delete("emoji-🧊\0exact"));
    bridge.index_mut().set_reflected_snapshot(snapshot);
    let config = MaintenanceConfig::new("key", "embedding", Metric::L2);
    attach_string_key_index(
        catalog.as_ref(),
        &table,
        snapshot,
        &config,
        bridge.index(),
        bridge.keys(),
    )
    .await
    .expect("attach");

    let reopened = catalog.load_table(&ident).await.expect("reopen table");
    let restored = load_string_key_index_for_snapshot(&reopened, snapshot, "embedding")
        .await
        .expect("load bridge")
        .expect("bridge exists");
    assert_eq!(restored.bridge.keys().key_for(1), Some("plain"));
    assert_eq!(restored.bridge.keys().key_for(2), None);
    assert_eq!(
        restored.bridge.query(&[1.0, 0.0], 4, 64).expect("query")[0].key,
        "plain"
    );
}
