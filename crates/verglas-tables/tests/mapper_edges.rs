//! Edge-case coverage for `classify()` and the builder: uncatalogued metadata
//! objects, bucket mismatch, unindexed keys, a table with no snapshots, and
//! no-op build operations.

mod support;

use std::sync::Arc;

use support::{BUCKET, FileSpec, SnapshotSpec};
use verglas_tables::catalog::TableIdent;
use verglas_tables::fetch::ObjectStoreFetch;
use verglas_tables::mapper::{BuildInputs, Classify, FileFormat, Mapper, MetaKind};

/// Builds a one-file table and returns the mapper, fetch, fixture, and table id.
async fn one_file_table(
    root: &str,
) -> (
    Arc<Mapper>,
    Arc<dyn object_store::ObjectStore>,
    support::FixtureTable,
    TableIdent,
) {
    let store = support::new_store();
    let files = vec![FileSpec {
        rel_path: "data/f.parquet".to_owned(),
        format: FileFormat::Parquet,
        bytes: support::parquet_flat(0),
        partition: vec![],
    }];
    let fixture = support::build_table(
        &store,
        root,
        &[SnapshotSpec {
            snapshot_id: 1,
            files,
        }],
        1,
    )
    .await;
    let ident = TableIdent::new(&["db"], "t");
    let mapper = Arc::new(Mapper::new());
    let fetch = ObjectStoreFetch::new(Arc::clone(&store));
    mapper
        .apply_table(
            &fetch,
            &BuildInputs {
                storage_binding_id: "managed-lakehouse".to_owned(),
                ident: ident.clone(),
                bucket: BUCKET.to_owned(),
                metadata_key: fixture.metadata_key.clone(),
                snapshot_ids: fixture.snapshot_ids.clone(),
                current_snapshot_id: Some(fixture.current_snapshot_id),
            },
        )
        .await
        .expect("build");
    (mapper, store, fixture, ident)
}

#[tokio::test]
async fn uncatalogued_metadata_object_classifies_as_other() {
    let (mapper, _store, fixture, ident) = one_file_table("warehouse/db/e1").await;
    let table = mapper.table_id(&ident).expect("id");
    // A file under `<root>/metadata/` we never enumerated (e.g. a stats blob).
    let key = format!("{}/metadata/stats-9.puffin", fixture.root);
    assert_eq!(
        mapper.classify(BUCKET, &key, None),
        Classify::TableMetadata {
            table,
            kind: MetaKind::Other
        }
    );
}

#[tokio::test]
async fn wrong_bucket_and_unindexed_key_are_unknown() {
    let (mapper, _store, fixture, _ident) = one_file_table("warehouse/db/e2").await;
    let data_key = format!("{}/data/f.parquet", fixture.root);

    // Right prefix, wrong bucket -> Unknown (bucket is verified).
    assert_eq!(
        mapper.classify("other-bucket", &data_key, None),
        Classify::Unknown
    );
    // Under the table prefix but not the metadata/ subtree and not indexed ->
    // Unknown (we do not fabricate a file identity).
    let ghost = format!("{}/data/not-indexed.parquet", fixture.root);
    assert_eq!(mapper.classify(BUCKET, &ghost, None), Classify::Unknown);
}

#[tokio::test]
async fn table_with_no_snapshots_still_classifies_metadata() {
    let store = support::new_store();
    let files = vec![FileSpec {
        rel_path: "data/f.parquet".to_owned(),
        format: FileFormat::Parquet,
        bytes: support::parquet_flat(0),
        partition: vec![],
    }];
    let fixture = support::build_table(
        &store,
        "warehouse/db/empty",
        &[SnapshotSpec {
            snapshot_id: 1,
            files,
        }],
        1,
    )
    .await;
    let ident = TableIdent::new(&["db"], "empty");
    let mapper = Arc::new(Mapper::new());
    let fetch = ObjectStoreFetch::new(Arc::clone(&store));
    // Build as if the table has no current snapshot yet (no snapshots walked).
    mapper
        .apply_table(
            &fetch,
            &BuildInputs {
                storage_binding_id: "managed-lakehouse".to_owned(),
                ident: ident.clone(),
                bucket: BUCKET.to_owned(),
                metadata_key: fixture.metadata_key.clone(),
                snapshot_ids: vec![],
                current_snapshot_id: None,
            },
        )
        .await
        .expect("build empty");
    let table = mapper.table_id(&ident).expect("id");

    // metadata.json still classifies; the (unwalked) data file does not.
    assert_eq!(
        mapper.classify(BUCKET, &fixture.metadata_key, None),
        Classify::TableMetadata {
            table,
            kind: MetaKind::MetadataJson
        }
    );
    let data_key = format!("{}/data/f.parquet", fixture.root);
    assert_eq!(mapper.classify(BUCKET, &data_key, None), Classify::Unknown);
    assert_eq!(
        mapper
            .state()
            .table(table)
            .expect("index")
            .current_snapshot_id(),
        None
    );
}

#[tokio::test]
async fn warm_footer_and_remove_are_no_ops_for_unknown_targets() {
    let (mapper, store, _fixture, _ident) = one_file_table("warehouse/db/e3").await;
    let fetch = ObjectStoreFetch::new(Arc::clone(&store));

    // Unknown table id -> Ok, nothing happens.
    mapper
        .warm_footer(
            &fetch,
            verglas_tables::mapper::TableId(9999),
            "whatever",
            "etag",
        )
        .await
        .expect("no-op warm");

    // Removing a never-seen table is a no-op.
    mapper.remove_table(&TableIdent::new(&["db"], "ghost"));
}

#[tokio::test]
async fn removed_table_no_longer_classifies() {
    let (mapper, _store, fixture, ident) = one_file_table("warehouse/db/e4").await;
    let data_key = format!("{}/data/f.parquet", fixture.root);
    assert!(matches!(
        mapper.classify(BUCKET, &data_key, None),
        Classify::TableData { .. }
    ));
    mapper.remove_table(&ident);
    assert_eq!(mapper.classify(BUCKET, &data_key, None), Classify::Unknown);
}
