//! Golden `classify()` tests across the fixture matrix (issue #49 acceptance).
//!
//! Builds real Iceberg tables (partitioned, multi-snapshot, schema-evolved,
//! nested-column, Avro, and mixed-format) in an in-memory store, drives the
//! mapper through its real build path (`apply_table` over `ObjectStoreFetch`),
//! and asserts `classify()` answers: metadata objects, whole-file data hits,
//! Parquet range → column-chunk resolution, `chunk: None` for Avro, and
//! per-file format classification in a mixed table.

mod support;

use std::sync::Arc;

use support::{BUCKET, FileSpec, SnapshotSpec};
use verglas_tables::catalog::TableIdent;
use verglas_tables::fetch::ObjectStoreFetch;
use verglas_tables::mapper::{BuildInputs, Classify, FileFormat, Mapper, MetaKind};

/// Applies one fixture table into a fresh mapper and returns both.
async fn apply(
    store: &Arc<dyn object_store::ObjectStore>,
    ident: TableIdent,
    fixture: &support::FixtureTable,
) -> Arc<Mapper> {
    let mapper = Arc::new(Mapper::new());
    let fetch = ObjectStoreFetch::new(Arc::clone(store));
    let inputs = BuildInputs {
        storage_binding_id: "managed-lakehouse".to_owned(),
        ident,
        bucket: BUCKET.to_owned(),
        metadata_key: fixture.metadata_key.clone(),
        snapshot_ids: fixture.snapshot_ids.clone(),
        current_snapshot_id: Some(fixture.current_snapshot_id),
    };
    mapper.apply_table(&fetch, &inputs).await.expect("build");
    mapper
}

#[tokio::test]
async fn partitioned_table_classifies_files_and_metadata() {
    let store = support::new_store();
    let files = vec![
        FileSpec {
            rel_path: "data/part=a/f0.parquet".to_owned(),
            format: FileFormat::Parquet,
            bytes: support::parquet_flat(0),
            partition: vec![("part".to_owned(), "a".to_owned())],
        },
        FileSpec {
            rel_path: "data/part=b/f1.parquet".to_owned(),
            format: FileFormat::Parquet,
            bytes: support::parquet_flat(1),
            partition: vec![("part".to_owned(), "b".to_owned())],
        },
    ];
    let fixture = support::build_table(
        &store,
        "warehouse/db/parts",
        &[SnapshotSpec {
            snapshot_id: 10,
            files,
        }],
        10,
    )
    .await;
    let ident = TableIdent::new(&["db"], "parts");
    let mapper = apply(&store, ident.clone(), &fixture).await;
    let table = mapper.table_id(&ident).expect("table id");

    // Each partitioned data file classifies to this table with a file id.
    for built in &fixture.files {
        match mapper.classify(BUCKET, &built.key, None) {
            Classify::TableData { table: t, .. } => assert_eq!(t, table),
            other => panic!("expected TableData for {}, got {other:?}", built.key),
        }
    }
    // metadata.json classifies as table metadata.
    assert!(matches!(
        mapper.classify(BUCKET, &fixture.metadata_key, None),
        Classify::TableMetadata {
            kind: MetaKind::MetadataJson,
            ..
        }
    ));
    // A key outside any table is Unknown.
    assert_eq!(
        mapper.classify(BUCKET, "warehouse/db/other/x.parquet", None),
        Classify::Unknown
    );
    // The partition tuple is captured on the file record.
    let state = mapper.state();
    let index = state.table(table).expect("index");
    let file = index.file_by_path(&fixture.files[0].key).expect("file");
    assert_eq!(
        file.partition.as_ref(),
        &[("part".to_owned(), Some("a".to_owned()))]
    );
}

#[tokio::test]
async fn parquet_range_resolves_to_column_chunk_nested() {
    let store = support::new_store();
    let files = vec![FileSpec {
        rel_path: "data/f0.parquet".to_owned(),
        format: FileFormat::Parquet,
        bytes: support::parquet_nested(0),
        partition: vec![],
    }];
    let fixture = support::build_table(
        &store,
        "warehouse/db/nested",
        &[SnapshotSpec {
            snapshot_id: 5,
            files,
        }],
        5,
    )
    .await;
    let ident = TableIdent::new(&["db"], "nested");
    let mapper = apply(&store, ident.clone(), &fixture).await;
    let table = mapper.table_id(&ident).expect("table id");
    let built = fixture.first_of(FileFormat::Parquet);

    // Before warming the footer, a range on the data file yields chunk: None.
    match mapper.classify(BUCKET, &built.key, Some(0..16)) {
        Classify::TableData { chunk: None, .. } => {}
        other => panic!("pre-warm expected chunk None, got {other:?}"),
    }

    // Warm the footer (off the hot path), then ranges resolve to chunks.
    let fetch = ObjectStoreFetch::new(Arc::clone(&store));
    mapper
        .warm_footer(&fetch, table, &built.key, &built.etag)
        .await
        .expect("warm footer");

    let state = mapper.state();
    let index = state.table(table).expect("index");
    let file = index.file_by_path(&built.key).expect("file");
    let spans = file.chunks().expect("chunks parsed").clone();
    assert!(
        spans.len() >= 6,
        "nested file should have many column chunks"
    );

    // A range starting inside a known chunk resolves to that column/row group.
    let span = &spans[spans.len() / 2];
    match mapper.classify(BUCKET, &built.key, Some(span.start + 1..span.start + 2)) {
        Classify::TableData {
            chunk: Some(chunk),
            table: t,
            ..
        } => {
            assert_eq!(t, table);
            assert_eq!(chunk.column, span.column);
            assert_eq!(chunk.row_group, span.row_group);
        }
        other => panic!("expected resolved chunk, got {other:?}"),
    }

    // A range past the last chunk is the footer region -> table metadata.
    let last_end = spans.last().expect("a chunk").end;
    match mapper.classify(BUCKET, &built.key, Some(last_end..last_end + 4)) {
        Classify::TableMetadata {
            kind: MetaKind::Footer { .. },
            ..
        } => {}
        other => panic!("expected footer metadata, got {other:?}"),
    }
}

#[tokio::test]
async fn avro_file_classifies_with_chunk_none() {
    let store = support::new_store();
    let files = vec![FileSpec {
        rel_path: "data/f0.avro".to_owned(),
        format: FileFormat::Avro,
        bytes: support::avro_file(0),
        partition: vec![],
    }];
    let fixture = support::build_table(
        &store,
        "warehouse/db/avro",
        &[SnapshotSpec {
            snapshot_id: 7,
            files,
        }],
        7,
    )
    .await;
    let ident = TableIdent::new(&["db"], "avro");
    let mapper = apply(&store, ident.clone(), &fixture).await;
    let table = mapper.table_id(&ident).expect("table id");
    let built = fixture.first_of(FileFormat::Avro);

    // Any range on an Avro data file is chunk: None (row-oriented, no footer).
    for range in [None, Some(0..16), Some(100..200)] {
        match mapper.classify(BUCKET, &built.key, range.clone()) {
            Classify::TableData {
                table: t,
                chunk: None,
                ..
            } => assert_eq!(t, table),
            other => panic!("expected Avro TableData chunk None, got {other:?}"),
        }
    }

    // Warming an Avro file is a guarded no-op: it never parses a footer and
    // never populates chunks.
    let fetch = ObjectStoreFetch::new(Arc::clone(&store));
    mapper
        .warm_footer(&fetch, table, &built.key, &built.etag)
        .await
        .expect("warm is a no-op");
    let state = mapper.state();
    let file = state
        .table(table)
        .expect("present")
        .file_by_path(&built.key)
        .expect("present");
    assert!(file.chunks().is_none(), "avro never gets column chunks");
}

#[tokio::test]
async fn mixed_format_table_classifies_per_file() {
    let store = support::new_store();
    // A single snapshot with both a Parquet and an Avro data file.
    let files = vec![
        FileSpec {
            rel_path: "data/p.parquet".to_owned(),
            format: FileFormat::Parquet,
            bytes: support::parquet_flat(0),
            partition: vec![],
        },
        FileSpec {
            rel_path: "data/a.avro".to_owned(),
            format: FileFormat::Avro,
            bytes: support::avro_file(0),
            partition: vec![],
        },
    ];
    let fixture = support::build_table(
        &store,
        "warehouse/db/mixed",
        &[SnapshotSpec {
            snapshot_id: 3,
            files,
        }],
        3,
    )
    .await;
    let ident = TableIdent::new(&["db"], "mixed");
    let mapper = apply(&store, ident.clone(), &fixture).await;
    let table = mapper.table_id(&ident).expect("table id");

    let parquet = fixture.first_of(FileFormat::Parquet);
    let avro = fixture.first_of(FileFormat::Avro);
    let fetch = ObjectStoreFetch::new(Arc::clone(&store));
    mapper
        .warm_footer(&fetch, table, &parquet.key, &parquet.etag)
        .await
        .expect("warm parquet");

    // The Parquet file resolves a range to a chunk; the Avro file does not —
    // classification is per-file by manifest file_format.
    let state = mapper.state();
    let index = state.table(table).expect("present");
    let span = index
        .file_by_path(&parquet.key)
        .expect("present")
        .chunks()
        .expect("present")[0];
    assert!(matches!(
        mapper.classify(BUCKET, &parquet.key, Some(span.start + 1..span.start + 2)),
        Classify::TableData { chunk: Some(_), .. }
    ));
    assert!(matches!(
        mapper.classify(BUCKET, &avro.key, Some(0..16)),
        Classify::TableData { chunk: None, .. }
    ));
}

#[tokio::test]
async fn schema_evolved_multisnapshot_time_travel_union() {
    let store = support::new_store();
    // Snapshot 1: narrow schema. Snapshot 2: evolved (wider) schema, a new file,
    // and the snapshot-1 file removed from the live set.
    let snap1 = SnapshotSpec {
        snapshot_id: 100,
        files: vec![FileSpec {
            rel_path: "data/v1.parquet".to_owned(),
            format: FileFormat::Parquet,
            bytes: support::parquet_flat(0),
            partition: vec![],
        }],
    };
    let snap2 = SnapshotSpec {
        snapshot_id: 200,
        files: vec![FileSpec {
            rel_path: "data/v2.parquet".to_owned(),
            format: FileFormat::Parquet,
            bytes: support::parquet_evolved(0),
            partition: vec![],
        }],
    };
    let fixture = support::build_table(&store, "warehouse/db/evo", &[snap1, snap2], 200).await;
    let ident = TableIdent::new(&["db"], "evo");
    let mapper = apply(&store, ident.clone(), &fixture).await;
    let table = mapper.table_id(&ident).expect("table id");

    let v1 = format!("{}/data/v1.parquet", fixture.root);
    let v2 = format!("{}/data/v2.parquet", fixture.root);
    // Both the current-snapshot file and the retained old-snapshot file resolve
    // (time-travel reads of retained snapshots classify).
    for key in [&v1, &v2] {
        match mapper.classify(BUCKET, key, None) {
            Classify::TableData { table: t, .. } => assert_eq!(t, table),
            other => panic!("expected TableData for {key}, got {other:?}"),
        }
    }
    let state = mapper.state();
    let index = state.table(table).expect("present");
    assert_eq!(index.current_snapshot_id(), Some(200));
    assert_eq!(index.snapshot_ids(), vec![100, 200]);
}
