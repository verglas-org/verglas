//! Direct tests of the parsing layers the mapper builds on: the Iceberg walk
//! (`walk_snapshot`), Parquet footer parsing, and the `(path, etag)` footer
//! memoization.

mod support;

use std::sync::Arc;

use support::{BUCKET, FileSpec, SnapshotSpec};
use verglas_tables::fetch::ObjectStoreFetch;
use verglas_tables::iceberg;
use verglas_tables::mapper::FileFormat;
use verglas_tables::parquet_meta::{FooterCache, parse_footer};

#[tokio::test]
async fn walk_snapshot_resolves_data_files_and_metadata_paths() {
    let store = support::new_store();
    let files = vec![
        FileSpec {
            rel_path: "data/p.parquet".to_owned(),
            format: FileFormat::Parquet,
            bytes: support::parquet_flat(0),
            partition: vec![("d".to_owned(), "1".to_owned())],
        },
        FileSpec {
            rel_path: "data/a.avro".to_owned(),
            format: FileFormat::Avro,
            bytes: support::avro_file(0),
            partition: vec![("d".to_owned(), "1".to_owned())],
        },
    ];
    let fixture = support::build_table(
        &store,
        "warehouse/db/walk",
        &[SnapshotSpec {
            snapshot_id: 9,
            files,
        }],
        9,
    )
    .await;
    let fetch = ObjectStoreFetch::new(Arc::clone(&store));

    let plan = iceberg::walk_snapshot(&fetch, BUCKET, &fixture.metadata_key, 9)
        .await
        .expect("walk");
    assert_eq!(plan.snapshot_id, 9);
    assert_eq!(plan.data_files.len(), 2);
    let parquet = plan
        .data_files
        .iter()
        .find(|f| f.format == FileFormat::Parquet)
        .expect("parquet entry");
    assert!(parquet.path.ends_with("data/p.parquet"));
    assert_eq!(
        parquet.partition,
        vec![("d".to_owned(), Some("1".to_owned()))]
    );
    assert_eq!(plan.manifest_keys.len(), 1);

    // A snapshot the metadata does not name is an error.
    assert!(
        iceberg::walk_snapshot(&fetch, BUCKET, &fixture.metadata_key, 999)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn walk_reads_a_large_manifest_via_the_growth_loop() {
    // A manifest with many entries exceeds the initial 64 KiB suffix window,
    // exercising the whole-object growth loop in the fetch layer.
    let store = support::new_store();
    let files: Vec<FileSpec> = (0..2_000)
        .map(|i| FileSpec {
            rel_path: format!("data/f{i}.avro"),
            format: FileFormat::Avro,
            bytes: vec![b'x'; 8],
            partition: vec![],
        })
        .collect();
    let fixture = support::build_table(
        &store,
        "warehouse/db/big",
        &[SnapshotSpec {
            snapshot_id: 1,
            files,
        }],
        1,
    )
    .await;
    let fetch = ObjectStoreFetch::new(Arc::clone(&store));
    let plan = iceberg::walk_snapshot(&fetch, BUCKET, &fixture.metadata_key, 1)
        .await
        .expect("walk large");
    assert_eq!(plan.data_files.len(), 2_000);
}

#[tokio::test]
async fn parse_footer_yields_sorted_column_chunks() {
    let store = support::new_store();
    let files = vec![FileSpec {
        rel_path: "data/f.parquet".to_owned(),
        format: FileFormat::Parquet,
        bytes: support::parquet_nested(0),
        partition: vec![],
    }];
    let fixture = support::build_table(
        &store,
        "warehouse/db/foot",
        &[SnapshotSpec {
            snapshot_id: 1,
            files,
        }],
        1,
    )
    .await;
    let built = fixture.first_of(FileFormat::Parquet);
    let fetch = ObjectStoreFetch::new(Arc::clone(&store));
    let path = verglas_core::CacheKey {
        bucket: BUCKET.to_owned(),
        key: built.key.clone(),
    };

    let spans = parse_footer(&fetch, &path).await.expect("footer");
    assert!(spans.len() >= 6, "nested parquet has several column chunks");
    // Sorted ascending by start offset, with each chunk non-empty.
    for pair in spans.windows(2) {
        assert!(pair[0].start <= pair[1].start);
    }
    assert!(spans.iter().all(|s| s.end > s.start));
}

#[tokio::test]
async fn footer_cache_memoizes_by_path_and_etag() {
    let store = support::new_store();
    let files = vec![FileSpec {
        rel_path: "data/f.parquet".to_owned(),
        format: FileFormat::Parquet,
        bytes: support::parquet_flat(0),
        partition: vec![],
    }];
    let fixture = support::build_table(
        &store,
        "warehouse/db/memo",
        &[SnapshotSpec {
            snapshot_id: 1,
            files,
        }],
        1,
    )
    .await;
    let built = fixture.first_of(FileFormat::Parquet);
    let fetch = ObjectStoreFetch::new(Arc::clone(&store));
    let path = verglas_core::CacheKey {
        bucket: BUCKET.to_owned(),
        key: built.key.clone(),
    };

    let cache = FooterCache::new();
    let first = cache
        .get_or_parse(&fetch, &path, &built.etag)
        .await
        .expect("first");
    let second = cache
        .get_or_parse(&fetch, &path, &built.etag)
        .await
        .expect("second");
    // Same (path, etag) returns the identical shared Arc — no re-parse.
    assert!(Arc::ptr_eq(&first, &second));

    // A different etag is a different version and parses independently.
    let other = cache
        .get_or_parse(&fetch, &path, "different-etag")
        .await
        .expect("other");
    assert!(!Arc::ptr_eq(&first, &other));
    assert_eq!(first.len(), other.len());
}

#[tokio::test]
async fn parse_footer_rejects_a_non_parquet_object() {
    let store = support::new_store();
    let files = vec![FileSpec {
        rel_path: "data/f.avro".to_owned(),
        format: FileFormat::Avro,
        bytes: support::avro_file(0),
        partition: vec![],
    }];
    let fixture = support::build_table(
        &store,
        "warehouse/db/bad",
        &[SnapshotSpec {
            snapshot_id: 1,
            files,
        }],
        1,
    )
    .await;
    let built = fixture.first_of(FileFormat::Avro);
    let fetch = ObjectStoreFetch::new(Arc::clone(&store));
    let path = verglas_core::CacheKey {
        bucket: BUCKET.to_owned(),
        key: built.key.clone(),
    };
    // Handing a non-Parquet object to the footer parser errors rather than
    // fabricating chunks (the mapper guards on format so this never runs in
    // production, but the parser must still refuse it).
    assert!(parse_footer(&fetch, &path).await.is_err());
}

#[test]
fn object_uri_parsing_and_format_parsing() {
    assert_eq!(
        iceberg::parse_object_uri("s3://lake/warehouse/db/t/metadata/v1.json"),
        Some((
            "lake".to_owned(),
            "warehouse/db/t/metadata/v1.json".to_owned()
        ))
    );
    assert_eq!(
        iceberg::parse_object_uri("s3a://b/k"),
        Some(("b".to_owned(), "k".to_owned()))
    );
    // No scheme, empty bucket, or empty key -> None.
    assert_eq!(iceberg::parse_object_uri("warehouse/db/t/x"), None);
    assert_eq!(iceberg::parse_object_uri("s3:///k"), None);

    assert_eq!(FileFormat::parse("parquet"), Some(FileFormat::Parquet));
    assert_eq!(FileFormat::parse("AVRO"), Some(FileFormat::Avro));
    assert_eq!(FileFormat::parse("Orc"), Some(FileFormat::Orc));
    assert_eq!(FileFormat::parse("csv"), None);
}
