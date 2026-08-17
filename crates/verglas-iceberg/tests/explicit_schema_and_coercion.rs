//! Hermetic tests for the explicit-schema table-create path and the JSON→Arrow
//! type coercion the commit path performs.
//!
//! These prove two domain-neutral capabilities the SDK builds on: creating a
//! table from an exact Arrow schema (arbitrary types, per-column nullability)
//! with a partition spec that mixes identity and month transforms across several
//! columns; and committing JSON rows whose decimal/date/int values are coerced to
//! the table's declared types without precision loss. No table meaning is assumed
//! — the columns are generic.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Date32Array, Decimal128Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use futures::{StreamExt, TryStreamExt};
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::Transform;
use iceberg::table::Table;
use iceberg::{Catalog, CatalogBuilder};
use serde_json::json;
use verglas_iceberg::parse_table_ident;
use verglas_iceberg::tables_api::{
    self, ColumnSpec, CommitRequest, CreateTableRequest, PartitionSpec,
};
use verglas_iceberg::write::{self, PartitionField};

/// Builds an in-process memory catalog over a fresh temp warehouse path.
async fn memory_catalog() -> Arc<dyn Catalog> {
    let warehouse = tempfile::tempdir().expect("warehouse tempdir");
    let catalog = MemoryCatalogBuilder::default()
        .load(
            "memory",
            HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.to_string(),
                warehouse.path().to_str().expect("utf8 path").to_string(),
            )]),
        )
        .await
        .expect("memory catalog");
    std::mem::forget(warehouse);
    Arc::new(catalog)
}

/// A generic schema exercising the exact types and nullability the create path
/// must preserve: an Int64, a Decimal128(38,18), nullable and non-nullable Utf8,
/// two identity partition sources, and a Date32 month-partition source.
fn explicit_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Decimal128(38, 18), false),
        Field::new("label", DataType::Utf8, true),
        Field::new("tag", DataType::Int64, true),
        Field::new("region", DataType::Utf8, false),
        Field::new("bucket", DataType::Utf8, false),
        Field::new("event_date", DataType::Date32, false),
    ]))
}

/// The days since the Unix epoch for a UTC calendar date, for a Date32 assertion.
fn days_from_epoch(year: i32, month: u32, day: u32) -> i32 {
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
    let date = chrono::NaiveDate::from_ymd_opt(year, month, day).expect("date");
    (date - epoch).num_days() as i32
}

/// Reads every row of the table's current snapshot into Arrow batches, so a test
/// can assert exact typed values (not the JSON projection).
async fn read_batches(table: &Table) -> Vec<RecordBatch> {
    let scan = table.scan().build().expect("scan");
    let tasks: Vec<_> = scan
        .plan_files()
        .await
        .expect("plan")
        .try_collect()
        .await
        .expect("collect tasks");
    let runtime = iceberg::Runtime::try_current().expect("runtime");
    let reader = ArrowReaderBuilder::new(table.file_io().clone(), runtime).build();
    let stream = futures::stream::iter(tasks.into_iter().map(Ok)).boxed();
    reader
        .read(stream)
        .expect("read")
        .stream()
        .try_collect()
        .await
        .expect("collect batches")
}

/// Acceptance: the create path builds exactly the declared schema (types and
/// nullability) and a partition spec that mixes two identity fields and a month
/// field across three columns, verified against the Iceberg table metadata.
#[tokio::test]
async fn explicit_schema_and_mixed_partition_spec() {
    let catalog = memory_catalog().await;
    let ident = parse_table_ident("data.points").expect("ident");
    let partitions = vec![
        PartitionField::identity("region"),
        PartitionField::identity("bucket"),
        PartitionField::month("event_date"),
    ];
    write::create_table_with_partitions(catalog.as_ref(), &ident, &explicit_schema(), &partitions)
        .await
        .expect("create");

    let table = catalog.load_table(&ident).await.expect("load");
    let schema = table.metadata().current_schema();

    // Exact types and nullability round-trip.
    let field = |name: &str| {
        schema
            .field_by_name(name)
            .unwrap_or_else(|| panic!("column {name} present"))
            .clone()
    };
    assert!(field("id").required, "id is non-nullable");
    assert!(!field("label").required, "label is nullable");
    assert!(!field("tag").required, "tag is nullable");
    assert!(field("event_date").required, "event_date is non-nullable");
    // The decimal keeps precision 38 / scale 18.
    match &field("amount").field_type.as_ref() {
        iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Decimal {
            precision,
            scale,
        }) => {
            assert_eq!((*precision, *scale), (38, 18));
        }
        other => panic!("amount should be decimal(38,18), got {other:?}"),
    }
    // event_date is a date.
    assert!(
        matches!(
            field("event_date").field_type.as_ref(),
            iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Date)
        ),
        "event_date should be a date column"
    );

    // The partition spec: region(identity), bucket(identity), event_date_month(month).
    let spec = table.metadata().default_partition_spec();
    let fields = spec.fields();
    assert_eq!(fields.len(), 3, "three partition fields");
    assert_eq!(fields[0].name, "region");
    assert_eq!(fields[0].transform, Transform::Identity);
    assert_eq!(fields[1].name, "bucket");
    assert_eq!(fields[1].transform, Transform::Identity);
    assert_eq!(fields[2].name, "event_date_month");
    assert_eq!(fields[2].transform, Transform::Month);
    // The month field's source is the event_date column.
    let event_date_id = schema.field_by_name("event_date").expect("event_date").id;
    assert_eq!(fields[2].source_id, event_date_id);
}

/// Acceptance: committing a JSON row into the explicit-schema table coerces a
/// decimal-string to Decimal128(38,18) without precision loss, a date-string to
/// Date32, and a JSON int to Int64; a null nullable column stays null. Verified by
/// reading the typed Arrow values back.
#[tokio::test]
async fn commit_coerces_decimal_date_and_int() {
    let catalog = memory_catalog().await;
    let ident = parse_table_ident("data.points").expect("ident");
    let partitions = vec![
        PartitionField::identity("region"),
        PartitionField::identity("bucket"),
        PartitionField::month("event_date"),
    ];
    write::create_table_with_partitions(catalog.as_ref(), &ident, &explicit_schema(), &partitions)
        .await
        .expect("create");

    // A decimal with 18 fractional digits, sent as a string to keep every digit.
    let response = tables_api::commit(
        catalog.as_ref(),
        &ident,
        CommitRequest {
            rows: vec![json!({
                "id": 7,
                "amount": "125000.123456789012345678",
                "label": null,
                "tag": null,
                "region": "us",
                "bucket": "b1",
                "event_date": "2026-07-20"
            })],
            idempotency_key: None,
        },
    )
    .await
    .expect("commit");
    assert_eq!(response.rows_committed, 1);

    let table = catalog.load_table(&ident).await.expect("load");
    let batches = read_batches(&table).await;
    let batch = batches.into_iter().find(|b| b.num_rows() > 0).expect("row");

    // The decimal is exact: 125000.123456789012345678 at scale 18 is the raw i128
    // 125000123456789012345678, with no rounding.
    let amount = batch
        .column_by_name("amount")
        .expect("amount col")
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("decimal128");
    assert_eq!(amount.precision(), 38);
    assert_eq!(amount.scale(), 18);
    assert_eq!(amount.value(0), 125_000_123_456_789_012_345_678_i128);

    // The date-string became the right Date32 (days since epoch).
    let event_date = batch
        .column_by_name("event_date")
        .expect("event_date col")
        .as_any()
        .downcast_ref::<Date32Array>()
        .expect("date32");
    assert_eq!(event_date.value(0), days_from_epoch(2026, 7, 20));

    // The null nullable column stayed null.
    let tag = batch.column_by_name("tag").expect("tag col");
    assert!(tag.is_null(0), "tag was committed as null");

    // The JSON projection reads the date back as the same string.
    let rows = tables_api::rows(catalog.as_ref(), &ident, None, None)
        .await
        .expect("rows");
    assert_eq!(rows.rows.len(), 1);
    assert_eq!(rows.rows[0]["event_date"].as_str(), Some("2026-07-20"));
}

/// Acceptance: the SDK-facing create request parses string type and transform
/// names (including `decimal128(38,18)` and `month`) into the same table the
/// typed create builds, and commit works against it.
#[tokio::test]
async fn create_table_request_parses_types_and_transforms() {
    let catalog = memory_catalog().await;
    let ident = parse_table_ident("data.viajson").expect("ident");

    let request = CreateTableRequest {
        schema: vec![
            ColumnSpec {
                name: "amount".to_owned(),
                type_name: "decimal128(38,18)".to_owned(),
                nullable: false,
            },
            ColumnSpec {
                name: "region".to_owned(),
                type_name: "string".to_owned(),
                nullable: false,
            },
            ColumnSpec {
                name: "event_date".to_owned(),
                type_name: "date32".to_owned(),
                nullable: false,
            },
        ],
        partitions: vec![
            PartitionSpec {
                source: "region".to_owned(),
                transform: "identity".to_owned(),
            },
            PartitionSpec {
                source: "event_date".to_owned(),
                transform: "month".to_owned(),
            },
        ],
    };
    let created = tables_api::create_table(catalog.as_ref(), &ident, request)
        .await
        .expect("create via request");
    assert_eq!(created.table, "data.viajson");
    assert_eq!(created.columns, vec!["amount", "region", "event_date"]);

    let table = catalog.load_table(&ident).await.expect("load");
    let spec = table.metadata().default_partition_spec();
    assert_eq!(spec.fields()[0].transform, Transform::Identity);
    assert_eq!(spec.fields()[1].transform, Transform::Month);
    assert_eq!(spec.fields()[1].name, "event_date_month");

    // The decimal column is exactly (38,18).
    let amount = table
        .metadata()
        .current_schema()
        .field_by_name("amount")
        .expect("amount");
    assert!(matches!(
        amount.field_type.as_ref(),
        iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Decimal {
            precision: 38,
            scale: 18
        })
    ));

    let response = tables_api::commit(
        catalog.as_ref(),
        &ident,
        CommitRequest {
            rows: vec![json!({
                "amount": "1.500000000000000000",
                "region": "eu",
                "event_date": "2026-01-31"
            })],
            idempotency_key: None,
        },
    )
    .await
    .expect("commit");
    assert_eq!(response.rows_committed, 1);
}

/// Acceptance: an unknown type name in a create request is rejected, naming the
/// offending column, and no table is created.
#[tokio::test]
async fn create_table_request_rejects_unknown_type() {
    let catalog = memory_catalog().await;
    let ident = parse_table_ident("data.bad").expect("ident");
    let request = CreateTableRequest {
        schema: vec![ColumnSpec {
            name: "weird".to_owned(),
            type_name: "quaternion".to_owned(),
            nullable: true,
        }],
        partitions: vec![],
    };
    let err = tables_api::create_table(catalog.as_ref(), &ident, request)
        .await
        .expect_err("unknown type is rejected");
    assert!(err.to_string().contains("weird"), "names the column: {err}");
    assert!(
        catalog.load_table(&ident).await.is_err(),
        "no table created"
    );
}
