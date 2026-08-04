//! Hermetic integration tests for the agent-facing table and query verbs
//! (issue #287), derived from the issue's acceptance criteria.
//!
//! These run fully in-process against an Iceberg `MemoryCatalog` over an
//! in-memory FileIO — no MinIO, no REST catalog, no network — so the whole
//! create → append → list/show/history → query → time-travel path is exercised
//! by `cargo test` alone. The REST-catalog + endpoint wiring is the same code
//! path (the operations take a `&dyn Catalog`); it is proven end-to-end by the
//! server-backed example session in the PR.

use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use futures::StreamExt;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::{Catalog, CatalogBuilder};
use verglas_iceberg::{
    TimeTravel, batch_to_json_rows_fragment, inspect, parse_table_ident, query, query_stream, write,
};

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
    // Keep the warehouse dir alive for the catalog's lifetime by leaking the
    // guard — the process is a short-lived test.
    std::mem::forget(warehouse);
    Arc::new(catalog)
}

/// Writes `contents` to a temp file with the given name, returning its path and
/// keeping the tempdir alive by leaking it (test process is short-lived).
fn source_file(name: &str, contents: &str) -> PathBuf {
    let dir = tempfile::tempdir().expect("source tempdir");
    let path = dir.path().join(name);
    let mut file = std::fs::File::create(&path).expect("create source");
    file.write_all(contents.as_bytes()).expect("write source");
    file.flush().expect("flush source");
    std::mem::forget(dir);
    path
}

/// Acceptance: create from CSV with ints/floats/strings/dates/nulls round-trips,
/// append works, show reflects the combined rows, and history records both
/// snapshots with operations and timestamps.
#[tokio::test]
async fn csv_create_append_show_history() {
    let catalog = memory_catalog().await;
    let ident = parse_table_ident("sales.orders").expect("ident");

    let first = source_file(
        "orders.csv",
        "id,amount,customer,ordered_on\n1,10.5,alice,2024-01-01\n2,20.0,,2024-01-02\n",
    );
    let create = write::create_table(catalog.as_ref(), &ident, &first, None)
        .await
        .expect("create");
    assert_eq!(create.records_added, 2);
    assert!(create.data_files_added >= 1);
    // Schema inference: id -> long, amount -> double, customer/ordered_on -> string.
    let by_name: HashMap<_, _> = create
        .schema
        .iter()
        .map(|f| (f.name.clone(), f.r#type.clone()))
        .collect();
    assert_eq!(by_name.get("id").map(String::as_str), Some("long"));
    assert_eq!(by_name.get("amount").map(String::as_str), Some("double"));
    assert_eq!(by_name.get("customer").map(String::as_str), Some("string"));

    let second = source_file(
        "more.csv",
        "id,amount,customer,ordered_on\n3,5.0,carol,2024-01-03\n",
    );
    let append = write::append(catalog.as_ref(), &ident, &second)
        .await
        .expect("append");
    assert_eq!(append.records_added, 1);

    let show = inspect::show(catalog.as_ref(), &ident).await.expect("show");
    assert_eq!(show.row_count, Some(3), "two rows plus one appended");
    assert!(show.file_count.unwrap_or(0) >= 2);
    assert!(show.current_snapshot_id.is_some());

    let history = inspect::history(catalog.as_ref(), &ident)
        .await
        .expect("history");
    assert_eq!(history.snapshots.len(), 2, "create + append");
    assert!(history.snapshots.iter().all(|s| s.operation == "append"));
    assert!(history.snapshots[0].timestamp_ms <= history.snapshots[1].timestamp_ms);
    assert!(!history.snapshots[1].timestamp.is_empty());
}

/// Acceptance: a JSONL nested object lands as a struct column.
#[tokio::test]
async fn jsonl_nested_field_becomes_a_struct_column() {
    let catalog = memory_catalog().await;
    let ident = parse_table_ident("events.raw").expect("ident");
    let src = source_file(
        "events.jsonl",
        "{\"id\":1,\"meta\":{\"source\":\"web\",\"weight\":2}}\n{\"id\":2,\"meta\":{\"source\":\"cli\",\"weight\":9}}\n",
    );
    let create = write::create_table(catalog.as_ref(), &ident, &src, None)
        .await
        .expect("create");
    let meta = create
        .schema
        .iter()
        .find(|f| f.name == "meta")
        .expect("meta column present");
    assert!(
        meta.r#type.contains("struct"),
        "nested object should be a struct column, got {}",
        meta.r#type
    );
}

/// Acceptance: create from Parquet (self-describing schema) works and the data
/// is queryable.
#[tokio::test]
async fn parquet_create_and_query() {
    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;

    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("people.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .expect("batch");
    let file = std::fs::File::create(&path).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
    writer.write(&batch).expect("write");
    writer.close().expect("close");

    let catalog = memory_catalog().await;
    let ident = parse_table_ident("hr.people").expect("ident");
    let create = write::create_table(catalog.as_ref(), &ident, &path, None)
        .await
        .expect("create");
    assert_eq!(create.records_added, 3);

    let report = query::query(catalog.clone(), "SELECT count(*) AS n FROM hr.people", None)
        .await
        .expect("query");
    assert_eq!(report.row_count, 1);
    assert_eq!(report.rows[0]["n"], serde_json::json!(3));
}

/// Acceptance: an unquoted table reference resolves case-insensitively, so a
/// table registered with an uppercase name is queryable without quoting. The
/// SQL layer folds unquoted identifiers to lowercase, so `custom_points_LOGS`
/// and `custom_points_logs` must both reach the same table, and the quoted
/// exact form must keep working. A table identifier is an identifier, not a
/// case-sensitive string.
///
/// Assumption (documented): the platform never produces two tables in one
/// namespace differing only by case (the write path and REST catalog name them
/// deterministically), so the case-insensitive fallback is unambiguous. Exact
/// matches still win, so even a hypothetical collision resolves the exact name.
#[tokio::test]
async fn unquoted_table_reference_resolves_case_insensitively() {
    let catalog = memory_catalog().await;
    // A table whose name carries uppercase letters.
    let ident = parse_table_ident("rlean.custom_points_LOGS").expect("ident");
    write::create_table(
        catalog.as_ref(),
        &ident,
        &source_file("logs.csv", "day,value\n2026-07-20,1\n2026-07-21,2\n"),
        None,
    )
    .await
    .expect("create");

    // Unquoted, uppercase-suffix name (what an operator naturally types).
    let upper = query::query(
        catalog.clone(),
        "SELECT count(*) AS n FROM rlean.custom_points_LOGS",
        None,
    )
    .await
    .expect("unquoted uppercase-suffix name resolves");
    assert_eq!(upper.rows[0]["n"], serde_json::json!(2));

    // Unquoted, all-lowercase spelling of the same table.
    let lower = query::query(
        catalog.clone(),
        "SELECT count(*) AS n FROM rlean.custom_points_logs",
        None,
    )
    .await
    .expect("unquoted lowercase of the same name resolves");
    assert_eq!(lower.rows[0]["n"], serde_json::json!(2));

    // Quoted exact form keeps working (never normalized).
    let quoted = query::query(
        catalog.clone(),
        "SELECT count(*) AS n FROM rlean.\"custom_points_LOGS\"",
        None,
    )
    .await
    .expect("quoted exact form still resolves");
    assert_eq!(quoted.rows[0]["n"], serde_json::json!(2));
}

/// Acceptance: a query with a join across two tables returns joined rows.
#[tokio::test]
async fn query_with_a_join() {
    let catalog = memory_catalog().await;

    let customers = parse_table_ident("shop.customers").expect("ident");
    let orders = parse_table_ident("shop.orders").expect("ident");
    write::create_table(
        catalog.as_ref(),
        &customers,
        &source_file("customers.csv", "cid,name\n1,alice\n2,bob\n"),
        None,
    )
    .await
    .expect("create customers");
    write::create_table(
        catalog.as_ref(),
        &orders,
        &source_file(
            "orders.csv",
            "oid,cid,total\n10,1,99.0\n11,2,5.0\n12,1,1.0\n",
        ),
        None,
    )
    .await
    .expect("create orders");

    let sql = "SELECT c.name AS name, count(*) AS orders \
               FROM shop.orders o JOIN shop.customers c ON o.cid = c.cid \
               GROUP BY c.name ORDER BY c.name";
    let report = query::query(catalog.clone(), sql, None)
        .await
        .expect("join query");
    assert_eq!(report.columns, vec!["name".to_owned(), "orders".to_owned()]);
    assert_eq!(report.row_count, 2);
    assert_eq!(report.rows[0]["name"], serde_json::json!("alice"));
    assert_eq!(report.rows[0]["orders"], serde_json::json!(2));
    assert_eq!(report.rows[1]["name"], serde_json::json!("bob"));
    assert_eq!(report.rows[1]["orders"], serde_json::json!(1));
}

/// Acceptance: a time-travel query at the pre-append snapshot returns the
/// pre-append rows, even after a later append changed the table.
#[tokio::test]
async fn time_travel_returns_pre_append_results() {
    let catalog = memory_catalog().await;
    let ident = parse_table_ident("metrics.daily").expect("ident");

    let create = write::create_table(
        catalog.as_ref(),
        &ident,
        &source_file("d1.csv", "day,value\n1,100\n2,200\n"),
        None,
    )
    .await
    .expect("create");
    let pre_append_snapshot = create.snapshot_id.expect("snapshot after create");

    write::append(
        catalog.as_ref(),
        &ident,
        &source_file("d2.csv", "day,value\n3,300\n"),
    )
    .await
    .expect("append");

    // Current state: 3 rows.
    let now = query::query(
        catalog.clone(),
        "SELECT count(*) AS n FROM metrics.daily",
        None,
    )
    .await
    .expect("current query");
    assert_eq!(now.rows[0]["n"], serde_json::json!(3));

    // Time-travel to the pre-append snapshot: 2 rows.
    let travel = TimeTravel {
        reference: pre_append_snapshot.to_string(),
        table: "metrics.daily".to_owned(),
    };
    let past = query::query(
        catalog.clone(),
        "SELECT count(*) AS n FROM daily",
        Some(travel),
    )
    .await
    .expect("time-travel query");
    assert_eq!(
        past.rows[0]["n"],
        serde_json::json!(2),
        "the pre-append snapshot has two rows"
    );
}

/// Acceptance: a wrong-schema append is rejected, names the offending column,
/// and leaves the table unchanged.
#[tokio::test]
async fn wrong_schema_append_names_the_column() {
    let catalog = memory_catalog().await;
    let ident = parse_table_ident("t.readings").expect("ident");
    write::create_table(
        catalog.as_ref(),
        &ident,
        &source_file("r1.csv", "sensor,value\na,1.0\n"),
        None,
    )
    .await
    .expect("create");

    // The append source has an extra column the table does not know.
    let bad = source_file("r2.csv", "sensor,value,unexpected\nb,2.0,x\n");
    let err = write::append(catalog.as_ref(), &ident, &bad)
        .await
        .expect_err("append must be rejected");
    let message = err.to_string();
    assert!(
        message.contains("unexpected"),
        "error must name the mismatched column, got: {message}"
    );

    // The table is unchanged: still one row from the create.
    let show = inspect::show(catalog.as_ref(), &ident).await.expect("show");
    assert_eq!(show.row_count, Some(1));
}

/// Acceptance: `list` returns the tables in a namespace, and the JSON shapes are
/// stable (the skill parses them).
#[tokio::test]
async fn list_and_json_output_shapes() {
    let catalog = memory_catalog().await;
    let ident = parse_table_ident("ns.tbl").expect("ident");
    let create = write::create_table(
        catalog.as_ref(),
        &ident,
        &source_file("x.csv", "a,b\n1,2\n"),
        None,
    )
    .await
    .expect("create");

    let list = inspect::list(catalog.as_ref(), Some("ns"))
        .await
        .expect("list");
    assert_eq!(list.tables.len(), 1);
    assert_eq!(list.tables[0].namespace, "ns");
    assert_eq!(list.tables[0].name, "tbl");

    // Stable JSON keys for `create`.
    let value = serde_json::to_value(&create).expect("serialize create");
    for key in [
        "table",
        "operation",
        "schema",
        "partition_by",
        "records_added",
        "data_files_added",
        "snapshot_id",
    ] {
        assert!(
            value.get(key).is_some(),
            "create JSON must carry `{key}`: {value}"
        );
    }
    assert_eq!(value["operation"], serde_json::json!("create"));
    assert_eq!(value["table"], serde_json::json!("ns.tbl"));
    // A schema field's stable shape.
    let field = &value["schema"][0];
    for key in ["id", "name", "type", "required"] {
        assert!(
            field.get(key).is_some(),
            "schema field must carry `{key}`: {field}"
        );
    }

    // Stable JSON keys for `show`.
    let show = inspect::show(catalog.as_ref(), &ident).await.expect("show");
    let show_value = serde_json::to_value(&show).expect("serialize show");
    for key in [
        "table",
        "schema",
        "partition_by",
        "row_count",
        "file_count",
        "size_bytes",
        "current_snapshot_id",
    ] {
        assert!(
            show_value.get(key).is_some(),
            "show JSON must carry `{key}`: {show_value}"
        );
    }
}

/// Acceptance: an identity partition column is recorded on the table and shown.
#[tokio::test]
async fn create_with_partition_column() {
    let catalog = memory_catalog().await;
    let ident = parse_table_ident("p.events").expect("ident");
    let create = write::create_table(
        catalog.as_ref(),
        &ident,
        &source_file("e.csv", "region,n\nus,1\neu,2\nus,3\n"),
        Some("region"),
    )
    .await
    .expect("create partitioned");
    assert_eq!(create.partition_by, vec!["region".to_owned()]);

    let show = inspect::show(catalog.as_ref(), &ident).await.expect("show");
    assert_eq!(show.partition_by, vec!["region".to_owned()]);
    assert_eq!(show.row_count, Some(3));
}

/// `query_stream` is a genuine pull-based stream, not `query`'s `.collect()`
/// wearing a different return type: a table with enough rows to force
/// DataFusion's execution to split its output (the default `batch_size` is
/// 8192 records, applied by the executor regardless of the underlying file's
/// row-group layout) yields more than one `RecordBatch` from polling the
/// stream directly — proving nothing upstream of the caller gathered the
/// whole result into memory before handing it back. This is the load-bearing
/// claim behind the query role's streaming `/v1/query`: memory tracks the
/// operator working set as execution proceeds, not the result size.
#[tokio::test]
async fn query_stream_yields_multiple_batches_without_collecting() {
    let catalog = memory_catalog().await;
    let ident = parse_table_ident("big.rows").expect("ident");

    let rows = 20_000;
    let mut csv = "id,note\n".to_owned();
    for i in 0..rows {
        csv.push_str(&format!("{i},row number {i}\n"));
    }
    write::create_table(
        catalog.as_ref(),
        &ident,
        &source_file("rows.csv", &csv),
        None,
    )
    .await
    .expect("create");

    let verglas_iceberg::QueryExecution {
        columns,
        batches: mut stream,
    } = query_stream(catalog.clone(), "SELECT * FROM big.rows", None)
        .await
        .expect("query_stream");
    assert_eq!(columns, vec!["id".to_owned(), "note".to_owned()]);

    let mut batch_count = 0usize;
    let mut total_rows = 0usize;
    while let Some(batch) = stream.next().await {
        let batch = batch.expect("batch");
        batch_count += 1;
        total_rows += batch.num_rows();
    }
    assert_eq!(total_rows, rows as usize, "every row must still arrive");
    assert!(
        batch_count > 1,
        "a {rows}-row scan should split into multiple batches, got {batch_count}"
    );
}

/// `batch_to_json_rows_fragment` — the per-batch chunk a streaming HTTP
/// response splices between its own `[`/`]` — strips the `ArrayWriter`'s
/// enclosing brackets and reports the batch's row count, and concatenating
/// two batches' fragments with a comma between them (what the HTTP layer
/// does across batch boundaries) parses back to the same rows `query`'s
/// fully-collected path would have produced for the same data.
#[tokio::test]
async fn batch_fragment_concatenation_matches_the_collected_report() {
    let catalog = memory_catalog().await;
    let ident = parse_table_ident("frag.rows").expect("ident");
    // Two small appends so the scan naturally has more than one data file /
    // batch boundary to exercise the splice-with-a-comma path.
    write::create_table(
        catalog.as_ref(),
        &ident,
        &source_file("first.csv", "id,name\n1,alice\n2,bob\n"),
        None,
    )
    .await
    .expect("create");
    write::append(
        catalog.as_ref(),
        &ident,
        &source_file("second.csv", "id,name\n3,carol\n"),
    )
    .await
    .expect("append");

    let collected = query::query(catalog.clone(), "SELECT * FROM frag.rows ORDER BY id", None)
        .await
        .expect("collected query");

    let verglas_iceberg::QueryExecution {
        batches: mut stream,
        ..
    } = query_stream(catalog.clone(), "SELECT * FROM frag.rows ORDER BY id", None)
        .await
        .expect("query_stream");
    let mut fragment = String::new();
    let mut streamed_row_count = 0usize;
    while let Some(batch) = stream.next().await {
        let batch = batch.expect("batch");
        let (bytes, n) = batch_to_json_rows_fragment(&batch).expect("fragment");
        if n == 0 {
            continue;
        }
        if streamed_row_count > 0 {
            fragment.push(',');
        }
        fragment.push_str(std::str::from_utf8(&bytes).expect("utf8"));
        streamed_row_count += n;
    }
    assert_eq!(streamed_row_count, collected.row_count);

    let reassembled = format!("[{fragment}]");
    let reassembled_rows: Vec<serde_json::Map<String, serde_json::Value>> =
        serde_json::from_str(&reassembled).expect("reassembled JSON parses");
    let collected_rows: Vec<serde_json::Map<String, serde_json::Value>> = collected.rows;
    assert_eq!(reassembled_rows, collected_rows);
}

/// An empty batch contributes no bytes and no rows — a streaming caller must
/// never emit a stray `[]` or an unwanted comma for it.
#[tokio::test]
async fn empty_batch_fragment_is_empty() {
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let empty = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(Vec::<i64>::new()))])
        .expect("empty batch");
    let (bytes, n) = batch_to_json_rows_fragment(&empty).expect("fragment");
    assert_eq!(n, 0);
    assert!(bytes.is_empty());
}
