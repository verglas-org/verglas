//! Opt-in end-to-end coverage for an acknowledged Iceberg write notification.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use futures::{StreamExt, stream};
use verglas_sdk::{
    Client, ClientError, ColumnSpec, ConnectOptions, TableDefinition, TableSubscriptionEvent,
};

/// A real write must wake one durable table subscription and remain queryable.
#[tokio::test]
#[ignore = "requires a running Verglas deployment and VERGLAS_E2E_TOKEN"]
async fn iceberg_write_wakes_durable_subscription() {
    let endpoint = std::env::var("VERGLAS_E2E_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:8345".to_owned());
    let query_endpoint =
        std::env::var("VERGLAS_E2E_QUERY_ENDPOINT").unwrap_or_else(|_| endpoint.clone());
    let s3_endpoint = std::env::var("VERGLAS_E2E_S3_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:8333".to_owned());
    let token = std::env::var("VERGLAS_E2E_TOKEN").expect("VERGLAS_E2E_TOKEN");
    let database_name =
        std::env::var("VERGLAS_E2E_DATABASE").unwrap_or_else(|_| "rlean".to_owned());
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    let configured_table = std::env::var("VERGLAS_E2E_TABLE").ok();
    let table_name = configured_table
        .clone()
        .unwrap_or_else(|| format!("e2e.follow_probe_{suffix}"));

    let client = Client::connect(
        ConnectOptions::new(&endpoint)
            .with_query_uri(query_endpoint)
            .with_access_uri(&endpoint)
            .with_s3_endpoint(s3_endpoint)
            .with_token(token),
    )
    .await
    .expect("connect");
    let database = client.database(&database_name).expect("database");
    if configured_table.is_none() {
        database
            .ensure_table(
                &table_name,
                &TableDefinition {
                    schema: vec![ColumnSpec::required("probe_id", "int64")],
                    partitions: vec![],
                },
            )
            .await
            .expect("ensure probe table");
    }

    let group = format!("e2e-follow-{suffix}");
    let owner = format!("e2e-owner-{suffix}");
    let mut changes = database
        .subscribe(&group, &owner, [table_name.as_str()], 60)
        .expect("subscribe");
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(10), changes.next())
            .await
            .expect("subscription connect deadline")
            .expect("subscription event")
            .expect("subscription connect"),
        TableSubscriptionEvent::Connected
    ));

    let schema = Arc::new(Schema::new(vec![Field::new(
        "probe_id",
        DataType::Int64,
        false,
    )]));
    let probe_id = i64::try_from(suffix % i64::MAX as u128).expect("probe id");
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![probe_id]))])
        .expect("probe batch");
    let result = database
        .append_stream(
            &table_name,
            stream::iter([Ok::<_, ClientError>(batch)]),
            &format!("e2e-follow-{suffix}"),
        )
        .await
        .expect("append probe");
    assert_eq!(result.rows_committed, 1);

    let delivery = loop {
        let event = tokio::time::timeout(Duration::from_secs(20), changes.next())
            .await
            .expect("commit delivery deadline")
            .expect("commit event")
            .expect("valid commit event");
        if let TableSubscriptionEvent::Delivery(delivery) = event {
            break delivery;
        }
    };
    assert_eq!(delivery.change.table, table_name);
    database
        .ack(&group, &delivery.receipt)
        .await
        .expect("ack delivery");

    let mut rows = database
        .query_stream(&format!(
            "SELECT probe_id FROM {table_name} WHERE probe_id = {probe_id}"
        ))
        .await
        .expect("query probe");
    let row_count = rows
        .next()
        .await
        .expect("query batch")
        .expect("valid query batch")
        .num_rows();
    assert_eq!(row_count, 1);
}
