//! Stateless Iceberg proposal acceptance tests for the runtime host capability.

use std::error::Error;
use std::path::Path;
use std::sync::Arc;

use iceberg::io::LocalFsStorageFactory;
use iceberg::spec::TableMetadata;
use serde_json::{Value, json};
use verglas_do_wasm::{HostError, Request};
use verglas_iceberg::{SinkCompression, deterministic_sink_file_id};
use verglas_runtime::{
    CatalogCommitService, CatalogCommitServiceConfig, IcebergCatalogCommitService,
    MAX_CATALOG_COMMIT_BODY_BYTES,
};

/// Builds a stateless proposal service backed by a host-owned filesystem factory.
fn local_service(path: &Path) -> Result<IcebergCatalogCommitService, Box<dyn Error>> {
    let warehouse = path.to_str().ok_or("warehouse path is not UTF-8")?;
    let config = CatalogCommitServiceConfig::new(
        "primary",
        "bucket",
        "analytics",
        "events",
        SinkCompression::Zstd,
    )
    .with_warehouse(warehouse);
    Ok(IcebergCatalogCommitService::new(
        Arc::new(LocalFsStorageFactory),
        config,
    ))
}

/// Builds the configured Sink table create operation.
fn create_table_request(warehouse: &str) -> Request {
    arbitrary_create_table_request(warehouse, &["analytics"], "events", None)
}

/// Builds a Sink proposal operation with the supplied current metadata location.
fn sink_request(current_metadata_location: Option<&str>) -> Request {
    let batch_id = "batch-runtime";
    let file_id = deterministic_sink_file_id("primary", batch_id);
    let sql_digest = "a".repeat(64);
    let body = json!({
        "operation": "commit-sink-batch",
        "current_metadata_location": current_metadata_location,
        "request": {
            "batch_id": batch_id,
            "file_id": file_id,
            "sink_id": "primary",
            "pipeline_id": "pipeline",
            "sql_digest": sql_digest,
            "source": "source",
            "first_sequence": 1,
            "last_sequence": 2,
            "bucket": "bucket",
            "namespace": "analytics",
            "table": "events",
            "format": "parquet",
            "compression": "zstd",
            "roll_interval_seconds": 60,
            "roll_size_bytes": 1024,
            "records": [{"id": 1}, {"id": 2}]
        }
    });
    Request {
        method: "POST".to_owned(),
        uri: "https://verglas.internal/catalog/commit".to_owned(),
        headers: vec![
            ("content-type".to_owned(), "application/json".to_owned()),
            ("x-verglas-sink-id".to_owned(), "primary".to_owned()),
            ("x-verglas-batch-id".to_owned(), batch_id.to_owned()),
            ("x-verglas-file-id".to_owned(), file_id),
            ("x-verglas-pipeline-id".to_owned(), "pipeline".to_owned()),
            ("x-verglas-sql-digest".to_owned(), sql_digest),
        ],
        body: serde_json::to_vec(&body).expect("valid JSON"),
        ws: None,
    }
}

/// Builds a create-table operation for an arbitrary REST namespace and table.
fn arbitrary_create_table_request(
    warehouse: &str,
    namespace: &[&str],
    name: &str,
    location: Option<&str>,
) -> Request {
    let body = json!({
        "operation": "create-table",
        "warehouse": warehouse,
        "namespace": namespace,
        "request": {
            "name": name,
            "schema": {
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    {"id": 1, "name": "id", "required": false, "type": "long"}
                ]
            },
            "location": location,
        }
    });
    Request {
        method: "POST".to_owned(),
        uri: "https://verglas.internal/catalog/commit".to_owned(),
        headers: vec![("content-type".to_owned(), "application/json".to_owned())],
        body: serde_json::to_vec(&body).expect("valid JSON"),
        ws: None,
    }
}

/// Builds a registration operation for an existing metadata location.
fn register_table_request(metadata_location: &str) -> Request {
    let body = json!({
        "operation": "register-table",
        "metadata_location": metadata_location,
    });
    Request {
        method: "POST".to_owned(),
        uri: "https://verglas.internal/catalog/commit".to_owned(),
        headers: vec![("content-type".to_owned(), "application/json".to_owned())],
        body: serde_json::to_vec(&body).expect("valid JSON"),
        ws: None,
    }
}

/// Builds a standard commit operation for an arbitrary REST identifier.
fn arbitrary_table_commit_request(
    current_metadata_location: &str,
    namespace: &[&str],
    name: &str,
    request_json: Value,
) -> Request {
    let body = json!({
        "operation": "commit-table",
        "current_metadata_location": current_metadata_location,
        "request_json": serde_json::to_string(&json!({
            "identifier": {"namespace": namespace, "name": name},
            "requirements": request_json["requirements"],
            "updates": request_json["updates"],
        }))
        .expect("valid JSON"),
    });
    Request {
        method: "POST".to_owned(),
        uri: "https://verglas.internal/catalog/commit".to_owned(),
        headers: vec![("content-type".to_owned(), "application/json".to_owned())],
        body: serde_json::to_vec(&body).expect("valid JSON"),
        ws: None,
    }
}

/// Builds a standard commit operation with a supplied identifier and exact REST document.
fn identified_table_commit_request(
    current_metadata_location: &str,
    identifier: Value,
    request_json: Value,
) -> Request {
    let body = json!({
        "operation": "commit-table",
        "current_metadata_location": current_metadata_location,
        "request_json": serde_json::to_string(&json!({
            "identifier": identifier,
            "requirements": request_json["requirements"],
            "updates": request_json["updates"],
        }))
        .expect("valid JSON"),
    });
    Request {
        method: "POST".to_owned(),
        uri: "https://verglas.internal/catalog/commit".to_owned(),
        headers: vec![("content-type".to_owned(), "application/json".to_owned())],
        body: serde_json::to_vec(&body).expect("valid JSON"),
        ws: None,
    }
}

/// Registration loads complete standard metadata without creating host-side state.
#[tokio::test]
async fn register_table_returns_full_metadata_without_state() -> Result<(), Box<dyn Error>> {
    let warehouse = tempfile::tempdir()?;
    let service = local_service(warehouse.path())?;
    let warehouse_text = warehouse
        .path()
        .to_str()
        .ok_or("warehouse path is not UTF-8")?;
    let created: Value = serde_json::from_slice(
        &service
            .commit(create_table_request(warehouse_text))
            .await?
            .body,
    )?;
    let location = created["metadata-location"]
        .as_str()
        .ok_or("metadata location")?;
    let response = service.commit(register_table_request(location)).await?;
    let registered: Value = serde_json::from_slice(&response.body)?;
    assert_eq!(response.status, 200);
    assert_eq!(
        registered["metadata-location"],
        created["metadata-location"]
    );
    assert_eq!(registered["metadata"], created["metadata"]);
    assert!(registered.get("state").is_none());
    assert_eq!(registered.as_object().map(|object| object.len()), Some(2));
    Ok(())
}

/// Public REST proposals accept valid namespace and table identities outside the Sink fence.
#[tokio::test]
async fn public_table_operations_accept_arbitrary_namespace_and_table() -> Result<(), Box<dyn Error>>
{
    let warehouse = tempfile::tempdir()?;
    let service = local_service(warehouse.path())?;
    let warehouse_text = warehouse
        .path()
        .to_str()
        .ok_or("warehouse path is not UTF-8")?;
    let namespace = ["sales", "daily"];
    let created: Value = serde_json::from_slice(
        &service
            .commit(arbitrary_create_table_request(
                warehouse_text,
                &namespace,
                "orders",
                None,
            ))
            .await?
            .body,
    )?;
    assert_eq!(
        created["metadata"]["location"],
        format!("{warehouse_text}/sales/daily/orders")
    );
    let current = created["metadata-location"]
        .as_str()
        .ok_or("metadata location")?;
    let table_uuid = created["metadata"]["table-uuid"]
        .as_str()
        .ok_or("table UUID")?;
    let commit_request = json!({
        "requirements": [{"type": "assert-table-uuid", "uuid": table_uuid}],
        "updates": [{"action": "set-properties", "updates": {"owner": "rest"}}]
    });
    let committed: Value = serde_json::from_slice(
        &service
            .commit(arbitrary_table_commit_request(
                current,
                &namespace,
                "orders",
                commit_request,
            ))
            .await?
            .body,
    )?;
    assert_eq!(committed["metadata"]["properties"]["owner"], "rest");
    let registered: Value = serde_json::from_slice(
        &service
            .commit(register_table_request(
                committed["metadata-location"]
                    .as_str()
                    .ok_or("committed metadata location")?,
            ))
            .await?
            .body,
    )?;
    assert_eq!(
        registered["metadata"]["location"],
        created["metadata"]["location"]
    );
    Ok(())
}

/// A requested table location with a local parent escape is a bounded bad request.
#[tokio::test]
async fn create_table_rejects_requested_warehouse_escape() -> Result<(), Box<dyn Error>> {
    let warehouse = tempfile::tempdir()?;
    let service = local_service(warehouse.path())?;
    let warehouse_text = warehouse
        .path()
        .to_str()
        .ok_or("warehouse path is not UTF-8")?;
    let escaped = format!("{warehouse_text}/../escape");
    let response = service
        .commit(arbitrary_create_table_request(
            warehouse_text,
            &["sales"],
            "orders",
            Some(&escaped),
        ))
        .await?;
    let value: Value = serde_json::from_slice(&response.body)?;
    assert_eq!(response.status, 400);
    assert!(value["error"]["message"].as_str().is_some());
    Ok(())
}

/// Ambiguous table locations fail as bad requests before any host storage access.
#[tokio::test]
async fn create_table_rejects_ambiguous_location_characters() -> Result<(), Box<dyn Error>> {
    let warehouse = tempfile::tempdir()?;
    let service = local_service(warehouse.path())?;
    let warehouse_text = warehouse
        .path()
        .to_str()
        .ok_or("warehouse path is not UTF-8")?;
    for suffix in [
        "sales//orders",
        "sales\\orders",
        "sales/orders?other",
        "sales/\u{0}orders",
    ] {
        let location = format!("{warehouse_text}/{suffix}");
        let response = service
            .commit(arbitrary_create_table_request(
                warehouse_text,
                &["sales"],
                "orders",
                Some(&location),
            ))
            .await?;
        assert_eq!(response.status, 400, "accepted {location:?}");
    }
    Ok(())
}

/// Namespace and table path traversal segments are rejected before any write.
#[tokio::test]
async fn create_table_rejects_namespace_and_table_traversal() -> Result<(), Box<dyn Error>> {
    let warehouse = tempfile::tempdir()?;
    let service = local_service(warehouse.path())?;
    let warehouse_text = warehouse
        .path()
        .to_str()
        .ok_or("warehouse path is not UTF-8")?;
    for (namespace, name) in [
        (&["sales", ".."] as &[&str], "orders"),
        (&["sales"], "../orders"),
    ] {
        let response = service
            .commit(arbitrary_create_table_request(
                warehouse_text,
                namespace,
                name,
                None,
            ))
            .await?;
        assert_eq!(response.status, 400);
    }
    Ok(())
}

/// Current metadata locations outside the warehouse are rejected as bad requests.
#[tokio::test]
async fn operations_reject_current_metadata_warehouse_escape() -> Result<(), Box<dyn Error>> {
    let warehouse = tempfile::tempdir()?;
    let service = local_service(warehouse.path())?;
    let warehouse_text = warehouse
        .path()
        .to_str()
        .ok_or("warehouse path is not UTF-8")?;
    let escaped = format!("{warehouse_text}/../escape/metadata/v1.metadata.json");
    let table_request = json!({"requirements": [], "updates": []});
    let table_response = service
        .commit(identified_table_commit_request(
            &escaped,
            json!({"namespace": ["analytics"], "name": "events"}),
            table_request,
        ))
        .await?;
    assert_eq!(table_response.status, 400);
    let register_response = service.commit(register_table_request(&escaped)).await?;
    assert_eq!(register_response.status, 400);
    let sink_response = service.commit(sink_request(Some(&escaped))).await?;
    assert_eq!(sink_response.status, 400);
    Ok(())
}

/// A valid in-warehouse metadata read failure remains a host storage error.
#[tokio::test]
async fn missing_current_metadata_is_host_storage_failure() -> Result<(), Box<dyn Error>> {
    let warehouse = tempfile::tempdir()?;
    let service = local_service(warehouse.path())?;
    let warehouse_text = warehouse
        .path()
        .to_str()
        .ok_or("warehouse path is not UTF-8")?;
    let missing = format!("{warehouse_text}/analytics/events/metadata/v999.metadata.json");
    let request = json!({"requirements": [], "updates": []});
    let error = service
        .commit(identified_table_commit_request(
            &missing,
            json!({"namespace": ["analytics"], "name": "events"}),
            request,
        ))
        .await
        .expect_err("missing metadata must reach host storage");
    assert!(matches!(error, HostError::Backend { .. }));
    Ok(())
}

/// Malformed JSON is a bounded client error, not a binding transport failure.
#[tokio::test]
async fn malformed_catalog_body_returns_http_400() -> Result<(), Box<dyn Error>> {
    let warehouse = tempfile::tempdir()?;
    let service = local_service(warehouse.path())?;
    let response = service
        .commit(Request {
            method: "POST".to_owned(),
            uri: "https://verglas.internal/catalog/commit".to_owned(),
            headers: vec![("content-type".to_owned(), "application/json".to_owned())],
            body: b"{".to_vec(),
            ws: None,
        })
        .await?;
    assert_eq!(response.status, 400);
    let body: Value = serde_json::from_slice(&response.body)?;
    assert!(body["error"]["message"].as_str().is_some());
    Ok(())
}

/// The private capability enforces its hard body ceiling before JSON parsing.
#[tokio::test]
async fn oversized_catalog_body_returns_http_400() -> Result<(), Box<dyn Error>> {
    let warehouse = tempfile::tempdir()?;
    let service = local_service(warehouse.path())?;
    let response = service
        .commit(Request {
            method: "POST".to_owned(),
            uri: "https://verglas.internal/catalog/commit".to_owned(),
            headers: vec![("content-type".to_owned(), "application/json".to_owned())],
            body: vec![b' '; MAX_CATALOG_COMMIT_BODY_BYTES + 1],
            ws: None,
        })
        .await?;
    assert_eq!(response.status, 400);
    Ok(())
}

/// Sink publication writes data and metadata while returning the full immutable proposal.
#[tokio::test]
async fn sink_batch_returns_metadata_proposal_without_catalog_head() -> Result<(), Box<dyn Error>> {
    let warehouse = tempfile::tempdir()?;
    let service = local_service(warehouse.path())?;
    let response = service.commit(sink_request(None)).await?;
    assert_eq!(response.status, 200);
    let value: Value = serde_json::from_slice(&response.body)?;
    assert_eq!(value["committed"], true);
    assert_eq!(value["rows_committed"], 2);
    let metadata_location = value["metadata_location"]
        .as_str()
        .ok_or("metadata location")?;
    assert!(value["snapshot_id"].as_str().is_some());
    let file_io = iceberg::io::FileIO::new_with_fs();
    let metadata = TableMetadata::read_from(&file_io, metadata_location).await?;
    assert_eq!(
        metadata.current_snapshot_id().map(|id| id.to_string()),
        value["snapshot_id"].as_str().map(ToOwned::to_owned)
    );
    assert!(
        file_io
            .exists(&format!(
                "{}/data/{}",
                metadata.location(),
                deterministic_sink_file_id("primary", "batch-runtime")
            ))
            .await?
    );
    Ok(())
}

/// Sink publication reads the supplied current metadata and returns only a new proposal.
#[tokio::test]
async fn sink_batch_advances_supplied_metadata_proposal() -> Result<(), Box<dyn Error>> {
    let warehouse = tempfile::tempdir()?;
    let service = local_service(warehouse.path())?;
    let warehouse_text = warehouse
        .path()
        .to_str()
        .ok_or("warehouse path is not UTF-8")?;
    let created: Value = serde_json::from_slice(
        &service
            .commit(create_table_request(warehouse_text))
            .await?
            .body,
    )?;
    let current = created["metadata-location"]
        .as_str()
        .ok_or("metadata location")?;
    let response = service.commit(sink_request(Some(current))).await?;
    let value: Value = serde_json::from_slice(&response.body)?;
    assert_eq!(response.status, 200);
    assert_ne!(value["metadata_location"].as_str(), Some(current));
    assert_eq!(
        value["metadata"]["location"],
        created["metadata"]["location"]
    );
    assert_eq!(
        value["metadata"]["snapshots"].as_array().map(Vec::len),
        Some(1)
    );
    Ok(())
}

/// A failed Iceberg requirement is returned as a conflict response.
#[tokio::test]
async fn table_commit_requirement_conflict_returns_http_409() -> Result<(), Box<dyn Error>> {
    let warehouse = tempfile::tempdir()?;
    let service = local_service(warehouse.path())?;
    let warehouse_text = warehouse
        .path()
        .to_str()
        .ok_or("warehouse path is not UTF-8")?;
    let created: Value = serde_json::from_slice(
        &service
            .commit(create_table_request(warehouse_text))
            .await?
            .body,
    )?;
    let current = created["metadata-location"]
        .as_str()
        .ok_or("metadata location")?;
    let request = json!({
        "requirements": [{
            "type": "assert-table-uuid",
            "uuid": "00000000-0000-4000-8000-000000000099"
        }],
        "updates": [{"action": "set-properties", "updates": {"owner": "never-written"}}]
    });
    let response = service
        .commit(arbitrary_table_commit_request(
            current,
            &["analytics"],
            "events",
            request,
        ))
        .await?;
    assert_eq!(response.status, 409);
    let value: Value = serde_json::from_slice(&response.body)?;
    assert!(value["error"]["message"].as_str().is_some());
    Ok(())
}

/// The exact request_json document preserves signed 64-bit Iceberg values.
#[tokio::test]
async fn table_commit_preserves_exact_request_json_i64_values() -> Result<(), Box<dyn Error>> {
    let warehouse = tempfile::tempdir()?;
    let service = local_service(warehouse.path())?;
    let warehouse_text = warehouse
        .path()
        .to_str()
        .ok_or("warehouse path is not UTF-8")?;
    let created: Value = serde_json::from_slice(
        &service
            .commit(create_table_request(warehouse_text))
            .await?
            .body,
    )?;
    let current = created["metadata-location"]
        .as_str()
        .ok_or("metadata location")?;
    let request = json!({
        "requirements": [],
        "updates": [{
            "action": "remove-snapshots",
            "snapshot-ids": [9223372036854775807_i64]
        }]
    });
    let response = service
        .commit(arbitrary_table_commit_request(
            current,
            &["analytics"],
            "events",
            request,
        ))
        .await?;
    assert_eq!(response.status, 200);
    let value: Value = serde_json::from_slice(&response.body)?;
    assert_ne!(value["metadata-location"].as_str(), Some(current));
    Ok(())
}
