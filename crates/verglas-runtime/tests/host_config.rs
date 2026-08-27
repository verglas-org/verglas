//! Production Catalog host configuration tests.
//!
//! These tests exercise the operator-owned JSON boundary before any origin or
//! event socket is opened. The fixture intentionally names every runtime-owned
//! setting while leaving credentials to the backend ambient/file chain.

use std::path::Path;

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::any;
use serde_json::{Value, json};
use verglas_runtime::CatalogHostConfig;

/// Builds one complete production-shaped Catalog host configuration document.
fn document(cache_dir: &Path) -> Value {
    json!({
        "origin": {
            "storage_binding_id": "catalog-origin",
            "bucket": "lake",
            "scheme": "s3",
            "backend": {
                "provider": "s3",
                "bucket": "lake",
                "endpoint": "http://127.0.0.1:9000",
                "allow_http": true,
                "region": "us-east-1"
            }
        },
        "cache": {
            "dir": cache_dir,
            "capacity_bytes": "64MB",
            "dram_bytes": "64MB",
            "data_block_bytes": "1MB"
        },
        "warehouse": "s3://lake/warehouse",
        "sink": {
            "sink_id": "primary",
            "namespace": "analytics",
            "table": "events",
            "compression": "zstd"
        }
    })
}

/// Returns one status from the local origin probe fixture.
async fn fixed_status(State(status): State<StatusCode>) -> StatusCode {
    status
}

/// Starts a local origin that deterministically rejects the probe credentials.
async fn rejecting_origin()
-> Result<(String, tokio::task::JoinHandle<Result<(), std::io::Error>>), std::io::Error> {
    let app = Router::new()
        .fallback(any(fixed_status))
        .with_state(StatusCode::FORBIDDEN);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener, app).await });
    Ok((endpoint, server))
}

/// A complete JSON document parses and validates without exposing credentials.
#[test]
fn parses_complete_catalog_host_configuration() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let config: CatalogHostConfig = serde_json::from_value(document(directory.path()))?;
    config.validate()?;
    assert_eq!(config.origin().storage_binding_id(), "catalog-origin");
    assert_eq!(config.origin().bucket(), "lake");
    assert_eq!(config.origin().scheme(), "s3");
    assert_eq!(config.cache().dir, directory.path());
    assert_eq!(config.warehouse(), "s3://lake/warehouse");
    assert_eq!(config.sink().sink_id(), "primary");
    assert_eq!(config.sink().namespace(), "analytics");
    assert_eq!(config.sink().table(), "events");
    assert_eq!(config.sink().compression().as_str(), "zstd");
    Ok(())
}

/// Unknown top-level operator fields are rejected rather than ignored.
#[test]
fn rejects_unknown_top_level_fields() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let mut value = document(directory.path());
    value["unexpected"] = Value::Bool(true);
    let error = serde_json::from_value::<CatalogHostConfig>(value).expect_err("unknown field");
    assert!(error.to_string().contains("unknown field"));
    Ok(())
}

/// Unknown nested backend fields are rejected by the same strict boundary.
#[test]
fn rejects_unknown_backend_fields() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let mut value = document(directory.path());
    value["origin"]["backend"]["credential_literal"] = Value::String("secret".to_owned());
    let error = serde_json::from_value::<CatalogHostConfig>(value).expect_err("unknown field");
    assert!(error.to_string().contains("unknown field"));
    Ok(())
}

/// A stable logical Worker bucket may map to one deployment-specific provider bucket.
#[test]
fn accepts_exact_physical_bucket_behind_logical_worker_bucket()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let mut value = document(directory.path());
    value["origin"]["backend"]["bucket"] = Value::String("other".to_owned());
    let config: CatalogHostConfig = serde_json::from_value(value)?;
    config.validate()?;
    assert_eq!(config.origin().bucket(), "lake");
    Ok(())
}

/// Wildcard bucket scope is rejected even when it happens to include this bucket.
#[test]
fn rejects_backend_bucket_globs() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let mut value = document(directory.path());
    value["origin"]["backend"]["bucket_globs"] = json!(["*"]);
    let config: CatalogHostConfig = serde_json::from_value(value)?;
    let error = config.validate().expect_err("bucket glob");
    assert!(error.to_string().contains("one exact bucket"));
    Ok(())
}

/// A warehouse rooted at another scheme or bucket is rejected.
#[test]
fn rejects_warehouse_prefix_mismatch() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let mut value = document(directory.path());
    value["warehouse"] = Value::String("s3://other/warehouse".to_owned());
    let config: CatalogHostConfig = serde_json::from_value(value)?;
    let error = config.validate().expect_err("warehouse mismatch");
    assert!(
        error
            .to_string()
            .contains("warehouse must be under s3://lake")
    );
    Ok(())
}

/// Ambiguous warehouse paths are rejected before backend construction.
#[test]
fn rejects_ambiguous_warehouse_paths() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    for warehouse in [
        "s3://lake/../escape",
        "s3://lake/warehouse//table",
        "s3://lake/warehouse\\table",
        "s3://lake/warehouse?other",
    ] {
        let mut value = document(directory.path());
        value["warehouse"] = Value::String(warehouse.to_owned());
        let config: CatalogHostConfig = serde_json::from_value(value)?;
        assert!(config.validate().is_err(), "accepted {warehouse:?}");
    }
    Ok(())
}

/// Local URI schemes cannot turn the production host into a memory/file fallback.
#[test]
fn rejects_local_origin_scheme() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let mut value = document(directory.path());
    value["origin"]["scheme"] = Value::String("memory".to_owned());
    value["warehouse"] = Value::String("memory://lake/warehouse".to_owned());
    let config: CatalogHostConfig = serde_json::from_value(value)?;
    let error = config.validate().expect_err("local scheme");
    assert!(error.to_string().contains("local and in-memory"));
    Ok(())
}

/// A valid operator file loads every supported compression spelling strictly.
#[test]
fn loads_valid_file_and_all_compression_fences() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    for compression in ["zstd", "snappy", "gzip", "lz4", "uncompressed"] {
        let mut value = document(directory.path());
        value["sink"]["compression"] = Value::String(compression.to_owned());
        let path = directory.path().join(format!("catalog-{compression}.json"));
        std::fs::write(&path, serde_json::to_vec(&value)?)?;
        assert_eq!(
            CatalogHostConfig::load(&path)?
                .sink()
                .compression()
                .as_str(),
            compression
        );
    }
    let mut invalid = document(directory.path());
    invalid["sink"]["compression"] = Value::String("brotli".to_owned());
    assert!(serde_json::from_value::<CatalogHostConfig>(invalid).is_err());
    Ok(())
}

/// Invalid identity and cache budgets fail before any backend construction.
#[test]
fn rejects_invalid_identity_and_cache_budgets() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    for (pointer, invalid) in [
        ("/origin/storage_binding_id", "bad binding"),
        ("/origin/bucket", "bad/bucket"),
        ("/origin/scheme", "bad:scheme"),
        ("/sink/sink_id", "bad id"),
        ("/sink/namespace", "analytics..private"),
        ("/sink/table", "bad/table"),
    ] {
        let mut value = document(directory.path());
        *value.pointer_mut(pointer).ok_or("fixture pointer")? = Value::String(invalid.to_owned());
        let config: CatalogHostConfig = serde_json::from_value(value)?;
        assert!(config.validate().is_err(), "accepted {pointer}={invalid:?}");
    }
    for (pointer, bytes) in [
        ("/cache/dram_bytes", "1B"),
        ("/cache/capacity_bytes", "1MB"),
    ] {
        let mut value = document(directory.path());
        *value.pointer_mut(pointer).ok_or("fixture pointer")? = Value::String(bytes.to_owned());
        let config: CatalogHostConfig = serde_json::from_value(value)?;
        assert!(config.validate().is_err(), "accepted {pointer}={bytes}");
    }
    Ok(())
}

/// An unreadable operator path fails as a configuration error.
#[test]
fn unreadable_configuration_path_fails() {
    let error = CatalogHostConfig::load("/path/that/does/not/exist/catalog.json")
        .expect_err("missing configuration");
    assert!(error.to_string().contains("could not be read"));
}

/// Service construction probes the exact origin before opening Foyer or an event socket.
#[tokio::test]
async fn rejected_origin_probe_fails_service_construction() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempfile::tempdir()?;
    let credentials = directory.path().join("credentials");
    std::fs::write(
        &credentials,
        "[default]\naws_access_key_id = AK\naws_secret_access_key = secret\n",
    )?;
    let (endpoint, server) = rejecting_origin().await?;
    let mut value = document(directory.path());
    value["origin"]["backend"]["endpoint"] = Value::String(endpoint);
    value["origin"]["backend"]["credentials_file"] =
        Value::String(credentials.display().to_string());
    value["origin"]["backend"]["retry"] = json!({
        "max_retries": 0,
        "initial_backoff_ms": 1,
        "max_backoff_ms": 1,
        "budget_ms": 50
    });
    let config: CatalogHostConfig = serde_json::from_value(value)?;
    let error = match config.build_catalog_commit_service().await {
        Ok(_) => return Err("rejected origin probe unexpectedly succeeded".into()),
        Err(error) => error,
    };
    assert!(error.to_string().contains("origin probe failed"));
    server.abort();
    Ok(())
}
