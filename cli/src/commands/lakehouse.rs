//! `verglas lakehouse` — the control plane's tenant-scoped Iceberg warehouse
//! surface (`/v1/lakehouses`): list this deployment's lakehouses or create one,
//! managed or on the caller's own S3 bucket.

use std::error::Error;

use serde_json::{Value, json};

use crate::cli::{LakehouseCommand, LakehouseCreateArgs};

/// Dispatches `verglas lakehouse` against the control plane.
pub async fn run(
    command: LakehouseCommand,
    endpoint: &str,
    token: Option<&str>,
    json_output: bool,
) -> Result<(), Box<dyn Error>> {
    let tenant =
        crate::connection_profile::tenant_id().ok_or("not signed in; run `verglas login`")?;
    match command {
        LakehouseCommand::List => {
            let response: Value = crate::auth::cloud_call(token.map(str::to_owned), |bearer| {
                let endpoint = endpoint.to_owned();
                let tenant = tenant.clone();
                async move {
                    let server = crate::backend::server(&endpoint, bearer.as_deref())?;
                    server
                        .get(&format!("/v1/lakehouses?tenant_id={tenant}"))
                        .await
                }
            })
            .await?;
            if json_output {
                println!("{}", serde_json::to_string_pretty(&response)?);
                return Ok(());
            }
            let rows = response["lakehouses"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            for row in rows {
                println!(
                    "{}\t{}",
                    row["name"].as_str().unwrap_or("-"),
                    row["storage"]["mode"].as_str().unwrap_or("-"),
                );
            }
            Ok(())
        }
        LakehouseCommand::Create(args) => {
            let storage = storage_body(&args)?;
            let response: Value = crate::auth::cloud_call(token.map(str::to_owned), |bearer| {
                let endpoint = endpoint.to_owned();
                let body = json!({ "tenant_id": tenant, "name": args.name, "storage": storage });
                async move {
                    let server = crate::backend::server(&endpoint, bearer.as_deref())?;
                    server.post_json("/v1/lakehouses", &body).await
                }
            })
            .await?;
            if json_output {
                println!("{}", serde_json::to_string_pretty(&response)?);
            } else {
                println!("Created lakehouse {}.", args.name);
            }
            Ok(())
        }
    }
}

/// Builds the create request's storage object: managed by default, or an
/// external-s3 profile when `--data-path` names the caller's bucket. The
/// bucket credentials come from the environment, never argv.
fn storage_body(args: &LakehouseCreateArgs) -> Result<Value, Box<dyn Error>> {
    let Some(data_path) = args.data_path.as_deref() else {
        return Ok(json!({ "mode": "managed" }));
    };
    let rest = data_path
        .strip_prefix("s3://")
        .ok_or("--data-path must look like s3://bucket/prefix")?;
    let (bucket, key_prefix) = rest.split_once('/').unwrap_or((rest, ""));
    if bucket.is_empty() {
        return Err("--data-path must name a bucket".into());
    }
    let access_key_id = std::env::var("VERGLAS_STORAGE_ACCESS_KEY_ID")
        .map_err(|_| "set VERGLAS_STORAGE_ACCESS_KEY_ID for --data-path")?;
    let secret_access_key = std::env::var("VERGLAS_STORAGE_SECRET_ACCESS_KEY")
        .map_err(|_| "set VERGLAS_STORAGE_SECRET_ACCESS_KEY for --data-path")?;
    let mut profile = json!({
        "bucket": bucket,
        "key_prefix": key_prefix,
        "access_key_id": access_key_id,
        "secret_access_key": secret_access_key,
    });
    if let Some(endpoint) = args.storage_endpoint.as_deref() {
        profile["endpoint"] = Value::String(endpoint.to_owned());
    }
    if let Some(region) = args.storage_region.as_deref() {
        profile["region"] = Value::String(region.to_owned());
    }
    Ok(json!({ "mode": "external-s3", "profile": profile }))
}
