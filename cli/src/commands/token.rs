//! `verglas token` — scoped access tokens on the control plane
//! (`/v0/tokens`): mint producer/reader credentials limited to named scopes,
//! list them without values, revoke by id or name.

use std::error::Error;

use serde_json::{Value, json};

use crate::cli::{TokenCommand, TokenCreateArgs, TokenRevokeArgs};

/// Percent-encodes one query-parameter value (RFC 3986 unreserved set).
fn urlencode(value: &str) -> String {
    value
        .bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (byte as char).to_string()
            }
            other => format!("%{other:02X}"),
        })
        .collect()
}

/// Dispatches `verglas token` against the control plane.
pub async fn run(
    command: TokenCommand,
    endpoint: &str,
    token: Option<&str>,
    json_output: bool,
) -> Result<(), Box<dyn Error>> {
    match command {
        TokenCommand::Create(TokenCreateArgs { name, scopes }) => {
            // The spec-mirror accepts creation parameters as query params;
            // scope repeats per grant.
            let mut path = format!("/v0/tokens?name={}", urlencode(&name));
            for scope in &scopes {
                path.push_str("&scope=");
                path.push_str(&urlencode(scope));
            }
            let response: Value = crate::auth::cloud_call(token.map(str::to_owned), |bearer| {
                let endpoint = endpoint.to_owned();
                let path = path.clone();
                async move {
                    let server = crate::backend::server(&endpoint, bearer.as_deref())?;
                    server.post_json(&path, &json!({})).await
                }
            })
            .await?;
            if json_output {
                println!("{}", serde_json::to_string_pretty(&response)?);
                return Ok(());
            }
            let value = response["token"].as_str().unwrap_or_default();
            println!("{value}");
            eprintln!("This token is shown once; store it now.");
            Ok(())
        }
        TokenCommand::List => {
            let response: Value = crate::auth::cloud_call(token.map(str::to_owned), |bearer| {
                let endpoint = endpoint.to_owned();
                async move {
                    let server = crate::backend::server(&endpoint, bearer.as_deref())?;
                    server.get("/v0/tokens").await
                }
            })
            .await?;
            if json_output {
                println!("{}", serde_json::to_string_pretty(&response)?);
                return Ok(());
            }
            for row in response["tokens"].as_array().cloned().unwrap_or_default() {
                let scopes = row["scopes"]
                    .as_array()
                    .map(|scopes| {
                        scopes
                            .iter()
                            .filter_map(Value::as_str)
                            .collect::<Vec<_>>()
                            .join(",")
                    })
                    .unwrap_or_default();
                println!(
                    "{}\t{}\t{}",
                    row["id"].as_str().unwrap_or("-"),
                    row["name"].as_str().unwrap_or("-"),
                    scopes,
                );
            }
            Ok(())
        }
        TokenCommand::Revoke(TokenRevokeArgs { token_id }) => {
            let _: Value = crate::auth::cloud_call(token.map(str::to_owned), |bearer| {
                let endpoint = endpoint.to_owned();
                let token_id = token_id.clone();
                async move {
                    let server = crate::backend::server(&endpoint, bearer.as_deref())?;
                    server.delete(&format!("/v0/tokens/{token_id}")).await
                }
            })
            .await?;
            if !json_output {
                println!("Revoked {token_id}.");
            }
            Ok(())
        }
    }
}
