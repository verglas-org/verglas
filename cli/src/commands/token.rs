//! `verglas token` — scoped access tokens on the control plane
//! (`/v1/tokens`): mint producer/reader credentials limited to named scopes,
//! list them without values, revoke by id.

use std::error::Error;

use serde_json::{Value, json};

use crate::cli::{TokenCommand, TokenCreateArgs, TokenRevokeArgs};

/// Dispatches `verglas token` against the control plane.
pub async fn run(
    command: TokenCommand,
    endpoint: &str,
    token: Option<&str>,
    json_output: bool,
) -> Result<(), Box<dyn Error>> {
    let server = crate::backend::server(endpoint, token)?;
    match command {
        TokenCommand::Create(TokenCreateArgs { name, scopes }) => {
            let response: Value = server
                .post_json("/v1/tokens", &json!({ "name": name, "scopes": scopes }))
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
            let response: Value = server.get("/v1/tokens").await?;
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
                    row["token_id"].as_str().unwrap_or("-"),
                    row["name"].as_str().unwrap_or("-"),
                    scopes,
                );
            }
            Ok(())
        }
        TokenCommand::Revoke(TokenRevokeArgs { token_id }) => {
            // A revoke success is a 204 with no body; only a non-success
            // status is an error, not the empty-body decode.
            match server
                .delete::<Value>(&format!("/v1/tokens/{token_id}"))
                .await
            {
                Ok(_) => {}
                Err(verglas_sdk::server::ServerError::Decode(_)) => {}
                Err(error) => return Err(error.into()),
            }
            if !json_output {
                println!("Revoked {token_id}.");
            }
            Ok(())
        }
    }
}
