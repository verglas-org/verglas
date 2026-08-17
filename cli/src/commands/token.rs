//! `verglas token` commands for scoped credential lifecycle.
//!
//! The access service derives the actor from the supplied bearer credential.
//! This command never prints the one-time plaintext token; it writes it to the
//! local owner-only credentials file and prints only safe inventory metadata.

use std::path::Path;

use verglas_sdk::{
    AccessTokenCreateRequest, AccessTokenGrant, AccessTokenSummary, Client, ConnectOptions,
};

use crate::cli::{TokenCommand, TokenCreateArgs, TokenRevokeArgs};
use crate::credentials::{StoredToken, remove_token, save_token};

/// Runs one token lifecycle command against the configured access service.
/// Every verb routes its call through `auth::with_reauth_required` so a 401
/// fails loud with guidance to re-run `verglas login`; there is no refresh.
pub async fn run(
    command: TokenCommand,
    endpoint: &str,
    bearer: Option<&str>,
    credentials_path: &Path,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let bearer = bearer.ok_or("not signed in; run `verglas login`")?.to_owned();
    match command {
        TokenCommand::Create(args) => create(endpoint, bearer, credentials_path, args, json).await,
        TokenCommand::List => list(endpoint, bearer, json).await,
        TokenCommand::Revoke(args) => revoke(endpoint, bearer, credentials_path, args, json).await,
    }
}

/// Creates an SDK client that targets only the standalone access service.
async fn access_client(endpoint: &str, bearer: &str) -> Result<Client, verglas_sdk::ClientError> {
    Client::connect(
        ConnectOptions::new(endpoint)
            .with_access_uri(endpoint)
            .with_query_uri(endpoint)
            .with_s3_endpoint("http://127.0.0.1:8333")
            .with_token(bearer),
    )
    .await
}

/// Mints a token and writes its one-time plaintext value to local secure storage.
async fn create(
    endpoint: &str,
    bearer: String,
    credentials_path: &Path,
    args: TokenCreateArgs,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let request = request_from_args(args)?;
    let issued = crate::auth::with_reauth_required(bearer, move |bearer| {
        let endpoint = endpoint.to_owned();
        let request = request.clone();
        async move {
            let client = access_client(&endpoint, &bearer).await?;
            client.create_access_token(&request).await
        }
    })
    .await?;
    save_token(
        credentials_path,
        endpoint,
        StoredToken {
            token: issued.token,
            token_id: issued.summary.id.clone(),
        },
    )?;
    render_summary(&issued.summary, json, Some(credentials_path))
}

/// Lists token metadata without ever materializing plaintext values.
async fn list(endpoint: &str, bearer: String, json: bool) -> Result<(), Box<dyn std::error::Error>> {
    let tokens = crate::auth::with_reauth_required(bearer, move |bearer| {
        let endpoint = endpoint.to_owned();
        async move {
            let client = access_client(&endpoint, &bearer).await?;
            client.list_access_tokens().await
        }
    })
    .await?;
    if json {
        println!("{}", serde_json::to_string_pretty(&tokens)?);
    } else if tokens.is_empty() {
        println!("No access tokens found.");
    } else {
        println!("ID\tNAME\tAUDIENCE\tEXPIRES\tREVOKED");
        for token in tokens {
            println!(
                "{}\t{}\t{}\t{}\t{}",
                token.id,
                token.name,
                token.audience,
                token
                    .expires_at
                    .map_or_else(|| "never".to_owned(), |value| value.to_string()),
                token
                    .revoked_at
                    .map_or_else(|| "active".to_owned(), |value| value.to_string()),
            );
        }
    }
    Ok(())
}

/// Revokes a token and removes the matching local credential record.
async fn revoke(
    endpoint: &str,
    bearer: String,
    credentials_path: &Path,
    args: TokenRevokeArgs,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let id = args.id.clone();
    let summary = crate::auth::with_reauth_required(bearer, move |bearer| {
        let endpoint = endpoint.to_owned();
        let id = id.clone();
        async move {
            let client = access_client(&endpoint, &bearer).await?;
            client.revoke_access_token(&id).await
        }
    })
    .await?;
    remove_token(credentials_path, endpoint, &summary.id)?;
    render_summary(&summary, json, None)
}

/// Converts validated CLI options into the access-service create request.
fn request_from_args(
    args: TokenCreateArgs,
) -> Result<AccessTokenCreateRequest, Box<dyn std::error::Error>> {
    if args.name.trim().is_empty() {
        return Err("token name must not be empty".into());
    }
    if args.audience.trim().is_empty() {
        return Err("token audience must not be empty".into());
    }
    let mut request = AccessTokenCreateRequest::new(args.name, args.audience);
    if let Some(expires_in_seconds) = args.expires_in_seconds {
        if expires_in_seconds == 0 {
            return Err("--expires-in-seconds must be greater than zero".into());
        }
        request = request.with_expiration_seconds(expires_in_seconds);
    }
    for grant in args.grants {
        request = request.with_grant(parse_grant(&grant)?);
    }
    Ok(request)
}

/// Parses one `RESOURCE=action,action` declaration without accepting ambiguous syntax.
fn parse_grant(value: &str) -> Result<AccessTokenGrant, Box<dyn std::error::Error>> {
    let (resource_id, actions) = value
        .split_once('=')
        .ok_or("--grant must use RESOURCE=action,action")?;
    let resource_id = resource_id.trim();
    if resource_id.is_empty() {
        return Err("--grant resource must not be empty".into());
    }
    let actions = actions
        .split(',')
        .map(str::trim)
        .filter(|action| !action.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if actions.is_empty() {
        return Err("--grant must contain at least one action".into());
    }
    Ok(AccessTokenGrant::new(resource_id, actions))
}

/// Renders non-secret token metadata after create or revoke.
fn render_summary(
    summary: &AccessTokenSummary,
    json: bool,
    saved_to: Option<&Path>,
) -> Result<(), Box<dyn std::error::Error>> {
    if json {
        println!("{}", serde_json::to_string_pretty(summary)?);
    } else {
        println!(
            "{} token {}",
            if summary.revoked_at.is_some() {
                "revoked"
            } else {
                "created"
            },
            summary.id
        );
        if let Some(path) = saved_to {
            println!("Stored the one-time credential at {}.", path.display());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Rejects grants that cannot map to one resource and finite action list.
    #[test]
    fn parse_grant_rejects_ambiguous_values() {
        for value in ["", "analytics", "=query", "analytics="] {
            assert!(parse_grant(value).is_err(), "{value}");
        }
    }
}
