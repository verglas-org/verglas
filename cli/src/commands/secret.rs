//! Creates URI-scoped secrets through the local access service.
//!
//! Secret material is accepted only from a hidden interactive prompt or
//! standard input so it is never exposed in process arguments.

use std::io::{IsTerminal, Read};

use reqwest::Url;
use serde::Serialize;

use crate::cli::{SecretCommand, SecretCreateArgs, SecretType};

/// Secret creation request sent to the local access service.
#[derive(Clone, Serialize)]
struct CreateSecretRequest {
    /// Stable secret name.
    name: String,
    /// Credential type used during binding resolution.
    #[serde(rename = "type")]
    secret_type: SecretRequestType,
    /// URI prefix authorized by the credentials.
    scope: String,
    /// Opaque secret material encrypted by the access service.
    value: String,
}

/// Secret type serialized for the local access service.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "kebab-case")]
enum SecretRequestType {
    /// S3-compatible credentials.
    S3,
    /// Iceberg REST bearer or signing credentials.
    IcebergRest,
}

/// Runs one secret command against the selected local server.
pub async fn run(
    command: SecretCommand,
    endpoint: &str,
    token: Option<&str>,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        SecretCommand::Create(args) => create(endpoint, token, args, json).await,
    }
}

/// Reads secret material and sends it to the access service once.
async fn create(
    endpoint: &str,
    token: Option<&str>,
    args: SecretCreateArgs,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    validate_name(&args.name)?;
    let secret_type = match args.secret_type {
        SecretType::S3 => {
            validate_scope(&args.scope, &["s3"])?;
            SecretRequestType::S3
        }
        SecretType::IcebergRest => {
            validate_scope(&args.scope, &["http", "https"])?;
            SecretRequestType::IcebergRest
        }
    };
    let value = read_secret()?;
    let name = args.name;
    let request = CreateSecretRequest {
        name: name.clone(),
        secret_type,
        scope: args.scope,
        value,
    };
    let response = post_json(endpoint, token, &request).await?;
    if json {
        println!("{}", serde_json::to_string(&response)?);
    } else {
        println!("created secret {name}");
    }
    Ok(())
}

/// Reads a secret from a no-echo terminal prompt or a redirected stdin stream.
fn read_secret() -> Result<String, Box<dyn std::error::Error>> {
    let value = if std::io::stdin().is_terminal() {
        rpassword::prompt_password("Secret value: ")?
    } else {
        let mut value = String::new();
        std::io::stdin().read_to_string(&mut value)?;
        value.trim_end_matches(['\r', '\n']).to_owned()
    };
    if value.is_empty() {
        return Err("secret value must not be empty".into());
    }
    Ok(value)
}

/// Rejects names that cannot be stable local resource identifiers.
fn validate_name(name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut characters = name.chars();
    let Some(first) = characters.next() else {
        return Err("resource name must not be empty".into());
    };
    if !(first.is_ascii_alphabetic() || first == '_')
        || !characters.all(|character| {
            character.is_ascii_alphanumeric() || character == '_' || character == '-'
        })
    {
        return Err("resource name must start with a letter or underscore and contain only ASCII letters, digits, underscores, or hyphens".into());
    }
    Ok(())
}

/// Validates a secret scope against the credential type's URI schemes.
fn validate_scope(scope: &str, schemes: &[&str]) -> Result<(), Box<dyn std::error::Error>> {
    let parsed = Url::parse(scope).map_err(|error| format!("invalid secret scope: {error}"))?;
    if !schemes.contains(&parsed.scheme()) || parsed.host_str().is_none() {
        return Err(format!(
            "secret scope must be an absolute {} URI",
            schemes.join(" or ")
        )
        .into());
    }
    Ok(())
}

/// Sends an opaque secret request and returns the service's JSON response. A
/// 401 fails loud through `auth::with_reauth` with guidance to re-run
/// `verglas login`; there is no refresh.
async fn post_json(
    endpoint: &str,
    token: Option<&str>,
    body: &CreateSecretRequest,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let url = Url::parse(endpoint)?.join("/v1/secrets")?;
    let response = crate::auth::with_reauth(token.map(str::to_owned), |bearer| {
        let url = url.clone();
        let body = body.clone();
        async move {
            let mut request = reqwest::Client::new().post(url).json(&body);
            if let Some(bearer) = bearer {
                request = request.bearer_auth(bearer);
            }
            request.send().await?.error_for_status()?.json().await
        }
    })
    .await?;
    Ok(response)
}
