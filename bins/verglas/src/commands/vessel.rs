//! Local Vessel registry and private HTTP proxy commands.

use std::io::Read;
use std::path::Path;

use reqwest::{Client, Method, Response};
use serde_json::Value;

use crate::cli::{VesselAddArgs, VesselCommand};

const RUNTIME_URL_ENV: &str = "VERGLAS_CONTAINER_RUNTIME_URL";
const RUNTIME_TOKEN_ENV: &str = "VERGLAS_CONTAINER_RUNTIME_TOKEN";
const DEFAULT_RUNTIME_URL: &str = "http://127.0.0.1:8360";

/// Executes one local Vessel command.
pub async fn run(command: VesselCommand, json: bool) -> Result<(), Box<dyn std::error::Error>> {
    let client = RuntimeClient::from_environment()?;
    match command {
        VesselCommand::Add(args) => add(&client, &args, json).await,
        VesselCommand::List => {
            print_response(client.send(Method::GET, "/v1/vessels", None).await?, json).await
        }
        VesselCommand::Get(args) => {
            print_response(
                client
                    .send(Method::GET, &format!("/v1/vessels/{}", args.name), None)
                    .await?,
                json,
            )
            .await
        }
        VesselCommand::Remove(args) => {
            client
                .send(Method::DELETE, &format!("/v1/vessels/{}", args.name), None)
                .await?;
            Ok(())
        }
        VesselCommand::Curl(args) => {
            validate_proxy_path(&args.path)?;
            let method = Method::from_bytes(args.method.as_bytes())?;
            let body = if args.data_stdin {
                let mut input = String::new();
                std::io::stdin().read_to_string(&mut input)?;
                Some(serde_json::from_str(&input)?)
            } else {
                args.data.as_deref().map(serde_json::from_str).transpose()?
            };
            print_response(
                client
                    .send(
                        method,
                        &format!("/v1/vessels/{}/http{}", args.name, args.path),
                        body,
                    )
                    .await?,
                json,
            )
            .await
        }
        VesselCommand::Query(args) => {
            let body: Value = serde_json::from_str(&args.request)?;
            print_response(
                client
                    .send(
                        Method::POST,
                        &format!("/v1/vessels/{}/http/v1/query", args.name),
                        Some(body),
                    )
                    .await?,
                json,
            )
            .await
        }
    }
}

/// Reads and submits one Vessel declaration without adding secret fields.
async fn add(
    client: &RuntimeClient,
    args: &VesselAddArgs,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let body = read_manifest(&args.file)?;
    let name = body
        .get("name")
        .and_then(Value::as_str)
        .ok_or("Vessel manifest requires name")?;
    print_response(
        client
            .send(Method::PUT, &format!("/v1/vessels/{name}"), Some(body))
            .await?,
        json,
    )
    .await
}

/// Decodes YAML or JSON into the common JSON wire representation.
fn read_manifest(path: &Path) -> Result<Value, Box<dyn std::error::Error>> {
    let text = std::fs::read_to_string(path)?;
    if path.extension().and_then(|value| value.to_str()) == Some("json") {
        Ok(serde_json::from_str(&text)?)
    } else {
        Ok(serde_yaml::from_str(&text)?)
    }
}

/// Rejects absolute URLs so calls cannot bypass the manager's Vessel lookup.
fn validate_proxy_path(path: &str) -> Result<(), Box<dyn std::error::Error>> {
    if path.starts_with('/') && !path.starts_with("//") {
        Ok(())
    } else {
        Err("Vessel HTTP path must start with one '/'".into())
    }
}

/// Prints a successful response as compact or pretty JSON when possible.
async fn print_response(response: Response, json: bool) -> Result<(), Box<dyn std::error::Error>> {
    let status = response.status();
    let bytes = response.bytes().await?;
    if !status.is_success() {
        return Err(format!(
            "runtime returned HTTP {status}: {}",
            String::from_utf8_lossy(&bytes)
        )
        .into());
    }
    if bytes.is_empty() {
        return Ok(());
    }
    if let Ok(value) = serde_json::from_slice::<Value>(&bytes) {
        if json {
            println!("{}", serde_json::to_string(&value)?);
        } else {
            println!("{}", serde_json::to_string_pretty(&value)?);
        }
    } else {
        println!("{}", String::from_utf8_lossy(&bytes));
    }
    Ok(())
}

/// Authenticated client for the trusted local runtime manager.
struct RuntimeClient {
    endpoint: String,
    token: String,
    client: Client,
}

impl RuntimeClient {
    /// Builds a client from the dedicated runtime endpoint and bearer token.
    fn from_environment() -> Result<Self, Box<dyn std::error::Error>> {
        let endpoint =
            std::env::var(RUNTIME_URL_ENV).unwrap_or_else(|_| DEFAULT_RUNTIME_URL.to_owned());
        let token = std::env::var(RUNTIME_TOKEN_ENV)
            .map_err(|_| format!("{RUNTIME_TOKEN_ENV} is required for Vessel operations"))?;
        Ok(Self {
            endpoint: endpoint.trim_end_matches('/').to_owned(),
            token,
            client: Client::new(),
        })
    }

    /// Sends one authenticated request and validates its status before returning.
    async fn send(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<Response, Box<dyn std::error::Error>> {
        let mut request = self
            .client
            .request(method, format!("{}{path}", self.endpoint))
            .bearer_auth(&self.token);
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await?;
        if response.status().is_success() {
            Ok(response)
        } else {
            let status = response.status();
            let body = response.text().await?;
            Err(format!("runtime returned HTTP {status}: {body}").into())
        }
    }
}
