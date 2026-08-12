//! Declares queue resources through the tenant access API.
//! Message operations belong to the SDK and never create a queue implicitly.

use reqwest::{Method, Url};
use serde_json::{Value, json};

use crate::cli::{QueueCommand, QueueNameArgs};

/// Runs one explicit queue lifecycle command.
pub async fn run(
    command: QueueCommand,
    endpoint: &str,
    token: Option<&str>,
    json_output: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        QueueCommand::Create(args) => create(endpoint, token, args, json_output).await,
        QueueCommand::List => list(endpoint, token, json_output).await,
        QueueCommand::Show(args) => show(endpoint, token, args, json_output).await,
        QueueCommand::Delete(args) => delete(endpoint, token, args, json_output).await,
    }
}

/// Creates one queue and waits for both managed deployments to become usable.
async fn create(
    endpoint: &str,
    token: Option<&str>,
    args: QueueNameArgs,
    json_output: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    validate_name(&args.name)?;
    let response = request(
        endpoint,
        token,
        Method::POST,
        "/v1/queues",
        Some(json!({ "name": args.name })),
    )
    .await?;
    print_resource(response, json_output, "created queue")
}

/// Lists every explicitly declared queue.
async fn list(
    endpoint: &str,
    token: Option<&str>,
    json_output: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let response = request(endpoint, token, Method::GET, "/v1/queues", None).await?;
    print_resource(response, json_output, "queues")
}

/// Shows one queue declaration and its dedicated deployments.
async fn show(
    endpoint: &str,
    token: Option<&str>,
    args: QueueNameArgs,
    json_output: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    validate_name(&args.name)?;
    let response = request(
        endpoint,
        token,
        Method::GET,
        &format!("/v1/queues/{}", args.name),
        None,
    )
    .await?;
    print_resource(response, json_output, "queue")
}

/// Deletes the queue service before its queue-owned Neon deployment.
async fn delete(
    endpoint: &str,
    token: Option<&str>,
    args: QueueNameArgs,
    json_output: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    validate_name(&args.name)?;
    request(
        endpoint,
        token,
        Method::DELETE,
        &format!("/v1/queues/{}", args.name),
        None,
    )
    .await?;
    if json_output {
        println!("{}", json!({ "deleted": args.name }));
    } else {
        println!("deleted queue {}", args.name);
    }
    Ok(())
}

/// Sends one authenticated resource request and fails on every non-success response.
async fn request(
    endpoint: &str,
    token: Option<&str>,
    method: Method,
    path: &str,
    body: Option<Value>,
) -> Result<Option<Value>, Box<dyn std::error::Error>> {
    let mut url = Url::parse(endpoint)?;
    url.set_path(path);
    let client = reqwest::Client::new();
    let mut request = client.request(method, url);
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    if let Some(body) = body {
        request = request.json(&body);
    }
    let response = request.send().await?;
    if !response.status().is_success() {
        let status = response.status();
        let detail = response.text().await?;
        return Err(format!("queue API returned HTTP {status}: {detail}").into());
    }
    if response.status() == reqwest::StatusCode::NO_CONTENT {
        return Ok(None);
    }
    Ok(Some(response.json().await?))
}

/// Prints bounded JSON in either compact or readable form.
fn print_resource(
    response: Option<Value>,
    json_output: bool,
    label: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let response = response.ok_or("queue API returned no resource")?;
    if json_output {
        println!("{}", serde_json::to_string(&response)?);
    } else {
        println!("{label}: {}", serde_json::to_string_pretty(&response)?);
    }
    Ok(())
}

/// Enforces the same DNS-safe queue name accepted by the server plan.
fn validate_name(name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let valid = !name.is_empty()
        && name.len() <= 48
        && name.bytes().enumerate().all(|(index, byte)| match byte {
            b'a'..=b'z' | b'0'..=b'9' => true,
            b'-' => index > 0 && index + 1 < name.len(),
            _ => false,
        })
        && !name.contains("--");
    if !valid {
        return Err("queue name must be a lowercase DNS-safe identifier".into());
    }
    Ok(())
}
