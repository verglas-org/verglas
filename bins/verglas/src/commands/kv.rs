//! Direct CLI access to the built-in persistent KV engine.

use std::io::Write;

use reqwest::header::{AUTHORIZATION, ETAG};

use crate::cli::{KvCommand, KvKeyArgs, KvSetArgs};

/// Runs one KV command against the selected server endpoint.
pub async fn run(
    command: KvCommand,
    endpoint: &str,
    token: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    match command {
        KvCommand::Set(args) => set(&client, endpoint, token, args).await,
        KvCommand::Get(args) => get(&client, endpoint, token, args).await,
    }
}

/// Sets one raw UTF-8 value and prints its opaque committed version.
async fn set(
    client: &reqwest::Client,
    endpoint: &str,
    token: Option<&str>,
    args: KvSetArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut request = client
        .put(kv_url(endpoint, &args.namespace, &args.key)?)
        .body(args.value);
    if let Some(token) = token {
        request = request.header(AUTHORIZATION, format!("Bearer {token}"));
    }
    if let Some(ttl) = args.ttl {
        request = request.header("x-verglas-ttl-seconds", ttl);
    }
    let response = request.send().await?.error_for_status()?;
    let version = response
        .headers()
        .get(ETAG)
        .ok_or("KV set response omitted ETag")?
        .to_str()?;
    println!("{version}");
    Ok(())
}

/// Gets one raw value and writes it unchanged followed by a terminal newline.
async fn get(
    client: &reqwest::Client,
    endpoint: &str,
    token: Option<&str>,
    args: KvKeyArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut request = client.get(kv_url(endpoint, &args.namespace, &args.key)?);
    if let Some(token) = token {
        request = request.header(AUTHORIZATION, format!("Bearer {token}"));
    }
    let bytes = request.send().await?.error_for_status()?.bytes().await?;
    let mut stdout = std::io::stdout().lock();
    stdout.write_all(&bytes)?;
    stdout.write_all(b"\n")?;
    Ok(())
}

/// Builds an encoded KV URL without allowing either argument to change scope.
fn kv_url(
    endpoint: &str,
    namespace: &str,
    key: &str,
) -> Result<reqwest::Url, Box<dyn std::error::Error>> {
    let mut url = reqwest::Url::parse(endpoint)?;
    {
        let mut segments = url
            .path_segments_mut()
            .map_err(|_| "server endpoint cannot carry path segments")?;
        segments
            .pop_if_empty()
            .push("v1")
            .push("kv")
            .push(namespace)
            .push(key);
    }
    Ok(url)
}
