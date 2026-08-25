//! Serves one preprovisioned Durable Object bucket over an authenticated S3
//! endpoint. Bucket allocation and credential derivation remain cloud control-
//! plane responsibilities; this process only enforces and forwards them.

use std::error::Error;
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use verglas_core::config::Backend;
use verglas_s3::{
    BackendStore, BackendStores, NoopInvalidation, PassthroughList, PassthroughRead,
    PassthroughWrite,
};

/// Strict coordinates for one object-owned S3 endpoint.
struct Config {
    listen: SocketAddr,
    storage_binding_id: String,
    public_bucket: String,
    origin_bucket: String,
    endpoint: String,
    region: String,
    origin_credentials: PathBuf,
    client_credentials: PathBuf,
}

impl Config {
    /// Parses the explicit prototype command line without environment aliases.
    fn parse() -> Result<Self, String> {
        let mut arguments = std::env::args().skip(1);
        let mut listen = None;
        let mut storage_binding_id = None;
        let mut public_bucket = None;
        let mut origin_bucket = None;
        let mut endpoint = None;
        let mut region = None;
        let mut origin_credentials = None;
        let mut client_credentials = None;
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--listen" => {
                    let value = next_value(&mut arguments, "--listen")?;
                    listen =
                        Some(value.parse().map_err(|error| {
                            format!("--listen is not a socket address: {error}")
                        })?);
                }
                "--storage-binding-id" => {
                    storage_binding_id = Some(next_value(&mut arguments, "--storage-binding-id")?);
                }
                "--public-bucket" => {
                    public_bucket = Some(next_value(&mut arguments, "--public-bucket")?);
                }
                "--origin-bucket" => {
                    origin_bucket = Some(next_value(&mut arguments, "--origin-bucket")?);
                }
                "--endpoint" => endpoint = Some(next_value(&mut arguments, "--endpoint")?),
                "--region" => region = Some(next_value(&mut arguments, "--region")?),
                "--origin-credentials" => {
                    origin_credentials = Some(PathBuf::from(next_value(
                        &mut arguments,
                        "--origin-credentials",
                    )?));
                }
                "--client-credentials" => {
                    client_credentials = Some(PathBuf::from(next_value(
                        &mut arguments,
                        "--client-credentials",
                    )?));
                }
                "--help" => return Err(usage().to_owned()),
                other => return Err(format!("unknown argument {other}\n{}", usage())),
            }
        }
        Ok(Self {
            listen: listen.ok_or_else(|| format!("missing --listen\n{}", usage()))?,
            storage_binding_id: required(storage_binding_id, "--storage-binding-id")?,
            public_bucket: required(public_bucket, "--public-bucket")?,
            origin_bucket: required(origin_bucket, "--origin-bucket")?,
            endpoint: required(endpoint, "--endpoint")?,
            region: required(region, "--region")?,
            origin_credentials: origin_credentials
                .ok_or_else(|| format!("missing --origin-credentials\n{}", usage()))?,
            client_credentials: client_credentials
                .ok_or_else(|| format!("missing --client-credentials\n{}", usage()))?,
        })
    }
}

/// Reads one required option value.
fn next_value(
    arguments: &mut impl Iterator<Item = String>,
    option: &str,
) -> Result<String, String> {
    arguments
        .next()
        .ok_or_else(|| format!("missing value for {option}"))
}

/// Rejects an absent or empty string option.
fn required(value: Option<String>, option: &str) -> Result<String, String> {
    value
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("missing {option}\n{}", usage()))
}

/// Returns the one supported invocation shape.
fn usage() -> &'static str {
    "usage: verglas-s3-proxy --listen ADDR --storage-binding-id ID --public-bucket NAME --origin-bucket NAME --endpoint URL --region REGION --origin-credentials PATH --client-credentials PATH"
}

/// Reads one AWS-format credentials file without exposing either key in argv.
fn credentials(path: &PathBuf) -> Result<(String, String), Box<dyn Error>> {
    let contents = fs::read_to_string(path)?;
    verglas_backend::read_aws_keypair(&contents, "default")
        .ok_or_else(|| format!("credentials file {} has no default keypair", path.display()).into())
}

/// Runs the exact-bucket proxy until it receives a termination signal.
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = match Config::parse() {
        Ok(config) => config,
        Err(message) => {
            eprintln!("{message}");
            return Err("invalid verglas-s3-proxy arguments".into());
        }
    };
    let client_credentials = credentials(&config.client_credentials)?;
    let backend = Backend {
        bucket: Some(config.origin_bucket.clone()),
        endpoint: Some(config.endpoint.clone()),
        region: Some(config.region),
        allow_http: config.endpoint.starts_with("http://"),
        credentials_file: Some(config.origin_credentials.display().to_string()),
        ..Backend::default()
    };
    let backing: Arc<dyn BackendStores> =
        BackendStore::from_config(config.storage_binding_id.clone(), &backend);
    let stores: Arc<dyn BackendStores> = Arc::new(verglas_backend::BucketAliasStores::new(
        backing,
        config.storage_binding_id.clone(),
        config.public_bucket,
        config.origin_bucket,
    ));
    let app = verglas_s3::router_with_passthrough(
        config.storage_binding_id,
        PassthroughRead::new(Arc::clone(&stores)),
        PassthroughWrite::new(Arc::clone(&stores)),
        Arc::new(PassthroughList::new(Arc::clone(&stores))),
        Arc::new(NoopInvalidation),
        Some(client_credentials),
        Some(stores),
        None,
        None,
    );
    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
