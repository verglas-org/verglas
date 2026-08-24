//! Command-line entry point for the OSS Durable Object gateway.

use std::error::Error;
use std::net::SocketAddr;
use std::path::PathBuf;

use tokio::net::TcpListener;
use verglas_gateway::{Gateway, Manifest};

/// Strict command-line configuration for one gateway process.
struct Config {
    manifest: PathBuf,
    listen: SocketAddr,
    celld_control: PathBuf,
    data_root: PathBuf,
}

impl Config {
    /// Parses the one supported command-line shape without compatibility aliases.
    fn parse() -> Result<Self, String> {
        let mut arguments = std::env::args().skip(1);
        let mut manifest = None;
        let mut listen = None;
        let mut celld_control = None;
        let mut data_root = None;
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--manifest" => {
                    manifest = Some(PathBuf::from(next_value(&mut arguments, "--manifest")?));
                }
                "--listen" => {
                    let value = next_value(&mut arguments, "--listen")?;
                    listen =
                        Some(value.parse::<SocketAddr>().map_err(|error| {
                            format!("--listen is not a socket address: {error}")
                        })?);
                }
                "--celld-control" => {
                    celld_control = Some(PathBuf::from(next_value(
                        &mut arguments,
                        "--celld-control",
                    )?));
                }
                "--data-root" => {
                    data_root = Some(PathBuf::from(next_value(&mut arguments, "--data-root")?));
                }
                "--help" => return Err(usage().to_owned()),
                other => return Err(format!("unknown argument {other}\n{}", usage())),
            }
        }
        Ok(Self {
            manifest: manifest.ok_or_else(|| format!("missing --manifest\n{}", usage()))?,
            listen: listen.ok_or_else(|| format!("missing --listen\n{}", usage()))?,
            celld_control: celld_control
                .ok_or_else(|| format!("missing --celld-control\n{}", usage()))?,
            data_root: data_root.ok_or_else(|| format!("missing --data-root\n{}", usage()))?,
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

/// Returns the one supported command-line shape.
fn usage() -> &'static str {
    "usage: verglas-gateway --manifest PATH --listen ADDR --celld-control PATH --data-root PATH\npublic /* routes run the Worker fetch; /do/<binding>/<name>/* is internal/debug only"
}

/// Loads the manifest and serves its resident DO routes.
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = match Config::parse() {
        Ok(config) => config,
        Err(message) => {
            eprintln!("{message}");
            return Err("invalid verglas-gateway arguments".into());
        }
    };
    let manifest = Manifest::from_path(&config.manifest)?;
    let listener = TcpListener::bind(config.listen).await?;
    let gateway = Gateway::new(&manifest, config.celld_control, config.data_root);
    gateway.serve(listener).await?;
    Ok(())
}
