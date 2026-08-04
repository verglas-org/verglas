//! `verglas-write` isolated logical table writer.

use verglas_write_node::VERSION;
use verglas_write_node::config::WriteConfig;

/// Parses `--config`, `--ports-file`, and version flags.
fn parse_args() -> Result<Option<(String, Option<String>)>, String> {
    let mut config = None;
    let mut ports_file = None;
    let mut args = std::env::args().skip(1);
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--version" | "-V" => return Ok(None),
            "--config" => {
                config = Some(
                    args.next()
                        .ok_or_else(|| "--config requires a path".to_owned())?,
                );
            }
            "--ports-file" => {
                ports_file = Some(
                    args.next()
                        .ok_or_else(|| "--ports-file requires a path".to_owned())?,
                );
            }
            other => return Err(format!("unrecognized argument `{other}`")),
        }
    }
    Ok(Some((
        config.ok_or_else(|| "--config <path> is required".to_owned())?,
        ports_file,
    )))
}

/// Loads configuration and serves the write role.
#[tokio::main]
async fn main() {
    let invocation = match parse_args() {
        Ok(invocation) => invocation,
        Err(error) => {
            eprintln!("verglas-write: {error}");
            std::process::exit(1);
        }
    };
    let Some((config_path, ports_file)) = invocation else {
        println!("verglas-write {VERSION}");
        return;
    };
    let _ = rustls::crypto::ring::default_provider().install_default();
    let config = match WriteConfig::load(std::path::Path::new(&config_path)) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("verglas-write: {error}");
            std::process::exit(1);
        }
    };
    if let Err(error) =
        verglas_write_node::serve::run(&config, ports_file.map(std::path::PathBuf::from)).await
    {
        eprintln!("verglas-write: {error}");
        std::process::exit(1);
    }
}
