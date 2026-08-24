//! AC1-only resident Worker helper with an explicit local Turso test seam.
//!
//! This binary is available only with the `ac1-test-support` Cargo feature. It
//! retains the production `WorkerRuntime` and `EventEndpoint` process chain, but
//! calls `TursoStore::open_for_test` instead of contacting a Turso service. The
//! feature is not enabled by any production gateway target.

use std::error::Error;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use verglas_do_turso::TursoStore;
use verglas_do_wasm::{
    ArtifactStore, ComponentDigest, CwasmCache, DirArtifactStore, WorkerRuntime,
};
use verglas_runtime::EventEndpoint;

/// Explicit arguments needed by the AC1 test helper.
struct Config {
    do_id: String,
    data_dir: PathBuf,
    component_digest: ComponentDigest,
    component_dir: PathBuf,
    cwasm_cache_dir: Option<PathBuf>,
    event_socket: PathBuf,
}

impl Config {
    /// Parses the production launch shape while discarding only network credentials.
    fn parse() -> Result<Self, String> {
        let mut args = std::env::args().skip(1);
        let mut do_id = None;
        let mut data_dir = None;
        let mut digest = None;
        let mut component_dir = None;
        let mut cache_dir = None;
        let mut event_socket = None;
        let mut turso_url = None;
        let mut token_file = None;
        while let Some(argument) = args.next() {
            let value = |args: &mut dyn Iterator<Item = String>, option: &str| {
                args.next()
                    .ok_or_else(|| format!("missing value for {option}"))
            };
            match argument.as_str() {
                "--do-id" => do_id = Some(value(&mut args, "--do-id")?),
                "--data-dir" => data_dir = Some(PathBuf::from(value(&mut args, "--data-dir")?)),
                "--turso-url" => turso_url = Some(value(&mut args, "--turso-url")?),
                "--turso-token-file" => {
                    token_file = Some(PathBuf::from(value(&mut args, "--turso-token-file")?))
                }
                "--component-digest" => {
                    let value = value(&mut args, "--component-digest")?;
                    digest =
                        Some(ComponentDigest::from_str(&value).map_err(|error| error.to_string())?);
                }
                "--component-dir" => {
                    component_dir = Some(PathBuf::from(value(&mut args, "--component-dir")?))
                }
                "--cwasm-cache-dir" => {
                    cache_dir = Some(PathBuf::from(value(&mut args, "--cwasm-cache-dir")?))
                }
                "--event-socket" => {
                    event_socket = Some(PathBuf::from(value(&mut args, "--event-socket")?))
                }
                other => return Err(format!("unknown argument `{other}`")),
            }
        }
        if turso_url.is_none() || token_file.is_none() {
            return Err(
                "Turso URL and token-file arguments are required even in AC1 seam".to_owned(),
            );
        }
        Ok(Self {
            do_id: required_text(do_id, "--do-id")?,
            data_dir: data_dir.ok_or_else(|| "missing --data-dir".to_owned())?,
            component_digest: digest.ok_or_else(|| "missing --component-digest".to_owned())?,
            component_dir: component_dir.ok_or_else(|| "missing --component-dir".to_owned())?,
            cwasm_cache_dir: cache_dir,
            event_socket: event_socket.ok_or_else(|| "missing --event-socket".to_owned())?,
        })
    }
}

/// Returns one nonempty required text argument.
fn required_text(value: Option<String>, option: &str) -> Result<String, String> {
    let value = value.ok_or_else(|| format!("missing {option}"))?;
    if value.is_empty() {
        return Err(format!("{option} cannot be empty"));
    }
    Ok(value)
}

/// Runs the real event endpoint over a local-only Turso test store.
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::parse().map_err(|error| error.to_string())?;
    let bytes = DirArtifactStore::new(&config.component_dir)
        .fetch(config.component_digest)
        .await?;
    let runtime = match config.cwasm_cache_dir.as_ref() {
        Some(cache_dir) => {
            let cache = CwasmCache::new(cache_dir);
            WorkerRuntime::load_with_cache(
                wasmtime::Config::new(),
                Some((&cache, config.component_digest)),
                &bytes,
            )?
        }
        None => WorkerRuntime::load(wasmtime::Config::new(), &bytes)?,
    };
    let store =
        Arc::new(TursoStore::open_for_test(config.data_dir.join("turso.db"), config.do_id).await?);
    let mut endpoint = EventEndpoint::bind(config.event_socket, store, Arc::new(runtime)).await?;
    tokio::select! {
        result = endpoint.run() => result?,
        signal = tokio::signal::ctrl_c() => signal?,
    }
    Ok(())
}
