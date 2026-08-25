//! Resident Durable Object Worker process backed by one embedded Turso database.
//!
//! Startup is fail-closed: the process requires only a local data root and DO
//! identity before it opens the event socket. Component bytes remain
//! digest-verified and optional compiled-cache failures remain fatal.

use std::error::Error;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use verglas_do_turso::TursoStore;
use verglas_do_wasm::{
    ArtifactStore, ComponentDigest, CwasmCache, DirArtifactStore, WorkerRuntime,
};
use verglas_runtime::{CatalogHostConfig, EventEndpoint};

/// Command-line configuration for one resident Durable Object process.
struct Config {
    /// Durable Object identity used by Turso and outbox keys.
    do_id: String,
    /// DO data root containing the local Turso database and sidecars.
    data_dir: PathBuf,
    /// Optional source component digest.
    component_digest: Option<ComponentDigest>,
    /// Directory containing the digest-named component bytes.
    component_dir: Option<PathBuf>,
    /// Optional Wasmtime compiled component cache directory.
    cwasm_cache_dir: Option<PathBuf>,
    /// Private NDJSON event socket path.
    event_socket: Option<PathBuf>,
    /// Optional strict operator configuration for the Catalog host capability.
    catalog_host_config: Option<PathBuf>,
}

impl Config {
    /// Parses only the Turso-backed runtime argument surface.
    fn parse() -> Result<Self, String> {
        let mut arguments = std::env::args().skip(1);
        let mut do_id = None;
        let mut data_dir = None;
        let mut component_digest = None;
        let mut component_dir = None;
        let mut cwasm_cache_dir = None;
        let mut event_socket = None;
        let mut catalog_host_config = None;
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--do-id" => do_id = Some(next_value(&mut arguments, "--do-id")?),
                "--data-dir" => {
                    data_dir = Some(PathBuf::from(next_value(&mut arguments, "--data-dir")?));
                }
                "--component-digest" => {
                    let value = next_value(&mut arguments, "--component-digest")?;
                    component_digest =
                        Some(ComponentDigest::from_str(&value).map_err(|error| error.to_string())?);
                }
                "--component-dir" => {
                    component_dir = Some(PathBuf::from(next_value(
                        &mut arguments,
                        "--component-dir",
                    )?));
                }
                "--cwasm-cache-dir" => {
                    cwasm_cache_dir = Some(PathBuf::from(next_value(
                        &mut arguments,
                        "--cwasm-cache-dir",
                    )?));
                }
                "--event-socket" => {
                    event_socket =
                        Some(PathBuf::from(next_value(&mut arguments, "--event-socket")?));
                }
                "--catalog-host-config" => {
                    catalog_host_config = Some(PathBuf::from(next_value(
                        &mut arguments,
                        "--catalog-host-config",
                    )?));
                }
                unknown => return Err(format!("unknown argument `{unknown}`")),
            }
        }
        let do_id = required_text(do_id, "--do-id")?;
        let data_dir = data_dir.ok_or_else(|| "missing --data-dir".to_owned())?;
        if component_digest.is_some() != component_dir.is_some() {
            return Err(
                "--component-digest and --component-dir must be supplied together".to_owned(),
            );
        }
        if cwasm_cache_dir.is_some() && component_digest.is_none() {
            return Err("--cwasm-cache-dir requires a verified component".to_owned());
        }
        if event_socket.is_some() && component_digest.is_none() {
            return Err("--event-socket requires a verified component".to_owned());
        }
        if catalog_host_config.is_some() && event_socket.is_none() {
            return Err(
                "--catalog-host-config requires a verified component event socket".to_owned(),
            );
        }
        Ok(Self {
            do_id,
            data_dir,
            component_digest,
            component_dir,
            cwasm_cache_dir,
            event_socket,
            catalog_host_config,
        })
    }
}

/// Runs one resident Turso-backed Durable Object process until shutdown.
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::parse().map_err(|error| format!("{error}\n{}", usage()))?;
    let catalog_commit = match config.catalog_host_config.as_ref() {
        Some(path) => Some(
            CatalogHostConfig::load(path)?
                .build_catalog_commit_service()
                .await?,
        ),
        None => None,
    };
    let runtime = load_runtime(&config).await?.map(Arc::new);
    let store =
        Arc::new(TursoStore::open(config.data_dir.join("turso.db"), config.do_id.clone()).await?);
    match config.event_socket {
        Some(event_socket) => {
            let runtime = runtime.ok_or("--event-socket requires a verified component")?;
            let mut endpoint = match catalog_commit {
                Some(service) => {
                    EventEndpoint::bind_with_catalog_commit_service(
                        event_socket,
                        Arc::clone(&store),
                        runtime,
                        service,
                    )
                    .await?
                }
                None => EventEndpoint::bind(event_socket, Arc::clone(&store), runtime).await?,
            };
            tokio::select! {
                result = endpoint.run() => result?,
                signal = tokio::signal::ctrl_c() => signal?,
            }
            drop(endpoint);
            store.shutdown_fence().await?;
        }
        None => {
            tokio::signal::ctrl_c().await?;
            store.shutdown_fence().await?;
        }
    }
    Ok(())
}

/// Loads and verifies the optional component and compiled cache.
async fn load_runtime(config: &Config) -> Result<Option<WorkerRuntime>, Box<dyn Error>> {
    let (Some(digest), Some(component_dir)) =
        (config.component_digest, config.component_dir.as_ref())
    else {
        return Ok(None);
    };
    let bytes = DirArtifactStore::new(component_dir).fetch(digest).await?;
    let engine_config = wasmtime::Config::new();
    let runtime = match config.cwasm_cache_dir.as_ref() {
        Some(cache_dir) => {
            let cache = CwasmCache::new(cache_dir);
            WorkerRuntime::load_with_cache(engine_config, Some((&cache, digest)), &bytes)?
        }
        None => WorkerRuntime::load(engine_config, &bytes)?,
    };
    Ok(Some(runtime))
}

/// Returns the next value for one named command-line option.
fn next_value(
    arguments: &mut impl Iterator<Item = String>,
    option: &str,
) -> Result<String, String> {
    arguments
        .next()
        .ok_or_else(|| format!("missing value for {option}"))
}

/// Requires a nonempty text option value.
fn required_text(value: Option<String>, option: &str) -> Result<String, String> {
    let value = value.ok_or_else(|| format!("missing {option}"))?;
    if value.is_empty() {
        return Err(format!("{option} cannot be empty"));
    }
    Ok(value)
}

/// Describes the accepted runtime argument surface.
fn usage() -> &'static str {
    "usage: verglas-runtime --do-id ID --data-dir PATH [--component-digest HEX --component-dir PATH [--cwasm-cache-dir PATH] --event-socket PATH] [--catalog-host-config PATH]"
}
