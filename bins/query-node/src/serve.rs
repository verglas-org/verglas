//! Startup: open the catalog, size and request the initial memory grant,
//! serve the admin router, and shut down cleanly on idle or on a signal.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::Mutex;
use verglas_core::grant::{MemoryGrant, MemoryGrantHost, MemoryGrantRequest};
use verglas_iceberg::{Connection, TimeTravel, catalog};

use crate::admin::{self, AppState};
use crate::config::QueryConfig;
use crate::sizing;

/// How long this worker serves with no request before it exits on its own —
/// "killed after use" without needing an external watchdog for the common
/// case. Not a config knob: an idle query worker costing nothing more than
/// this window is the whole point of launching one on demand.
const IDLE_SHUTDOWN: Duration = Duration::from_secs(300);

/// How often the idle-shutdown loop checks the clock.
const IDLE_CHECK_INTERVAL: Duration = Duration::from_secs(15);

/// Resolves the SigV4 keypair for a configured credentials file, or `None`
/// (anonymous) when unset.
fn resolve_keypair(
    credentials_file: &Option<String>,
    credentials_profile: &Option<String>,
) -> Result<Option<(String, String)>, String> {
    let Some(path) = credentials_file else {
        return Ok(None);
    };
    let profile = credentials_profile.as_deref().unwrap_or("default");
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("cannot read credentials file `{path}`: {e}"))?;
    let keypair = verglas_backend::read_aws_keypair(&text, profile).ok_or_else(|| {
        format!("credentials file `{path}` has no complete `[{profile}]` profile")
    })?;
    Ok(Some(keypair))
}

/// Builds the [`Connection`] this instance queries through: the configured
/// cache S3 endpoint (never a direct object-store path) and the configured
/// cache-owned Iceberg metadata endpoint. Public so the `estimate` CLI mode (`bin/main.rs`,
/// a separate crate from this library) can open the same connection without
/// starting a server.
pub fn connection_for(config: &QueryConfig) -> Result<Connection, String> {
    let bearer_token = match std::env::var(verglas_core::RUN_BEARER_TOKEN_ENV) {
        Ok(value) if value.is_empty() => {
            return Err(format!(
                "{} must not be empty when set",
                verglas_core::RUN_BEARER_TOKEN_ENV
            ));
        }
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => {
            return Err(format!(
                "{} is required for every query run",
                verglas_core::RUN_BEARER_TOKEN_ENV
            ));
        }
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err(format!(
                "{} must contain valid UTF-8",
                verglas_core::RUN_BEARER_TOKEN_ENV
            ));
        }
    };
    connection_for_bearer(config, bearer_token)
}

/// Builds a connection using the explicitly inherited run bearer.
fn connection_for_bearer(config: &QueryConfig, bearer_token: String) -> Result<Connection, String> {
    let (access_key_id, secret_access_key) = match resolve_keypair(
        &config.cache.credentials_file,
        &config.cache.credentials_profile,
    )? {
        Some((id, secret)) => (Some(id), Some(secret)),
        None => (None, None),
    };
    Ok(Connection {
        catalog_uri: config.metadata.uri.clone(),
        token: Some(bearer_token),
        warehouse: None,
        s3_endpoint: Some(config.cache.s3_endpoint.clone()),
        region: config
            .cache
            .region
            .clone()
            .unwrap_or_else(|| "us-east-1".to_owned()),
        access_key_id,
        secret_access_key,
    })
}

/// Runs the query role: opens the catalog, requests the initial grant (sized
/// from `for_query` when the launcher named the query it's starting this
/// worker for), serves the admin router, and returns once shut down.
pub async fn run(
    config: &QueryConfig,
    grant_host: Arc<dyn MemoryGrantHost>,
    for_query: Option<String>,
    ports_file: Option<std::path::PathBuf>,
) -> Result<(), String> {
    // Bind before catalog preparation so the parent can connect while this
    // ephemeral worker loads Iceberg metadata. The accepted request remains
    // queued by the listener until Axum starts serving below.
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", config.listen.admin_port))
        .await
        .map_err(|e| format!("cannot bind admin port {}: {e}", config.listen.admin_port))?;
    let bound_addr = listener
        .local_addr()
        .map_err(|e| format!("cannot read bound admin address: {e}"))?;
    tracing::info!(port = bound_addr.port(), "query worker listener bound");
    if let Some(path) = &ports_file {
        report_port(path, "admin", bound_addr);
    }

    let connection = connection_for(config)?;
    let generation_bearer = connection.token.clone();
    let catalog = catalog::open_catalog(&connection)
        .await
        .map_err(|e| format!("cannot open catalog: {e}"))?;

    let initial_grant = match &for_query {
        Some(sql) => sizing::request_for_query(grant_host.as_ref(), catalog.clone(), sql, None)
            .await
            .unwrap_or_else(|e| {
                tracing::warn!("initial grant estimate/request failed, starting at the floor: {e}");
                MemoryGrant {
                    bytes: sizing::MINIMUM_GRANT_BYTES,
                }
            }),
        None => grant_host
            .request(MemoryGrantRequest::new(
                sizing::MINIMUM_GRANT_BYTES,
                sizing::MINIMUM_GRANT_BYTES,
            ))
            .await
            .unwrap_or(MemoryGrant {
                bytes: sizing::MINIMUM_GRANT_BYTES,
            }),
    };
    tracing::info!(bytes = initial_grant.bytes, "initial memory grant");

    let last_activity = Arc::new(AtomicU64::new(now_secs()));
    let prepared_catalog = admin::PreparedQueryCatalog::open(
        catalog.clone(),
        Some(&config.metadata.uri),
        generation_bearer,
        config.memory.limit_bytes,
        config.memory.spill_path.clone(),
    )
    .await
    .map_err(|e| format!("cannot prepare catalog session: {e}"))?;
    let state = AppState {
        catalog,
        prepared_catalog,
        grant_host: grant_host.clone(),
        grant: Arc::new(Mutex::new(initial_grant)),
        last_activity: last_activity.clone(),
        estimate_on_request: config.memory.estimate_on_request,
    };

    let app = admin::router(state.clone());
    tracing::info!(port = bound_addr.port(), "serving");

    let idle_watch = tokio::spawn(idle_shutdown_watch(last_activity));

    let server = axum::serve(listener, app).with_graceful_shutdown(shutdown_signal());
    let result = server.await.map_err(|e| format!("server error: {e}"));

    idle_watch.abort();
    state.grant_host.release(*state.grant.lock().await).await;
    result
}

/// Resolves once SIGTERM or SIGINT/ctrl-c arrives — a clean shutdown either
/// way a container or an executor stops this worker.
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        let Ok(mut sigterm) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        else {
            std::future::pending::<()>().await;
            return;
        };
        sigterm.recv().await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => tracing::info!("received SIGINT, shutting down"),
        _ = terminate => tracing::info!("received SIGTERM, shutting down"),
    }
}

/// Exits the process once `last_activity` has been more than [`IDLE_SHUTDOWN`]
/// in the past — "killed after use" without an external watchdog. This calls
/// `std::process::exit` directly rather than signaling the axum server to
/// stop: an idle worker with an open listener and no in-flight requests has
/// nothing left to drain.
async fn idle_shutdown_watch(last_activity: Arc<AtomicU64>) {
    let mut interval = tokio::time::interval(IDLE_CHECK_INTERVAL);
    loop {
        interval.tick().await;
        let idle_for = now_secs().saturating_sub(last_activity.load(Ordering::Relaxed));
        if idle_for >= IDLE_SHUTDOWN.as_secs() {
            tracing::info!(idle_seconds = idle_for, "idle timeout, exiting");
            std::process::exit(0);
        }
    }
}

/// Appends `<role> <ip:port>` to the ports file a parent (`bins/cache-node`'s
/// query-worker dispatcher, or a test) named with `--ports-file`, matching the
/// exact convention `verglas-cache-node --ports-file` uses (issue #194) so a poller
/// written against one works against both. Best-effort: a write failure is
/// logged and swallowed — it never fails startup, since serving does not
/// depend on the parent learning the port.
fn report_port(path: &std::path::Path, role: &str, addr: std::net::SocketAddr) {
    use std::io::Write as _;
    let line = format!("{role} {addr}\n");
    match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
    {
        Ok(mut file) => {
            if let Err(e) = file.write_all(line.as_bytes()) {
                tracing::warn!("could not write {role} port to ports file: {e}");
            }
        }
        Err(e) => tracing::warn!("could not open ports file `{}`: {e}", path.display()),
    }
}

/// Current unix time in whole seconds, `0` if the clock is somehow before the
/// epoch.
fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Parses a `--sql`/`--at-table`/`--at-reference` estimate request from
/// argv-style flags into a [`TimeTravel`], for the `estimate` CLI mode
/// (`crate::main`), which answers an estimate without starting the server.
pub fn time_travel_from_flags(
    table: Option<String>,
    reference: Option<String>,
) -> Option<TimeTravel> {
    match (table, reference) {
        (Some(table), Some(reference)) => Some(TimeTravel { table, reference }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    use axum::Json;
    use axum::extract::State;
    use axum::http::{HeaderMap, StatusCode, Uri, header};
    use axum::routing::get;
    use serde_json::json;

    /// Returns a minimal token-free role configuration for connection tests.
    fn config(metadata_uri: &str) -> QueryConfig {
        QueryConfig::from_toml_str(&format!(
            "[cache]\ns3_endpoint = \"http://127.0.0.1:8333\"\n\n\
             [metadata]\nuri = \"{metadata_uri}\"\n"
        ))
        .expect("query config")
    }

    /// The inherited run bearer becomes catalog client authority without entering role config.
    #[test]
    fn scoped_bearer_is_applied_only_to_the_connection() {
        let config = config("http://127.0.0.1:8334/v1/databases/analytics/catalog");
        let serialized = toml::to_string(&config).expect("serialize config");
        let token = "scoped-token-that-must-not-persist";

        let connection =
            connection_for_bearer(&config, token.to_owned()).expect("scoped connection");

        assert_eq!(connection.token.as_deref(), Some(token));
        assert!(!serialized.contains(token));
        assert!(!config.summary().contains(token));
    }

    /// A real REST-catalog bootstrap carries the inherited bearer to the protected gateway.
    #[tokio::test]
    async fn scoped_bearer_authorizes_catalog_bootstrap() {
        let authorized = Arc::new(AtomicBool::new(false));
        let authorized_handler =
            |State(authorized): State<Arc<AtomicBool>>, headers: HeaderMap, uri: Uri| async move {
                if headers
                    .get(header::AUTHORIZATION)
                    .and_then(|value| value.to_str().ok())
                    != Some("Bearer scoped-query-token")
                {
                    return (StatusCode::UNAUTHORIZED, Json(json!({})));
                }
                authorized.store(true, Ordering::SeqCst);
                let body = if uri.path().ends_with("/config") {
                    json!({"defaults": {}, "overrides": {}})
                } else {
                    json!({"namespaces": []})
                };
                (StatusCode::OK, Json(body))
            };
        let app = axum::Router::new()
            .route(
                "/v1/databases/analytics/catalog/v1/config",
                get(authorized_handler),
            )
            .route(
                "/v1/databases/analytics/catalog/v1/namespaces",
                get(authorized_handler),
            )
            .with_state(authorized.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind catalog fixture");
        let address = listener.local_addr().expect("catalog address");
        tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve catalog fixture");
        });
        let config = config(&format!("http://{address}/v1/databases/analytics/catalog"));
        let connection = connection_for_bearer(&config, "scoped-query-token".to_owned())
            .expect("scoped connection");

        let catalog = verglas_iceberg::catalog::open_catalog(&connection)
            .await
            .expect("authorized catalog bootstrap");
        catalog
            .list_namespaces(None)
            .await
            .expect("authorized namespace discovery");

        assert!(authorized.load(Ordering::SeqCst));
    }
}
