//! Mandatory standalone authorization service for one Verglas tenant stack.
//!
//! The process owns no scheduler state. It persists canonical authorization
//! objects in `verglas_permissions`, evaluates relations through OpenFGA, and
//! fails startup when either dependency is unavailable.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::extract::{Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::{Next, from_fn_with_state};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use clap::Parser;
use verglas_authz::{AccessService, Authorizer};
use verglas_authz_openfga::{OpenFgaPolicyEngine, bootstrap};
use verglas_authz_postgres::PostgresAuthorizationRepository;

/// Standalone service configuration supplied by the tenant deployment.
#[derive(Debug, Parser)]
#[command(name = "verglas-access", version)]
struct Args {
    /// Dedicated `verglas_permissions` logical database.
    #[arg(long, env = "VERGLAS_ACCESS_DATABASE_URL")]
    database_url: String,
    /// Tenant-local OpenFGA HTTP endpoint.
    #[arg(long, env = "VERGLAS_OPENFGA_ENDPOINT")]
    openfga_endpoint: String,
    /// Dedicated OpenFGA store name discovered or created at startup.
    #[arg(long, env = "VERGLAS_OPENFGA_STORE", default_value = "verglas")]
    openfga_store: String,
    /// Pre-shared token used only between access and OpenFGA.
    #[arg(long, env = "VERGLAS_OPENFGA_TOKEN")]
    openfga_token: String,
    /// Pre-shared token accepted from trusted tenant services.
    #[arg(long, env = "VERGLAS_ACCESS_SERVICE_TOKEN")]
    service_token: String,
    /// Address serving health and authorization routes.
    #[arg(long, env = "VERGLAS_ACCESS_LISTEN", default_value = "0.0.0.0:8345")]
    listen: SocketAddr,
}

/// Starts the fail-closed authorization service.
#[tokio::main]
async fn main() {
    if let Err(error) = run(Args::parse()).await {
        eprintln!("verglas-access: {error}");
        std::process::exit(1);
    }
}

/// Resolves mandatory dependencies and serves until shutdown.
async fn run(args: Args) -> Result<(), String> {
    if args.service_token.is_empty() {
        return Err("VERGLAS_ACCESS_SERVICE_TOKEN must not be empty".to_owned());
    }
    let repository = PostgresAuthorizationRepository::connect(&args.database_url)
        .await
        .map_err(|error| error.to_string())?;
    let openfga = bootstrap(
        &args.openfga_endpoint,
        &args.openfga_store,
        &args.openfga_token,
    )
    .await
    .map_err(|error| error.to_string())?;
    let policy = OpenFgaPolicyEngine::new(openfga).map_err(|error| error.to_string())?;
    let authorizer: Arc<dyn Authorizer> =
        Arc::new(AccessService::new(Arc::new(repository), Arc::new(policy)));
    let token: Arc<str> = Arc::from(args.service_token);
    let protected = verglas_rest::access::router(authorizer)
        .layer(from_fn_with_state(token, require_service_token));
    let app = Router::new()
        .route("/healthz", get(health))
        .merge(protected);
    let listener = tokio::net::TcpListener::bind(args.listen)
        .await
        .map_err(|error| error.to_string())?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown())
        .await
        .map_err(|error| error.to_string())
}

/// Reports liveness after both durable dependencies passed startup.
async fn health() -> StatusCode {
    StatusCode::NO_CONTENT
}

/// Rejects every access API request without the configured service credential.
async fn require_service_token(
    State(token): State<Arc<str>>,
    request: Request,
    next: Next,
) -> Response {
    let expected = format!("Bearer {token}");
    let authorized = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value == expected);
    if authorized {
        next.run(request).await
    } else {
        StatusCode::UNAUTHORIZED.into_response()
    }
}

/// Waits for an ordinary process termination signal.
async fn shutdown() {
    let interrupt = async {
        if tokio::signal::ctrl_c().await.is_err() {
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = interrupt => {},
        () = terminate => {},
    }
}
