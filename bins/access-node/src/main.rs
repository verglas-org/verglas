//! Mandatory standalone authorization service for one Verglas tenant stack.
//!
//! The process owns no scheduler state. It persists canonical authorization
//! objects in `verglas_permissions`, evaluates relations through OpenFGA, and
//! fails startup when either dependency is unavailable.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::extract::{Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::{Next, from_fn_with_state};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use clap::Parser;
use verglas_authz::{
    AccessService, Action, AeadSecretCipher, Authorizer, AuthzError, Grant, Principal,
    PrincipalKind, ResolveSecret, Resource, ResourceKind, SecretError, SecretKind, SecretService,
};
use verglas_authz_openfga::{OpenFgaPolicyEngine, bootstrap};
use verglas_authz_postgres::PostgresAuthorizationRepository;
use verglas_database::{
    DatabaseService, PostgresDatabaseRepository, ScopedSecretKind, ScopedSecretResolver,
    SecretResolutionError,
};

/// Database binding resolver backed by the authorization-owned secret service.
struct AccessSecretResolver {
    secrets: Arc<SecretService>,
    principal_id: Arc<str>,
}

#[async_trait::async_trait]
impl ScopedSecretResolver for AccessSecretResolver {
    /// Returns only the stable authorized resource ID; plaintext is discarded.
    async fn resolve_secret_id(
        &self,
        tenant_id: &str,
        kind: ScopedSecretKind,
        scope: &str,
    ) -> Result<String, SecretResolutionError> {
        let secret_kind = match kind {
            ScopedSecretKind::S3 => SecretKind::S3,
            ScopedSecretKind::IcebergRest => SecretKind::IcebergRest,
        };
        self.secrets
            .resolve(ResolveSecret::new(
                tenant_id,
                self.principal_id.as_ref(),
                secret_kind,
                scope,
            ))
            .await
            .map(|secret| secret.resource_id)
            .map_err(|error| map_secret_resolution_error(error, kind, scope))
    }
}

/// Preserves fail-closed secret resolution categories for database creation.
fn map_secret_resolution_error(
    error: SecretError,
    kind: ScopedSecretKind,
    scope: &str,
) -> SecretResolutionError {
    match error {
        SecretError::NotFound(_) => SecretResolutionError::NotFound {
            kind,
            scope: scope.to_owned(),
        },
        SecretError::Conflict(_) => SecretResolutionError::Ambiguous {
            kind,
            scope: scope.to_owned(),
        },
        SecretError::Forbidden(_) => SecretResolutionError::Unauthorized,
        SecretError::Invalid(message) | SecretError::Backend(message) => {
            SecretResolutionError::Backend(message)
        }
    }
}

/// Standalone service configuration supplied by the tenant deployment.
#[derive(Debug, Parser)]
#[command(name = "verglas-access", version)]
struct Args {
    /// Tenant whose authorization and secret domain this process serves.
    #[arg(long, env = "VERGLAS_TENANT_ID")]
    tenant_id: String,
    /// Existing service principal that owns secrets created through the public API.
    #[arg(long, env = "VERGLAS_ACCESS_SERVICE_PRINCIPAL")]
    service_principal: String,
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
    /// Hex-encoded 256-bit key used only to encrypt tenant secret values.
    #[arg(long, env = "VERGLAS_SECRET_ENCRYPTION_KEY", hide_env_values = true)]
    secret_encryption_key: String,
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
    if args.tenant_id.is_empty() {
        return Err("VERGLAS_TENANT_ID must not be empty".to_owned());
    }
    if args.service_principal.is_empty() {
        return Err("VERGLAS_ACCESS_SERVICE_PRINCIPAL must not be empty".to_owned());
    }
    if args.service_token.is_empty() {
        return Err("VERGLAS_ACCESS_SERVICE_TOKEN must not be empty".to_owned());
    }
    let repository = Arc::new(
        PostgresAuthorizationRepository::connect(&args.database_url)
            .await
            .map_err(|error| error.to_string())?,
    );
    let openfga = bootstrap(
        &args.openfga_endpoint,
        &args.openfga_store,
        &args.openfga_token,
    )
    .await
    .map_err(|error| error.to_string())?;
    let policy = OpenFgaPolicyEngine::new(openfga).map_err(|error| error.to_string())?;
    let authorizer: Arc<dyn Authorizer> =
        Arc::new(AccessService::new(repository.clone(), Arc::new(policy)));
    ensure_service_identity(
        authorizer.as_ref(),
        &args.tenant_id,
        &args.service_principal,
    )
    .await?;
    let encryption_key = hex::decode(&args.secret_encryption_key).map_err(|_| {
        "VERGLAS_SECRET_ENCRYPTION_KEY must be 64 hexadecimal characters".to_owned()
    })?;
    let cipher = AeadSecretCipher::new(&encryption_key).map_err(|error| error.to_string())?;
    let secrets = Arc::new(SecretService::new(
        authorizer.clone(),
        repository,
        Arc::new(cipher),
    ));
    let database_repository = PostgresDatabaseRepository::connect(&args.database_url)
        .await
        .map_err(|error| error.to_string())?;
    let database_service = Arc::new(DatabaseService::new(
        database_repository,
        AccessSecretResolver {
            secrets: secrets.clone(),
            principal_id: Arc::from(args.service_principal.clone()),
        },
    ));
    let token: Arc<str> = Arc::from(args.service_token);
    let protected = Router::new()
        .merge(verglas_rest::access::router_with_secrets(
            authorizer,
            secrets,
            args.tenant_id.clone(),
            args.service_principal,
        ))
        .merge(verglas_rest::database::router(
            database_service,
            args.tenant_id,
        ))
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

/// Idempotently bootstraps the tenant root and trusted local service principal.
async fn ensure_service_identity(
    authorizer: &dyn Authorizer,
    tenant_id: &str,
    principal_id: &str,
) -> Result<(), String> {
    let tenant = Resource::new(tenant_id, "tenant", ResourceKind::Tenant);
    match authorizer.create_resource(tenant).await {
        Ok(_) | Err(AuthzError::Conflict(_)) => {}
        Err(error) => return Err(format!("cannot bootstrap tenant resource: {error}")),
    }
    let principal = Principal::new(tenant_id, principal_id, PrincipalKind::ServiceAccount);
    match authorizer.create_principal(principal).await {
        Ok(_) | Err(AuthzError::Conflict(_)) => {}
        Err(error) => return Err(format!("cannot bootstrap service principal: {error}")),
    }
    let owner = Grant::new(
        "access-service-owner",
        tenant_id,
        principal_id,
        "tenant",
        BTreeSet::from([Action::Own]),
    );
    match authorizer.create_grant(owner).await {
        Ok(_) | Err(AuthzError::Conflict(_)) => Ok(()),
        Err(error) => Err(format!("cannot bootstrap service owner grant: {error}")),
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use verglas_authz::MemoryAuthorizer;

    #[tokio::test]
    async fn service_identity_bootstrap_is_idempotent() {
        let authorizer = MemoryAuthorizer::new();
        ensure_service_identity(&authorizer, "tenant-a", "local-service")
            .await
            .expect("first bootstrap");
        ensure_service_identity(&authorizer, "tenant-a", "local-service")
            .await
            .expect("second bootstrap");

        let principal = authorizer
            .get_principal("tenant-a", "local-service")
            .await
            .expect("principal");
        assert_eq!(principal.kind, PrincipalKind::ServiceAccount);
        let tenant = authorizer
            .get_resource("tenant-a", "tenant")
            .await
            .expect("tenant resource");
        assert_eq!(tenant.kind, ResourceKind::Tenant);
        assert_eq!(
            authorizer
                .list_grants("tenant-a")
                .await
                .expect("grants")
                .len(),
            1
        );
    }
}
