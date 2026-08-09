//! Mandatory access and database-control service for one Verglas tenant stack.
//!
//! The process owns no scheduler state. It persists authorization and database
//! declarations in `verglas_permissions`, evaluates relations through OpenFGA,
//! and reconciles the tenant's managed Lakekeeper and Neon resources.

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

mod database_runtime;
mod lakehouse_runtime;
mod postgres_runtime;

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
    /// Private tenant Lakekeeper management endpoint.
    #[arg(long, env = "VERGLAS_MANAGED_CATALOG_URI")]
    managed_catalog_uri: String,
    /// Managed object-store bucket used by Lakekeeper warehouses.
    #[arg(long, env = "VERGLAS_MANAGED_STORAGE_BUCKET")]
    managed_storage_bucket: String,
    /// Tenant prefix placed before each managed database warehouse.
    #[arg(long, env = "VERGLAS_MANAGED_STORAGE_PREFIX")]
    managed_storage_prefix: String,
    /// Private S3-compatible endpoint used by Lakekeeper.
    #[arg(long, env = "VERGLAS_MANAGED_STORAGE_ENDPOINT")]
    managed_storage_endpoint: String,
    /// Managed object-store region or `auto` for R2.
    #[arg(long, env = "VERGLAS_MANAGED_STORAGE_REGION")]
    managed_storage_region: String,
    /// Managed object-store access key retained by the access service.
    #[arg(long, env = "VERGLAS_MANAGED_STORAGE_ACCESS_KEY_ID")]
    managed_storage_access_key_id: String,
    /// Managed object-store secret retained by the access service.
    #[arg(
        long,
        env = "VERGLAS_MANAGED_STORAGE_SECRET_ACCESS_KEY",
        hide_env_values = true
    )]
    managed_storage_secret_access_key: String,
    /// Authenticated local desired-container API used for managed Neon.
    #[arg(long, env = "VERGLAS_CONTAINER_RUNTIME_URL")]
    container_runtime_url: String,
    /// Bearer credential accepted by the local desired-container API.
    #[arg(long, env = "VERGLAS_CONTAINER_RUNTIME_TOKEN", hide_env_values = true)]
    container_runtime_token: String,
    /// Selected cache-ring safekeeper address used by managed Neon compute.
    #[arg(long, env = "VERGLAS_MANAGED_POSTGRES_SAFEKEEPERS")]
    managed_postgres_safekeepers: String,
    /// Hex-encoded 256-bit key deriving restart-stable managed Postgres credentials.
    #[arg(
        long,
        env = "VERGLAS_MANAGED_POSTGRES_CREDENTIAL_KEY",
        hide_env_values = true
    )]
    managed_postgres_credential_key: String,
    /// Cache-routed S3 endpoint used by managed Neon pageservers.
    #[arg(long, env = "VERGLAS_MANAGED_POSTGRES_STORAGE_ENDPOINT")]
    managed_postgres_storage_endpoint: String,
    /// Cache-routed durable bucket used by managed Neon pageservers.
    #[arg(long, env = "VERGLAS_MANAGED_POSTGRES_STORAGE_BUCKET")]
    managed_postgres_storage_bucket: String,
    /// Region signed by managed Neon pageservers through the cache endpoint.
    #[arg(long, env = "VERGLAS_MANAGED_POSTGRES_STORAGE_REGION")]
    managed_postgres_storage_region: String,
    /// Cache endpoint access key used by managed Neon pageservers.
    #[arg(long, env = "VERGLAS_MANAGED_POSTGRES_STORAGE_ACCESS_KEY_ID")]
    managed_postgres_storage_access_key_id: String,
    /// Cache endpoint secret used by managed Neon pageservers.
    #[arg(
        long,
        env = "VERGLAS_MANAGED_POSTGRES_STORAGE_SECRET_ACCESS_KEY",
        hide_env_values = true
    )]
    managed_postgres_storage_secret_access_key: String,
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
    let lakehouse = lakehouse_runtime::LakekeeperProvisioner::new(
        &args.managed_catalog_uri,
        &args.managed_storage_bucket,
        &args.managed_storage_prefix,
        &args.managed_storage_endpoint,
        &args.managed_storage_region,
        &args.managed_storage_access_key_id,
        &args.managed_storage_secret_access_key,
    )
    .map_err(|error| error.to_string())?;
    let postgres_credential_key =
        hex::decode(&args.managed_postgres_credential_key).map_err(|_| {
            "VERGLAS_MANAGED_POSTGRES_CREDENTIAL_KEY must be 64 hexadecimal characters".to_owned()
        })?;
    let postgres = postgres_runtime::ManagedPostgresProvisioner::new(
        postgres_runtime::ManagedPostgresConfig {
            runtime_endpoint: args.container_runtime_url.clone(),
            runtime_token: args.container_runtime_token.clone(),
            tenant_id: args.tenant_id.clone(),
            remote_endpoint: args.managed_postgres_storage_endpoint.clone(),
            remote_bucket: args.managed_postgres_storage_bucket.clone(),
            remote_region: args.managed_postgres_storage_region.clone(),
            remote_access_key_id: args.managed_postgres_storage_access_key_id.clone(),
            remote_secret_access_key: args.managed_postgres_storage_secret_access_key.clone(),
            safekeepers: args.managed_postgres_safekeepers.clone(),
            credential_key: postgres_credential_key,
        },
    )
    .map_err(|error| error.to_string())?;
    let database_service = Arc::new(database_runtime::ProvisioningDatabaseManager::new(
        database_service,
        lakehouse,
        postgres,
    ));
    let recovery_failures = database_service
        .recover(&args.tenant_id)
        .await
        .map_err(|error| error.to_string())?;
    for failure in recovery_failures {
        eprintln!("verglas-access: database runtime recovery failed: {failure}");
    }
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
