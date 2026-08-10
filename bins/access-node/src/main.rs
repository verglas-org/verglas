//! Mandatory access and database-control service for one Verglas tenant stack.
//!
//! The process owns no scheduler state. It persists authorization and database
//! declarations in `verglas_permissions`, evaluates relations through OpenFGA,
//! and reconciles the tenant's managed Lakekeeper and Neon resources.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::http::StatusCode;
use axum::routing::get;
use clap::Parser;
use tokio::io::AsyncWriteExt;
use verglas_authz::{
    AccessService, AccessTokenService, AccessTokenSigner, Action, AeadSecretCipher, Authorizer,
    AuthzError, Grant, Principal, PrincipalKind, ResolveSecret, Resource, ResourceKind,
    SecretError, SecretKind, SecretService, TargetJwtSigner, TokenMintRequest, new_access_token_id,
};
use verglas_authz_openfga::{OpenFgaPolicyEngine, bootstrap};
use verglas_authz_postgres::PostgresAuthorizationRepository;
use verglas_database::{
    DatabaseManager, DatabaseService, PostgresDatabaseRepository, ScopedSecretKind,
    ScopedSecretResolver, SecretResolutionError,
};
use verglas_queue::{PostgresQueueRepository, QueueService};
use verglas_rest::database::DatabaseAuthorization;

mod data_plane_proxy;
mod database_runtime;
mod lakehouse_runtime;
mod postgres_runtime;
mod queue_runtime;

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
    #[arg(
        long,
        env = "VERGLAS_ACCESS_SERVICE_PRINCIPAL",
        default_value = "service/access"
    )]
    service_principal: String,
    /// Email address that receives the tenant's initial owner grant.
    #[arg(long, env = "VERGLAS_INITIAL_OWNER_EMAIL")]
    initial_owner_email: String,
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
    /// Base64-encoded 256-bit key used to sign revocable access tokens.
    #[arg(long, env = "VERGLAS_TOKEN_SIGNING_KEY", hide_env_values = true)]
    token_signing_key: String,
    /// Hex-encoded 256-bit key used to verify OS identity assertions.
    #[arg(long, env = "VERGLAS_IDENTITY_ASSERTION_KEY", hide_env_values = true)]
    identity_assertion_key: String,
    /// Base64-encoded Ed25519 seed used for short-lived target database JWTs.
    #[arg(long, env = "VERGLAS_TARGET_JWT_SIGNING_KEY", hide_env_values = true)]
    target_jwt_signing_key: String,
    /// Isolated directory receiving the Verglas server credential.
    #[arg(
        long,
        env = "VERGLAS_SERVER_TOKEN_DIRECTORY",
        default_value = "/var/run/verglas/server"
    )]
    server_token_directory: PathBuf,
    /// Isolated directory receiving the access service's Lakekeeper management credential.
    #[arg(
        long,
        env = "VERGLAS_ACCESS_TOKEN_DIRECTORY",
        default_value = "/var/run/verglas/access"
    )]
    access_token_directory: PathBuf,
    /// Isolated directory receiving the Lakekeeper policy credential.
    #[arg(
        long,
        env = "VERGLAS_LAKEKEEPER_TOKEN_DIRECTORY",
        default_value = "/var/run/verglas/lakekeeper"
    )]
    lakekeeper_token_directory: PathBuf,
    /// Isolated directory receiving the Neon policy credential.
    #[arg(
        long,
        env = "VERGLAS_NEON_TOKEN_DIRECTORY",
        default_value = "/var/run/verglas/neon"
    )]
    neon_token_directory: PathBuf,
    /// Private access-service origin reachable by managed database proxies.
    #[arg(
        long,
        env = "VERGLAS_ACCESS_INTERNAL_ENDPOINT",
        default_value = "http://verglas-access:8345"
    )]
    access_internal_endpoint: String,
    /// PEM certificate presented by managed Postgres proxy listeners.
    #[arg(long, env = "VERGLAS_MANAGED_POSTGRES_TLS_CERTIFICATE_FILE")]
    managed_postgres_tls_certificate_file: PathBuf,
    /// PEM private key paired with the managed Postgres proxy certificate.
    #[arg(long, env = "VERGLAS_MANAGED_POSTGRES_TLS_PRIVATE_KEY_FILE")]
    managed_postgres_tls_private_key_file: PathBuf,
    /// Hex-encoded 256-bit key used only to encrypt tenant secret values.
    #[arg(long, env = "VERGLAS_SECRET_ENCRYPTION_KEY", hide_env_values = true)]
    secret_encryption_key: String,
    /// Private tenant Lakekeeper management endpoint.
    #[arg(long, env = "VERGLAS_MANAGED_CATALOG_URI")]
    managed_catalog_uri: String,
    /// Private cache admin origin serving database-scoped catalog and query routes.
    #[arg(long, env = "VERGLAS_ADMIN_URL")]
    admin_url: String,
    /// Managed Lakehouse declaration that must exist before readiness.
    #[arg(long, env = "VERGLAS_DEFAULT_LAKEHOUSE")]
    default_lakehouse: Option<String>,
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
    let repository = Arc::new(
        PostgresAuthorizationRepository::connect(&args.database_url)
            .await
            .map_err(|error| error.to_string())?,
    );
    let openfga = bootstrap_openfga(
        &args.openfga_endpoint,
        &args.openfga_store,
        &args.openfga_token,
    )
    .await?;
    let policy = OpenFgaPolicyEngine::new(openfga).map_err(|error| error.to_string())?;
    let authorizer: Arc<dyn Authorizer> =
        Arc::new(AccessService::new(repository.clone(), Arc::new(policy)));
    ensure_tenant_identities(
        authorizer.as_ref(),
        &args.tenant_id,
        &args.service_principal,
        &args.initial_owner_email,
    )
    .await?;
    let token_signer = AccessTokenSigner::from_base64(&args.token_signing_key)
        .map_err(|error| error.to_string())?;
    let identity_assertion_key = decode_256_bit_hex(
        "VERGLAS_IDENTITY_ASSERTION_KEY",
        &args.identity_assertion_key,
    )?;
    let tokens = Arc::new(AccessTokenService::new(token_signer, repository.clone()));
    let target_jwt_signer = TargetJwtSigner::from_base64_derived(&args.target_jwt_signing_key)
        .map_err(|error| error.to_string())?;
    let credential_directories = InternalCredentialDirectories {
        access: args.access_token_directory,
        server: args.server_token_directory,
        lakekeeper: args.lakekeeper_token_directory,
        neon: args.neon_token_directory,
    };
    provision_internal_credentials(
        authorizer.clone(),
        tokens.clone(),
        &args.tenant_id,
        &args.service_principal,
        &credential_directories,
    )
    .await?;
    let neon_policy_token_file = credential_directories.neon.join("verglas-neon.token");
    let postgres_credential_directory = credential_directories.neon.join("postgres");
    spawn_internal_credential_rotation(
        authorizer.clone(),
        tokens.clone(),
        args.tenant_id.clone(),
        args.service_principal.clone(),
        credential_directories.clone(),
    );
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
        credential_directories
            .access
            .join("lakekeeper-management.token"),
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
            credential_key: postgres_credential_key.clone(),
            access_endpoint: args.access_internal_endpoint,
            policy_engine_token_file: neon_policy_token_file,
            tls_certificate_file: args.managed_postgres_tls_certificate_file,
            tls_private_key_file: args.managed_postgres_tls_private_key_file,
            credential_directory: postgres_credential_directory,
        },
    )
    .map_err(|error| error.to_string())?;
    let queue_repository = PostgresQueueRepository::connect(&args.database_url)
        .await
        .map_err(|error| error.to_string())?;
    let queue_provisioner = queue_runtime::ManagedQueueProvisioner::new(
        postgres.clone(),
        queue_runtime::QueueRuntimeConfig {
            runtime_endpoint: args.container_runtime_url.clone(),
            runtime_token: args.container_runtime_token.clone(),
            credential_key: postgres_credential_key,
        },
    )?;
    let queue_service = Arc::new(QueueService::new(
        queue_repository,
        queue_provisioner.clone(),
    ));
    let database_service = Arc::new(database_runtime::ProvisioningDatabaseManager::new(
        database_service,
        authorizer.clone(),
        args.tenant_id.clone(),
        lakehouse,
        postgres,
    ));
    let recovery = database_service.clone();
    let queue_recovery = queue_service.clone();
    let recovery_tenant = args.tenant_id.clone();
    let access_runtime =
        verglas_rest::access::AccessHttpRuntime::new(authorizer, tokens, args.tenant_id.clone())
            .with_identity_assertion_key(identity_assertion_key)
            .with_secrets(secrets)
            .with_target_jwt_signer(target_jwt_signer);
    let database_routes = verglas_rest::database::router(
        database_service,
        Arc::new(access_runtime.clone()),
        args.tenant_id.clone(),
    )
    .merge(data_plane_proxy::router(&args.admin_url)?);
    let protected_databases =
        verglas_rest::data_plane::protect(database_routes, access_runtime.clone());
    let queue_routes = verglas_rest::queue::router(
        queue_service.clone(),
        Arc::new(access_runtime.clone()),
        args.tenant_id.clone(),
    )
    .merge(verglas_rest::queue::data_router(
        queue_service,
        Arc::new(queue_provisioner),
        args.tenant_id.clone(),
    ));
    let protected_queues = verglas_rest::data_plane::protect(queue_routes, access_runtime.clone());
    let ready = Arc::new(AtomicBool::new(false));
    let health_ready = ready.clone();
    let app = Router::new()
        .route("/healthz", get(move || health(health_ready.clone())))
        .merge(verglas_rest::access::router(access_runtime.clone()))
        .merge(protected_databases)
        .merge(protected_queues);
    let listener = tokio::net::TcpListener::bind(args.listen)
        .await
        .map_err(|error| error.to_string())?;
    let server = tokio::spawn(
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown())
            .into_future(),
    );

    // Lakekeeper's Verglas authorizer calls this access listener while managed
    // warehouses are reconciled. Bind first, but keep health closed until every
    // durable database runtime has converged; no failed runtime is hidden behind
    // a background fallback loop.
    let recovery_failures = recovery
        .recover(&recovery_tenant)
        .await
        .map_err(|error| error.to_string())?;
    if !recovery_failures.is_empty() {
        server.abort();
        return Err(format!(
            "database runtime recovery failed: {}",
            recovery_failures.join("; ")
        ));
    }
    let queue_recovery_failures = queue_recovery
        .recover(&recovery_tenant)
        .await
        .map_err(|error| error.to_string())?;
    if !queue_recovery_failures.is_empty() {
        server.abort();
        return Err(format!(
            "queue runtime recovery failed: {}",
            queue_recovery_failures.join("; ")
        ));
    }
    if let Some(default_lakehouse) = args.default_lakehouse.as_deref() {
        let (database, created) = recovery
            .ensure_default_lakehouse(&recovery_tenant, default_lakehouse)
            .await
            .map_err(|error| format!("default managed Lakehouse failed: {error}"))?;
        let owner = verglas_rest::data_plane::AuthenticatedPrincipal {
            tenant_id: recovery_tenant.clone(),
            principal_id: initial_owner_principal_id(&args.initial_owner_email)?,
            token_id: "bootstrap/default-lakehouse".to_owned(),
            audience: verglas_rest::access::DATA_PLANE_AUDIENCE.to_owned(),
        };
        if let Err(error) = access_runtime
            .create_database_resource(
                &owner,
                database.id(),
                database.name(),
                verglas_database::DatabaseKind::Lakehouse,
            )
            .await
        {
            if created {
                let rollback = recovery
                    .delete_database(&recovery_tenant, default_lakehouse)
                    .await;
                let authorization_rollback = access_runtime
                    .delete_database_resource(&owner, database.id())
                    .await;
                server.abort();
                return Err(format!(
                    "default managed Lakehouse authorization failed: {error}; declaration rollback: {rollback:?}; authorization rollback: {authorization_rollback:?}"
                ));
            }
            server.abort();
            return Err(format!(
                "default managed Lakehouse authorization failed: {error}"
            ));
        }
    }
    ready.store(true, Ordering::Release);
    server
        .await
        .map_err(|error| format!("access server task failed: {error}"))?
        .map_err(|error| error.to_string())
}

/// Bootstraps the tenant-local OpenFGA store across a cold Postgres start.
///
/// OpenFGA bounds each datastore operation independently, so its first request
/// can return a backend deadline while Neon is still warming connections. A
/// retry always begins with the idempotent store/model discovery sequence;
/// therefore a timed-out create that committed is discovered instead of
/// duplicated. Contract/configuration failures remain immediate and fail
/// closed.
async fn bootstrap_openfga(
    endpoint: &str,
    store: &str,
    token: &str,
) -> Result<verglas_authz_openfga::OpenFgaConfig, String> {
    const MAX_ATTEMPTS: u32 = 8;
    let mut delay = Duration::from_millis(100);
    for attempt in 1..=MAX_ATTEMPTS {
        match bootstrap(endpoint, store, token).await {
            Ok(config) => return Ok(config),
            Err(AuthzError::Backend(error)) if attempt < MAX_ATTEMPTS => {
                eprintln!(
                    "verglas-access: OpenFGA bootstrap attempt {attempt}/{MAX_ATTEMPTS} failed: {error}; retrying in {}ms",
                    delay.as_millis()
                );
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(2));
            }
            Err(error) => return Err(error.to_string()),
        }
    }
    unreachable!("bounded OpenFGA bootstrap loop always returns")
}

/// Idempotently bootstraps the tenant root and trusted local service principal.
async fn ensure_tenant_identities(
    authorizer: &dyn Authorizer,
    tenant_id: &str,
    service_principal_id: &str,
    initial_owner_email: &str,
) -> Result<(), String> {
    let tenant = Resource::new(tenant_id, "tenant", ResourceKind::Tenant);
    match authorizer.create_resource(tenant).await {
        Ok(_) | Err(AuthzError::Conflict(_)) => {}
        Err(error) => return Err(format!("cannot bootstrap tenant resource: {error}")),
    }
    let lakekeeper =
        Resource::new(tenant_id, "lakekeeper", ResourceKind::Project).with_parent("tenant");
    match authorizer.create_resource(lakekeeper).await {
        Ok(_) | Err(AuthzError::Conflict(_)) => {}
        Err(error) => return Err(format!("cannot bootstrap Lakekeeper resource: {error}")),
    }
    let principal = Principal::new(
        tenant_id,
        service_principal_id,
        PrincipalKind::ServiceAccount,
    );
    match authorizer.create_principal(principal).await {
        Ok(_) | Err(AuthzError::Conflict(_)) => {}
        Err(error) => return Err(format!("cannot bootstrap service principal: {error}")),
    }
    let owner_principal_id = initial_owner_principal_id(initial_owner_email)?;
    let owner_principal = Principal::new(tenant_id, &owner_principal_id, PrincipalKind::User);
    match authorizer.create_principal(owner_principal).await {
        Ok(_) | Err(AuthzError::Conflict(_)) => {}
        Err(error) => return Err(format!("cannot bootstrap initial owner principal: {error}")),
    }
    let owner = Grant::new(
        "initial-owner",
        tenant_id,
        owner_principal_id,
        "tenant",
        BTreeSet::from([Action::Own]),
    );
    match authorizer.create_grant(owner).await {
        Ok(_) | Err(AuthzError::Conflict(_)) => Ok(()),
        Err(error) => Err(format!("cannot bootstrap service owner grant: {error}")),
    }
}

/// Maps one configured email to the stable tenant-local user principal identity.
fn initial_owner_principal_id(email: &str) -> Result<String, String> {
    let email = email.trim().to_lowercase();
    let Some((local, domain)) = email.split_once('@') else {
        return Err("VERGLAS_INITIAL_OWNER_EMAIL must be a valid email address".to_owned());
    };
    if local.is_empty()
        || domain.is_empty()
        || !domain.contains('.')
        || domain.contains('@')
        || email.len() > 254
    {
        return Err("VERGLAS_INITIAL_OWNER_EMAIL must be a valid email address".to_owned());
    }
    Ok(format!("user/{email}"))
}

/// Decodes one exact 256-bit hexadecimal configuration key.
fn decode_256_bit_hex(name: &str, value: &str) -> Result<[u8; 32], String> {
    let decoded =
        hex::decode(value).map_err(|_| format!("{name} must be 64 hexadecimal characters"))?;
    decoded
        .try_into()
        .map_err(|_| format!("{name} must be 64 hexadecimal characters"))
}

/// Mutually isolated credential directories mounted by exactly one consumer each.
#[derive(Clone)]
struct InternalCredentialDirectories {
    access: PathBuf,
    server: PathBuf,
    lakekeeper: PathBuf,
    neon: PathBuf,
}

/// Mints and atomically installs least-privilege credentials for autonomous services.
async fn provision_internal_credentials(
    authorizer: Arc<dyn Authorizer>,
    tokens: Arc<AccessTokenService>,
    tenant_id: &str,
    service_principal: &str,
    directories: &InternalCredentialDirectories,
) -> Result<(), String> {
    for directory in [
        &directories.access,
        &directories.server,
        &directories.lakekeeper,
        &directories.neon,
    ] {
        tokio::fs::create_dir_all(directory)
            .await
            .map_err(|error| format!("cannot create access token directory: {error}"))?;
        set_directory_permissions(directory).await?;
    }
    for (parent_id, directory, file_name, audience, actions) in [
        (
            service_principal,
            directories.access.as_path(),
            "lakekeeper-management.token",
            verglas_rest::access::DATA_PLANE_AUDIENCE,
            BTreeSet::from([
                Action::Discover,
                Action::Describe,
                Action::CreateChild,
                Action::Modify,
                Action::ManageGrants,
            ]),
        ),
        (
            "service/verglas-server",
            directories.server.as_path(),
            "verglas-server.token",
            verglas_rest::access::DATA_PLANE_AUDIENCE,
            // The data-plane server resolves managed Lakekeeper warehouse
            // configuration before it opens readiness. Lakekeeper authorizes
            // that lookup as `describe`; granting it at the tenant root lets
            // database and warehouse descendants inherit the authority
            // without minting per-database credentials.
            BTreeSet::from([Action::Discover, Action::Describe]),
        ),
        (
            "service/verglas-lakekeeper",
            directories.lakekeeper.as_path(),
            "verglas-lakekeeper.token",
            verglas_rest::access::POLICY_ENGINE_AUDIENCE,
            BTreeSet::new(),
        ),
        (
            "service/verglas-neon",
            directories.neon.as_path(),
            "verglas-neon.token",
            verglas_rest::access::POLICY_ENGINE_AUDIENCE,
            BTreeSet::new(),
        ),
    ] {
        ensure_internal_principal(authorizer.as_ref(), tenant_id, parent_id).await?;
        if parent_id == "service/verglas-lakekeeper" {
            match authorizer
                .create_grant(Grant::new(
                    "lakekeeper-control-service",
                    tenant_id,
                    parent_id,
                    "lakekeeper",
                    BTreeSet::from([Action::CreateChild, Action::Modify]),
                ))
                .await
            {
                Ok(_) | Err(AuthzError::Conflict(_)) => {}
                Err(error) => {
                    return Err(format!(
                        "cannot grant Lakekeeper control-resource authority: {error}"
                    ));
                }
            }
        }
        let token_id = new_access_token_id();
        let principal_id = format!("token/{token_id}");
        authorizer
            .create_principal(
                Principal::new(tenant_id, &principal_id, PrincipalKind::ServiceAccount)
                    .with_parent(parent_id),
            )
            .await
            .map_err(|error| format!("cannot create internal token principal: {error}"))?;
        if !actions.is_empty() {
            authorizer
                .create_grant(Grant::new(
                    format!("internal-token-grant/{token_id}"),
                    tenant_id,
                    &principal_id,
                    "tenant",
                    actions,
                ))
                .await
                .map_err(|error| format!("cannot grant internal token authority: {error}"))?;
        }
        let issued_at = unix_time();
        let expires_at = issued_at.saturating_add(24 * 60 * 60);
        let policy_version = authorizer
            .policy_version(tenant_id)
            .await
            .map_err(|error| format!("cannot read policy version: {error}"))?;
        let minted = tokens
            .mint(TokenMintRequest::new(
                &token_id,
                tenant_id,
                parent_id,
                &principal_id,
                format!("Internal {parent_id}"),
                audience,
                policy_version,
                issued_at,
                expires_at,
            ))
            .await
            .map_err(|error| format!("cannot mint internal credential: {error}"))?;
        write_credential(directory, file_name, &token_id, minted.token.expose()).await?;
    }
    Ok(())
}

/// Ensures one non-human parent identity without granting it tenant ownership.
async fn ensure_internal_principal(
    authorizer: &dyn Authorizer,
    tenant_id: &str,
    principal_id: &str,
) -> Result<(), String> {
    match authorizer
        .create_principal(Principal::new(
            tenant_id,
            principal_id,
            PrincipalKind::ServiceAccount,
        ))
        .await
    {
        Ok(_) | Err(AuthzError::Conflict(_)) => Ok(()),
        Err(error) => Err(format!("cannot bootstrap internal principal: {error}")),
    }
}

/// Replaces one credential file without exposing a partially written bearer.
async fn write_credential(
    directory: &Path,
    file_name: &str,
    token_id: &str,
    token: &str,
) -> Result<(), String> {
    let temporary = directory.join(format!(".{file_name}.{token_id}.tmp"));
    let destination = directory.join(file_name);
    let mut file = tokio::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .await
        .map_err(|error| format!("cannot create temporary credential: {error}"))?;
    file.write_all(token.as_bytes())
        .await
        .map_err(|error| format!("cannot write credential: {error}"))?;
    file.sync_all()
        .await
        .map_err(|error| format!("cannot sync credential: {error}"))?;
    set_file_permissions(&temporary).await?;
    tokio::fs::rename(&temporary, &destination)
        .await
        .map_err(|error| format!("cannot install credential: {error}"))
}

/// Allows a consumer with a different container UID to traverse its isolated volume.
async fn set_directory_permissions(path: &Path) -> Result<(), String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755))
            .await
            .map_err(|error| format!("cannot restrict credential directory: {error}"))?;
    }
    Ok(())
}

/// Makes one isolated-volume bearer immutable to its read-only consumer mount.
async fn set_file_permissions(path: &Path) -> Result<(), String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o444))
            .await
            .map_err(|error| format!("cannot restrict credential file: {error}"))?;
    }
    Ok(())
}

/// Rotates internal credentials halfway through their lifetime without stopping service.
fn spawn_internal_credential_rotation(
    authorizer: Arc<dyn Authorizer>,
    tokens: Arc<AccessTokenService>,
    tenant_id: String,
    service_principal: String,
    directories: InternalCredentialDirectories,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(12 * 60 * 60));
        interval.tick().await;
        loop {
            interval.tick().await;
            if let Err(error) = provision_internal_credentials(
                authorizer.clone(),
                tokens.clone(),
                &tenant_id,
                &service_principal,
                &directories,
            )
            .await
            {
                eprintln!("verglas-access: internal credential rotation failed: {error}");
            }
        }
    });
}

/// Returns the current Unix timestamp for credential validity intervals.
fn unix_time() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

/// Reports liveness after both durable dependencies passed startup.
async fn health(ready: Arc<AtomicBool>) -> StatusCode {
    if ready.load(Ordering::Acquire) {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::SERVICE_UNAVAILABLE
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
    use verglas_authz::{MemoryAccessTokenRegistry, MemoryAuthorizer};

    #[tokio::test]
    async fn tenant_identity_bootstrap_is_idempotent() {
        let authorizer = MemoryAuthorizer::new();
        ensure_tenant_identities(
            &authorizer,
            "tenant-a",
            "local-service",
            "Alice@Example.com",
        )
        .await
        .expect("first bootstrap");
        ensure_tenant_identities(
            &authorizer,
            "tenant-a",
            "local-service",
            "Alice@Example.com",
        )
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
        let owner = authorizer
            .get_principal("tenant-a", "user/alice@example.com")
            .await
            .expect("owner principal");
        assert_eq!(owner.kind, PrincipalKind::User);
        assert_eq!(
            authorizer
                .list_grants("tenant-a")
                .await
                .expect("grants")
                .len(),
            1
        );
    }

    #[test]
    fn initial_owner_identity_is_email_only_and_canonical() {
        assert_eq!(
            initial_owner_principal_id(" Alice@Example.com ").expect("email"),
            "user/alice@example.com"
        );
        assert!(initial_owner_principal_id("alice").is_err());
    }

    #[tokio::test]
    async fn internal_credentials_are_scoped_and_atomically_installed() {
        let authorizer = Arc::new(MemoryAuthorizer::new());
        authorizer
            .create_resource(Resource::new("tenant-a", "tenant", ResourceKind::Tenant))
            .await
            .expect("tenant");
        authorizer
            .create_resource(
                Resource::new("tenant-a", "lakekeeper", ResourceKind::Project)
                    .with_parent("tenant"),
            )
            .await
            .expect("lakekeeper root");
        let tokens = Arc::new(AccessTokenService::new(
            AccessTokenSigner::new([6; 32]),
            Arc::new(MemoryAccessTokenRegistry::new()),
        ));
        let directory = tempfile::tempdir().expect("temporary directory");
        let directories = InternalCredentialDirectories {
            access: directory.path().join("access"),
            server: directory.path().join("server"),
            lakekeeper: directory.path().join("lakekeeper"),
            neon: directory.path().join("neon"),
        };

        provision_internal_credentials(
            authorizer.clone(),
            tokens.clone(),
            "tenant-a",
            "service/access",
            &directories,
        )
        .await
        .expect("credentials");

        authorizer
            .create_resource(
                Resource::new("tenant-a", "database/default", ResourceKind::Database)
                    .with_parent("tenant"),
            )
            .await
            .expect("database resource");

        for (directory, file_name, audience) in [
            (
                directories.access.as_path(),
                "lakekeeper-management.token",
                verglas_rest::access::DATA_PLANE_AUDIENCE,
            ),
            (
                directories.server.as_path(),
                "verglas-server.token",
                verglas_rest::access::DATA_PLANE_AUDIENCE,
            ),
            (
                directories.lakekeeper.as_path(),
                "verglas-lakekeeper.token",
                verglas_rest::access::POLICY_ENGINE_AUDIENCE,
            ),
            (
                directories.neon.as_path(),
                "verglas-neon.token",
                verglas_rest::access::POLICY_ENGINE_AUDIENCE,
            ),
        ] {
            let token = tokio::fs::read_to_string(directory.join(file_name))
                .await
                .expect("credential file");
            let claims = tokens
                .authenticate(&token, "tenant-a", audience, unix_time())
                .await
                .expect("credential authentication");
            let wrong_audience = if audience == verglas_rest::access::DATA_PLANE_AUDIENCE {
                verglas_rest::access::POLICY_ENGINE_AUDIENCE
            } else {
                verglas_rest::access::DATA_PLANE_AUDIENCE
            };
            assert!(
                tokens
                    .authenticate(&token, "tenant-a", wrong_audience, unix_time())
                    .await
                    .is_err()
            );
            if file_name == "verglas-server.token" {
                for (resource_id, action) in [
                    ("tenant", Action::Discover),
                    ("database/default", Action::Describe),
                ] {
                    let decision = authorizer
                        .check(verglas_authz::AccessCheck::new(
                            "tenant-a",
                            claims.principal_id.clone(),
                            resource_id,
                            action,
                        ))
                        .await
                        .expect("decision");
                    assert!(
                        decision.allowed,
                        "server token lacks {action:?} on {resource_id}"
                    );
                }
            }
            if file_name == "lakekeeper-management.token" {
                for action in [Action::CreateChild, Action::Modify, Action::ManageGrants] {
                    let decision = authorizer
                        .check(verglas_authz::AccessCheck::new(
                            "tenant-a",
                            claims.principal_id.clone(),
                            "tenant",
                            action,
                        ))
                        .await
                        .expect("management decision");
                    assert!(decision.allowed, "management token lacks {action:?}");
                }
            }
        }
        assert!(!directory.path().join("root.token").exists());
    }
}
