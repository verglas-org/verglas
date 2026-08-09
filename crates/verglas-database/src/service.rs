//! Resolves scoped secret resource IDs and persists database compositions.
//!
//! Resolution returns only resource IDs. Plaintext credentials stay inside the
//! access service and are not retained by database plans or records.

use async_trait::async_trait;

use crate::{DatabasePlan, DatabaseRecord, DatabaseRepository, RepositoryError};

/// Secret kinds a database composition may bind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ScopedSecretKind {
    /// S3-compatible object-storage credentials.
    S3,
    /// Iceberg REST catalog credentials.
    IcebergRest,
}

/// Fail-closed scoped-secret resolution failures.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SecretResolutionError {
    /// No authorized secret covered the requested scope.
    #[error("no {kind:?} secret covers {scope}")]
    NotFound {
        /// Secret kind that was required.
        kind: ScopedSecretKind,
        /// Requested resource URI.
        scope: String,
    },
    /// More than one equally specific secret covered the requested scope.
    #[error("multiple {kind:?} secrets ambiguously cover {scope}")]
    Ambiguous {
        /// Secret kind that was required.
        kind: ScopedSecretKind,
        /// Requested resource URI.
        scope: String,
    },
    /// The caller lacks `use_secret` permission for the resolved resource.
    #[error("no matching secret is authorized for this principal")]
    Unauthorized,
    /// The access service could not complete resolution.
    #[error("secret resolution failed: {0}")]
    Backend(String),
}

/// Resource-ID-only boundary implemented by the access service.
#[async_trait]
pub trait ScopedSecretResolver: Send + Sync {
    /// Resolves the longest authorized scope and returns its immutable resource ID.
    async fn resolve_secret_id(
        &self,
        tenant_id: &str,
        kind: ScopedSecretKind,
        scope: &str,
    ) -> Result<String, SecretResolutionError>;
}

/// Database creation failures with no compatibility or fallback path.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DatabaseServiceError {
    /// A database with this tenant-local name already exists.
    #[error("database {tenant_id}/{name} already exists")]
    Duplicate {
        /// Tenant containing the duplicate name.
        tenant_id: String,
        /// Existing database name.
        name: String,
    },
    /// Required secret resolution failed closed.
    #[error(transparent)]
    Secret(#[from] SecretResolutionError),
    /// Durable repository access failed.
    #[error(transparent)]
    Repository(RepositoryError),
}

/// Resolves and persists immutable database bindings.
#[derive(Debug)]
pub struct DatabaseService<R, S> {
    repository: R,
    secret_resolver: S,
}

/// Object-safe database creation boundary used by the REST access service.
#[async_trait]
pub trait DatabaseCreator: Send + Sync {
    /// Resolves bindings and persists one validated database plan.
    async fn create_database(
        &self,
        plan: DatabasePlan,
    ) -> Result<DatabaseRecord, DatabaseServiceError>;
}

impl<R, S> DatabaseService<R, S>
where
    R: DatabaseRepository,
    S: ScopedSecretResolver,
{
    /// Creates a service over one durable repository and access-service resolver.
    pub fn new(repository: R, secret_resolver: S) -> Self {
        Self {
            repository,
            secret_resolver,
        }
    }

    /// Resolves binding resource IDs and inserts one immutable database record.
    pub async fn create(&self, plan: DatabasePlan) -> Result<DatabaseRecord, DatabaseServiceError> {
        let (data_path, catalog_uri, warehouse, storage_secret_id, catalog_secret_id) = match &plan
        {
            DatabasePlan::Postgres(_) => (None, None, None, None, None),
            DatabasePlan::Lakehouse(lakehouse) => {
                let storage_secret_id = match lakehouse.data_path() {
                    Some(data_path) => Some(
                        self.secret_resolver
                            .resolve_secret_id(plan.tenant_id(), ScopedSecretKind::S3, data_path)
                            .await?,
                    ),
                    None => None,
                };
                let catalog_secret_id = match lakehouse.catalog_uri() {
                    Some(catalog_uri) => Some(
                        self.secret_resolver
                            .resolve_secret_id(
                                plan.tenant_id(),
                                ScopedSecretKind::IcebergRest,
                                catalog_uri,
                            )
                            .await?,
                    ),
                    None => None,
                };
                (
                    lakehouse.data_path().map(str::to_owned),
                    lakehouse.catalog_uri().map(str::to_owned),
                    Some(lakehouse.warehouse().to_owned()),
                    storage_secret_id,
                    catalog_secret_id,
                )
            }
        };
        let record = DatabaseRecord::resolved(
            uuid::Uuid::new_v4().to_string(),
            plan.tenant_id().to_owned(),
            plan.name().to_owned(),
            plan.kind(),
            data_path,
            catalog_uri,
            warehouse,
            storage_secret_id,
            catalog_secret_id,
        );
        match self.repository.insert(record.clone()).await {
            Ok(()) => Ok(record),
            Err(RepositoryError::Duplicate { tenant_id, name }) => {
                Err(DatabaseServiceError::Duplicate { tenant_id, name })
            }
            Err(error) => Err(DatabaseServiceError::Repository(error)),
        }
    }
}

#[async_trait]
impl<R, S> DatabaseCreator for DatabaseService<R, S>
where
    R: DatabaseRepository,
    S: ScopedSecretResolver,
{
    async fn create_database(
        &self,
        plan: DatabasePlan,
    ) -> Result<DatabaseRecord, DatabaseServiceError> {
        self.create(plan).await
    }
}
