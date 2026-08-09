//! Resolves scoped secret resource IDs and persists database compositions.
//!
//! Resolution returns only resource IDs. Plaintext credentials stay inside the
//! access service and are not retained by database plans or records.

use async_trait::async_trait;

use crate::{
    CatalogRequest, DatabaseKind, DatabasePlan, DatabaseRecord, DatabaseRepository,
    PostgresEngineRequest, RepositoryError, StorageRequest,
};

/// Public database definition returned without internal tenant or secret IDs.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum DatabaseView {
    /// An Iceberg lakehouse and its resolved public storage and catalog choices.
    Lakehouse {
        /// Stable tenant-local database name.
        name: String,
        /// Managed storage or the configured customer data path.
        storage: StorageRequest,
        /// Managed Lakekeeper or the configured external catalog.
        catalog: CatalogRequest,
    },
    /// An independent managed Neon database.
    Postgres {
        /// Stable tenant-local database name.
        name: String,
        /// Managed Postgres engine declaration.
        engine: PostgresEngineRequest,
    },
}

impl DatabaseView {
    /// Returns the stable tenant-local database name.
    pub fn name(&self) -> &str {
        match self {
            Self::Lakehouse { name, .. } | Self::Postgres { name, .. } => name,
        }
    }

    /// Projects one durable record while withholding internal IDs.
    fn from_record(record: DatabaseRecord) -> Result<Self, RepositoryError> {
        match record.kind() {
            DatabaseKind::Lakehouse => {
                let storage = match record.data_path() {
                    Some(data_path) => StorageRequest::ScopedSecret {
                        data_path: data_path.to_owned(),
                    },
                    None => StorageRequest::Managed,
                };
                let catalog = match record.catalog_uri() {
                    Some(uri) => CatalogRequest::External {
                        uri: uri.to_owned(),
                        warehouse: record
                            .warehouse()
                            .ok_or_else(|| {
                                RepositoryError::Backend(
                                    "persisted lakehouse record has no warehouse".to_owned(),
                                )
                            })?
                            .to_owned(),
                    },
                    None => CatalogRequest::ManagedLakekeeper,
                };
                Ok(Self::Lakehouse {
                    name: record.name().to_owned(),
                    storage,
                    catalog,
                })
            }
            DatabaseKind::Postgres => Ok(Self::Postgres {
                name: record.name().to_owned(),
                engine: PostgresEngineRequest::ManagedNeon,
            }),
        }
    }
}

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
    /// No database exists under this tenant-local name.
    #[error("database {tenant_id}/{name} not found")]
    NotFound {
        /// Tenant searched by the service.
        tenant_id: String,
        /// Missing database name.
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

/// Object-safe database management boundary used by the REST access service.
#[async_trait]
pub trait DatabaseManager: Send + Sync {
    /// Resolves bindings and persists one validated database plan.
    async fn create_database(
        &self,
        plan: DatabasePlan,
    ) -> Result<DatabaseView, DatabaseServiceError>;

    /// Lists public database definitions for one tenant.
    async fn list_databases(
        &self,
        tenant_id: &str,
    ) -> Result<Vec<DatabaseView>, DatabaseServiceError>;

    /// Gets one public database definition by tenant-local name.
    async fn get_database(
        &self,
        tenant_id: &str,
        name: &str,
    ) -> Result<DatabaseView, DatabaseServiceError>;

    /// Deletes one database by tenant-local name.
    async fn delete_database(
        &self,
        tenant_id: &str,
        name: &str,
    ) -> Result<(), DatabaseServiceError>;
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

    /// Lists one tenant's database definitions in stable name order.
    pub async fn list(&self, tenant_id: &str) -> Result<Vec<DatabaseView>, DatabaseServiceError> {
        let mut databases = self
            .repository
            .list(tenant_id)
            .await
            .map_err(DatabaseServiceError::Repository)?;
        databases.sort_by(|left, right| left.name().cmp(right.name()));
        databases
            .into_iter()
            .map(DatabaseView::from_record)
            .collect::<Result<Vec<_>, _>>()
            .map_err(DatabaseServiceError::Repository)
    }

    /// Gets one tenant-local database without exposing its internal bindings.
    pub async fn get(
        &self,
        tenant_id: &str,
        name: &str,
    ) -> Result<DatabaseView, DatabaseServiceError> {
        let record = self
            .repository
            .get(tenant_id, name)
            .await
            .map_err(DatabaseServiceError::Repository)?
            .ok_or_else(|| DatabaseServiceError::NotFound {
                tenant_id: tenant_id.to_owned(),
                name: name.to_owned(),
            })?;
        DatabaseView::from_record(record).map_err(DatabaseServiceError::Repository)
    }

    /// Deletes exactly one tenant-local database or reports that it is absent.
    pub async fn delete(&self, tenant_id: &str, name: &str) -> Result<(), DatabaseServiceError> {
        if self
            .repository
            .delete(tenant_id, name)
            .await
            .map_err(DatabaseServiceError::Repository)?
        {
            return Ok(());
        }
        Err(DatabaseServiceError::NotFound {
            tenant_id: tenant_id.to_owned(),
            name: name.to_owned(),
        })
    }
}

#[async_trait]
impl<R, S> DatabaseManager for DatabaseService<R, S>
where
    R: DatabaseRepository,
    S: ScopedSecretResolver,
{
    async fn create_database(
        &self,
        plan: DatabasePlan,
    ) -> Result<DatabaseView, DatabaseServiceError> {
        DatabaseView::from_record(self.create(plan).await?)
            .map_err(DatabaseServiceError::Repository)
    }

    async fn list_databases(
        &self,
        tenant_id: &str,
    ) -> Result<Vec<DatabaseView>, DatabaseServiceError> {
        self.list(tenant_id).await
    }

    async fn get_database(
        &self,
        tenant_id: &str,
        name: &str,
    ) -> Result<DatabaseView, DatabaseServiceError> {
        self.get(tenant_id, name).await
    }

    async fn delete_database(
        &self,
        tenant_id: &str,
        name: &str,
    ) -> Result<(), DatabaseServiceError> {
        self.delete(tenant_id, name).await
    }
}
