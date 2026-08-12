//! Tenant database resources and deterministic provisioning plans.
//!
//! This crate records desired database composition. Provisioners consume the
//! plan; cache and query processes resolve its stable binding IDs at runtime.

use serde::{Deserialize, Serialize};

mod repository;
mod service;

pub use repository::{
    DatabaseRecord, DatabaseRepository, PostgresDatabaseRepository, RepositoryError,
};
pub use service::{
    DatabaseManager, DatabaseService, DatabaseServiceError, DatabaseView, ScopedSecretKind,
    ScopedSecretResolver, SecretResolutionError,
};

/// JSON request accepted by the local database resource API.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum CreateDatabaseRequest {
    /// An Iceberg lakehouse.
    Lakehouse {
        /// Stable database name.
        name: String,
        /// Managed or customer-owned object storage.
        storage: StorageRequest,
        /// Managed Lakekeeper or an external REST catalog.
        catalog: CatalogRequest,
    },
    /// An independent managed Neon database.
    Postgres {
        /// Stable database name.
        name: String,
        /// Required managed engine declaration.
        engine: PostgresEngineRequest,
    },
}

/// Object-storage selection on a lakehouse request.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "mode", rename_all = "kebab-case")]
pub enum StorageRequest {
    /// Tenant-managed object storage.
    Managed,
    /// S3 path resolved to an authorized scoped secret.
    ScopedSecret {
        /// Customer S3 prefix.
        data_path: String,
    },
}

/// Catalog selection on a lakehouse request.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "mode", rename_all = "kebab-case")]
pub enum CatalogRequest {
    /// The tenant's shared Lakekeeper deployment with a distinct warehouse.
    ManagedLakekeeper,
    /// Customer-operated Iceberg REST catalog.
    External {
        /// Catalog base URI.
        uri: String,
        /// Warehouse within that catalog.
        warehouse: String,
    },
}

/// Managed Postgres implementation selected by the public API.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "mode", rename_all = "kebab-case")]
pub enum PostgresEngineRequest {
    /// Verglas' managed Neon composition.
    ManagedNeon,
}

impl CreateDatabaseRequest {
    /// Converts the wire declaration into a validated tenant plan.
    pub fn plan(self, tenant_id: impl Into<String>) -> Result<DatabasePlan, PlanError> {
        let declaration = match self {
            Self::Postgres {
                name,
                engine: PostgresEngineRequest::ManagedNeon,
            } => CreateDatabase::new(name, DatabaseKind::Postgres),
            Self::Lakehouse {
                name,
                storage,
                catalog,
            } => {
                let mut declaration = CreateDatabase::new(name, DatabaseKind::Lakehouse);
                if let StorageRequest::ScopedSecret { data_path } = storage {
                    declaration = declaration.with_data_path(data_path);
                }
                if let CatalogRequest::External { uri, warehouse } = catalog {
                    declaration = declaration.with_catalog(uri).with_warehouse(warehouse);
                }
                declaration
            }
        };
        declaration.plan(tenant_id)
    }
}

/// Supported logical database engines.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DatabaseKind {
    /// An Iceberg lakehouse backed by Lakekeeper or an external REST catalog.
    Lakehouse,
    /// An independent managed Verglas Neon database.
    Postgres,
}

/// User intent before tenant-scoped secrets and managed resources are resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateDatabase {
    name: String,
    kind: DatabaseKind,
    data_path: Option<String>,
    catalog: Option<String>,
    warehouse: Option<String>,
}

impl CreateDatabase {
    /// Starts a database declaration.
    pub fn new(name: impl Into<String>, kind: DatabaseKind) -> Self {
        Self {
            name: name.into(),
            kind,
            data_path: None,
            catalog: None,
            warehouse: None,
        }
    }

    /// Selects customer-owned S3 storage.
    #[must_use]
    pub fn with_data_path(mut self, data_path: impl Into<String>) -> Self {
        self.data_path = Some(data_path.into());
        self
    }

    /// Selects an external Iceberg REST catalog.
    #[must_use]
    pub fn with_catalog(mut self, catalog: impl Into<String>) -> Self {
        self.catalog = Some(catalog.into());
        self
    }

    /// Selects a warehouse in the external catalog.
    #[must_use]
    pub fn with_warehouse(mut self, warehouse: impl Into<String>) -> Self {
        self.warehouse = Some(warehouse.into());
        self
    }

    /// Validates the declaration and creates a provisioning plan.
    pub fn plan(self, tenant_id: impl Into<String>) -> Result<DatabasePlan, PlanError> {
        validate_name(&self.name)?;
        let tenant_id = tenant_id.into();
        if tenant_id.trim().is_empty() {
            return Err(PlanError::EmptyTenant);
        }
        match self.kind {
            DatabaseKind::Postgres => {
                if self.data_path.is_some() || self.catalog.is_some() || self.warehouse.is_some() {
                    return Err(PlanError::PostgresLakehouseOptions);
                }
                Ok(DatabasePlan::Postgres(PostgresPlan {
                    tenant_id,
                    name: self.name,
                }))
            }
            DatabaseKind::Lakehouse => {
                if let Some(path) = &self.data_path {
                    validate_uri(path, &["s3://"], PlanError::InvalidDataPath)?;
                }
                match (&self.catalog, &self.warehouse) {
                    (Some(_), None) => return Err(PlanError::ExternalCatalogNeedsWarehouse),
                    (None, Some(_)) => return Err(PlanError::WarehouseNeedsExternalCatalog),
                    (Some(_), Some(_)) if self.data_path.is_none() => {
                        return Err(PlanError::ExternalCatalogNeedsDataPath);
                    }
                    _ => {}
                }
                if let Some(uri) = &self.catalog {
                    validate_uri(uri, &["http://", "https://"], PlanError::InvalidCatalogUri)?;
                }
                let warehouse = self.warehouse.clone().unwrap_or_else(|| self.name.clone());
                Ok(DatabasePlan::Lakehouse(LakehousePlan {
                    tenant_id,
                    name: self.name,
                    data_path: self.data_path,
                    catalog_uri: self.catalog,
                    warehouse,
                }))
            }
        }
    }
}

/// Validated desired state for one database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatabasePlan {
    /// A lakehouse binding within the tenant's catalog/cache bundle.
    Lakehouse(LakehousePlan),
    /// An independent managed Neon database.
    Postgres(PostgresPlan),
}

impl DatabasePlan {
    /// Returns the lakehouse plan when this is a lakehouse.
    pub fn lakehouse(&self) -> Option<&LakehousePlan> {
        match self {
            Self::Lakehouse(plan) => Some(plan),
            Self::Postgres(_) => None,
        }
    }

    /// Returns the tenant that owns this database.
    pub fn tenant_id(&self) -> &str {
        match self {
            Self::Lakehouse(plan) => &plan.tenant_id,
            Self::Postgres(plan) => &plan.tenant_id,
        }
    }

    /// Returns the stable tenant-local database name.
    pub fn name(&self) -> &str {
        match self {
            Self::Lakehouse(plan) => &plan.name,
            Self::Postgres(plan) => &plan.name,
        }
    }

    /// Returns the database engine kind.
    pub fn kind(&self) -> DatabaseKind {
        match self {
            Self::Lakehouse(_) => DatabaseKind::Lakehouse,
            Self::Postgres(_) => DatabaseKind::Postgres,
        }
    }
}

/// Lakehouse catalog and object-storage intent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LakehousePlan {
    tenant_id: String,
    name: String,
    data_path: Option<String>,
    catalog_uri: Option<String>,
    warehouse: String,
}

impl LakehousePlan {
    /// Returns the customer data prefix, or none for managed storage.
    pub fn data_path(&self) -> Option<&str> {
        self.data_path.as_deref()
    }

    /// Returns the external endpoint, or none for tenant Lakekeeper.
    pub fn catalog_uri(&self) -> Option<&str> {
        self.catalog_uri.as_deref()
    }

    /// Returns the database-specific warehouse identifier.
    pub fn warehouse(&self) -> &str {
        &self.warehouse
    }
}

/// Managed Neon database intent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostgresPlan {
    tenant_id: String,
    name: String,
}

/// Invalid or ambiguous database declarations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PlanError {
    /// The database name is not a stable resource name.
    #[error(
        "database name must start with a letter or underscore and contain only ASCII letters, digits, underscores, or hyphens"
    )]
    InvalidName,
    /// The tenant identity was empty.
    #[error("tenant id must not be empty")]
    EmptyTenant,
    /// Postgres was given lakehouse-only options.
    #[error("Postgres databases do not accept lakehouse binding options")]
    PostgresLakehouseOptions,
    /// Customer storage was not an absolute S3 URI.
    #[error("data path must be an absolute s3 URI")]
    InvalidDataPath,
    /// External catalog was not an absolute HTTP URI.
    #[error("catalog must be an absolute http or https URI")]
    InvalidCatalogUri,
    /// External catalog omitted its warehouse.
    #[error("external catalog requires a warehouse")]
    ExternalCatalogNeedsWarehouse,
    /// A warehouse was supplied without an external catalog.
    #[error("warehouse requires an external catalog")]
    WarehouseNeedsExternalCatalog,
    /// External catalog omitted its customer storage prefix.
    #[error("external catalog requires a customer data path")]
    ExternalCatalogNeedsDataPath,
}

/// Validates resource names shared by the CLI and service.
fn validate_name(name: &str) -> Result<(), PlanError> {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return Err(PlanError::InvalidName);
    };
    if !(first.is_ascii_alphabetic() || first == '_')
        || !chars.all(|value| value.is_ascii_alphanumeric() || value == '_' || value == '-')
    {
        return Err(PlanError::InvalidName);
    }
    Ok(())
}

/// Applies the minimal absolute-URI validation required by the contracts.
fn validate_uri(value: &str, prefixes: &[&str], error: PlanError) -> Result<(), PlanError> {
    if prefixes.iter().any(|prefix| {
        value
            .strip_prefix(prefix)
            .is_some_and(|rest| !rest.is_empty() && !rest.starts_with('/'))
    }) {
        Ok(())
    } else {
        Err(error)
    }
}
