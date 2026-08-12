//! Durable database resource records and their PostgreSQL repository.
//!
//! Records store only stable secret resource IDs. Credential plaintext remains
//! behind the access service and never crosses this persistence boundary.

use async_trait::async_trait;
use sqlx::postgres::{PgPool, PgPoolOptions, PgRow};
use sqlx::{Executor, Row};

use crate::DatabaseKind;

/// Database declarations are control-plane operations, not query traffic.
const TENANT_POOL_MAX_CONNECTIONS: u32 = 2;

/// Fully resolved database definition persisted for runtime provisioning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseRecord {
    id: String,
    tenant_id: String,
    name: String,
    kind: DatabaseKind,
    data_path: Option<String>,
    catalog_uri: Option<String>,
    warehouse: Option<String>,
    storage_secret_id: Option<String>,
    catalog_secret_id: Option<String>,
}

impl DatabaseRecord {
    /// Constructs a resolved record for insertion by the database service.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn resolved(
        id: String,
        tenant_id: String,
        name: String,
        kind: DatabaseKind,
        data_path: Option<String>,
        catalog_uri: Option<String>,
        warehouse: Option<String>,
        storage_secret_id: Option<String>,
        catalog_secret_id: Option<String>,
    ) -> Self {
        Self {
            id,
            tenant_id,
            name,
            kind,
            data_path,
            catalog_uri,
            warehouse,
            storage_secret_id,
            catalog_secret_id,
        }
    }

    /// Returns the immutable database resource ID.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Returns the owning tenant ID.
    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    /// Returns the stable tenant-local database name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the database engine kind.
    pub fn kind(&self) -> DatabaseKind {
        self.kind
    }

    /// Returns the external object-storage path when one is bound.
    pub fn data_path(&self) -> Option<&str> {
        self.data_path.as_deref()
    }

    /// Returns the external catalog URI when one is bound.
    pub fn catalog_uri(&self) -> Option<&str> {
        self.catalog_uri.as_deref()
    }

    /// Returns the database-specific Lakehouse warehouse.
    pub fn warehouse(&self) -> Option<&str> {
        self.warehouse.as_deref()
    }

    /// Returns the immutable S3 secret resource ID selected at creation.
    pub fn storage_secret_id(&self) -> Option<&str> {
        self.storage_secret_id.as_deref()
    }

    /// Returns the immutable Iceberg REST secret resource ID selected at creation.
    pub fn catalog_secret_id(&self) -> Option<&str> {
        self.catalog_secret_id.as_deref()
    }
}

/// Persistence errors surfaced without retry or fallback behavior.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RepositoryError {
    /// A tenant already owns a database with the requested name.
    #[error("database {tenant_id}/{name} already exists")]
    Duplicate {
        /// Tenant containing the duplicate name.
        tenant_id: String,
        /// Existing database name.
        name: String,
    },
    /// PostgreSQL or repository state could not be read or written.
    #[error("database repository failed: {0}")]
    Backend(String),
}

/// Durable storage boundary used by the database service.
#[async_trait]
pub trait DatabaseRepository: Send + Sync {
    /// Inserts a resolved database exactly once.
    async fn insert(&self, database: DatabaseRecord) -> Result<(), RepositoryError>;

    /// Gets a database by its tenant-local stable name.
    async fn get(
        &self,
        tenant_id: &str,
        name: &str,
    ) -> Result<Option<DatabaseRecord>, RepositoryError>;

    /// Lists every database belonging to one tenant in stable name order.
    async fn list(&self, tenant_id: &str) -> Result<Vec<DatabaseRecord>, RepositoryError>;

    /// Deletes one tenant-local database and reports whether it existed.
    async fn delete(&self, tenant_id: &str, name: &str) -> Result<bool, RepositoryError>;
}

/// PostgreSQL-backed repository in the shared `verglas_permissions` database.
#[derive(Debug, Clone)]
pub struct PostgresDatabaseRepository {
    pool: PgPool,
}

impl PostgresDatabaseRepository {
    /// Connects to `verglas_permissions` and creates the database resource table.
    pub async fn connect(database_url: &str) -> Result<Self, RepositoryError> {
        let pool = PgPoolOptions::new()
            .max_connections(TENANT_POOL_MAX_CONNECTIONS)
            .connect(database_url)
            .await
            .map_err(repository_database_error)?;
        Self::from_pool(pool).await
    }

    /// Uses an existing shared database pool and creates the resource table.
    pub async fn from_pool(pool: PgPool) -> Result<Self, RepositoryError> {
        initialize_table(&pool).await?;
        Ok(Self { pool })
    }
}

#[async_trait]
impl DatabaseRepository for PostgresDatabaseRepository {
    /// Inserts a database while the tenant/name unique constraint closes races.
    async fn insert(&self, database: DatabaseRecord) -> Result<(), RepositoryError> {
        let result = sqlx::query(
            "INSERT INTO verglas_databases (id, tenant_id, name, kind, data_path, catalog_uri, warehouse, storage_secret_id, catalog_secret_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        )
        .bind(database.id())
        .bind(database.tenant_id())
        .bind(database.name())
        .bind(database_kind_name(database.kind()))
        .bind(database.data_path())
        .bind(database.catalog_uri())
        .bind(database.warehouse())
        .bind(database.storage_secret_id())
        .bind(database.catalog_secret_id())
        .execute(&self.pool)
        .await;
        match result {
            Ok(_) => Ok(()),
            Err(error) if is_unique_violation(&error) => Err(RepositoryError::Duplicate {
                tenant_id: database.tenant_id().to_owned(),
                name: database.name().to_owned(),
            }),
            Err(error) => Err(repository_database_error(error)),
        }
    }

    /// Reads one complete resolved record without joining to secret ciphertext.
    async fn get(
        &self,
        tenant_id: &str,
        name: &str,
    ) -> Result<Option<DatabaseRecord>, RepositoryError> {
        let row = sqlx::query(
            "SELECT id, tenant_id, name, kind, data_path, catalog_uri, warehouse, storage_secret_id, catalog_secret_id FROM verglas_databases WHERE tenant_id = $1 AND name = $2",
        )
        .bind(tenant_id)
        .bind(name)
        .fetch_optional(&self.pool)
        .await
        .map_err(repository_database_error)?;
        row.map(|row| record_from_row(&row)).transpose()
    }

    /// Lists complete tenant records without joining to secret ciphertext.
    async fn list(&self, tenant_id: &str) -> Result<Vec<DatabaseRecord>, RepositoryError> {
        let rows = sqlx::query(
            "SELECT id, tenant_id, name, kind, data_path, catalog_uri, warehouse, storage_secret_id, catalog_secret_id FROM verglas_databases WHERE tenant_id = $1 ORDER BY name ASC",
        )
        .bind(tenant_id)
        .fetch_all(&self.pool)
        .await
        .map_err(repository_database_error)?;
        rows.iter().map(record_from_row).collect()
    }

    /// Deletes only the record addressed by both tenant and stable name.
    async fn delete(&self, tenant_id: &str, name: &str) -> Result<bool, RepositoryError> {
        let result =
            sqlx::query("DELETE FROM verglas_databases WHERE tenant_id = $1 AND name = $2")
                .bind(tenant_id)
                .bind(name)
                .execute(&self.pool)
                .await
                .map_err(repository_database_error)?;
        Ok(result.rows_affected() == 1)
    }
}

/// Creates the one current prototype table without migration/version machinery.
async fn initialize_table(pool: &PgPool) -> Result<(), RepositoryError> {
    pool.execute(
        "CREATE TABLE IF NOT EXISTS verglas_databases (\
            id TEXT PRIMARY KEY, \
            tenant_id TEXT NOT NULL, \
            name TEXT NOT NULL, \
            kind TEXT NOT NULL, \
            data_path TEXT, \
            catalog_uri TEXT, \
            warehouse TEXT, \
            storage_secret_id TEXT, \
            catalog_secret_id TEXT, \
            UNIQUE (tenant_id, name), \
            CHECK (kind IN ('lakehouse', 'postgres')), \
            CHECK ((kind = 'postgres' AND data_path IS NULL AND catalog_uri IS NULL AND warehouse IS NULL AND storage_secret_id IS NULL AND catalog_secret_id IS NULL) OR (kind = 'lakehouse' AND warehouse IS NOT NULL AND ((data_path IS NULL AND storage_secret_id IS NULL) OR (data_path IS NOT NULL AND storage_secret_id IS NOT NULL)) AND ((catalog_uri IS NULL AND catalog_secret_id IS NULL) OR (catalog_uri IS NOT NULL AND catalog_secret_id IS NOT NULL)))), \
            FOREIGN KEY (tenant_id, storage_secret_id) REFERENCES verglas_secrets.secrets (tenant_id, id), \
            FOREIGN KEY (tenant_id, catalog_secret_id) REFERENCES verglas_secrets.secrets (tenant_id, id)\
        )",
    )
    .await
    .map_err(repository_database_error)?;
    Ok(())
}

/// Converts one PostgreSQL row into its typed durable record.
fn record_from_row(row: &PgRow) -> Result<DatabaseRecord, RepositoryError> {
    let kind: String = row.get("kind");
    let kind = match kind.as_str() {
        "lakehouse" => DatabaseKind::Lakehouse,
        "postgres" => DatabaseKind::Postgres,
        value => {
            return Err(RepositoryError::Backend(format!(
                "unknown persisted database kind {value}"
            )));
        }
    };
    Ok(DatabaseRecord::resolved(
        row.get("id"),
        row.get("tenant_id"),
        row.get("name"),
        kind,
        row.get("data_path"),
        row.get("catalog_uri"),
        row.get("warehouse"),
        row.get("storage_secret_id"),
        row.get("catalog_secret_id"),
    ))
}

/// Serializes a database kind into the constrained table value.
fn database_kind_name(kind: DatabaseKind) -> &'static str {
    match kind {
        DatabaseKind::Lakehouse => "lakehouse",
        DatabaseKind::Postgres => "postgres",
    }
}

/// Reports whether PostgreSQL rejected the tenant/name unique constraint.
fn is_unique_violation(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(|database| database.code())
        .is_some_and(|code| code == "23505")
}

/// Removes SQLx internals from the public repository error contract.
fn repository_database_error(error: sqlx::Error) -> RepositoryError {
    RepositoryError::Backend(error.to_string())
}
