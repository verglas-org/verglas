//! Database service tests pin secret resolution and durable record semantics.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use verglas_database::{
    CreateDatabase, DatabaseKind, DatabaseRecord, DatabaseRepository, DatabaseService,
    DatabaseServiceError, RepositoryError, ScopedSecretKind, ScopedSecretResolver,
    SecretResolutionError,
};

/// Tenant, secret kind, and requested scope used by the resolver fixture.
type ResolutionKey = (String, ScopedSecretKind, String);

/// Stable resource IDs indexed by their scoped lookup key.
type ResolvedSecrets = BTreeMap<ResolutionKey, String>;

/// In-memory repository used to test service behavior without weakening the
/// production PostgreSQL durability boundary.
#[derive(Debug, Clone, Default)]
struct MemoryRepository {
    records: Arc<Mutex<BTreeMap<(String, String), DatabaseRecord>>>,
}

#[async_trait]
impl DatabaseRepository for MemoryRepository {
    async fn insert(&self, database: DatabaseRecord) -> Result<(), RepositoryError> {
        let key = (database.tenant_id().to_owned(), database.name().to_owned());
        let mut records = self.records.lock().expect("repository lock");
        if records.contains_key(&key) {
            return Err(RepositoryError::Duplicate {
                tenant_id: key.0,
                name: key.1,
            });
        }
        records.insert(key, database);
        Ok(())
    }

    async fn get(
        &self,
        tenant_id: &str,
        name: &str,
    ) -> Result<Option<DatabaseRecord>, RepositoryError> {
        Ok(self
            .records
            .lock()
            .expect("repository lock")
            .get(&(tenant_id.to_owned(), name.to_owned()))
            .cloned())
    }

    async fn list(&self, tenant_id: &str) -> Result<Vec<DatabaseRecord>, RepositoryError> {
        Ok(self
            .records
            .lock()
            .expect("repository lock")
            .values()
            .filter(|database| database.tenant_id() == tenant_id)
            .cloned()
            .collect())
    }

    async fn delete(&self, tenant_id: &str, name: &str) -> Result<bool, RepositoryError> {
        Ok(self
            .records
            .lock()
            .expect("repository lock")
            .remove(&(tenant_id.to_owned(), name.to_owned()))
            .is_some())
    }
}

/// Resolver fixture that returns stable resource IDs and records requested
/// secret kinds/scopes without ever handling a plaintext value.
#[derive(Debug, Clone, Default)]
struct MemoryResolver {
    secrets: Arc<Mutex<ResolvedSecrets>>,
    calls: Arc<Mutex<Vec<ResolutionKey>>>,
}

impl MemoryResolver {
    /// Registers one tenant-scoped secret resource ID.
    fn add(&self, tenant_id: &str, kind: ScopedSecretKind, scope: &str, id: &str) {
        self.secrets.lock().expect("resolver lock").insert(
            (tenant_id.to_owned(), kind, scope.to_owned()),
            id.to_owned(),
        );
    }

    /// Returns every resolution requested by the service.
    fn calls(&self) -> Vec<ResolutionKey> {
        self.calls.lock().expect("resolver lock").clone()
    }
}

#[async_trait]
impl ScopedSecretResolver for MemoryResolver {
    async fn resolve_secret_id(
        &self,
        tenant_id: &str,
        kind: ScopedSecretKind,
        scope: &str,
    ) -> Result<String, SecretResolutionError> {
        self.calls.lock().expect("resolver lock").push((
            tenant_id.to_owned(),
            kind,
            scope.to_owned(),
        ));
        self.secrets
            .lock()
            .expect("resolver lock")
            .get(&(tenant_id.to_owned(), kind, scope.to_owned()))
            .cloned()
            .ok_or_else(|| SecretResolutionError::NotFound {
                kind,
                scope: scope.to_owned(),
            })
    }
}

/// Constructs a service and retains clones for post-create assertions.
fn service() -> (
    DatabaseService<MemoryRepository, MemoryResolver>,
    MemoryRepository,
    MemoryResolver,
) {
    let repository = MemoryRepository::default();
    let resolver = MemoryResolver::default();
    (
        DatabaseService::new(repository.clone(), resolver.clone()),
        repository,
        resolver,
    )
}

#[tokio::test]
async fn managed_lakehouse_persists_database_specific_warehouse_without_secrets() {
    let (service, repository, resolver) = service();
    let record = service
        .create(
            CreateDatabase::new("analytics", DatabaseKind::Lakehouse)
                .plan("tenant-a")
                .expect("plan"),
        )
        .await
        .expect("create");

    assert_eq!(record.warehouse(), Some("analytics"));
    assert_eq!(record.storage_secret_id(), None);
    assert_eq!(record.catalog_secret_id(), None);
    assert!(resolver.calls().is_empty());
    assert_eq!(
        repository
            .get("tenant-a", "analytics")
            .await
            .expect("get")
            .expect("stored")
            .id(),
        record.id()
    );
}

#[tokio::test]
async fn managed_postgres_is_independent_and_does_not_resolve_storage() {
    let (service, repository, resolver) = service();
    let record = service
        .create(
            CreateDatabase::new("my_test_db", DatabaseKind::Postgres)
                .plan("tenant-a")
                .expect("plan"),
        )
        .await
        .expect("create");

    assert_eq!(record.kind(), DatabaseKind::Postgres);
    assert_eq!(record.warehouse(), None);
    assert_eq!(record.storage_secret_id(), None);
    assert_eq!(record.catalog_secret_id(), None);
    assert!(resolver.calls().is_empty());
    assert_eq!(
        repository
            .get("tenant-a", "my_test_db")
            .await
            .expect("get")
            .expect("stored")
            .id(),
        record.id()
    );
}

#[tokio::test]
async fn byo_storage_persists_resolved_s3_secret_and_managed_warehouse() {
    let (service, repository, resolver) = service();
    resolver.add(
        "tenant-a",
        ScopedSecretKind::S3,
        "s3://customer-bucket/team",
        "secret-s3-1",
    );
    let record = service
        .create(
            CreateDatabase::new("customer_lake", DatabaseKind::Lakehouse)
                .with_data_path("s3://customer-bucket/team")
                .plan("tenant-a")
                .expect("plan"),
        )
        .await
        .expect("create");

    assert_eq!(record.storage_secret_id(), Some("secret-s3-1"));
    assert_eq!(record.catalog_secret_id(), None);
    assert_eq!(record.warehouse(), Some("customer_lake"));
    assert_eq!(resolver.calls().len(), 1);
    assert_eq!(
        repository
            .get("tenant-a", "customer_lake")
            .await
            .expect("get")
            .expect("stored")
            .storage_secret_id(),
        Some("secret-s3-1")
    );
}

#[tokio::test]
async fn external_lakehouse_persists_both_secret_ids_and_explicit_warehouse() {
    let (service, repository, resolver) = service();
    resolver.add(
        "tenant-a",
        ScopedSecretKind::S3,
        "s3://customer-bucket/team",
        "secret-s3-1",
    );
    resolver.add(
        "tenant-a",
        ScopedSecretKind::IcebergRest,
        "https://catalog.customer.com",
        "secret-catalog-1",
    );
    let record = service
        .create(
            CreateDatabase::new("external_lake", DatabaseKind::Lakehouse)
                .with_data_path("s3://customer-bucket/team")
                .with_catalog("https://catalog.customer.com")
                .with_warehouse("customer_warehouse")
                .plan("tenant-a")
                .expect("plan"),
        )
        .await
        .expect("create");

    assert_eq!(record.storage_secret_id(), Some("secret-s3-1"));
    assert_eq!(record.catalog_secret_id(), Some("secret-catalog-1"));
    assert_eq!(record.warehouse(), Some("customer_warehouse"));
    assert_eq!(resolver.calls().len(), 2);
    let stored = repository
        .get("tenant-a", "external_lake")
        .await
        .expect("get")
        .expect("stored");
    assert_eq!(stored.id(), record.id());
    assert_eq!(stored.catalog_secret_id(), Some("secret-catalog-1"));
}

#[tokio::test]
async fn duplicate_name_fails_closed_and_preserves_the_original_stable_id() {
    let (service, repository, _) = service();
    let plan = || {
        CreateDatabase::new("analytics", DatabaseKind::Lakehouse)
            .plan("tenant-a")
            .expect("plan")
    };
    let original = service.create(plan()).await.expect("first create");
    let error = service
        .create(plan())
        .await
        .expect_err("duplicate must fail");
    assert!(matches!(error, DatabaseServiceError::Duplicate { .. }));
    let stored = repository
        .get("tenant-a", "analytics")
        .await
        .expect("get")
        .expect("stored");
    assert_eq!(stored.id(), original.id());
}

#[tokio::test]
async fn lists_only_the_requested_tenant_in_stable_name_order() {
    let (service, _, _) = service();
    for (tenant, name, kind) in [
        ("tenant-a", "zeta", DatabaseKind::Postgres),
        ("tenant-b", "hidden", DatabaseKind::Postgres),
        ("tenant-a", "analytics", DatabaseKind::Lakehouse),
    ] {
        service
            .create(CreateDatabase::new(name, kind).plan(tenant).expect("plan"))
            .await
            .expect("create");
    }

    let databases = service.list("tenant-a").await.expect("list");

    assert_eq!(
        databases
            .iter()
            .map(|database| database.name())
            .collect::<Vec<_>>(),
        vec!["analytics", "zeta"]
    );
}

#[tokio::test]
async fn gets_and_deletes_by_tenant_local_name_without_cross_tenant_access() {
    let (service, repository, _) = service();
    service
        .create(
            CreateDatabase::new("analytics", DatabaseKind::Lakehouse)
                .plan("tenant-a")
                .expect("plan"),
        )
        .await
        .expect("create");

    assert!(matches!(
        service.get("tenant-b", "analytics").await,
        Err(DatabaseServiceError::NotFound { .. })
    ));
    let database = service.get("tenant-a", "analytics").await.expect("get");
    assert_eq!(database.name(), "analytics");

    assert!(matches!(
        service.delete("tenant-b", "analytics").await,
        Err(DatabaseServiceError::NotFound { .. })
    ));
    assert!(
        repository
            .get("tenant-a", "analytics")
            .await
            .expect("repository get")
            .is_some()
    );

    service
        .delete("tenant-a", "analytics")
        .await
        .expect("delete");
    assert!(
        repository
            .get("tenant-a", "analytics")
            .await
            .expect("repository get")
            .is_none()
    );
    assert!(matches!(
        service.delete("tenant-a", "analytics").await,
        Err(DatabaseServiceError::NotFound { .. })
    ));
}
