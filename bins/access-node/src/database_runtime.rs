//! Transactional database declaration and managed runtime reconciliation.
//!
//! A create call is successful only after its managed Lakekeeper or Neon
//! runtime is usable. Failed provisioning removes the just-created declaration;
//! startup recovery reasserts every durable declaration after process restarts
//! without allowing one unavailable database to take the tenant API offline.

use std::sync::Arc;

use async_trait::async_trait;
use verglas_database::{
    CatalogRequest, DatabaseManager, DatabasePlan, DatabaseServiceError, DatabaseView,
    StorageRequest,
};

use crate::lakehouse_runtime::LakekeeperProvisioner;

/// Managed Lakehouse lifecycle required by the database manager.
#[async_trait]
pub(crate) trait ManagedLakehouseRuntime: Send + Sync {
    /// Initializes the tenant catalog service before database reconciliation.
    async fn bootstrap(&self) -> Result<(), DatabaseServiceError>;

    /// Makes one managed database warehouse usable.
    async fn ensure_database(&self, name: &str) -> Result<(), DatabaseServiceError>;

    /// Removes one managed database warehouse.
    async fn delete_database(&self, name: &str) -> Result<(), DatabaseServiceError>;
}

/// Managed Postgres lifecycle required by the database manager.
#[async_trait]
pub(crate) trait ManagedPostgresRuntime: Send + Sync {
    /// Makes one managed Neon database usable.
    async fn ensure_database(&self, name: &str) -> Result<(), DatabaseServiceError>;

    /// Removes one managed Neon database runtime.
    async fn delete_database(&self, name: &str) -> Result<(), DatabaseServiceError>;
}

#[async_trait]
impl ManagedLakehouseRuntime for LakekeeperProvisioner {
    /// Bootstraps Lakekeeper's default project idempotently.
    async fn bootstrap(&self) -> Result<(), DatabaseServiceError> {
        LakekeeperProvisioner::bootstrap(self).await
    }

    /// Ensures the database-specific Lakekeeper warehouse exists.
    async fn ensure_database(&self, name: &str) -> Result<(), DatabaseServiceError> {
        self.ensure_warehouse(name).await
    }

    /// Deletes the database-specific Lakekeeper warehouse.
    async fn delete_database(&self, name: &str) -> Result<(), DatabaseServiceError> {
        self.delete_warehouse(name).await
    }
}

/// Database manager that couples durable declarations to their managed runtimes.
pub(crate) struct ProvisioningDatabaseManager<L, P> {
    inner: Arc<dyn DatabaseManager>,
    lakehouse: L,
    postgres: P,
}

impl<L, P> ProvisioningDatabaseManager<L, P>
where
    L: ManagedLakehouseRuntime,
    P: ManagedPostgresRuntime,
{
    /// Wraps the durable database service with mandatory managed provisioners.
    pub(crate) fn new(inner: Arc<dyn DatabaseManager>, lakehouse: L, postgres: P) -> Self {
        Self {
            inner,
            lakehouse,
            postgres,
        }
    }

    /// Reasserts every durable managed database and returns isolated failures.
    pub(crate) async fn recover(
        &self,
        tenant_id: &str,
    ) -> Result<Vec<String>, DatabaseServiceError> {
        let mut failures = Vec::new();
        if let Err(error) = self.lakehouse.bootstrap().await {
            failures.push(format!("managed lakehouse: {error}"));
            return Ok(failures);
        }
        for database in self.inner.list_databases(tenant_id).await? {
            if let Err(error) = self.ensure_view(&database).await {
                failures.push(format!("{}: {error}", database.name()));
            }
        }
        Ok(failures)
    }

    /// Reconciles the managed runtime represented by one public definition.
    async fn ensure_view(&self, database: &DatabaseView) -> Result<(), DatabaseServiceError> {
        match database {
            DatabaseView::Lakehouse {
                name,
                storage: StorageRequest::Managed,
                catalog: CatalogRequest::ManagedLakekeeper,
            } => self.lakehouse.ensure_database(name).await,
            DatabaseView::Postgres { name, .. } => self.postgres.ensure_database(name).await,
            DatabaseView::Lakehouse { name, .. } => {
                Err(DatabaseServiceError::Provisioning(format!(
                    "database {name} requires a scoped external runtime binding that is not active"
                )))
            }
        }
    }

    /// Removes the managed runtime represented by one public definition.
    async fn delete_view(&self, database: &DatabaseView) -> Result<(), DatabaseServiceError> {
        match database {
            DatabaseView::Lakehouse {
                name,
                storage: StorageRequest::Managed,
                catalog: CatalogRequest::ManagedLakekeeper,
            } => self.lakehouse.delete_database(name).await,
            DatabaseView::Postgres { name, .. } => self.postgres.delete_database(name).await,
            DatabaseView::Lakehouse { name, .. } => {
                Err(DatabaseServiceError::Provisioning(format!(
                    "database {name} requires a scoped external runtime binding that is not active"
                )))
            }
        }
    }

    /// Rejects declarations whose secret-bearing runtime binding is not implemented.
    fn validate_runtime(plan: &DatabasePlan) -> Result<(), DatabaseServiceError> {
        let Some(lakehouse) = plan.lakehouse() else {
            return Ok(());
        };
        if lakehouse.data_path().is_some() || lakehouse.catalog_uri().is_some() {
            return Err(DatabaseServiceError::Provisioning(format!(
                "database {} requires a scoped external runtime binding that is not active",
                plan.name()
            )));
        }
        Ok(())
    }
}

#[async_trait]
impl<L, P> DatabaseManager for ProvisioningDatabaseManager<L, P>
where
    L: ManagedLakehouseRuntime,
    P: ManagedPostgresRuntime,
{
    /// Persists a validated declaration, reconciles its runtime, and rolls back on failure.
    async fn create_database(
        &self,
        plan: DatabasePlan,
    ) -> Result<DatabaseView, DatabaseServiceError> {
        Self::validate_runtime(&plan)?;
        let tenant_id = plan.tenant_id().to_owned();
        let name = plan.name().to_owned();
        let database = self.inner.create_database(plan).await?;
        if let Err(provisioning) = self.ensure_view(&database).await {
            if let Err(rollback) = self.inner.delete_database(&tenant_id, &name).await {
                return Err(DatabaseServiceError::Provisioning(format!(
                    "{provisioning}; declaration rollback failed: {rollback}"
                )));
            }
            return Err(provisioning);
        }
        Ok(database)
    }

    /// Lists the durable declarations already reconciled by startup recovery.
    async fn list_databases(
        &self,
        tenant_id: &str,
    ) -> Result<Vec<DatabaseView>, DatabaseServiceError> {
        self.inner.list_databases(tenant_id).await
    }

    /// Gets one durable declaration already reconciled by startup recovery.
    async fn get_database(
        &self,
        tenant_id: &str,
        name: &str,
    ) -> Result<DatabaseView, DatabaseServiceError> {
        self.inner.get_database(tenant_id, name).await
    }

    /// Removes the managed runtime before deleting its durable declaration.
    async fn delete_database(
        &self,
        tenant_id: &str,
        name: &str,
    ) -> Result<(), DatabaseServiceError> {
        let database = self.inner.get_database(tenant_id, name).await?;
        self.delete_view(&database).await?;
        self.inner.delete_database(tenant_id, name).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use verglas_database::{
        CatalogRequest, CreateDatabase, DatabaseKind, PostgresEngineRequest, StorageRequest,
    };

    use super::*;

    /// In-memory durable manager used to observe rollback ordering.
    #[derive(Default)]
    struct MemoryManager {
        databases: Mutex<Vec<DatabaseView>>,
        events: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl DatabaseManager for MemoryManager {
        /// Persists one declaration in memory.
        async fn create_database(
            &self,
            plan: DatabasePlan,
        ) -> Result<DatabaseView, DatabaseServiceError> {
            let view = match plan.kind() {
                DatabaseKind::Lakehouse => DatabaseView::Lakehouse {
                    name: plan.name().to_owned(),
                    storage: StorageRequest::Managed,
                    catalog: CatalogRequest::ManagedLakekeeper,
                },
                DatabaseKind::Postgres => DatabaseView::Postgres {
                    name: plan.name().to_owned(),
                    engine: PostgresEngineRequest::ManagedNeon,
                },
            };
            self.databases.lock().expect("databases").push(view.clone());
            Ok(view)
        }

        /// Lists the in-memory declarations.
        async fn list_databases(
            &self,
            _tenant_id: &str,
        ) -> Result<Vec<DatabaseView>, DatabaseServiceError> {
            Ok(self.databases.lock().expect("databases").clone())
        }

        /// Gets one in-memory declaration.
        async fn get_database(
            &self,
            _tenant_id: &str,
            name: &str,
        ) -> Result<DatabaseView, DatabaseServiceError> {
            self.databases
                .lock()
                .expect("databases")
                .iter()
                .find(|database| database.name() == name)
                .cloned()
                .ok_or_else(|| DatabaseServiceError::NotFound {
                    tenant_id: "tenant-a".to_owned(),
                    name: name.to_owned(),
                })
        }

        /// Deletes one in-memory declaration.
        async fn delete_database(
            &self,
            _tenant_id: &str,
            name: &str,
        ) -> Result<(), DatabaseServiceError> {
            self.events
                .lock()
                .expect("events")
                .push(format!("record:{name}"));
            self.databases
                .lock()
                .expect("databases")
                .retain(|database| database.name() != name);
            Ok(())
        }
    }

    /// Deterministic managed runtime used by lifecycle tests.
    #[derive(Default)]
    struct FakeRuntime {
        bootstrap_fail: bool,
        fail: bool,
        ensured: Mutex<Vec<String>>,
        deleted: Mutex<Vec<String>>,
        events: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl ManagedLakehouseRuntime for FakeRuntime {
        /// Accepts bootstrap in tests.
        async fn bootstrap(&self) -> Result<(), DatabaseServiceError> {
            if self.bootstrap_fail {
                return Err(DatabaseServiceError::Provisioning(
                    "catalog unavailable".to_owned(),
                ));
            }
            Ok(())
        }

        /// Records or rejects a managed runtime ensure.
        async fn ensure_database(&self, name: &str) -> Result<(), DatabaseServiceError> {
            if self.fail {
                return Err(DatabaseServiceError::Provisioning("failed".to_owned()));
            }
            self.ensured.lock().expect("ensured").push(name.to_owned());
            Ok(())
        }

        /// Records managed runtime deletion.
        async fn delete_database(&self, name: &str) -> Result<(), DatabaseServiceError> {
            self.deleted.lock().expect("deleted").push(name.to_owned());
            self.events
                .lock()
                .expect("events")
                .push(format!("runtime:{name}"));
            Ok(())
        }
    }

    #[async_trait]
    impl ManagedPostgresRuntime for FakeRuntime {
        /// Records or rejects a managed runtime ensure.
        async fn ensure_database(&self, name: &str) -> Result<(), DatabaseServiceError> {
            ManagedLakehouseRuntime::ensure_database(self, name).await
        }

        /// Records managed runtime deletion.
        async fn delete_database(&self, name: &str) -> Result<(), DatabaseServiceError> {
            ManagedLakehouseRuntime::delete_database(self, name).await
        }
    }

    #[tokio::test]
    async fn failed_runtime_provisioning_removes_the_new_declaration() {
        let inner = Arc::new(MemoryManager::default());
        let manager = ProvisioningDatabaseManager::new(
            inner.clone(),
            FakeRuntime {
                fail: true,
                ..FakeRuntime::default()
            },
            FakeRuntime::default(),
        );
        let plan = CreateDatabase::new("analytics", DatabaseKind::Lakehouse)
            .plan("tenant-a")
            .expect("plan");

        manager
            .create_database(plan)
            .await
            .expect_err("provisioning must fail");

        assert!(
            inner
                .list_databases("tenant-a")
                .await
                .expect("list")
                .is_empty(),
            "inactive database definitions must be rolled back"
        );
    }

    #[tokio::test]
    async fn delete_removes_the_runtime_before_the_durable_record() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let inner = Arc::new(MemoryManager {
            events: events.clone(),
            ..MemoryManager::default()
        });
        let manager = ProvisioningDatabaseManager::new(
            inner,
            FakeRuntime {
                events: events.clone(),
                ..FakeRuntime::default()
            },
            FakeRuntime::default(),
        );
        let plan = CreateDatabase::new("analytics", DatabaseKind::Lakehouse)
            .plan("tenant-a")
            .expect("plan");
        manager.create_database(plan).await.expect("create");

        manager
            .delete_database("tenant-a", "analytics")
            .await
            .expect("delete");

        assert_eq!(
            events.lock().expect("events").as_slice(),
            ["runtime:analytics", "record:analytics"]
        );
    }

    #[tokio::test]
    async fn startup_reconciles_persisted_postgres_definitions() {
        let inner = Arc::new(MemoryManager::default());
        let postgres_plan = CreateDatabase::new("operational", DatabaseKind::Postgres)
            .plan("tenant-a")
            .expect("plan");
        inner
            .create_database(postgres_plan)
            .await
            .expect("persisted database");
        let manager =
            ProvisioningDatabaseManager::new(inner, FakeRuntime::default(), FakeRuntime::default());

        assert!(
            manager
                .recover("tenant-a")
                .await
                .expect("recovery")
                .is_empty()
        );

        assert_eq!(
            manager.postgres.ensured.lock().expect("ensured").as_slice(),
            ["operational"]
        );
    }

    #[tokio::test]
    async fn startup_isolates_one_unavailable_database_runtime() {
        let inner = Arc::new(MemoryManager::default());
        let postgres_plan = CreateDatabase::new("operational", DatabaseKind::Postgres)
            .plan("tenant-a")
            .expect("plan");
        inner
            .create_database(postgres_plan)
            .await
            .expect("persisted database");
        let manager = ProvisioningDatabaseManager::new(
            inner,
            FakeRuntime::default(),
            FakeRuntime {
                fail: true,
                ..FakeRuntime::default()
            },
        );

        let failures = manager.recover("tenant-a").await.expect("recovery");

        assert_eq!(
            failures,
            ["operational: database runtime provisioning failed: failed"]
        );
    }

    #[tokio::test]
    async fn startup_retries_an_unavailable_managed_catalog() {
        let manager = ProvisioningDatabaseManager::new(
            Arc::new(MemoryManager::default()),
            FakeRuntime {
                bootstrap_fail: true,
                ..FakeRuntime::default()
            },
            FakeRuntime::default(),
        );

        let failures = manager.recover("tenant-a").await.expect("recovery");

        assert_eq!(
            failures,
            ["managed lakehouse: database runtime provisioning failed: catalog unavailable"]
        );
    }
}
