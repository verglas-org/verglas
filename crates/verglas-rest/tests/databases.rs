//! Tenant-local database creation API composition (#84).

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;
use verglas_database::{
    DatabaseRecord, DatabaseRepository, DatabaseService, RepositoryError, ScopedSecretKind,
    ScopedSecretResolver, SecretResolutionError,
};

#[derive(Debug, Default)]
struct MemoryRepository(Mutex<Vec<DatabaseRecord>>);

#[async_trait]
impl DatabaseRepository for MemoryRepository {
    async fn insert(&self, database: DatabaseRecord) -> Result<(), RepositoryError> {
        self.0.lock().expect("lock").push(database);
        Ok(())
    }

    async fn get(
        &self,
        tenant_id: &str,
        name: &str,
    ) -> Result<Option<DatabaseRecord>, RepositoryError> {
        Ok(self
            .0
            .lock()
            .expect("lock")
            .iter()
            .find(|database| database.tenant_id() == tenant_id && database.name() == name)
            .cloned())
    }
}

#[derive(Debug)]
struct Resolver;

#[async_trait]
impl ScopedSecretResolver for Resolver {
    async fn resolve_secret_id(
        &self,
        _tenant_id: &str,
        kind: ScopedSecretKind,
        _scope: &str,
    ) -> Result<String, SecretResolutionError> {
        Ok(match kind {
            ScopedSecretKind::S3 => "customer_s3",
            ScopedSecretKind::IcebergRest => "customer_catalog",
        }
        .to_owned())
    }
}

#[tokio::test]
async fn create_database_injects_tenant_and_persists_resolved_binding_ids() {
    let service = Arc::new(DatabaseService::new(MemoryRepository::default(), Resolver));
    let app = verglas_rest::database::router(service, "tenant-a".to_owned());
    let response = app
        .oneshot(
            Request::post("/v1/databases")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "name": "external_lake",
                        "type": "lakehouse",
                        "storage": {
                            "mode": "scoped-secret",
                            "data_path": "s3://customer-bucket/team"
                        },
                        "catalog": {
                            "mode": "external",
                            "uri": "https://catalog.customer.com",
                            "warehouse": "customer_warehouse"
                        }
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::CREATED);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let record: serde_json::Value = serde_json::from_slice(&body).expect("record");
    assert_eq!(record["tenant_id"], "tenant-a");
    assert_eq!(record["storage_secret_id"], "customer_s3");
    assert_eq!(record["catalog_secret_id"], "customer_catalog");
}
