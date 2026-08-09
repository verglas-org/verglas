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

    async fn list(&self, tenant_id: &str) -> Result<Vec<DatabaseRecord>, RepositoryError> {
        Ok(self
            .0
            .lock()
            .expect("lock")
            .iter()
            .filter(|database| database.tenant_id() == tenant_id)
            .cloned()
            .collect())
    }

    async fn delete(&self, tenant_id: &str, name: &str) -> Result<bool, RepositoryError> {
        let mut records = self.0.lock().expect("lock");
        let Some(index) = records
            .iter()
            .position(|database| database.tenant_id() == tenant_id && database.name() == name)
        else {
            return Ok(false);
        };
        records.remove(index);
        Ok(true)
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
    assert_eq!(
        record,
        serde_json::json!({
            "type": "lakehouse",
            "name": "external_lake",
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
    );
}

#[tokio::test]
async fn database_collection_and_item_routes_are_tenant_scoped() {
    let service = Arc::new(DatabaseService::new(MemoryRepository::default(), Resolver));
    let app = verglas_rest::database::router(service, "tenant-a".to_owned());
    for body in [
        serde_json::json!({
            "type": "postgres",
            "name": "warehouse",
            "engine": { "mode": "managed-neon" }
        }),
        serde_json::json!({
            "type": "lakehouse",
            "name": "analytics",
            "storage": { "mode": "managed" },
            "catalog": { "mode": "managed-lakekeeper" }
        }),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::post("/v1/databases")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::CREATED);
    }

    let response = app
        .clone()
        .oneshot(
            Request::get("/v1/databases")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let list: serde_json::Value = serde_json::from_slice(&body).expect("list");
    assert_eq!(list["databases"].as_array().expect("databases").len(), 2);
    assert_eq!(list["databases"][0]["name"], "analytics");
    assert_eq!(list["databases"][1]["name"], "warehouse");

    let response = app
        .clone()
        .oneshot(
            Request::get("/v1/databases/warehouse")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let database: serde_json::Value = serde_json::from_slice(&body).expect("database");
    assert_eq!(
        database,
        serde_json::json!({
            "type": "postgres",
            "name": "warehouse",
            "engine": { "mode": "managed-neon" }
        })
    );

    let response = app
        .clone()
        .oneshot(
            Request::delete("/v1/databases/warehouse")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    for method in ["GET", "DELETE"] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri("/v1/databases/warehouse")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
