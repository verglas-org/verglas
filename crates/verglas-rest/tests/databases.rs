//! Tenant-local database creation API composition (#84).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;
use verglas_authz::{
    AccessTokenService, AccessTokenSigner, Authorizer, MemoryAccessTokenRegistry, MemoryAuthorizer,
    Principal, PrincipalKind, Resource, ResourceKind,
};
use verglas_database::{
    DatabaseKind, DatabaseManager, DatabasePlan, DatabaseRecord, DatabaseRepository,
    DatabaseService, DatabaseServiceError, DatabaseView, RepositoryError, ScopedSecretKind,
    ScopedSecretResolver, SecretResolutionError,
};
use verglas_rest::data_plane::AuthenticatedPrincipal;
use verglas_rest::database::{DatabaseAuthorization, DatabaseAuthorizationError};

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

#[derive(Debug, Default)]
struct RecordingAuthorization(Mutex<Vec<String>>);

#[async_trait]
impl DatabaseAuthorization for RecordingAuthorization {
    async fn create_database_resource(
        &self,
        principal: &AuthenticatedPrincipal,
        database: &str,
        kind: DatabaseKind,
    ) -> Result<(), DatabaseAuthorizationError> {
        self.0.lock().expect("lock").push(format!(
            "create:{}/{database}:{}:{kind:?}",
            principal.tenant_id, principal.principal_id,
        ));
        Ok(())
    }

    async fn delete_database_resource(
        &self,
        principal: &AuthenticatedPrincipal,
        database: &str,
    ) -> Result<(), DatabaseAuthorizationError> {
        self.0.lock().expect("lock").push(format!(
            "delete:{}/{database}:{}",
            principal.tenant_id, principal.principal_id
        ));
        Ok(())
    }
}

fn principal() -> AuthenticatedPrincipal {
    AuthenticatedPrincipal {
        tenant_id: "tenant-a".to_owned(),
        principal_id: "user/owner@example.com".to_owned(),
        token_id: "token-1".to_owned(),
        audience: "data-plane".to_owned(),
    }
}

#[tokio::test]
async fn database_kind_grants_only_its_required_backend_service() {
    let authorizer = Arc::new(MemoryAuthorizer::new());
    authorizer
        .create_resource(Resource::new("tenant-a", "tenant", ResourceKind::Tenant))
        .await
        .expect("tenant resource");
    authorizer
        .create_principal(Principal::new(
            "tenant-a",
            "user/owner@example.com",
            PrincipalKind::User,
        ))
        .await
        .expect("creator principal");
    let runtime = verglas_rest::access::AccessHttpRuntime::new(
        authorizer.clone(),
        Arc::new(AccessTokenService::new(
            AccessTokenSigner::new([7; 32]),
            Arc::new(MemoryAccessTokenRegistry::new()),
        )),
        "tenant-a",
    );

    runtime
        .create_database_resource(&principal(), "lake", DatabaseKind::Lakehouse)
        .await
        .expect("lakehouse authorization");
    runtime
        .create_database_resource(&principal(), "pg", DatabaseKind::Postgres)
        .await
        .expect("postgres authorization");

    let grants = authorizer.list_grants("tenant-a").await.expect("grants");
    let lake_services: Vec<_> = grants
        .iter()
        .filter(|grant| grant.resource_id == "database/lake")
        .map(|grant| grant.principal_id.as_str())
        .collect();
    assert!(lake_services.contains(&"service/verglas-lakekeeper"));
    assert!(!lake_services.contains(&"service/verglas-neon"));
    let postgres_services: Vec<_> = grants
        .iter()
        .filter(|grant| grant.resource_id == "database/pg")
        .map(|grant| grant.principal_id.as_str())
        .collect();
    assert!(postgres_services.contains(&"service/verglas-neon"));
    assert!(!postgres_services.contains(&"service/verglas-lakekeeper"));
}

#[derive(Debug)]
struct FailingDatabaseManager;

#[async_trait]
impl DatabaseManager for FailingDatabaseManager {
    async fn create_database(
        &self,
        _plan: DatabasePlan,
    ) -> Result<DatabaseView, DatabaseServiceError> {
        Err(DatabaseServiceError::Provisioning(
            "test failure".to_owned(),
        ))
    }

    async fn list_databases(
        &self,
        _tenant_id: &str,
    ) -> Result<Vec<DatabaseView>, DatabaseServiceError> {
        Ok(Vec::new())
    }

    async fn get_database(
        &self,
        tenant_id: &str,
        name: &str,
    ) -> Result<DatabaseView, DatabaseServiceError> {
        Err(DatabaseServiceError::NotFound {
            tenant_id: tenant_id.to_owned(),
            name: name.to_owned(),
        })
    }

    async fn delete_database(
        &self,
        _tenant_id: &str,
        _name: &str,
    ) -> Result<(), DatabaseServiceError> {
        Ok(())
    }
}

#[derive(Debug, Default)]
struct TrackingDatabaseManager(AtomicBool);

#[async_trait]
impl DatabaseManager for TrackingDatabaseManager {
    async fn create_database(
        &self,
        plan: DatabasePlan,
    ) -> Result<DatabaseView, DatabaseServiceError> {
        self.0.store(true, Ordering::Release);
        Ok(DatabaseView::Postgres {
            id: "database-id".to_owned(),
            name: plan.name().to_owned(),
            engine: verglas_database::PostgresEngineRequest::ManagedNeon,
        })
    }

    async fn list_databases(
        &self,
        _tenant_id: &str,
    ) -> Result<Vec<DatabaseView>, DatabaseServiceError> {
        Ok(Vec::new())
    }

    async fn get_database(
        &self,
        tenant_id: &str,
        name: &str,
    ) -> Result<DatabaseView, DatabaseServiceError> {
        Err(DatabaseServiceError::NotFound {
            tenant_id: tenant_id.to_owned(),
            name: name.to_owned(),
        })
    }

    async fn delete_database(
        &self,
        _tenant_id: &str,
        _name: &str,
    ) -> Result<(), DatabaseServiceError> {
        self.0.store(false, Ordering::Release);
        Ok(())
    }
}

#[derive(Debug)]
struct FailingAuthorization;

#[async_trait]
impl DatabaseAuthorization for FailingAuthorization {
    async fn create_database_resource(
        &self,
        _principal: &AuthenticatedPrincipal,
        _database: &str,
        _kind: DatabaseKind,
    ) -> Result<(), DatabaseAuthorizationError> {
        Err(DatabaseAuthorizationError::new("test failure"))
    }

    async fn delete_database_resource(
        &self,
        _principal: &AuthenticatedPrincipal,
        _database: &str,
    ) -> Result<(), DatabaseAuthorizationError> {
        Ok(())
    }
}

#[tokio::test]
async fn failed_authorization_registration_rolls_back_the_database() {
    let service = Arc::new(TrackingDatabaseManager::default());
    let app = verglas_rest::database::router(
        service.clone(),
        Arc::new(FailingAuthorization),
        "tenant-a".to_owned(),
    )
    .layer(axum::Extension(principal()));
    let response = app
        .oneshot(
            Request::post("/v1/databases")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "type": "postgres",
                        "name": "warehouse",
                        "engine": { "mode": "managed-neon" }
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert!(!service.0.load(Ordering::Acquire));
}

#[tokio::test]
async fn failed_database_creation_never_creates_an_authorization_resource() {
    let authorization = Arc::new(RecordingAuthorization::default());
    let app = verglas_rest::database::router(
        Arc::new(FailingDatabaseManager),
        authorization.clone(),
        "tenant-a".to_owned(),
    )
    .layer(axum::Extension(principal()));
    let response = app
        .oneshot(
            Request::post("/v1/databases")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "type": "postgres",
                        "name": "warehouse",
                        "engine": { "mode": "managed-neon" }
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert!(authorization.0.lock().expect("lock").is_empty());
}

#[tokio::test]
async fn create_database_injects_tenant_and_persists_resolved_binding_ids() {
    let service = Arc::new(DatabaseService::new(MemoryRepository::default(), Resolver));
    let authorization = Arc::new(RecordingAuthorization::default());
    let app = verglas_rest::database::router(service, authorization.clone(), "tenant-a".to_owned())
        .layer(axum::Extension(principal()));
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
    assert_eq!(
        authorization.0.lock().expect("lock").as_slice(),
        ["create:tenant-a/external_lake:user/owner@example.com:Lakehouse"]
    );
}

#[tokio::test]
async fn database_collection_and_item_routes_are_tenant_scoped() {
    let service = Arc::new(DatabaseService::new(MemoryRepository::default(), Resolver));
    let authorization = Arc::new(RecordingAuthorization::default());
    let app = verglas_rest::database::router(service, authorization.clone(), "tenant-a".to_owned())
        .layer(axum::Extension(principal()));
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
    assert!(
        authorization
            .0
            .lock()
            .expect("lock")
            .contains(&"delete:tenant-a/warehouse:user/owner@example.com".to_owned())
    );

    let response = app
        .clone()
        .oneshot(
            Request::get("/v1/databases/warehouse")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}
