use std::sync::{Arc, Mutex};

use axum::{Json, Router, extract::State, routing::post};
use lakekeeper::{
    api::RequestMetadataTestBuilder,
    service::{
        GenericTableId, NamespaceId, ProjectId, RoleId, ServerId, TableId, TagDefinitionId, ViewId,
        WarehouseId,
        authz::{Authorizer, NamespaceParent},
    },
};
use lakekeeper_authz_verglas::{VerglasAuthorizer, VerglasAuthzConfig};
use serde_json::{Value, json};

#[tokio::test]
async fn lifecycle_hooks_publish_every_resource_with_its_stable_parent() {
    let seen = Arc::new(Mutex::new(Vec::<Value>::new()));
    let app = Router::new()
        .route(
            "/v1/access/policy/resources",
            post(
                |State(seen): State<Arc<Mutex<Vec<Value>>>>, Json(body): Json<Value>| async move {
                    seen.lock()
                        .expect("seen lock")
                        .push(json!({"resource": body}));
                    Json(json!({"synced": true}))
                },
            ),
        )
        .with_state(seen.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind sync service");
    let endpoint = url::Url::parse(&format!(
        "http://{}/",
        listener.local_addr().expect("local address")
    ))
    .expect("endpoint");
    tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });
    let credentials = tempfile::NamedTempFile::new().expect("credential file");
    std::fs::write(credentials.path(), "policy-token").expect("credential");
    let authorizer = VerglasAuthorizer::try_new(
        ServerId::new_random(),
        VerglasAuthzConfig {
            endpoint,
            workload_credential_file: credentials.path().to_owned(),
            tenant_id: "tenant-a".to_owned(),
        },
    )
    .expect("authorizer");
    let metadata = RequestMetadataTestBuilder::builder()
        .verglas_database_id("db-analytics")
        .build();
    let project = ProjectId::new_random();
    let warehouse = WarehouseId::new_random();
    let root_namespace = NamespaceId::new_random();
    let child_namespace = NamespaceId::new_random();
    let table = TableId::new_random();
    let view = ViewId::new_random();
    let generic_table = GenericTableId::new_random();
    let role = RoleId::new_random();
    let tag = TagDefinitionId::new_random();
    authorizer
        .create_project(&metadata, &project)
        .await
        .expect("project");
    authorizer
        .create_warehouse(&metadata, warehouse, &project)
        .await
        .expect("warehouse");
    authorizer
        .create_namespace(
            &metadata,
            root_namespace,
            NamespaceParent::Warehouse(warehouse),
        )
        .await
        .expect("root namespace");
    authorizer
        .create_namespace(
            &metadata,
            child_namespace,
            NamespaceParent::Namespace(root_namespace),
        )
        .await
        .expect("child namespace");
    authorizer
        .create_table(&metadata, warehouse, table, child_namespace)
        .await
        .expect("table");
    authorizer
        .create_view(&metadata, warehouse, view, child_namespace)
        .await
        .expect("view");
    authorizer
        .create_generic_table(&metadata, warehouse, generic_table, child_namespace)
        .await
        .expect("generic table");
    authorizer
        .create_role(&metadata, role, Arc::new(project.clone()))
        .await
        .expect("role");
    authorizer
        .create_tag(&metadata, tag, Arc::new(project.clone()))
        .await
        .expect("tag");

    let root = "lakekeeper";
    let project_resource = format!("{root}/project/{project}");
    let warehouse_resource = format!("warehouse/{warehouse}");
    let root_namespace_resource = format!("namespace/{root_namespace}");
    let child_namespace_resource = format!("namespace/{child_namespace}");
    assert_eq!(
        seen.lock().expect("seen lock").as_slice(),
        &[
            json!({"resource":{"id":project_resource,"kind":"project","parent_id":root}}),
            json!({"resource":{"id":warehouse_resource,"kind":"warehouse","parent_id":"database/db-analytics"}}),
            json!({"resource":{"id":root_namespace_resource,"kind":"namespace","parent_id":warehouse_resource}}),
            json!({"resource":{"id":child_namespace_resource,"kind":"namespace","parent_id":root_namespace_resource}}),
            json!({"resource":{"id":format!("warehouse/{warehouse}/table/{table}"),"kind":"table","parent_id":child_namespace_resource}}),
            json!({"resource":{"id":format!("warehouse/{warehouse}/view/{view}"),"kind":"view","parent_id":child_namespace_resource}}),
            json!({"resource":{"id":format!("warehouse/{warehouse}/generic-table/{generic_table}"),"kind":"generic_table","parent_id":child_namespace_resource}}),
            json!({"resource":{"id":format!("{root}/role/{role}"),"kind":"role","parent_id":project_resource}}),
            json!({"resource":{"id":format!("{root}/tag/{tag}"),"kind":"tag","parent_id":project_resource}}),
        ]
    );
}

#[tokio::test]
async fn lifecycle_sync_failure_aborts_the_hook() {
    let app = Router::new().route(
        "/v1/access/policy/resources",
        post(|| async { axum::http::StatusCode::FORBIDDEN }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind sync service");
    let endpoint = url::Url::parse(&format!(
        "http://{}/",
        listener.local_addr().expect("local address")
    ))
    .expect("endpoint");
    tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });
    let credentials = tempfile::NamedTempFile::new().expect("credential file");
    std::fs::write(credentials.path(), "policy-token").expect("credential");
    let authorizer = VerglasAuthorizer::try_new(
        ServerId::new_random(),
        VerglasAuthzConfig {
            endpoint,
            workload_credential_file: credentials.path().to_owned(),
            tenant_id: "tenant-a".to_owned(),
        },
    )
    .expect("authorizer");
    let error = authorizer
        .create_project(
            &RequestMetadataTestBuilder::builder().build(),
            &ProjectId::new_random(),
        )
        .await
        .expect_err("forbidden sync fails closed");
    assert_eq!(error.error.code, 503);
}

#[tokio::test]
async fn delete_hooks_remove_the_exact_resource_projections() {
    let seen = Arc::new(Mutex::new(Vec::<Value>::new()));
    let app = Router::new()
        .route(
            "/v1/access/policy/resources/{id}",
            axum::routing::delete(
                |State(seen): State<Arc<Mutex<Vec<Value>>>>,
                 axum::extract::Path(id): axum::extract::Path<String>| async move {
                    seen.lock()
                        .expect("seen lock")
                        .push(json!({"resource": id}));
                    Json(json!({"deleted": true}))
                },
            ),
        )
        .with_state(seen.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind sync service");
    let endpoint = url::Url::parse(&format!(
        "http://{}/",
        listener.local_addr().expect("local address")
    ))
    .expect("endpoint");
    tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });
    let credentials = tempfile::NamedTempFile::new().expect("credential file");
    std::fs::write(credentials.path(), "policy-token").expect("credential");
    let authorizer = VerglasAuthorizer::try_new(
        ServerId::new_random(),
        VerglasAuthzConfig {
            endpoint,
            workload_credential_file: credentials.path().to_owned(),
            tenant_id: "tenant-a".to_owned(),
        },
    )
    .expect("authorizer");
    let metadata = RequestMetadataTestBuilder::builder().build();
    let project = ProjectId::new_random();
    let warehouse = WarehouseId::new_random();
    let namespace = NamespaceId::new_random();
    let table = TableId::new_random();
    let view = ViewId::new_random();
    let generic_table = GenericTableId::new_random();
    let role = RoleId::new_random();
    let tag = TagDefinitionId::new_random();
    authorizer
        .delete_role(&metadata, role)
        .await
        .expect("delete role");
    authorizer
        .delete_tag(&metadata, tag)
        .await
        .expect("delete tag");
    authorizer
        .delete_table(warehouse, table)
        .await
        .expect("delete table");
    authorizer
        .delete_view(warehouse, view)
        .await
        .expect("delete view");
    authorizer
        .delete_generic_table(warehouse, generic_table)
        .await
        .expect("delete generic table");
    authorizer
        .delete_namespace(&metadata, namespace)
        .await
        .expect("delete namespace");
    authorizer
        .delete_warehouse(&metadata, warehouse)
        .await
        .expect("delete warehouse");
    authorizer
        .delete_project(&metadata, &project)
        .await
        .expect("delete project");

    let root = "lakekeeper";
    assert_eq!(
        seen.lock().expect("seen lock").as_slice(),
        &[
            json!({"resource":format!("{root}/role/{role}")}),
            json!({"resource":format!("{root}/tag/{tag}")}),
            json!({"resource":format!("warehouse/{warehouse}/table/{table}")}),
            json!({"resource":format!("warehouse/{warehouse}/view/{view}")}),
            json!({"resource":format!("warehouse/{warehouse}/generic-table/{generic_table}")}),
            json!({"resource":format!("namespace/{namespace}")}),
            json!({"resource":format!("warehouse/{warehouse}")}),
            json!({"resource":format!("{root}/project/{project}")}),
        ]
    );
}
