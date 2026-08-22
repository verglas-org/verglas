//! Exercises `VerglasAuthorizer` itself (not just `DecisionClient`) for the
//! resource families a standard pyiceberg session touches: warehouse config
//! (`GET /v1/config`, list namespaces) and namespace checks (create
//! namespace, describe namespace on load/create table). Proves the same
//! `warehouse/*` admin scope that already authorizes tables/views now also
//! authorizes namespaces, and that a foreign warehouse's admin scope does not.

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde_json::json;
use verglas_catalog_authz::{CredentialAuthzConfig, VerglasAuthorizer};
use verglas_catalog_core::{
    api::RequestMetadataTestBuilder,
    service::{
        NamespaceId, NamespaceWithParent, ResolvedWarehouse, ServerId, WarehouseId,
        authz::{Authorizer, CatalogNamespaceAction, CatalogWarehouseAction},
    },
};

const PRIVATE_KEY: &[u8] = br"-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgsHH3Wz16tzOPDx6s
vfReek+K6/PLMnCFA6aVCSOY8WuhRANCAAS8cXseHtyqf+PlE7kFmHafsWUmVtP2
XDMDVnDYF7ntzli7QbLSC93UbnQGCRhMceMUAmHITAlC6mt0rwuosDE+
-----END PRIVATE KEY-----";

const JWKS: &str = r#"{
  "keys": [{
    "kty": "EC",
    "crv": "P-256",
    "kid": "control-key-1",
    "alg": "ES256",
    "use": "sig",
    "x": "vHF7Hh7cqn_j5RO5BZh2n7FlJlbT9lwzA1Zw2Be57c4",
    "y": "WLtBstIL3dRudAYJGExx4xQCYchMCULqa3SvC6iwMT4"
  }]
}"#;

fn token(scope: serde_json::Value) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_secs();
    let claims = json!({
        "iss": "https://control.example.test",
        "aud": "catalog",
        "sub": "user/alice@example.com",
        "tenant_id": "tenant-a",
        "jti": "authorizer-acceptance",
        "exp": now + 300,
        "scope": scope,
    });
    let mut header = Header::new(Algorithm::ES256);
    header.kid = Some("control-key-1".to_owned());
    encode(
        &header,
        &claims,
        &EncodingKey::from_ec_pem(PRIVATE_KEY).expect("private key"),
    )
    .expect("credential")
}

fn authorizer() -> VerglasAuthorizer {
    VerglasAuthorizer::try_new(
        ServerId::new_random(),
        CredentialAuthzConfig {
            issuer: "https://control.example.test".to_owned(),
            jwks: JWKS.to_owned(),
            tenant_id: "tenant-a".to_owned(),
        },
    )
    .expect("authorizer")
}

/// `warehouse/*` admin — the control plane's only self-serve grant — must
/// authorize the config, list-namespaces, and create-namespace checks a
/// pyiceberg session makes against its own warehouse.
#[tokio::test]
async fn warehouse_admin_scope_authorizes_the_pyiceberg_session() {
    let warehouse = ResolvedWarehouse::new_random();
    let namespace =
        NamespaceWithParent::test_default(NamespaceId::new_random(), warehouse.warehouse_id);
    let metadata = RequestMetadataTestBuilder::builder()
        .verglas_bearer_token(token(
            json!([{"resource": "warehouse/*", "action": "admin"}]),
        ))
        .build();
    let authorizer = authorizer();

    let warehouse_decisions = authorizer
        .are_allowed_warehouse_actions_impl(
            &metadata,
            None,
            &[
                (&warehouse, CatalogWarehouseAction::GetConfig),
                (&warehouse, CatalogWarehouseAction::ListNamespaces),
                (
                    &warehouse,
                    CatalogWarehouseAction::CreateNamespace {
                        name: Some("orders".to_owned()),
                        properties: Arc::new(BTreeMap::new()),
                    },
                ),
            ],
        )
        .await
        .expect("warehouse checks");
    assert!(
        warehouse_decisions.iter().all(|d| d.allowed),
        "warehouse/* admin must authorize config, list-namespaces, and create-namespace"
    );

    let namespace_decisions = authorizer
        .are_allowed_namespace_actions_impl(
            &metadata,
            None,
            &warehouse,
            &HashMap::new(),
            &[
                (&namespace, CatalogNamespaceAction::GetMetadata),
                (
                    &namespace,
                    CatalogNamespaceAction::CreateTable {
                        name: Some("orders".to_owned()),
                        table_id: None,
                        properties: Arc::new(BTreeMap::new()),
                    },
                ),
            ],
        )
        .await
        .expect("namespace checks");
    assert!(
        namespace_decisions.iter().all(|d| d.allowed),
        "warehouse/* admin must authorize namespace describe and create-table-child checks"
    );
}

/// A grant rooted at a different warehouse must not reach either the config
/// check or the namespace checks for this warehouse.
#[tokio::test]
async fn foreign_warehouse_scope_denies_the_pyiceberg_session() {
    let warehouse = ResolvedWarehouse::new_random();
    let other_warehouse_id = WarehouseId::new_random();
    let namespace =
        NamespaceWithParent::test_default(NamespaceId::new_random(), warehouse.warehouse_id);
    let metadata = RequestMetadataTestBuilder::builder()
        .verglas_bearer_token(token(json!([
            {"resource": format!("warehouse/{other_warehouse_id}/*"), "action": "admin"}
        ])))
        .build();
    let authorizer = authorizer();

    let warehouse_decisions = authorizer
        .are_allowed_warehouse_actions_impl(
            &metadata,
            None,
            &[(&warehouse, CatalogWarehouseAction::GetConfig)],
        )
        .await
        .expect("warehouse check");
    assert!(
        warehouse_decisions.iter().all(|d| !d.allowed),
        "a foreign warehouse grant must not authorize this warehouse's config check"
    );

    let namespace_decisions = authorizer
        .are_allowed_namespace_actions_impl(
            &metadata,
            None,
            &warehouse,
            &HashMap::new(),
            &[(&namespace, CatalogNamespaceAction::GetMetadata)],
        )
        .await
        .expect("namespace check");
    assert!(
        namespace_decisions.iter().all(|d| !d.allowed),
        "a foreign warehouse grant must not authorize this warehouse's namespaces"
    );
}
