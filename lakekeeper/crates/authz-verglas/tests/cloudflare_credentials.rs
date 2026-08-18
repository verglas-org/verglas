//! Contract tests for local Cloudflare-issued credential verification.

use std::time::{SystemTime, UNIX_EPOCH};

use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use lakekeeper_authz_verglas::{DecisionClient, VerglasAction};
use serde_json::json;

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

/// Verifies a scoped token without any remote policy service.
#[tokio::test]
async fn verifies_cloudflare_jwks_scopes_locally() {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_secs();
    let claims = json!({
        "iss": "https://control.example.test",
        "aud": "lakekeeper",
        "sub": "user/alice@example.com",
        "tenant_id": "tenant-a",
        "jti": "credential-1",
        "exp": now + 300,
        "scope": [{"resource": "warehouse/analytics/*", "action": "query"}]
    });
    let mut header = Header::new(Algorithm::ES256);
    header.kid = Some("control-key-1".to_owned());
    let token = encode(
        &header,
        &claims,
        &EncodingKey::from_ec_pem(PRIVATE_KEY).expect("private key"),
    )
    .expect("credential");
    let verifier =
        DecisionClient::new("https://control.example.test", JWKS, "tenant-a").expect("JWKS");

    let granted = verifier
        .authorize(
            &token,
            "warehouse/analytics/table/orders",
            VerglasAction::Query,
        )
        .await
        .expect("local verification");
    assert!(granted.allowed);
    assert_eq!(granted.principal_id, "user/alice@example.com");

    let denied = verifier
        .authorize(
            &token,
            "warehouse/finance/table/payroll",
            VerglasAction::Query,
        )
        .await
        .expect("local verification");
    assert!(!denied.allowed);
}
