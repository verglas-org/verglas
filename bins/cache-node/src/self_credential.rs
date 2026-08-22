//! Self-issued catalog credential for a node that hosts its own catalog.
//!
//! The hosted Iceberg catalog authorizes every caller against the control
//! plane's credential contract: an ES256 bearer carrying `tenant_id`, `sub`,
//! `jti`, and a `scope` list, verified against the configured JWKS. A node
//! serving that catalog is itself such a caller when its semantic store opens
//! graphs, so it holds a signing key whose public half sits in that JWKS and
//! mints its own short-lived credential.
//!
//! Exempting loopback callers from authorization would leave the local path
//! unverified and diverge from the deployed one. One verification path runs
//! everywhere; only the key's origin differs.

use std::path::{Path, PathBuf};

use base64::Engine as _;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use serde::Serialize;

/// Audience the hosted catalog requires on every caller credential.
const CATALOG_AUDIENCE: &str = "catalog";

/// Key id published in the node's JWKS and echoed in each minted header.
const SELF_KEY_ID: &str = "verglas-node-self";

/// Lifetime of one minted credential: long enough to outlive a slow catalog
/// round trip, short enough that a leaked token expires before it is useful.
const CREDENTIAL_TTL_SECS: u64 = 300;

/// One resource/action grant, matching the control plane's credential scope.
#[derive(Debug, Serialize)]
struct SelfScope {
    resource: String,
    action: String,
}

/// Claims the hosted catalog's authorizer requires and reads.
#[derive(Debug, Serialize)]
struct SelfClaims {
    iss: String,
    sub: String,
    aud: String,
    exp: u64,
    jti: String,
    tenant_id: String,
    scope: Vec<SelfScope>,
}

/// Signing key whose public half the node publishes for its own catalog.
#[derive(Debug)]
pub struct SelfCredentialIssuer {
    issuer: String,
    tenant_id: String,
    jwks: String,
    key: EncodingKey,
}

impl SelfCredentialIssuer {
    /// Loads the node's signing key, generating one on first use.
    ///
    /// The key persists beside the node's other state because a key that
    /// changed on restart would invalidate the JWKS this node publishes and
    /// every credential minted against it.
    /// @param dir - directory holding node-local state.
    /// @param issuer - issuer the hosted catalog validates against.
    /// @param tenant_id - tenant the hosted catalog expects on every caller.
    /// @returns an issuer able to mint catalog credentials.
    pub fn load_or_create(
        dir: &Path,
        issuer: impl Into<String>,
        tenant_id: impl Into<String>,
    ) -> std::io::Result<Self> {
        let path: PathBuf = dir.join("catalog-signing-key.pem");
        let key = match std::fs::read_to_string(&path) {
            Ok(pem) => rcgen::KeyPair::from_pem(&pem).map_err(|error| {
                std::io::Error::other(format!("reading {}: {error}", path.display()))
            })?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let generated = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256)
                    .map_err(|error| {
                        std::io::Error::other(format!("generating catalog signing key: {error}"))
                    })?;
                std::fs::write(&path, generated.serialize_pem())?;
                restrict_to_owner(&path)?;
                generated
            }
            Err(error) => {
                return Err(std::io::Error::other(format!(
                    "reading {}: {error}",
                    path.display()
                )));
            }
        };
        // `public_key_raw` is the SEC1 uncompressed point (`0x04 || x || y`),
        // which is exactly the two coordinates a JWK carries.
        let raw = key.public_key_raw();
        if raw.len() != 65 {
            return Err(std::io::Error::other(
                "catalog signing key is not an uncompressed P-256 point",
            ));
        }
        let encoder = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let jwks = serde_json::json!({
            "keys": [{
                "kty": "EC",
                "crv": "P-256",
                "alg": "ES256",
                "use": "sig",
                "kid": SELF_KEY_ID,
                "x": encoder.encode(&raw[1..33]),
                "y": encoder.encode(&raw[33..65]),
            }]
        })
        .to_string();
        Ok(Self {
            issuer: issuer.into(),
            tenant_id: tenant_id.into(),
            jwks,
            key: EncodingKey::from_ec_pem(key.serialize_pem().as_bytes()).map_err(|error| {
                std::io::Error::other(format!("loading catalog signing key: {error}"))
            })?,
        })
    }

    /// The one-key JWKS the hosted catalog must trust to verify minted tokens.
    /// @returns a JWKS document as JSON.
    pub fn jwks(&self) -> &str {
        &self.jwks
    }

    /// Mints one short-lived credential granting every action on this tenant.
    ///
    /// The node operates the catalog it is calling, so the grant is total: a
    /// narrower scope would enumerate every resource the semantic store
    /// touches and fail closed the moment that set grew.
    /// @returns a signed ES256 bearer token.
    /// @throws when the system clock predates the Unix epoch or signing fails.
    pub fn mint(&self) -> std::io::Result<String> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|error| {
                std::io::Error::other(format!("system clock predates the epoch: {error}"))
            })?
            .as_secs();
        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some(SELF_KEY_ID.to_owned());
        let claims = SelfClaims {
            iss: self.issuer.clone(),
            sub: "verglas-cache-node".to_owned(),
            aud: CATALOG_AUDIENCE.to_owned(),
            exp: now + CREDENTIAL_TTL_SECS,
            // Second-granularity, so two tokens minted in the same second
            // share an id. The authorizer only carries `jti` through as
            // `token_id` and keeps no replay cache, so this is an identifier
            // rather than a nonce. Making it unique is the extension point if
            // replay prevention ever lands.
            jti: format!("self-{now}"),
            tenant_id: self.tenant_id.clone(),
            // An empty prefix with the trailing wildcard matches every
            // resource id the authorizer builds.
            scope: vec![SelfScope {
                resource: "/*".to_owned(),
                action: "admin".to_owned(),
            }],
        };
        jsonwebtoken::encode(&header, &claims, &self.key)
            .map_err(|error| std::io::Error::other(format!("signing catalog credential: {error}")))
    }
}

/// Restricts a freshly written key to its owner on Unix hosts.
/// @param path - key file to restrict.
/// @returns unit, or the underlying permission error.
fn restrict_to_owner(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}
/// Combines the configured JWKS with the node's own published key.
///
/// A node hosting its own catalog verifies two kinds of caller: the control
/// plane's, whose keys arrive in configuration, and itself. Both sets must be
/// present for either to authorize.
/// @param configured - JWKS document from `catalog_server.authz_jwks`.
/// @param own - the single-key JWKS this node publishes.
/// @returns a JWKS document carrying every key from both.
/// @throws when either document is not a JWKS object with a `keys` array.
pub fn merge_jwks(configured: &str, own: &str) -> std::io::Result<String> {
    let read = |document: &str, label: &str| -> std::io::Result<Vec<serde_json::Value>> {
        let parsed: serde_json::Value = serde_json::from_str(document)
            .map_err(|error| std::io::Error::other(format!("{label} is not JSON: {error}")))?;
        parsed
            .get("keys")
            .and_then(serde_json::Value::as_array)
            .cloned()
            .ok_or_else(|| std::io::Error::other(format!("{label} has no \"keys\" array")))
    };
    let mut keys = read(configured, "catalog_server.authz_jwks")?;
    keys.extend(read(own, "node catalog key")?);
    Ok(serde_json::json!({ "keys": keys }).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A one-key JWKS standing in for the control plane's configured keys.
    fn configured_jwks() -> String {
        serde_json::json!({
            "keys": [{ "kty": "EC", "crv": "P-256", "kid": "control-plane", "x": "a", "y": "b" }]
        })
        .to_string()
    }

    /// The key is generated once and reused, so a restart keeps minting tokens
    /// the catalog's published JWKS still verifies. Regenerating per boot would
    /// invalidate the JWKS the running catalog trusts.
    #[test]
    fn the_signing_key_survives_a_restart() {
        let dir = tempfile::tempdir().expect("state directory");
        let first = SelfCredentialIssuer::load_or_create(
            dir.path(),
            "https://issuer.test".to_owned(),
            "tenant".to_owned(),
        )
        .expect("first issuer");
        let second = SelfCredentialIssuer::load_or_create(
            dir.path(),
            "https://issuer.test".to_owned(),
            "tenant".to_owned(),
        )
        .expect("second issuer");
        assert_eq!(
            first.jwks(),
            second.jwks(),
            "reopening must reuse the stored key, not mint a new one"
        );
    }

    /// The private key never becomes group- or world-readable.
    #[cfg(unix)]
    #[test]
    fn the_signing_key_is_owner_only() {
        use std::os::unix::fs::PermissionsExt as _;

        let dir = tempfile::tempdir().expect("state directory");
        SelfCredentialIssuer::load_or_create(
            dir.path(),
            "https://issuer.test".to_owned(),
            "tenant".to_owned(),
        )
        .expect("issuer");
        let key = dir.path().join("catalog-signing-key.pem");
        let mode = std::fs::metadata(&key)
            .expect("key metadata")
            .permissions()
            .mode();
        assert_eq!(
            mode & 0o077,
            0,
            "the signing key must not be readable by others"
        );
    }

    /// A minted token carries every claim the authorizer reads, and expires.
    /// `CredentialClaims` in `verglas-catalog-authz` deserializes `tenant_id`,
    /// `sub`, `jti`, and `scope`; a missing one is a hard rejection.
    #[test]
    fn a_minted_token_carries_the_claims_the_authorizer_requires() {
        let dir = tempfile::tempdir().expect("state directory");
        let issuer = SelfCredentialIssuer::load_or_create(
            dir.path(),
            "https://issuer.test".to_owned(),
            "tenant-a".to_owned(),
        )
        .expect("issuer");
        let token = issuer.mint().expect("mint");

        let mut parts = token.split('.');
        let header: serde_json::Value = serde_json::from_slice(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(parts.next().expect("header"))
                .expect("header base64"),
        )
        .expect("header json");
        let claims: serde_json::Value = serde_json::from_slice(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(parts.next().expect("claims"))
                .expect("claims base64"),
        )
        .expect("claims json");

        assert_eq!(header["alg"], "ES256");
        assert_eq!(header["kid"], SELF_KEY_ID);
        assert_eq!(claims["aud"], CATALOG_AUDIENCE);
        assert_eq!(claims["iss"], "https://issuer.test");
        assert_eq!(claims["tenant_id"], "tenant-a");
        assert!(claims["sub"].is_string());
        assert!(claims["jti"].is_string());
        assert_eq!(claims["scope"][0]["resource"], "/*");
        assert_eq!(claims["scope"][0]["action"], "admin");

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after epoch")
            .as_secs();
        let exp = claims["exp"].as_u64().expect("exp is a number");
        assert!(exp > now, "a minted token must not already be expired");
        assert!(
            exp <= now + CREDENTIAL_TTL_SECS,
            "a minted token must not outlive its TTL"
        );
    }

    /// The merged document carries both the configured keys and the node's own,
    /// so the catalog verifies control-plane callers and this node alike.
    #[test]
    fn merging_keeps_every_key_from_both_documents() {
        let dir = tempfile::tempdir().expect("state directory");
        let issuer = SelfCredentialIssuer::load_or_create(
            dir.path(),
            "https://issuer.test".to_owned(),
            "tenant".to_owned(),
        )
        .expect("issuer");

        let merged = merge_jwks(&configured_jwks(), issuer.jwks()).expect("merge");
        let parsed: serde_json::Value = serde_json::from_str(&merged).expect("merged json");
        let keys = parsed["keys"].as_array().expect("keys array");
        assert_eq!(keys.len(), 2, "both keys must survive the merge");
        let ids: Vec<&str> = keys.iter().filter_map(|key| key["kid"].as_str()).collect();
        assert!(ids.contains(&"control-plane"));
        assert!(ids.contains(&SELF_KEY_ID));
    }

    /// A malformed JWKS fails loudly. Silently dropping it would leave the
    /// catalog trusting fewer keys than the operator configured.
    #[test]
    fn merging_rejects_a_document_that_is_not_a_jwks() {
        let dir = tempfile::tempdir().expect("state directory");
        let issuer = SelfCredentialIssuer::load_or_create(
            dir.path(),
            "https://issuer.test".to_owned(),
            "tenant".to_owned(),
        )
        .expect("issuer");

        assert!(merge_jwks("not json", issuer.jwks()).is_err());
        assert!(merge_jwks("{}", issuer.jwks()).is_err());
        assert!(merge_jwks(&configured_jwks(), "{}").is_err());
    }
}
