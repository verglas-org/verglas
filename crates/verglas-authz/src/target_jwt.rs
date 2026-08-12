//! Ed25519 target JWTs and public JWKS documents for database credential exchange.
//!
//! This module uses a dedicated asymmetric key. It never reuses the symmetric
//! access-token signer because external targets must verify with public material only.

use std::fmt;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::{Signature, Signer as _, SigningKey, Verifier as _, VerifyingKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::{AuthzError, TenantId, validate_identifier};

/// A request to issue one target-specific JWT for an authorized database connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetJwtRequest {
    /// Target service that will accept the assertion.
    pub audience: String,
    /// Principal for which the target should establish an identity.
    pub subject: String,
    /// Stable credential or connection identity for replay and audit correlation.
    pub token_id: String,
    /// Tenant boundary that owns the database.
    pub tenant_id: TenantId,
    /// Stable universal database resource identity.
    pub database_id: String,
    /// Unix timestamp at which the assertion becomes valid.
    pub issued_at: u64,
    /// Unix timestamp after which the target must reject the assertion.
    pub expires_at: u64,
}

impl TargetJwtRequest {
    /// Constructs one target assertion request.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        audience: impl Into<String>,
        subject: impl Into<String>,
        token_id: impl Into<String>,
        tenant_id: impl Into<TenantId>,
        database_id: impl Into<String>,
        issued_at: u64,
        expires_at: u64,
    ) -> Self {
        Self {
            audience: audience.into(),
            subject: subject.into(),
            token_id: token_id.into(),
            tenant_id: tenant_id.into(),
            database_id: database_id.into(),
            issued_at,
            expires_at,
        }
    }

    /// Rejects malformed target, identity, and validity fields before signing.
    pub fn validate(&self) -> Result<(), AuthzError> {
        validate_identifier("target_jwt.audience", &self.audience)?;
        validate_identifier("target_jwt.subject", &self.subject)?;
        validate_identifier("target_jwt.token_id", &self.token_id)?;
        validate_identifier("target_jwt.tenant_id", &self.tenant_id)?;
        validate_identifier("target_jwt.database_id", &self.database_id)?;
        if self.expires_at <= self.issued_at {
            return Err(AuthzError::Token(
                "target JWT expiration must be after issuance".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Claims encoded in the payload of a target JWT.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct TargetJwtClaims {
    /// Target service expected to accept the assertion.
    #[serde(rename = "aud")]
    pub audience: String,
    /// Target-side principal identity.
    #[serde(rename = "sub")]
    pub subject: String,
    /// Stable credential or connection identity.
    #[serde(rename = "jti")]
    pub token_id: String,
    /// Tenant boundary that owns the target database.
    pub tenant_id: TenantId,
    /// Stable database resource identity.
    pub database_id: String,
    /// Unix issuance timestamp.
    #[serde(rename = "iat")]
    pub issued_at: u64,
    /// Unix expiration timestamp.
    #[serde(rename = "exp")]
    pub expires_at: u64,
}

impl TargetJwtClaims {
    /// Verifies target, database, identifier shape, and validity interval.
    pub fn validate(
        &self,
        expected_audience: &str,
        expected_database_id: &str,
        now: u64,
    ) -> Result<(), AuthzError> {
        TargetJwtRequest::new(
            &self.audience,
            &self.subject,
            &self.token_id,
            &self.tenant_id,
            &self.database_id,
            self.issued_at,
            self.expires_at,
        )
        .validate()?;
        if self.audience != expected_audience {
            return Err(AuthzError::Token(
                "target JWT audience does not match".to_owned(),
            ));
        }
        if self.database_id != expected_database_id {
            return Err(AuthzError::Token(
                "target JWT database does not match".to_owned(),
            ));
        }
        if now < self.issued_at || now > self.expires_at {
            return Err(AuthzError::Token(
                "target JWT is outside its validity interval".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Public JSON Web Key advertised to targets through the JWKS endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct PublicJwk {
    /// Key type required by RFC 8037 for Ed25519 public keys.
    pub kty: String,
    /// Edwards-curve name required by RFC 8037.
    pub crv: String,
    /// Base64url-encoded 32-byte public key.
    pub x: String,
    /// Stable key identifier carried in every signed JWT header.
    pub kid: String,
    /// JWT algorithm used with this public key.
    pub alg: String,
    /// Intentional JWKS use restriction.
    #[serde(rename = "use")]
    pub usage: String,
}

/// JWKS document containing the public verifier for one signing key.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct PublicJwkSet {
    /// Active public keys. Rotation can publish multiple entries through a future issuer layer.
    pub keys: Vec<PublicJwk>,
}

/// Target JWT bearer material that cannot be serialized or exposed through debug output.
pub struct SecretTargetJwt(String);

impl SecretTargetJwt {
    /// Returns the compact JWT only for immediate delivery to the selected target.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretTargetJwt {
    /// Redacts JWT material from diagnostic output.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretTargetJwt([REDACTED])")
    }
}

/// Ed25519 issuer for one target-JWT key ID.
pub struct TargetJwtSigner {
    key_id: String,
    signing_key: SigningKey,
}

impl TargetJwtSigner {
    /// Constructs an issuer from one validated key ID and 32-byte Ed25519 secret seed.
    pub fn new(key_id: impl Into<String>, seed: [u8; 32]) -> Result<Self, AuthzError> {
        let key_id = key_id.into();
        validate_identifier("target_jwt.kid", &key_id)?;
        Ok(Self {
            key_id,
            signing_key: SigningKey::from_bytes(&seed),
        })
    }

    /// Decodes exactly 32 bytes of standard base64 Ed25519 seed material.
    pub fn from_base64(key_id: impl Into<String>, encoded: &str) -> Result<Self, AuthzError> {
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|_| AuthzError::Invalid("target JWT key must be base64".to_owned()))?;
        let seed: [u8; 32] = decoded.try_into().map_err(|_| {
            AuthzError::Invalid("target JWT key must decode to exactly 32 bytes".to_owned())
        })?;
        Self::new(key_id, seed)
    }

    /// Decodes an Ed25519 seed and derives a stable public-key fingerprint as its key ID.
    pub fn from_base64_derived(encoded: &str) -> Result<Self, AuthzError> {
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|_| AuthzError::Invalid("target JWT key must be base64".to_owned()))?;
        let seed: [u8; 32] = decoded.try_into().map_err(|_| {
            AuthzError::Invalid("target JWT key must decode to exactly 32 bytes".to_owned())
        })?;
        Self::from_seed_derived(seed)
    }

    /// Derives a public key fingerprint without exposing or storing the Ed25519 seed.
    pub fn from_seed_derived(seed: [u8; 32]) -> Result<Self, AuthzError> {
        let signing_key = SigningKey::from_bytes(&seed);
        let digest = Sha256::digest(signing_key.verifying_key().to_bytes());
        let key_id = format!("ed25519-{}", hex::encode(&digest[..12]));
        Self::new(key_id, seed)
    }

    /// Signs one target assertion with the protected Ed25519 key.
    pub fn mint(&self, request: TargetJwtRequest) -> Result<SecretTargetJwt, AuthzError> {
        request.validate()?;
        let header = JwtHeader {
            algorithm: "EdDSA",
            token_type: "JWT",
            key_id: &self.key_id,
        };
        let claims = TargetJwtClaims {
            audience: request.audience,
            subject: request.subject,
            token_id: request.token_id,
            tenant_id: request.tenant_id,
            database_id: request.database_id,
            issued_at: request.issued_at,
            expires_at: request.expires_at,
        };
        let encoded_header = encode_json(&header)?;
        let encoded_claims = encode_json(&claims)?;
        let signed = format!("{encoded_header}.{encoded_claims}");
        let signature = self.signing_key.sign(signed.as_bytes());
        Ok(SecretTargetJwt(format!(
            "{signed}.{}",
            URL_SAFE_NO_PAD.encode(signature.to_bytes())
        )))
    }

    /// Verifies a JWT's header, Ed25519 signature, and target-specific claims.
    pub fn verify(
        &self,
        raw: &str,
        expected_audience: &str,
        expected_database_id: &str,
        now: u64,
    ) -> Result<TargetJwtClaims, AuthzError> {
        let (encoded_header, encoded_claims, encoded_signature) = split_jwt(raw)?;
        let header: JwtHeaderOwned = decode_json(encoded_header)?;
        if header.algorithm != "EdDSA" || header.token_type != "JWT" || header.key_id != self.key_id
        {
            return Err(AuthzError::Token(
                "target JWT header is not accepted".to_owned(),
            ));
        }
        let signature_bytes = URL_SAFE_NO_PAD
            .decode(encoded_signature)
            .map_err(|_| AuthzError::Token("target JWT signature is malformed".to_owned()))?;
        let signature = Signature::from_slice(&signature_bytes)
            .map_err(|_| AuthzError::Token("target JWT signature is invalid".to_owned()))?;
        self.signing_key
            .verifying_key()
            .verify(
                format!("{encoded_header}.{encoded_claims}").as_bytes(),
                &signature,
            )
            .map_err(|_| AuthzError::Token("target JWT signature is invalid".to_owned()))?;
        let claims: TargetJwtClaims = decode_json(encoded_claims)?;
        claims.validate(expected_audience, expected_database_id, now)?;
        Ok(claims)
    }

    /// Returns the public Ed25519 verifier in a target-consumable JWKS document.
    #[must_use]
    pub fn jwks(&self) -> PublicJwkSet {
        PublicJwkSet {
            keys: vec![PublicJwk {
                kty: "OKP".to_owned(),
                crv: "Ed25519".to_owned(),
                x: URL_SAFE_NO_PAD.encode(self.signing_key.verifying_key().to_bytes()),
                kid: self.key_id.clone(),
                alg: "EdDSA".to_owned(),
                usage: "sig".to_owned(),
            }],
        }
    }
}

impl fmt::Debug for TargetJwtSigner {
    /// Redacts the private Ed25519 seed from diagnostic output.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TargetJwtSigner")
            .field("key_id", &self.key_id)
            .field("signing_key", &"[REDACTED]")
            .finish()
    }
}

/// JWT protected header emitted by this signer.
#[derive(Serialize)]
struct JwtHeader<'a> {
    /// JOSE algorithm identifier.
    #[serde(rename = "alg")]
    algorithm: &'a str,
    /// JWT type marker.
    #[serde(rename = "typ")]
    token_type: &'a str,
    /// Active verification key identifier.
    #[serde(rename = "kid")]
    key_id: &'a str,
}

/// Parsed JWT protected header used during local verification tests and services.
#[derive(Deserialize)]
struct JwtHeaderOwned {
    /// JOSE algorithm identifier.
    #[serde(rename = "alg")]
    algorithm: String,
    /// JWT type marker.
    #[serde(rename = "typ")]
    token_type: String,
    /// Active verification key identifier.
    #[serde(rename = "kid")]
    key_id: String,
}

/// Encodes one compact-JWT segment without padding.
fn encode_json(value: &impl Serialize) -> Result<String, AuthzError> {
    serde_json::to_vec(value)
        .map(|json| URL_SAFE_NO_PAD.encode(json))
        .map_err(|error| AuthzError::Token(format!("could not encode target JWT: {error}")))
}

/// Decodes one bounded base64url JSON segment.
fn decode_json<T: for<'a> Deserialize<'a>>(encoded: &str) -> Result<T, AuthzError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| AuthzError::Token("target JWT payload is malformed".to_owned()))?;
    serde_json::from_slice(&bytes)
        .map_err(|_| AuthzError::Token("target JWT payload is invalid".to_owned()))
}

/// Splits a bounded compact JWT without accepting unprotected extra segments.
fn split_jwt(raw: &str) -> Result<(&str, &str, &str), AuthzError> {
    if raw.len() > 16 * 1024 {
        return Err(AuthzError::Token(
            "target JWT exceeds the size limit".to_owned(),
        ));
    }
    let mut segments = raw.split('.');
    let header = segments
        .next()
        .filter(|segment| !segment.is_empty())
        .ok_or_else(|| AuthzError::Token("target JWT header is missing".to_owned()))?;
    let claims = segments
        .next()
        .filter(|segment| !segment.is_empty())
        .ok_or_else(|| AuthzError::Token("target JWT claims are missing".to_owned()))?;
    let signature = segments
        .next()
        .filter(|segment| !segment.is_empty())
        .ok_or_else(|| AuthzError::Token("target JWT signature is missing".to_owned()))?;
    if segments.next().is_some() {
        return Err(AuthzError::Token(
            "target JWT has too many segments".to_owned(),
        ));
    }
    Ok((header, claims, signature))
}

/// Returns the public verifier for tests and target-side adapters that need key bytes.
pub fn verifying_key_from_jwk(jwk: &PublicJwk) -> Result<VerifyingKey, AuthzError> {
    if jwk.kty != "OKP" || jwk.crv != "Ed25519" || jwk.alg != "EdDSA" || jwk.usage != "sig" {
        return Err(AuthzError::Token(
            "target JWK is not an Ed25519 signing key".to_owned(),
        ));
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(&jwk.x)
        .map_err(|_| AuthzError::Token("target JWK public key is malformed".to_owned()))?;
    let bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| AuthzError::Token("target JWK public key length is invalid".to_owned()))?;
    VerifyingKey::from_bytes(&bytes)
        .map_err(|_| AuthzError::Token("target JWK public key is invalid".to_owned()))
}
