//! Static access-key lookup for M1 SigV4 validation (issue #7).
//!
//! Callers present Verglas-issued credentials; the s3s service reconstructs
//! canonical requests and compares signatures against secrets returned here.
//! Credential issuance belongs to the owning deployment.

use std::collections::HashMap;

use s3s::auth::{S3Auth, SecretKey};
use s3s::{S3Error, S3ErrorCode, S3Result};

/// In-memory `S3Auth` backed by the server config's static key list.
#[derive(Debug)]
pub struct StaticAuth {
    /// Access key ID to secret map.
    keys: HashMap<String, SecretKey>,
}

impl StaticAuth {
    /// Builds a provider from zero or more `(access_key_id, secret_access_key)` pairs.
    pub fn new<I>(pairs: I) -> Self
    where
        I: IntoIterator<Item = (String, String)>,
    {
        let keys = pairs
            .into_iter()
            .map(|(access_key_id, secret_access_key)| {
                (access_key_id, SecretKey::from(secret_access_key))
            })
            .collect();
        StaticAuth { keys }
    }

    /// Builds an authenticator for one explicitly configured credential pair.
    pub fn from_single(
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
    ) -> Self {
        Self::new([(access_key_id.into(), secret_access_key.into())])
    }
}

#[async_trait::async_trait]
impl S3Auth for StaticAuth {
    /// Returns the secret for a known access key, or the S3-shaped
    /// `InvalidAccessKeyId` error real endpoints emit for unknown keys.
    async fn get_secret_key(&self, access_key: &str) -> S3Result<SecretKey> {
        match self.keys.get(access_key) {
            Some(secret) => Ok(secret.clone()),
            None => Err(S3Error::with_message(
                S3ErrorCode::InvalidAccessKeyId,
                "The AWS Access Key Id you provided does not exist in our records.",
            )),
        }
    }
}
