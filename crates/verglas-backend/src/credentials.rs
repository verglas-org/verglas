//! Provider-specific origin credential adapters.
//!
//! Each adapter returns the concrete credential type its object-store client
//! needs at request signing time. This module keeps refresh semantics out of
//! endpoint configuration: S3 uses the AWS default chain or a rotating AWS-INI
//! file, while Azure shared keys and SAS tokens can rotate from their file.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use aws_credential_types::provider::ProvideCredentials;
use object_store::CredentialProvider;
use object_store::aws::{AwsCredential, AwsCredentialProvider};
use object_store::azure::{AzureAccessKey, AzureCredential, AzureCredentialProvider, split_sas};
use verglas_core::config::Backend;

use crate::settings::{AzureAuth, AzureCredentials, parse_aws_credentials};

/// A credential-provider failure that must be returned without origin retries.
#[derive(Debug)]
pub(crate) struct OriginCredentialError {
    /// Plain-language diagnostic without credential material.
    detail: String,
}

impl std::fmt::Display for OriginCredentialError {
    /// Renders the operator-safe refresh diagnostic.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl std::error::Error for OriginCredentialError {}

/// Builds the provider that signs this server's requests to its S3 origin.
///
/// A named credentials file is read afresh for each signing attempt, so an
/// operator or credential agent can replace a temporary session without a
/// server restart. Without a file, the AWS SDK default chain owns refresh for
/// environment, SSO, web-identity, ECS, and EC2 instance-role credentials.
pub(crate) fn s3_credentials(backend: &Backend) -> AwsCredentialProvider {
    match backend
        .credentials_file
        .as_ref()
        .filter(|file| !file.is_empty())
    {
        Some(file) => Arc::new(FileCredentials::new(
            PathBuf::from(file),
            backend
                .credentials_profile
                .as_deref()
                .filter(|profile| !profile.is_empty())
                .unwrap_or("default")
                .to_owned(),
        )),
        None => Arc::new(DefaultChain::default()),
    }
}

/// Converts a credential-resolution failure into an object-store error without
/// exposing any credential material.
fn credential_error(detail: impl Into<String>) -> object_store::Error {
    object_store::Error::Generic {
        store: "OriginCredentials",
        source: Box::new(OriginCredentialError {
            detail: detail.into(),
        }),
    }
}

/// Builds an Azure provider that re-reads an operator-managed credentials file
/// for every origin request. Client-secret OAuth uses the Azure SDK adapter's
/// own token cache instead, because it refreshes access tokens from the stable
/// client-secret triple.
pub(crate) fn azure_file_credentials(file: PathBuf, account: String) -> AzureCredentialProvider {
    Arc::new(FileAzureCredentials { file, account })
}

/// A provider that reads an operator-managed Azure credentials file on demand.
#[derive(Debug)]
struct FileAzureCredentials {
    /// Flat key=value credentials-file path.
    file: PathBuf,
    /// Storage account chosen when this client was built; changing it requires
    /// a new endpoint, so a rotating file may not alter it in place.
    account: String,
}

#[async_trait]
impl CredentialProvider for FileAzureCredentials {
    type Credential = AzureCredential;

    /// Reads the current Azure shared key or SAS token for one existing account.
    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        let text = tokio::fs::read_to_string(&self.file)
            .await
            .map_err(|error| {
                credential_error(format!(
                    "cannot read backend.credentials_file `{}`: {error}",
                    self.file.display()
                ))
            })?;
        let credentials = AzureCredentials::from_file_text(&text).map_err(|error| {
            credential_error(format!(
                "cannot refresh Azure credentials from backend.credentials_file `{}`: {error}",
                self.file.display()
            ))
        })?;
        if credentials.account != self.account {
            return Err(credential_error(format!(
                "backend.credentials_file `{}` changed Azure storage account from `{}` to `{}`; restart with the new endpoint",
                self.file.display(),
                self.account,
                credentials.account
            )));
        }
        let credential = match credentials.auth {
            AzureAuth::AccountKey(key) => AzureAccessKey::try_new(&key)
                .map(AzureCredential::AccessKey)
                .map_err(|_| {
                    credential_error(format!(
                        "backend.credentials_file `{}` has an invalid Azure account key",
                        self.file.display()
                    ))
                })?,
            AzureAuth::SasToken(token) => split_sas(&token)
                .map(AzureCredential::SASToken)
                .map_err(|_| {
                    credential_error(format!(
                        "backend.credentials_file `{}` has an invalid Azure SAS token",
                        self.file.display()
                    ))
                })?,
            AzureAuth::ClientSecret { .. } => {
                return Err(credential_error(format!(
                    "backend.credentials_file `{}` changed from a shared key or SAS token to Azure client-secret OAuth; restart to rebuild the OAuth provider",
                    self.file.display()
                )));
            }
        };
        Ok(Arc::new(credential))
    }
}

/// A provider that reads an operator-managed AWS credentials file on demand.
#[derive(Debug)]
struct FileCredentials {
    /// AWS-INI credentials-file path.
    file: PathBuf,
    /// Named profile to read from the file.
    profile: String,
}

impl FileCredentials {
    /// Creates a provider for one file and profile.
    fn new(file: PathBuf, profile: String) -> Self {
        Self { file, profile }
    }
}

#[async_trait]
impl CredentialProvider for FileCredentials {
    type Credential = AwsCredential;

    /// Reads the current file contents and returns the selected profile's keys.
    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        let text = tokio::fs::read_to_string(&self.file)
            .await
            .map_err(|error| {
                credential_error(format!(
                    "cannot read backend.credentials_file `{}`: {error}",
                    self.file.display()
                ))
            })?;
        let (key_id, secret_key, token) =
            parse_aws_credentials(&text, &self.profile).ok_or_else(|| {
                credential_error(format!(
                    "backend.credentials_file `{}` has no complete `[{}]` profile",
                    self.file.display(),
                    self.profile
                ))
            })?;
        Ok(Arc::new(AwsCredential {
            key_id,
            secret_key,
            token,
        }))
    }
}

/// A lazy adapter from the AWS SDK default chain to `object_store` signing.
#[derive(Default)]
struct DefaultChain {
    /// The chain is asynchronous to build, so defer it until the first origin
    /// request rather than blocking server startup on IMDS or SSO.
    chain:
        tokio::sync::OnceCell<aws_config::default_provider::credentials::DefaultCredentialsChain>,
}

impl std::fmt::Debug for DefaultChain {
    /// Omits provider internals and any credential material from debug output.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DefaultChain")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl CredentialProvider for DefaultChain {
    type Credential = AwsCredential;

    /// Resolves a current credential from the SDK chain, which refreshes
    /// temporary sessions before they expire.
    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        let chain = self
            .chain
            .get_or_init(|| {
                aws_config::default_provider::credentials::DefaultCredentialsChain::builder()
                    .build()
            })
            .await;
        let credentials = chain.provide_credentials().await.map_err(|error| {
            credential_error(format!("cannot refresh AWS origin credentials: {error}"))
        })?;
        Ok(Arc::new(AwsCredential {
            key_id: credentials.access_key_id().to_owned(),
            secret_key: credentials.secret_access_key().to_owned(),
            token: credentials.session_token().map(str::to_owned),
        }))
    }
}

#[cfg(test)]
mod tests {
    //! Tests for terminal credential-refresh error handling.

    use super::credential_error;
    use crate::retry::{Retryability, classify};

    /// Keeps a failed credential refresh out of the origin retry budget.
    #[test]
    fn credential_refresh_failure_is_not_retried() {
        assert_eq!(
            classify(&credential_error("temporary session expired")),
            Retryability::No
        );
    }
}
