//! Provider-neutral origin client construction.
//!
//! Each variant owns the identity rules for one object-store provider while the
//! backend registry sees one uniform typed/raw client factory. Credential types
//! stay provider-specific: AWS signs SigV4, Azure signs shared-key/SAS/OAuth
//! requests, and GCP refreshes OAuth credentials through application defaults.

use std::sync::Arc;

use verglas_core::config::{Backend, BackendProvider};

use crate::{BackendError, MultipartObjectStore, RawS3};

/// The configured origin provider and its provider-specific identity adapter.
#[derive(Debug, Clone)]
pub(crate) enum OriginProvider {
    /// S3-compatible storage using AWS SigV4 credentials.
    S3(S3Origin),
    /// Azure Blob Storage using Azure shared-key, SAS, or OAuth credentials.
    Azure(AzureOrigin),
    /// Google Cloud Storage using service-account or application-default OAuth.
    Gcp(GcpOrigin),
}

impl OriginProvider {
    /// Selects the provider-specific adapter from immutable backend config.
    pub(crate) fn from_backend(backend: Backend) -> Self {
        match backend.provider {
            BackendProvider::S3 => Self::S3(S3Origin { backend }),
            BackendProvider::Azure => Self::Azure(AzureOrigin { backend }),
            BackendProvider::Gcp => Self::Gcp(GcpOrigin { backend }),
        }
    }

    /// Builds this provider's typed object-store client for one served bucket.
    pub(crate) fn build_typed(
        &self,
        bucket: &str,
    ) -> Result<Arc<dyn MultipartObjectStore>, BackendError> {
        match self {
            Self::S3(origin) => origin.build_typed(bucket),
            Self::Azure(origin) => origin.build_typed(bucket),
            Self::Gcp(origin) => origin.build_typed(bucket),
        }
    }

    /// Builds the byte-exact raw client when this provider has an S3 surface.
    pub(crate) fn build_raw(&self, bucket: &str) -> Result<Arc<RawS3>, BackendError> {
        match self {
            Self::S3(origin) => origin.build_raw(bucket),
            Self::Azure(origin) => origin.build_raw(bucket),
            Self::Gcp(origin) => origin.build_raw(bucket),
        }
    }

    /// Returns the immutable backend policy shared by all served buckets.
    pub(crate) fn backend(&self) -> &Backend {
        match self {
            Self::S3(origin) => &origin.backend,
            Self::Azure(origin) => &origin.backend,
            Self::Gcp(origin) => &origin.backend,
        }
    }
}

/// S3-compatible origin configuration and AWS credential adapter.
#[derive(Debug, Clone)]
pub(crate) struct S3Origin {
    /// Immutable endpoint and credential-source configuration.
    backend: Backend,
}

impl S3Origin {
    /// Builds the typed S3 client, whose provider refreshes each origin signature.
    fn build_typed(&self, bucket: &str) -> Result<Arc<dyn MultipartObjectStore>, BackendError> {
        crate::build_s3(bucket, &self.backend).map(|store| store as Arc<dyn MultipartObjectStore>)
    }

    /// Builds the byte-exact S3 client for protocol features outside object-store.
    fn build_raw(&self, bucket: &str) -> Result<Arc<RawS3>, BackendError> {
        crate::build_raw(bucket, &self.backend)
    }
}

/// Azure Blob origin configuration and Azure credential adapter.
#[derive(Debug, Clone)]
pub(crate) struct AzureOrigin {
    /// Immutable endpoint and credential-source configuration.
    backend: Backend,
}

impl AzureOrigin {
    /// Builds the typed Azure client, whose adapter refreshes the selected identity.
    fn build_typed(&self, bucket: &str) -> Result<Arc<dyn MultipartObjectStore>, BackendError> {
        crate::build_azure(bucket, &self.backend)
            .map(|store| store as Arc<dyn MultipartObjectStore>)
    }

    /// Refuses raw S3 construction because Azure has no byte-exact S3 surface.
    fn build_raw(&self, bucket: &str) -> Result<Arc<RawS3>, BackendError> {
        crate::build_raw(bucket, &self.backend)
    }
}

/// Google Cloud Storage origin configuration and GCP credential adapter.
#[derive(Debug, Clone)]
pub(crate) struct GcpOrigin {
    /// Immutable endpoint and credential-source configuration.
    backend: Backend,
}

impl GcpOrigin {
    /// Builds the typed GCP client, whose application-default identity refreshes tokens.
    fn build_typed(&self, bucket: &str) -> Result<Arc<dyn MultipartObjectStore>, BackendError> {
        crate::build_gcp(bucket, &self.backend).map(|store| store as Arc<dyn MultipartObjectStore>)
    }

    /// Refuses raw S3 construction because GCP has no byte-exact S3 surface.
    fn build_raw(&self, bucket: &str) -> Result<Arc<RawS3>, BackendError> {
        crate::build_raw(bucket, &self.backend)
    }
}
