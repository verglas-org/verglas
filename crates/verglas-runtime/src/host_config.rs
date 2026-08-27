//! Strict operator configuration for the runtime-owned Catalog capability.
//!
//! This module parses one JSON document, validates the exact origin and Sink
//! fences, and constructs the host-only backend, Foyer cache, and Iceberg
//! proposal writer. The resulting capability contains credentials only in the
//! host backend and never exposes them to a component.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::Deserialize;
use verglas_backend::{BackendStore, BackendStores, BucketAliasStores, StartupProbeError};
use verglas_cache::HybridCacheEngine;
use verglas_core::config::{Backend, Cache};
use verglas_do_turso::TursoCasStorage;
use verglas_iceberg::SinkCompression;
use verglas_s3::PassthroughRead;

use crate::{
    CatalogCommitServiceConfig, IcebergCatalogCommitService, OriginStorageConfig,
    OriginStorageError, OriginStorageFactory,
};

/// A failure while loading, validating, or constructing Catalog host state.
#[derive(Debug, thiserror::Error)]
pub enum CatalogHostConfigError {
    /// The operator configuration file could not be read.
    #[error("catalog host config `{path}` could not be read: {source}")]
    Read {
        /// Path that was requested by the runtime command line.
        path: PathBuf,
        /// Filesystem failure returned by the operating system.
        #[source]
        source: io::Error,
    },
    /// The configuration file is not valid strict JSON.
    #[error("catalog host config JSON is invalid: {0}")]
    Json(#[from] serde_json::Error),
    /// The parsed document violates a host startup invariant.
    #[error("catalog host config is invalid: {0}")]
    Invalid(String),
    /// The exact configured origin could not be reached or authenticated.
    #[error("catalog host origin probe failed: {0}")]
    Probe(#[source] Box<StartupProbeError>),
    /// The backend or Foyer cache could not be opened for the exact route.
    #[error("catalog host origin could not be opened: {0}")]
    Origin(#[from] OriginStorageError),
}

impl From<StartupProbeError> for CatalogHostConfigError {
    /// Boxes the detailed backend failure without inflating every validation result.
    fn from(error: StartupProbeError) -> Self {
        Self::Probe(Box::new(error))
    }
}

/// One exact origin route and its provider construction settings.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogOriginConfig {
    /// Immutable runtime binding identity used in every origin cache key.
    pub storage_binding_id: String,
    /// Stable logical bucket visible to the precompiled Catalog component.
    pub bucket: String,
    /// URI scheme accepted by Iceberg locations for this route.
    pub scheme: String,
    /// Provider, endpoint, retry, and ambient/file credential settings.
    pub backend: Backend,
}

impl CatalogOriginConfig {
    /// Returns the exact storage binding identity.
    pub fn storage_binding_id(&self) -> &str {
        &self.storage_binding_id
    }

    /// Returns the exact origin bucket.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Returns the only URI scheme accepted by the origin factory.
    pub fn scheme(&self) -> &str {
        &self.scheme
    }
}

/// The immutable Sink identity fence admitted by one Catalog runtime child.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SinkFence {
    /// Sink identity that owns every accepted proposal.
    pub sink_id: String,
    /// Dotted Iceberg namespace owned by the Sink.
    pub namespace: String,
    /// Iceberg table name owned by the Sink.
    pub table: String,
    /// Parquet codec that every accepted Sink batch must use.
    #[serde(deserialize_with = "deserialize_compression")]
    pub compression: SinkCompression,
}

impl SinkFence {
    /// Returns the exact Sink identity.
    pub fn sink_id(&self) -> &str {
        &self.sink_id
    }

    /// Returns the configured dotted namespace.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Returns the configured table name.
    pub fn table(&self) -> &str {
        &self.table
    }

    /// Returns the immutable Parquet codec.
    pub fn compression(&self) -> SinkCompression {
        self.compression
    }
}

/// One strict operator document for an `ICEBERG_COMMIT` runtime child.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogHostConfig {
    /// Exact origin binding, bucket, scheme, and provider settings.
    pub origin: CatalogOriginConfig,
    /// Foyer DRAM/NVMe settings and hard cache budgets.
    pub cache: Cache,
    /// Warehouse URI that must remain below the configured scheme and bucket.
    pub warehouse: String,
    /// Exact Sink identity and compression fence.
    pub sink: SinkFence,
}

/// Host capabilities sharing one per-object origin and one Foyer cache.
pub struct DurableHostState {
    /// Turso's S3-CAS virtual filesystem for this object.
    pub turso: TursoCasStorage,
    /// Catalog's optional Iceberg commit capability over the same cache.
    pub catalog: Arc<IcebergCatalogCommitService>,
}

impl CatalogHostConfig {
    /// Reads and validates one strict JSON operator configuration file.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, CatalogHostConfigError> {
        let path = path.as_ref().to_path_buf();
        let bytes = fs::read(&path).map_err(|source| CatalogHostConfigError::Read {
            path: path.clone(),
            source,
        })?;
        let config = serde_json::from_slice::<Self>(&bytes)?;
        config.validate()?;
        Ok(config)
    }

    /// Validates all exact-route, warehouse, Sink, and cache invariants.
    pub fn validate(&self) -> Result<(), CatalogHostConfigError> {
        validate_name("origin storage binding", &self.origin.storage_binding_id)?;
        validate_name("origin bucket", &self.origin.bucket)?;
        validate_scheme(&self.origin.scheme)?;
        if self
            .origin
            .backend
            .bucket
            .as_deref()
            .is_none_or(str::is_empty)
        {
            return Err(CatalogHostConfigError::Invalid(format!(
                "backend must serve one physical bucket for logical bucket `{}`",
                self.origin.bucket,
            )));
        }
        if !self.origin.backend.bucket_globs.is_empty() {
            return Err(CatalogHostConfigError::Invalid(
                "Catalog origin backend must serve one exact bucket without globs".to_owned(),
            ));
        }
        self.cache
            .validate()
            .map_err(CatalogHostConfigError::Invalid)?;
        validate_cache_budgets(&self.cache)?;
        validate_warehouse(&self.warehouse, &self.origin.scheme, &self.origin.bucket)?;
        validate_sink(&self.sink)?;
        Ok(())
    }

    /// Returns the exact origin configuration.
    pub fn origin(&self) -> &CatalogOriginConfig {
        &self.origin
    }

    /// Returns the Foyer configuration and hard budgets.
    pub fn cache(&self) -> &Cache {
        &self.cache
    }

    /// Returns the host-pinned warehouse URI.
    pub fn warehouse(&self) -> &str {
        &self.warehouse
    }

    /// Returns the exact Sink fence.
    pub fn sink(&self) -> &SinkFence {
        &self.sink
    }

    /// Constructs the host-only backend, origin factory, and Catalog service.
    pub async fn build_catalog_commit_service(
        &self,
    ) -> Result<Arc<IcebergCatalogCommitService>, CatalogHostConfigError> {
        Ok(self.build_durable_host_state().await?.catalog)
    }

    /// Builds Turso and Iceberg storage over one Foyer instance. The configured
    /// physical bucket must belong to exactly one Durable Object.
    pub async fn build_durable_host_state(
        &self,
    ) -> Result<DurableHostState, CatalogHostConfigError> {
        self.validate()?;
        let store =
            BackendStore::from_config(self.origin.storage_binding_id.clone(), &self.origin.backend);
        store.probe().await?;
        let physical_bucket = self.origin.backend.bucket.as_deref().ok_or_else(|| {
            CatalogHostConfigError::Invalid("backend bucket is required".to_owned())
        })?;
        let backing: Arc<dyn BackendStores> = store;
        let stores: Arc<dyn BackendStores> = Arc::new(BucketAliasStores::new(
            backing,
            self.origin.storage_binding_id.clone(),
            self.origin.bucket.clone(),
            physical_bucket,
        ));
        let cache = HybridCacheEngine::new(PassthroughRead::new(Arc::clone(&stores)), &self.cache)
            .await
            .map_err(|error| OriginStorageError::Cache(error.to_string()))?;
        let turso = TursoCasStorage::new(
            Arc::clone(&stores),
            cache.clone(),
            self.origin.storage_binding_id.clone(),
            self.origin.bucket.clone(),
            "turso",
        )
        .map_err(|error| CatalogHostConfigError::Invalid(error.to_string()))?;
        let origin_config = OriginStorageConfig::new(
            self.origin.storage_binding_id.clone(),
            self.origin.bucket.clone(),
            self.cache.clone(),
        )
        .with_scheme(self.origin.scheme.clone());
        let factory = OriginStorageFactory::with_cache(stores, origin_config, cache)?;
        let service_config = CatalogCommitServiceConfig::new(
            self.sink.sink_id.clone(),
            self.origin.bucket.clone(),
            self.sink.namespace.clone(),
            self.sink.table.clone(),
            self.sink.compression,
        )
        .with_warehouse(self.warehouse.clone());
        let catalog = Arc::new(IcebergCatalogCommitService::new(
            Arc::new(factory),
            service_config,
        ));
        Ok(DurableHostState { turso, catalog })
    }
}

/// Deserializes only the exact lowercase Sink codec spellings.
fn deserialize_compression<'de, D>(deserializer: D) -> Result<SinkCompression, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    match value.as_str() {
        "zstd" => Ok(SinkCompression::Zstd),
        "snappy" => Ok(SinkCompression::Snappy),
        "gzip" => Ok(SinkCompression::Gzip),
        "lz4" => Ok(SinkCompression::Lz4),
        "uncompressed" => Ok(SinkCompression::Uncompressed),
        _ => Err(serde::de::Error::custom(format!(
            "unsupported Sink compression `{value}`"
        ))),
    }
}

/// Validates one host identity component without accepting path routing.
fn validate_name(label: &str, value: &str) -> Result<(), CatalogHostConfigError> {
    if value.trim().is_empty()
        || value.len() > 256
        || value.chars().any(|character| {
            character.is_control()
                || character.is_whitespace()
                || matches!(character, '/' | '\\' | ':')
        })
    {
        return Err(CatalogHostConfigError::Invalid(format!(
            "{label} must be one nonempty name"
        )));
    }
    Ok(())
}

/// Validates one URI scheme and excludes local or in-memory routes.
fn validate_scheme(scheme: &str) -> Result<(), CatalogHostConfigError> {
    if scheme.trim().is_empty()
        || scheme.len() > 32
        || scheme.chars().any(|character| {
            character.is_control()
                || character.is_whitespace()
                || matches!(character, '/' | '\\' | ':')
        })
    {
        return Err(CatalogHostConfigError::Invalid(
            "origin scheme must be one nonempty URI scheme".to_owned(),
        ));
    }
    if matches!(scheme, "file" | "memory") {
        return Err(CatalogHostConfigError::Invalid(
            "local and in-memory URI schemes are not origin routes".to_owned(),
        ));
    }
    Ok(())
}

/// Validates the hard DRAM and persistent budgets required by Foyer.
fn validate_cache_budgets(cache: &Cache) -> Result<(), CatalogHostConfigError> {
    verglas_cache::validate_cache_budgets(cache)
        .map_err(|error| CatalogHostConfigError::Invalid(error.to_string()))
}

/// Requires the warehouse to stay below the exact configured URI authority.
fn validate_warehouse(
    warehouse: &str,
    scheme: &str,
    bucket: &str,
) -> Result<(), CatalogHostConfigError> {
    let root = format!("{scheme}://{bucket}");
    let under_root = if warehouse == root {
        true
    } else if let Some(relative) = warehouse.strip_prefix(&format!("{root}/")) {
        !relative.is_empty()
            && !relative.contains(['\\', '?', '#'])
            && relative
                .split('/')
                .all(|segment| !segment.is_empty() && segment != "." && segment != "..")
    } else {
        false
    };
    if warehouse.trim().is_empty()
        || warehouse.len() > 1024
        || warehouse
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
        || !under_root
    {
        return Err(CatalogHostConfigError::Invalid(format!(
            "warehouse must be under {root}"
        )));
    }
    Ok(())
}

/// Validates the four immutable Sink fence values before service construction.
fn validate_sink(sink: &SinkFence) -> Result<(), CatalogHostConfigError> {
    validate_name("Sink id", &sink.sink_id)?;
    if sink.namespace.trim().is_empty()
        || sink.namespace.len() > 512
        || sink.namespace.split('.').any(|segment| {
            segment.is_empty()
                || segment == "."
                || segment == ".."
                || segment.chars().any(|character| {
                    character.is_control()
                        || character.is_whitespace()
                        || matches!(character, '/' | '\\')
                })
        })
    {
        return Err(CatalogHostConfigError::Invalid(
            "Sink namespace is invalid".to_owned(),
        ));
    }
    validate_name("Sink table", &sink.table)
}
