//! Opening an Iceberg REST catalog with catalog-vended write credentials (#145).
//!
//! The SDK is the whole write path: no server sits between it and the object
//! store. `open_catalog` builds a REST catalog pointed at the caller's
//! `[catalog]` (uri/warehouse/bearer), and asks the catalog to vend per-table
//! storage credentials on every table load/create by sending
//! `X-Iceberg-Access-Delegation: vended-credentials` (the Iceberg REST spec's
//! standard delegation header — the R2 Data Catalog and other conformant
//! catalogs answer with scoped S3 credentials in the response `config` map,
//! which `iceberg-catalog-rest` merges straight into the table's `FileIO`
//! props). No static access key is configured here, so those vended
//! credentials are what every data-file write actually uses.

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::{Catalog, CatalogBuilder};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
};
use iceberg_storage_opendal::OpenDalStorageFactory;

use super::error::Result;
use super::storage::FixedPartStorageFactory;

/// The Iceberg REST spec's access-delegation header, requesting per-table
/// vended credentials on every table response.
const ACCESS_DELEGATION_HEADER: &str = "header.X-Iceberg-Access-Delegation";

/// The delegation mode every conformant REST catalog recognizes for
/// short-lived, table-scoped storage credentials.
const VENDED_CREDENTIALS: &str = "vended-credentials";

/// Resolved coordinates for the tenant's Iceberg REST catalog. Mirrors the
/// `[catalog]` section of the agent config file; the caller (typically the
/// CLI) is responsible for reading that file.
#[derive(Debug, Clone)]
pub struct CatalogConfig {
    /// Base URI of the Iceberg REST catalog (no trailing slash required).
    pub uri: String,
    /// Warehouse identifier, required by multi-warehouse catalogs (the R2 Data
    /// Catalog's account/bucket-scoped warehouse id).
    pub warehouse: Option<String>,
    /// Bearer token presented on every catalog request.
    pub bearer: Option<String>,
}

/// Opens a REST [`Catalog`] from `config`. Data files are written through the
/// table's `FileIO`, which is the S3-compatible OpenDAL backend wrapped so its
/// multipart uploads use fixed-size parts (required by strict S3-compatible
/// stores, issue #308's fix ported client-side) and populated with whatever
/// credentials the catalog vends per table — this function sets no static S3
/// keypair.
pub async fn open_catalog(config: &CatalogConfig) -> Result<Arc<dyn Catalog>> {
    let mut props: HashMap<String, String> = HashMap::new();
    props.insert(REST_CATALOG_PROP_URI.to_owned(), config.uri.clone());
    if let Some(warehouse) = &config.warehouse {
        props.insert(REST_CATALOG_PROP_WAREHOUSE.to_owned(), warehouse.clone());
    }
    if let Some(bearer) = &config.bearer {
        props.insert("token".to_owned(), bearer.clone());
    }
    props.insert(
        ACCESS_DELEGATION_HEADER.to_owned(),
        VENDED_CREDENTIALS.to_owned(),
    );

    let storage = FixedPartStorageFactory::new(OpenDalStorageFactory::S3 {
        configured_scheme: "s3".to_owned(),
        customized_credential_load: None,
    });
    let catalog = RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(storage))
        .load("verglas-sdk", props)
        .await?;
    Ok(Arc::new(catalog))
}
