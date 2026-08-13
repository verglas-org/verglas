//! Building an Iceberg REST catalog from a resolved [`Connection`].
//!
//! Production verbs speak directly to the configured Iceberg REST catalog.
//! Data files are read and
//! written through the S3 endpoint the connection names (the server endpoint, by
//! default), so a warm query stays local and a write gets cache residency and
//! write-back. The storage factory is the S3 OpenDAL backend configured with
//! that endpoint and the endpoint keypair.
//!
//! Hermetic tests build their own [`iceberg::Catalog`] (a `MemoryCatalog` over a
//! local filesystem warehouse) and call the operation functions directly, so
//! this REST wiring is exercised by the server-backed integration path.

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::io::{
    S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION, S3_SECRET_ACCESS_KEY,
};
use iceberg::{Catalog, CatalogBuilder};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
};
use iceberg_storage_opendal::OpenDalStorageFactory;

use crate::conn::Connection;
use crate::error::Result;
use crate::storage::FixedPartStorageFactory;

/// Builds a REST [`Catalog`] from `connection`. The S3 storage factory routes
/// FileIO through the connection's endpoint with its keypair and region, in
/// path-style addressing (what MinIO and the Verglas endpoint serve).
pub async fn open_catalog(connection: &Connection) -> Result<Arc<dyn Catalog>> {
    let mut props: HashMap<String, String> = HashMap::new();
    props.insert(
        REST_CATALOG_PROP_URI.to_owned(),
        connection.catalog_uri.clone(),
    );
    if let Some(warehouse) = &connection.warehouse {
        props.insert(REST_CATALOG_PROP_WAREHOUSE.to_owned(), warehouse.clone());
    }
    if let Some(token) = &connection.token {
        props.insert("token".to_owned(), token.clone());
    }

    // S3 FileIO settings so data files travel through the endpoint. Path-style
    // addressing is what MinIO and the Verglas endpoint expect.
    if let Some(endpoint) = connection.s3_endpoints.first() {
        props.insert(S3_ENDPOINT.to_owned(), endpoint.clone());
    }
    props.insert(S3_REGION.to_owned(), connection.region.clone());
    props.insert(S3_PATH_STYLE_ACCESS.to_owned(), "true".to_owned());
    if let Some(id) = &connection.access_key_id {
        props.insert(S3_ACCESS_KEY_ID.to_owned(), id.clone());
    }
    if let Some(secret) = &connection.secret_access_key {
        props.insert(S3_SECRET_ACCESS_KEY.to_owned(), secret.clone());
    }

    // Wrap the S3 storage so data-file multipart uploads use fixed-size parts.
    // Variable-size parts are AWS-only; stricter S3-compatible stores reject them (issue #308).
    let storage = FixedPartStorageFactory::new(
        OpenDalStorageFactory::S3 {
            configured_scheme: "s3".to_owned(),
            customized_credential_load: None,
        },
        connection.s3_endpoints.clone(),
    )?;
    let catalog = RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(storage))
        .load("verglas", props)
        .await?;
    Ok(Arc::new(catalog))
}
