//! Shallow Iceberg REST transport for on-prem catalog proxying and polling.
//! Successful reads share one bounded response cache; successful mutations
//! write through to the configured catalog and invalidate those reads.

mod managed;
mod registry;
mod rest;

use std::fmt;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

pub use managed::{
    ManagedCatalogClient, ManagedCatalogError, ManagedCatalogRequest, ManagedCatalogResponse,
};
pub use registry::{
    CatalogBinding, CatalogBindingId, CatalogRegistry, CatalogRuntimeRegistry, DatabaseId,
    RegistryError, StorageBindingId,
};
pub use rest::{CatalogGateway, CatalogResponse, RestCatalogSource};

/// One authoritative catalog-pointer mutation supplied by a transactional catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogMutation {
    /// Monotonic catalog outbox sequence. Gaps are valid.
    pub sequence: u64,
    /// Stable delivery id used to absorb retries.
    pub event_id: String,
    /// Catalog warehouse containing the table.
    pub warehouse_id: String,
    /// Stable catalog table id.
    pub table_id: String,
    /// Namespace levels used by the Iceberg REST identifier.
    pub namespace: Vec<String>,
    /// Table name within `namespace`.
    pub table: String,
    /// Namespace before a rename, otherwise absent.
    #[serde(default)]
    pub previous_namespace: Option<Vec<String>>,
    /// Table name before a rename, otherwise absent.
    #[serde(default)]
    pub previous_table: Option<String>,
    /// Catalog mutation operation such as `updateTable` or `dropTable`.
    pub operation: String,
    /// Newly committed immutable Iceberg metadata pointer, absent for a drop.
    pub metadata_location: Option<String>,
    /// Newly committed snapshot id when the publisher knows it.
    pub snapshot_id: Option<i64>,
}

/// A table identity in an Iceberg catalog.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TableIdent {
    /// Namespace levels, outermost first.
    pub namespace: Vec<String>,
    /// Table name within the namespace.
    pub name: String,
}

impl TableIdent {
    /// Builds an identity from namespace levels and a table name.
    pub fn new(namespace: &[&str], name: &str) -> Self {
        Self {
            namespace: namespace.iter().map(|value| (*value).to_owned()).collect(),
            name: name.to_owned(),
        }
    }

    /// Returns the dotted fully qualified table name.
    pub fn dotted(&self) -> String {
        let mut value = self.namespace.join(".");
        if !value.is_empty() {
            value.push('.');
        }
        value.push_str(&self.name);
        value
    }
}

impl fmt::Display for TableIdent {
    /// Writes the dotted fully qualified table name.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.dotted())
    }
}

/// The current catalog pointer for one table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableState {
    /// Object-store location of the current metadata JSON.
    pub metadata_location: String,
    /// Current snapshot id, or none before the first snapshot.
    pub current_snapshot_id: Option<i64>,
}

/// An error sending or decoding a shallow catalog request.
#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    /// The HTTP request failed.
    #[error("catalog request failed: {0}")]
    Http(#[from] reqwest::Error),
    /// The catalog returned a non-success status.
    #[error("catalog returned HTTP {status} for {url}")]
    Status {
        /// Returned HTTP status.
        status: u16,
        /// Request URL.
        url: String,
    },
    /// The response body did not carry the required shallow fields.
    #[error("malformed catalog response from {url}: {detail}")]
    Malformed {
        /// Request URL.
        url: String,
        /// Decode failure detail.
        detail: String,
    },
    /// SigV4 signing could not produce an authenticated request.
    #[error("catalog request signing failed: {detail}")]
    Auth {
        /// Signing failure detail.
        detail: String,
    },
}

/// The shallow catalog operations required by polling.
#[async_trait]
pub trait CatalogSource: Send + Sync + 'static {
    /// Lists every table visible to the configured catalog identity.
    async fn list_tables(&self) -> Result<Vec<TableIdent>, CatalogError>;

    /// Reads one table's metadata location and current snapshot id.
    async fn table_pointer(&self, table: &TableIdent) -> Result<TableState, CatalogError>;
}
