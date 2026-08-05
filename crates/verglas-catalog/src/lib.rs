//! Shallow Iceberg REST transport for on-prem catalog proxying and polling.
//! Successful reads share one bounded response cache; successful mutations
//! write through to the configured catalog and invalidate those reads.

mod rest;

use std::fmt;

use async_trait::async_trait;

pub use rest::{CatalogGateway, CatalogResponse, RestCatalogSource};

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
