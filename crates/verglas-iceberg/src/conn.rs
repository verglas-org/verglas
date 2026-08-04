//! The resolved connection the engine opens a catalog from.
//!
//! This is a plain data record: everything [`crate::catalog::open_catalog`] needs
//! to reach an Iceberg REST catalog and route data-file IO through an S3
//! endpoint. It carries no resolution logic — callers resolve their own
//! config, environment, or server probe into one of these and hand it to the
//! engine. The engine reads and writes wherever the endpoint points
//! (WHITEPAPER §7.4): the server points it at its own S3 surface for cache
//! residency; the cloud committer points it straight at object storage.

/// A fully resolved connection: everything an operation needs to reach the
/// catalog and write or read data files.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Connection {
    /// Iceberg REST catalog base URI (no trailing `/v1`).
    pub catalog_uri: String,
    /// Optional catalog bearer token.
    pub token: Option<String>,
    /// Optional catalog warehouse identifier.
    pub warehouse: Option<String>,
    /// S3 data endpoint URL, when one is known. Data files and queries are
    /// routed through it so a server in the path gives cache residency.
    pub s3_endpoint: Option<String>,
    /// SigV4 signing region.
    pub region: String,
    /// Endpoint access key id, when known.
    pub access_key_id: Option<String>,
    /// Endpoint secret access key, when known.
    pub secret_access_key: Option<String>,
}
