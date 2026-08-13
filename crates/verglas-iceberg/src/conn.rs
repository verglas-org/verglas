//! The resolved connection the engine opens a catalog from.
//!
//! This is a plain data record: everything [`crate::catalog::open_catalog`] needs
//! to reach an Iceberg REST catalog and route data-file IO through an S3
//! endpoint. It carries no resolution logic — callers resolve their own
//! config, environment, or server probe into one of these and hand it to the
//! engine. The engine reads and writes wherever the endpoint points
//! (WHITEPAPER §7.4): the self-hosted server typically points it at its own S3
//! surface for cache residency; a direct object-storage endpoint is equally
//! valid.

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
    /// S3 data endpoints assigned to this workload's cache ring. A direct
    /// object-store connection uses an empty list and its provider defaults.
    pub s3_endpoints: Vec<String>,
    /// SigV4 signing region.
    pub region: String,
    /// Endpoint access key id, when known.
    pub access_key_id: Option<String>,
    /// Endpoint secret access key, when known.
    pub secret_access_key: Option<String>,
}
