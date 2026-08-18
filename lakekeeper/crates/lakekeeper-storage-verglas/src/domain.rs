//! Serializable Lakekeeper catalog documents stored in warehouse CRaft groups.
//!
//! These wire records deliberately carry stable identifiers and Iceberg REST
//! metadata locations. Lakekeeper reconstructs richer response objects at its
//! HTTP boundary instead of persisting process-local domain structs.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use verglas_consensus::CatalogEntity;

/// A persisted warehouse declaration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WarehouseDocument {
    /// Stable Lakekeeper warehouse UUID text.
    pub id: String,
    /// Owning Lakekeeper project UUID text.
    pub project_id: String,
    /// User-visible warehouse name.
    pub name: String,
    /// Object-storage root used for table locations.
    pub storage_location: String,
    /// Whether the warehouse accepts hosted catalog mutations.
    pub active: bool,
    /// Monotonic CRaft document version for authorization and compare-and-swap context.
    #[serde(default)]
    pub version: i64,
    /// Whether Lakekeeper governance protects this warehouse from destructive actions.
    #[serde(default)]
    pub protected: bool,
}

/// A persisted namespace declaration and property map.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct NamespaceDocument {
    /// Stable Lakekeeper namespace UUID text.
    pub id: String,
    /// Owning warehouse UUID text.
    pub warehouse_id: String,
    /// Parent namespace UUID text, when this is a nested namespace.
    pub parent_id: Option<String>,
    /// Namespace levels in canonical stored case.
    pub name: Vec<String>,
    /// Iceberg namespace properties.
    pub properties: BTreeMap<String, String>,
    /// Monotonic CRaft document version for authorization and compare-and-swap context.
    #[serde(default)]
    pub version: i64,
    /// Whether Lakekeeper governance protects this namespace from destructive actions.
    #[serde(default)]
    pub protected: bool,
}

/// A persisted Iceberg table registration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TableDocument {
    /// Stable Lakekeeper table UUID text.
    pub id: String,
    /// Owning warehouse UUID text.
    pub warehouse_id: String,
    /// Owning namespace UUID text.
    pub namespace_id: String,
    /// Table name in canonical stored case.
    pub name: String,
    /// Immutable current Iceberg metadata object location.
    pub metadata_location: String,
    /// Immutable physical table root location.
    pub location: String,
    /// Monotonic CRaft document version for authorization and compare-and-swap context.
    #[serde(default)]
    pub version: i64,
    /// Whether Lakekeeper governance protects this table from destructive actions.
    #[serde(default)]
    pub protected: bool,
}

/// A persisted Iceberg view registration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ViewDocument {
    /// Stable Lakekeeper view UUID text.
    pub id: String,
    /// Owning warehouse UUID text.
    pub warehouse_id: String,
    /// Owning namespace UUID text.
    pub namespace_id: String,
    /// View name in canonical stored case.
    pub name: String,
    /// Immutable current Iceberg view metadata object location.
    pub metadata_location: String,
    /// Monotonic CRaft document version for authorization and compare-and-swap context.
    #[serde(default)]
    pub version: i64,
    /// Whether Lakekeeper governance protects this view from destructive actions.
    #[serde(default)]
    pub protected: bool,
}

/// Returns the CRaft collection that owns a document type.
pub trait CatalogDocument {
    /// Returns the corresponding durable catalog collection.
    fn entity() -> CatalogEntity;
}

impl CatalogDocument for WarehouseDocument {
    /// Returns the warehouse collection.
    fn entity() -> CatalogEntity {
        CatalogEntity::Warehouse
    }
}

impl CatalogDocument for NamespaceDocument {
    /// Returns the namespace collection.
    fn entity() -> CatalogEntity {
        CatalogEntity::Namespace
    }
}

impl CatalogDocument for TableDocument {
    /// Returns the table collection.
    fn entity() -> CatalogEntity {
        CatalogEntity::Table
    }
}

impl CatalogDocument for ViewDocument {
    /// Returns the view collection.
    fn entity() -> CatalogEntity {
        CatalogEntity::View
    }
}
