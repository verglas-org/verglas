//! Typed contracts for the optional on-prem Rill dashboard API.

use serde::{Deserialize, Serialize};

/// Request body for creating a dashboard from an Iceberg table.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct CreateDashboardRequest<'a> {
    /// Dotted Iceberg table identifier.
    pub table: &'a str,
    /// Optional stable dashboard resource name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<&'a str>,
}

/// One Verglas-owned Rill dashboard.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct DashboardInfo {
    /// Stable Rill resource name.
    pub name: String,
    /// Dotted Iceberg table identifier backing the dashboard.
    pub table: String,
    /// Browser-facing Rill Explore URL.
    pub url: String,
}

/// Dashboards managed by Verglas in the configured Rill project.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct DashboardList {
    /// Verglas-owned dashboards, ordered by name.
    pub dashboards: Vec<DashboardInfo>,
}

/// Acknowledgement after deleting one dashboard's owned resources.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct DashboardDeleted {
    /// Dashboard resource name that was deleted.
    pub deleted: String,
}
