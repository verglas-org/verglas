use lakekeeper::service::authz::{
    CatalogGenericTableAction, CatalogNamespaceAction, CatalogProjectAction, CatalogRoleAction,
    CatalogServerAction, CatalogTableAction, CatalogTagAction, CatalogUserAction,
    CatalogViewAction, CatalogWarehouseAction,
};
use serde::Serialize;

/// Backend-neutral Verglas action names accepted by the decision endpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VerglasAction {
    Discover,
    Describe,
    Query,
    Modify,
    CreateChild,
    Execute,
    ManageGrants,
}

impl VerglasAction {
    pub(crate) fn from_server(action: &CatalogServerAction) -> Self {
        match action {
            CatalogServerAction::CreateProject { .. } | CatalogServerAction::ProvisionUsers => {
                Self::CreateChild
            }
            CatalogServerAction::UpdateUsers | CatalogServerAction::DeleteUsers => Self::Modify,
            CatalogServerAction::ListUsers => Self::Discover,
        }
    }

    pub(crate) fn from_project(action: &CatalogProjectAction) -> Self {
        match action {
            CatalogProjectAction::CreateWarehouse { .. }
            | CatalogProjectAction::CreateRole { .. }
            | CatalogProjectAction::CreateTag { .. } => Self::CreateChild,
            CatalogProjectAction::Delete
            | CatalogProjectAction::Rename
            | CatalogProjectAction::ModifyTaskQueueConfig => Self::Modify,
            CatalogProjectAction::GetMetadata
            | CatalogProjectAction::GetEndpointStatistics
            | CatalogProjectAction::GetTaskQueueConfig
            | CatalogProjectAction::GetProjectTasks => Self::Describe,
            CatalogProjectAction::ListWarehouses
            | CatalogProjectAction::IncludeInList
            | CatalogProjectAction::ListRoles
            | CatalogProjectAction::SearchRoles
            | CatalogProjectAction::ListTags => Self::Discover,
            CatalogProjectAction::ControlProjectTasks => Self::Execute,
        }
    }

    pub(crate) fn from_warehouse(action: &CatalogWarehouseAction) -> Self {
        match action {
            CatalogWarehouseAction::CreateNamespace { .. } => Self::CreateChild,
            CatalogWarehouseAction::Delete
            | CatalogWarehouseAction::UpdateStorage
            | CatalogWarehouseAction::Deactivate
            | CatalogWarehouseAction::Activate
            | CatalogWarehouseAction::Rename
            | CatalogWarehouseAction::ModifySoftDeletion
            | CatalogWarehouseAction::ModifyTaskQueueConfig
            | CatalogWarehouseAction::SetProtection
            | CatalogWarehouseAction::SetFormatVersionPolicy
            | CatalogWarehouseAction::ManageTags => Self::Modify,
            CatalogWarehouseAction::GetMetadata
            | CatalogWarehouseAction::GetConfig
            | CatalogWarehouseAction::Use
            | CatalogWarehouseAction::GetTaskQueueConfig
            | CatalogWarehouseAction::GetAllTasks
            | CatalogWarehouseAction::GetEndpointStatistics => Self::Describe,
            CatalogWarehouseAction::ListNamespaces
            | CatalogWarehouseAction::ListEverything
            | CatalogWarehouseAction::IncludeInList
            | CatalogWarehouseAction::ListDeletedTabulars => Self::Discover,
            CatalogWarehouseAction::ControlAllTasks => Self::Execute,
        }
    }

    pub(crate) fn from_namespace(action: &CatalogNamespaceAction) -> Self {
        match action {
            CatalogNamespaceAction::CreateTable { .. }
            | CatalogNamespaceAction::CreateView { .. }
            | CatalogNamespaceAction::CreateNamespace { .. }
            | CatalogNamespaceAction::CreateGenericTable { .. } => Self::CreateChild,
            CatalogNamespaceAction::Delete { .. }
            | CatalogNamespaceAction::UpdateProperties { .. }
            | CatalogNamespaceAction::SetProtection
            | CatalogNamespaceAction::ManageTags => Self::Modify,
            CatalogNamespaceAction::GetMetadata => Self::Describe,
            CatalogNamespaceAction::ListTables
            | CatalogNamespaceAction::ListViews
            | CatalogNamespaceAction::ListNamespaces
            | CatalogNamespaceAction::ListEverything
            | CatalogNamespaceAction::IncludeInList
            | CatalogNamespaceAction::ListGenericTables => Self::Discover,
        }
    }

    /// Maps one Lakekeeper table operation to its minimum safe Verglas permission.
    #[must_use]
    pub fn from_table(action: &CatalogTableAction) -> Self {
        match action {
            CatalogTableAction::ReadData => Self::Query,
            CatalogTableAction::GetMetadata | CatalogTableAction::GetTasks => Self::Describe,
            CatalogTableAction::IncludeInList => Self::Discover,
            CatalogTableAction::ControlTasks => Self::Execute,
            CatalogTableAction::Drop { .. }
            | CatalogTableAction::WriteData
            | CatalogTableAction::Commit { .. }
            | CatalogTableAction::Rename
            | CatalogTableAction::Undrop
            | CatalogTableAction::SetProtection
            | CatalogTableAction::ManageTags => Self::Modify,
        }
    }

    pub(crate) fn from_view(action: &CatalogViewAction) -> Self {
        match action {
            CatalogViewAction::Select => Self::Query,
            CatalogViewAction::GetMetadata | CatalogViewAction::GetTasks => Self::Describe,
            CatalogViewAction::IncludeInList => Self::Discover,
            CatalogViewAction::ControlTasks => Self::Execute,
            CatalogViewAction::Drop { .. }
            | CatalogViewAction::Commit { .. }
            | CatalogViewAction::Rename
            | CatalogViewAction::Undrop
            | CatalogViewAction::SetProtection
            | CatalogViewAction::ManageTags => Self::Modify,
        }
    }

    pub(crate) fn from_generic_table(action: &CatalogGenericTableAction) -> Self {
        match action {
            CatalogGenericTableAction::ReadData => Self::Query,
            CatalogGenericTableAction::GetMetadata | CatalogGenericTableAction::GetTasks => {
                Self::Describe
            }
            CatalogGenericTableAction::IncludeInList => Self::Discover,
            CatalogGenericTableAction::ControlTasks => Self::Execute,
            CatalogGenericTableAction::Drop
            | CatalogGenericTableAction::WriteData
            | CatalogGenericTableAction::Rename
            | CatalogGenericTableAction::Undrop
            | CatalogGenericTableAction::SetProtection
            | CatalogGenericTableAction::ManageTags => Self::Modify,
        }
    }

    pub(crate) fn from_user(action: CatalogUserAction) -> Self {
        match action {
            CatalogUserAction::Read => Self::Describe,
            CatalogUserAction::Update | CatalogUserAction::Delete => Self::Modify,
            CatalogUserAction::ReadRoleAssignments => Self::ManageGrants,
        }
    }

    pub(crate) fn from_role(action: &CatalogRoleAction) -> Self {
        match action {
            CatalogRoleAction::Read | CatalogRoleAction::ReadMetadata => Self::Describe,
            CatalogRoleAction::Delete
            | CatalogRoleAction::Update
            | CatalogRoleAction::UpdateSourceSystem { .. } => Self::Modify,
            CatalogRoleAction::ManageRoleAssignments | CatalogRoleAction::ReadRoleAssignments => {
                Self::ManageGrants
            }
        }
    }

    pub(crate) fn from_tag(action: CatalogTagAction) -> Self {
        match action {
            CatalogTagAction::Read | CatalogTagAction::ReadAttachments => Self::Describe,
            CatalogTagAction::Update
            | CatalogTagAction::Delete
            | CatalogTagAction::Apply
            | CatalogTagAction::Remove => Self::Modify,
        }
    }
}
