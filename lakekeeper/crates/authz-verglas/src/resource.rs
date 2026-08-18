use lakekeeper::service::{
    GenericTableId, NamespaceId, ProjectId, RoleId, TableId, TagDefinitionId, ViewId, WarehouseId,
    authn::UserId,
};

/// Deterministically maps Lakekeeper entities into dynamic Verglas database trees.
#[derive(Clone, Debug, Default)]
pub struct ResourceMapper;

impl ResourceMapper {
    /// Creates the canonical multi-database resource mapper.
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    /// Shared Lakekeeper control-plane root pre-provisioned by Verglas.
    #[must_use]
    pub fn control_root(&self) -> &'static str {
        "lakekeeper"
    }

    #[must_use]
    pub fn project(&self, id: &ProjectId) -> String {
        format!("lakekeeper/project/{id}")
    }

    #[must_use]
    pub fn warehouse(&self, id: WarehouseId) -> String {
        format!("warehouse/{id}")
    }

    #[must_use]
    pub fn namespace(&self, id: NamespaceId) -> String {
        format!("namespace/{id}")
    }

    #[must_use]
    pub fn table(&self, warehouse_id: WarehouseId, id: TableId) -> String {
        format!("warehouse/{warehouse_id}/table/{id}")
    }

    #[must_use]
    pub fn view(&self, warehouse_id: WarehouseId, id: ViewId) -> String {
        format!("warehouse/{warehouse_id}/view/{id}")
    }

    #[must_use]
    pub fn generic_table(&self, warehouse_id: WarehouseId, id: GenericTableId) -> String {
        format!("warehouse/{warehouse_id}/generic-table/{id}")
    }

    #[must_use]
    pub fn user(&self, id: &UserId) -> String {
        format!("user/{id}")
    }

    #[must_use]
    pub fn role(&self, id: RoleId) -> String {
        format!("lakekeeper/role/{id}")
    }

    #[must_use]
    pub fn tag(&self, id: TagDefinitionId) -> String {
        format!("lakekeeper/tag/{id}")
    }
}
