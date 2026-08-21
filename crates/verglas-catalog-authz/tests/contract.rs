use verglas_catalog_authz::{ResourceMapper, VerglasAction};
use verglas_catalog_core::service::{ProjectId, TableId, WarehouseId, authz::CatalogTableAction};

#[test]
fn maps_catalog_ids_without_a_static_database_root() {
    let mapper = ResourceMapper::new();
    let project = ProjectId::new_random();
    let warehouse = WarehouseId::new_random();
    let table = TableId::new_random();

    assert_eq!(
        mapper.project(&project),
        format!("catalog/project/{project}")
    );
    assert_eq!(
        mapper.table(warehouse, table),
        format!("warehouse/{warehouse}/table/{table}")
    );
}

#[test]
fn maps_table_data_operations_to_least_privilege_actions() {
    assert_eq!(
        VerglasAction::from_table(&CatalogTableAction::ReadData),
        VerglasAction::Query
    );
    assert_eq!(
        VerglasAction::from_table(&CatalogTableAction::WriteData),
        VerglasAction::Modify
    );
    assert_eq!(
        VerglasAction::from_table(&CatalogTableAction::Rename),
        VerglasAction::Modify
    );
}
