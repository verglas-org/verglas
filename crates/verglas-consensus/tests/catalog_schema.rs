//! Durable catalog-document transaction acceptance tests.

use verglas_consensus::{CatalogAction, CatalogBatch, CatalogEntity, CatalogRequirement};

/// Builds an opaque Lakekeeper-domain record under a typed entity namespace.
fn record(entity: CatalogEntity, id: &str, document: &str) -> CatalogAction {
    CatalogAction::PutRecord {
        entity,
        id: id.to_owned(),
        document: document.to_owned(),
    }
}

/// A transaction can atomically create independent hosted catalog domain records.
#[test]
fn catalog_batch_accepts_typed_hosted_domain_records() {
    let batch = CatalogBatch::new(
        vec![CatalogRequirement::RecordAbsent {
            entity: CatalogEntity::Warehouse,
            id: "warehouse/default".to_owned(),
        }],
        vec![record(
            CatalogEntity::Warehouse,
            "warehouse/default",
            r#"{"name":"default","status":"active"}"#,
        )],
    );
    assert!(batch.is_ok());
}

/// A malformed document cannot become a replicated durable catalog record.
#[test]
fn catalog_batch_rejects_malformed_hosted_domain_record() {
    let batch = CatalogBatch::new(
        Vec::new(),
        vec![record(CatalogEntity::Table, "table/a", "not-json")],
    );
    assert!(batch.is_err());
}
