//! Public object-kind contract for cloud composition.

use verglas_do_engine::{ObjectKind, ObjectPolicy};

#[test]
fn durable_and_non_durable_objects_have_distinct_authority_contracts() {
    let sql = ObjectPolicy::new(ObjectKind::Sql, true).expect("SQL DO policy");
    assert!(sql.requires_durable_authority());
    assert!(sql.offload_enabled());
    assert!(sql.requires_checkpoint_before_stop());

    let lakehouse = ObjectPolicy::new(ObjectKind::Lakehouse, false).expect("Lakehouse DO policy");
    assert!(lakehouse.requires_durable_authority());
    assert!(!lakehouse.offload_enabled());
    assert!(lakehouse.requires_checkpoint_before_stop());

    let query = ObjectPolicy::new(ObjectKind::Query, false).expect("query object policy");
    assert!(!query.requires_durable_authority());
    assert!(!query.requires_checkpoint_before_stop());
}

#[test]
fn non_durable_query_objects_cannot_enable_transaction_offload() {
    let error = ObjectPolicy::new(ObjectKind::Query, true).expect_err("query has no commit stream");
    assert!(error.to_string().contains("query object"));
}
