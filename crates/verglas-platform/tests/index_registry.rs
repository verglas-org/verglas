//! System-table tests for the `verglas_sys.indexes` vector-index registry: a
//! declared index lands as a row keyed by `<cluster_id>/<target>/<field>`,
//! carrying its cluster id, metric, params, and (once built) reflected snapshot
//! and blob reference; state changes and build updates are revision-only
//! appends; the same target+field on two clusters are two independent rows; and
//! the per-cluster running view is what a reboot rehydrates.

mod support;

use support::TestCatalog;
use verglas_platform::{IndexSpec, SystemCatalog, SystemState, TARGET_KIND_TABLE, index_row_name};

fn spec(cluster_id: &str, target: &str, field: &str) -> IndexSpec {
    IndexSpec {
        name: index_row_name(cluster_id, target, field),
        target_kind: TARGET_KIND_TABLE.to_owned(),
        target: target.to_owned(),
        field: field.to_owned(),
        metric: "l2".to_owned(),
        params: r#"{"r":64,"l":100,"alpha":1.2,"idField":"id"}"#.to_owned(),
        cluster_id: cluster_id.to_owned(),
        reflected_snapshot: None,
        blob_ref: None,
        created_by: "test".to_owned(),
    }
}

/// Declaring an index lands one row, keyed by name, in `Running` at revision 1,
/// carrying the cluster id and every declared field through a scan round-trip.
#[tokio::test]
async fn declare_lands_row_with_cluster_id() {
    let tc = TestCatalog::new().await;
    let sys = SystemCatalog::new(tc.catalog.clone());

    let row = sys
        .register_index(spec("host-a", "tbl:default.docs", "embedding"))
        .await
        .expect("register index");
    assert_eq!(row.name, "host-a/tbl:default.docs/embedding");
    assert_eq!(row.cluster_id, "host-a");
    assert_eq!(row.state, SystemState::Running);
    assert_eq!(row.revision, 1);
    assert_eq!(row.reflected_snapshot, None);
    assert_eq!(row.blob_ref, None);

    let listed = sys.list_indexes().await.expect("list");
    assert_eq!(listed.len(), 1);
    let got = sys
        .get_index("host-a/tbl:default.docs/embedding")
        .await
        .expect("get")
        .expect("present");
    assert_eq!(got.metric, "l2");
    assert_eq!(got.field, "embedding");
    assert_eq!(got.target, "tbl:default.docs");
    assert!(got.params.contains("\"idField\":\"id\""));
}

/// A build update and a state change are each a revision-only append: the store
/// is never mutated in place, the fields change, the revision increments, and
/// prior revisions stay readable.
#[tokio::test]
async fn build_and_state_changes_append_revisions() {
    let tc = TestCatalog::new().await;
    let sys = SystemCatalog::new(tc.catalog.clone());
    let name = index_row_name("host-a", "tbl:default.docs", "embedding");

    sys.register_index(spec("host-a", "tbl:default.docs", "embedding"))
        .await
        .expect("declare");

    // A build lands a reflected snapshot + blob ref as a new revision.
    let built = sys
        .set_index_build(
            &name,
            987654321,
            "/cache/vector-index/blob.puffin".to_owned(),
        )
        .await
        .expect("build");
    assert_eq!(built.revision, 2);
    assert_eq!(built.reflected_snapshot, Some(987654321));
    assert_eq!(
        built.blob_ref.as_deref(),
        Some("/cache/vector-index/blob.puffin")
    );
    assert_eq!(built.state, SystemState::Running);

    // Pausing is another revision that flips only the state.
    let paused = sys
        .set_index_state(&name, SystemState::Paused)
        .await
        .expect("pause");
    assert_eq!(paused.revision, 3);
    assert_eq!(paused.state, SystemState::Paused);
    // The build fields carry forward across the state change.
    assert_eq!(paused.reflected_snapshot, Some(987654321));

    let history = sys.index_revisions(&name).await.expect("revisions");
    assert_eq!(history.len(), 3, "three appended revisions, none mutated");
    assert_eq!(history[0].state, SystemState::Running);
    assert_eq!(history[0].reflected_snapshot, None);

    // The current view keeps only the highest revision.
    let current = sys.list_indexes().await.expect("list");
    assert_eq!(current.len(), 1);
    assert_eq!(current[0].revision, 3);
}

/// The same target+field on two clusters are two independent rows; the
/// per-cluster running view returns only the local cluster's running rows.
#[tokio::test]
async fn per_cluster_rows_are_independent() {
    let tc = TestCatalog::new().await;
    let sys = SystemCatalog::new(tc.catalog.clone());

    sys.register_index(spec("host-a", "tbl:default.docs", "embedding"))
        .await
        .expect("a");
    sys.register_index(spec("host-b", "tbl:default.docs", "embedding"))
        .await
        .expect("b");
    // A second, paused index on host-a that the running view must exclude.
    let paused_name = index_row_name("host-a", "tbl:default.notes", "vec");
    sys.register_index(spec("host-a", "tbl:default.notes", "vec"))
        .await
        .expect("a2");
    sys.set_index_state(&paused_name, SystemState::Paused)
        .await
        .expect("pause a2");

    assert_eq!(sys.list_indexes().await.expect("all").len(), 3);

    let host_a = sys
        .list_running_indexes_for_cluster("host-a")
        .await
        .expect("host-a running");
    assert_eq!(host_a.len(), 1, "one running row for host-a");
    assert_eq!(host_a[0].name, "host-a/tbl:default.docs/embedding");

    let host_b = sys
        .list_running_indexes_for_cluster("host-b")
        .await
        .expect("host-b running");
    assert_eq!(host_b.len(), 1);
    assert_eq!(host_b[0].cluster_id, "host-b");
}

/// A state change on an unknown index is a clear NotFound, not a silent no-op.
#[tokio::test]
async fn unknown_index_state_change_is_not_found() {
    let tc = TestCatalog::new().await;
    let sys = SystemCatalog::new(tc.catalog.clone());
    let err = sys
        .set_index_state("nope", SystemState::Paused)
        .await
        .expect_err("should be NotFound");
    assert!(err.to_string().contains("not found"));
}
