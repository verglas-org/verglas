//! Platform system-table watermarks.

mod support;

use support::TestCatalog;
use verglas_platform::SystemCatalog;

#[tokio::test]
async fn watermark_store_is_append_only_and_deployment_scoped() {
    let tc = TestCatalog::new().await;
    let sys = SystemCatalog::new(tc.catalog.clone());

    // Nothing set yet: no row.
    let none = sys.get_watermark("dep-a").await.expect("get before set");
    assert!(none.is_none(), "no watermark before the first set");

    // First set is revision 1.
    let first = sys
        .set_watermark("dep-a", "snap-1".to_owned())
        .await
        .expect("first set");
    assert_eq!(first.deployment, "dep-a");
    assert_eq!(first.watermark, "snap-1");
    assert_eq!(first.revision, 1);

    // A second set appends revision 2 and the read returns the latest.
    let second = sys
        .set_watermark("dep-a", "snap-2".to_owned())
        .await
        .expect("second set");
    assert_eq!(second.revision, 2);
    let current = sys
        .get_watermark("dep-a")
        .await
        .expect("get after set")
        .expect("row exists");
    assert_eq!(current.watermark, "snap-2");
    assert_eq!(current.revision, 2);

    // Another deployment is untouched.
    assert!(
        sys.get_watermark("dep-b")
            .await
            .expect("get other")
            .is_none(),
        "deployments are independent"
    );
}
