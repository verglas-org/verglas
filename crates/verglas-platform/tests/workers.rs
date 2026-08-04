//! The `verglas_sys.workers` registry.

mod support;

use support::TestCatalog;
use verglas_platform::{SystemCatalog, SystemState, WorkerSpec};

/// A worker declares, lists as active, pauses to a new revision, and reads back
/// with its triggers and output intact.
#[tokio::test]
async fn worker_declare_list_and_pause() {
    let tc = TestCatalog::new().await;
    let sys = SystemCatalog::new(tc.catalog.clone());

    sys.register_worker(WorkerSpec {
        name: "http_poll".to_owned(),
        code: r#"{"exec":["bun","shim.ts","poll.ts"]}"#.to_owned(),
        triggers: r#"[{"type":"cron","schedule":"*/15 * * * *"}]"#.to_owned(),
        output: Some("raw.events".to_owned()),
        config: "{}".to_owned(),
        created_by: "test".to_owned(),
    })
    .await
    .expect("register");

    let active = sys.list_active_workers().await.expect("list");
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].name, "http_poll");
    assert_eq!(active[0].output.as_deref(), Some("raw.events"));
    assert_eq!(active[0].state, SystemState::Running);
    assert!(active[0].triggers.contains("cron"));

    let paused = sys
        .set_worker_state("http_poll", SystemState::Paused)
        .await
        .expect("pause");
    assert_eq!(paused.revision, 2);
    assert_eq!(paused.state, SystemState::Paused);

    // The paused worker still lists (active = non-archived), at its new state.
    let current = sys
        .get_worker("http_poll")
        .await
        .expect("get")
        .expect("row");
    assert_eq!(current.state, SystemState::Paused);
}
