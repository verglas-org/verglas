//! HTTP transport boundary tests for OpenRaft peer RPC admission and recovery.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tower::ServiceExt;
use verglas_cluster::{OpenGroupFn, RaftRpcRegistry};

#[tokio::test]
async fn raft_router_rejects_unauthenticated_and_unknown_targets() {
    let registry = RaftRpcRegistry::new("shared-secret");

    let denied = registry
        .router()
        .oneshot(
            Request::post("/consensus/v1/warehouse-a/7/vote")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);

    let unknown = registry
        .router()
        .oneshot(
            Request::post("/consensus/v1/warehouse-a/7/vote")
                .header("x-verglas-cluster-secret", "shared-secret")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(unknown.status(), StatusCode::NOT_FOUND);
}

/// Authenticated Raft traffic reopens a configured local voter before lookup.
#[tokio::test]
async fn raft_router_reopens_configured_target_before_lookup() {
    let registry = RaftRpcRegistry::new("shared-secret");
    let opens = Arc::new(AtomicUsize::new(0));
    let opened_groups = Arc::clone(&opens);
    let opener: OpenGroupFn = Arc::new(move |_group| {
        let opened_groups = Arc::clone(&opened_groups);
        Box::pin(async move {
            opened_groups.fetch_add(1, Ordering::Relaxed);
            Ok(())
        })
    });
    registry.set_opener(opener).await;
    registry.register_target(7).await;

    let denied = registry
        .router()
        .oneshot(
            Request::post("/consensus/v1/warehouse-a/7/vote")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);

    let unknown = registry
        .router()
        .oneshot(
            Request::post("/consensus/v1/warehouse-a/8/vote")
                .header("x-verglas-cluster-secret", "shared-secret")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(unknown.status(), StatusCode::NOT_FOUND);
    assert_eq!(opens.load(Ordering::Relaxed), 0);

    for route in ["append", "snapshot", "vote"] {
        let response = registry
            .router()
            .oneshot(
                Request::post(format!("/consensus/v1/warehouse-a/7/{route}"))
                    .header("x-verglas-cluster-secret", "shared-secret")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
    assert_eq!(opens.load(Ordering::Relaxed), 3);
}
