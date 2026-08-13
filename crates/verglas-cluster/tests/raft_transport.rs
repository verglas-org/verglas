//! HTTP transport boundary tests for OpenRaft peer RPC admission.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;
use verglas_cluster::RaftRpcRegistry;

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
