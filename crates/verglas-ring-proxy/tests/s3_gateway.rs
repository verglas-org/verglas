//! Contract tests for forwarding every Iceberg object class through the ring pool.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::{Body, Bytes};
use axum::http::Request;
use axum::routing::any;
use http_body_util::BodyExt;
use tower::ServiceExt;
use verglas_ring_proxy::{EndpointPool, s3_router};

#[tokio::test]
async fn iceberg_writes_and_reads_use_the_same_pooled_ingress() {
    let calls = Arc::new(Mutex::new(Vec::<(usize, String, String)>::new()));
    let mut endpoints = Vec::new();
    for member in 0..4 {
        let calls = Arc::clone(&calls);
        let app = Router::new().fallback(any(move |request: Request<Body>| {
            let calls = Arc::clone(&calls);
            async move {
                calls.lock().expect("calls").push((
                    member,
                    request.method().to_string(),
                    request.uri().to_string(),
                ));
                (
                    [("x-ring-member", member.to_string())],
                    Bytes::from_static(b"ok"),
                )
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind backend");
        endpoints.push(format!(
            "http://{}",
            listener.local_addr().expect("address")
        ));
        tokio::spawn(async move { axum::serve(listener, app).await });
    }

    let proxy = s3_router(EndpointPool::new(endpoints).expect("pool"));
    let objects = [
        "/warehouse/db/table/data/a.parquet",
        "/warehouse/db/table/delete/a.parquet",
        "/warehouse/db/table/metadata/snap.avro",
        "/warehouse/db/table/metadata/manifest-list.avro",
        "/warehouse/db/table/metadata/v2.metadata.json",
        "/warehouse/db/table/metadata/stats.puffin",
    ];
    let mut assigned = HashMap::new();
    for path in objects {
        let put = proxy
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(path)
                    .body(Body::from("object"))
                    .expect("PUT"),
            )
            .await
            .expect("proxy PUT");
        assert!(put.status().is_success());
        let member = put.headers()["x-ring-member"]
            .to_str()
            .expect("member")
            .to_owned();
        assigned.insert(path, member);

        let get = proxy
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(path)
                    .body(Body::empty())
                    .expect("GET"),
            )
            .await
            .expect("proxy GET");
        assert_eq!(get.headers()["x-ring-member"], assigned[path]);
        assert_eq!(
            get.into_body().collect().await.expect("body").to_bytes(),
            "ok"
        );
    }

    let multipart_path = "/warehouse/db/table/data/large.parquet";
    let mut multipart_members = Vec::new();
    for query in [
        "?uploads",
        "?partNumber=1&uploadId=u",
        "?partNumber=2&uploadId=u",
        "?uploadId=u",
    ] {
        let response = proxy
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("{multipart_path}{query}"))
                    .body(Body::empty())
                    .expect("multipart request"),
            )
            .await
            .expect("proxy multipart");
        multipart_members.push(
            response.headers()["x-ring-member"]
                .to_str()
                .expect("member")
                .to_owned(),
        );
    }
    assert!(multipart_members.windows(2).all(|pair| pair[0] == pair[1]));
}
