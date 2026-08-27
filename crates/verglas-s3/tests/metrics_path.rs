//! Integration test for the request metrics wired into the S3 front-end (#46).
//!
//! Drives real GET/HEAD/LIST/PUT requests through the router with a
//! [`NodeMetrics`] handle attached, then asserts the request-duration histogram
//! and the request counter moved — the "after driving requests, the counters
//! move" acceptance test. The passthrough reader labels every serve
//! `passthrough` (no cache in its path), which is what the histogram records.

use std::sync::Arc;

use bytes::Bytes;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, PutOptions, PutPayload};
use verglas_core::metrics::{MetricsSnapshot, NodeMetrics, TierSize, render};
use verglas_core::read::ServedTier;
use verglas_s3::{
    BackendStore, NoopInvalidation, PassthroughList, PassthroughRead, PassthroughWrite,
};

/// The bucket the in-memory origin serves.
const BUCKET: &str = "test-bucket";

/// An empty scrape-time snapshot: this test asserts on the LIVE request
/// families, which come from the metrics handle, not the snapshot.
fn empty_snapshot() -> MetricsSnapshot {
    MetricsSnapshot {
        bytes_served: vec![(ServedTier::Dram, 0)],
        cache_hits: 0,
        cache_misses: 0,
        cache_admitted: 0,
        cache_rejected: 0,
        cache_tiers: vec![(
            ServedTier::Dram,
            TierSize {
                used: 0,
                capacity: 0,
            },
        )],
        fill_inflight: 0,
        backend_requests: vec![("success", 0)],
        backend_retries: 0,
        breaker_trips: 0,
        breaker_rejections: 0,
        breaker_state: "closed",
    }
}

/// Serves an axum router on an ephemeral loopback port, returning its base URL.
async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    format!("http://{addr}")
}

/// Boots the front-end over an in-memory origin holding one object, with a
/// metrics handle attached; returns the base URL and the shared handle.
async fn serve_with_metrics(key: &str, body: Bytes) -> (String, Arc<NodeMetrics>) {
    let store = Arc::new(InMemory::new());
    store
        .put_opts(
            &Path::from(key),
            PutPayload::from(body),
            PutOptions::default(),
        )
        .await
        .expect("seed object");
    let metrics = Arc::new(NodeMetrics::new().expect("build metrics"));
    let app = verglas_s3::router_with_passthrough(
        "default",
        PassthroughRead::new(BackendStore::single("default", BUCKET, store.clone())),
        PassthroughWrite::new(BackendStore::single("default", BUCKET, store.clone())),
        Arc::new(PassthroughList::new(BackendStore::single(
            "default", BUCKET, store,
        ))),
        Arc::new(NoopInvalidation),
        None,
        None,
        None,
        Some(metrics.clone()),
    );
    (serve(app).await, metrics)
}

/// After driving a GET, a HEAD, a LIST, and a PUT through the front-end, the
/// live request families carry a series for each op, each labelled with the
/// serving tier, status, and default tenant — the counters moved.
#[tokio::test]
async fn requests_move_the_metric_counters() {
    let body = Bytes::from(vec![7u8; 64 * 1024]);
    let (base, metrics) = serve_with_metrics("data/part-0.parquet", body.clone()).await;

    // GET: reads the whole object (drains the body so the histogram observes).
    let got = reqwest::get(format!("{base}/{BUCKET}/data/part-0.parquet"))
        .await
        .expect("GET");
    assert_eq!(got.status(), 200);
    let bytes = got.bytes().await.expect("GET body");
    assert_eq!(bytes.len(), body.len());

    // HEAD.
    let head = reqwest::Client::new()
        .head(format!("{base}/{BUCKET}/data/part-0.parquet"))
        .send()
        .await
        .expect("HEAD");
    assert_eq!(head.status(), 200);

    // LIST.
    let list = reqwest::get(format!("{base}/{BUCKET}?list-type=2"))
        .await
        .expect("LIST");
    assert_eq!(list.status(), 200);

    // PUT a new object.
    let put = reqwest::Client::new()
        .put(format!("{base}/{BUCKET}/data/new.parquet"))
        .body(vec![1u8; 128])
        .send()
        .await
        .expect("PUT");
    assert!(put.status().is_success());

    // Scrape: the render encodes the live families. Each op appears with its
    // labels; the request-duration histogram carries the get/passthrough series.
    let text = render(&metrics, &empty_snapshot());
    assert!(
        text.contains("verglas_requests_total"),
        "requests_total family present"
    );
    assert!(text.contains("op=\"get\""), "a GET was counted:\n{text}");
    assert!(text.contains("op=\"head\""), "a HEAD was counted");
    assert!(text.contains("op=\"list\""), "a LIST was counted");
    assert!(text.contains("op=\"put\""), "a PUT was counted");
    assert!(
        text.contains("tenant=\"default\""),
        "the default tenant label"
    );
    // The GET streamed through the passthrough reader, so its duration histogram
    // is labelled with the passthrough tier.
    assert!(
        text.contains("verglas_request_duration_seconds_bucket")
            && text.contains("tier=\"passthrough\""),
        "the GET duration was observed under the passthrough tier:\n{text}"
    );
    // The GET's histogram recorded exactly one observation.
    assert!(
        text.contains("verglas_request_duration_seconds_count{op=\"get\",tier=\"passthrough\"} 1"),
        "one GET duration observation:\n{text}"
    );
}
