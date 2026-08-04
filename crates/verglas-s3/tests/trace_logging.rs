//! Request trace-id logging (#61), mapped to the acceptance criteria:
//! `x-amz-request-id` is returned on every response, and the id the client sees
//! is the id the logs carry — so a client-reported request id reconstructs the
//! request from the logs. Here that is proven within one node: the completion
//! event and the response header carry the same id.

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStoreExt, PutPayload};
use serde_json::Value;
use tracing_subscriber::fmt::MakeWriter;
use verglas_core::metrics::NodeMetrics;
use verglas_s3::{
    BackendStore, NoopInvalidation, PassthroughList, PassthroughRead, PassthroughWrite,
};

/// The bucket the in-memory origin serves.
const BUCKET: &str = "test-bucket";

/// A `MakeWriter` appending every formatted log line to a shared buffer.
#[derive(Clone, Default)]
struct SharedBuf(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for SharedBuf {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("buffer lock").extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for SharedBuf {
    type Writer = SharedBuf;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Parses the buffer into one JSON object per emitted line.
fn captured_events(buf: &SharedBuf) -> Vec<Value> {
    let bytes = buf.0.lock().expect("buffer lock").clone();
    String::from_utf8(bytes)
        .expect("log output is utf8")
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str::<Value>(l).expect("each log line is one JSON object"))
        .collect()
}

/// A GET returns `x-amz-request-id`, and the request-completion log event
/// carries that same id — so the id in a client error reconstructs the request
/// from the logs. Single-threaded runtime so the server tasks and the
/// thread-local capturing subscriber share one thread.
#[tokio::test(flavor = "current_thread")]
async fn request_id_is_consistent_across_the_response_header_and_the_completion_event() {
    let buf = SharedBuf::default();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .flatten_event(true)
        .with_current_span(true)
        .with_span_list(false)
        .with_max_level(tracing::Level::INFO)
        .with_writer(buf.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    // Seed one object into the in-memory origin.
    let store = Arc::new(InMemory::new());
    store
        .put(
            &Path::from("data/part-0.parquet"),
            PutPayload::from(Bytes::from_static(b"hello parquet")),
        )
        .await
        .expect("seed object");

    // The router with node metrics wired, so the GET records a completion event.
    let metrics = Arc::new(NodeMetrics::new().expect("metrics registry"));
    let app = verglas_s3::router_with_passthrough(
        PassthroughRead::new(BackendStore::single(BUCKET, store.clone())),
        PassthroughWrite::new(BackendStore::single(BUCKET, store.clone())),
        Arc::new(PassthroughList::new(BackendStore::single(BUCKET, store))),
        Arc::new(NoopInvalidation),
        None,
        None,
        None,
        None,
        Some(metrics),
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });

    let response = reqwest::get(format!("http://{addr}/{BUCKET}/data/part-0.parquet"))
        .await
        .expect("request");
    assert!(response.status().is_success());
    let header_id = response
        .headers()
        .get("x-amz-request-id")
        .and_then(|v| v.to_str().ok())
        .expect("x-amz-request-id is returned on every response")
        .to_owned();
    // Drain the body so the server-side completion guard drops and logs.
    let body = response.bytes().await.expect("body");
    assert_eq!(&body[..], b"hello parquet");
    // The guard's Drop runs on the server task once the body is dropped; yield so
    // that task makes progress on this single-threaded runtime before we read.
    tokio::task::yield_now().await;

    let events = captured_events(&buf);
    let completion = events
        .iter()
        .find(|e| e.get("message").and_then(Value::as_str) == Some("s3 request complete"))
        .unwrap_or_else(|| panic!("expected a completion event, got: {events:#?}"));

    assert_eq!(
        completion.get("request_id").and_then(Value::as_str),
        Some(header_id.as_str()),
        "the completion event's request_id matches x-amz-request-id"
    );
    assert_eq!(
        completion.get("op").and_then(Value::as_str),
        Some("get"),
        "the completion line records the operation"
    );
    assert!(
        completion.get("duration_ms").is_some(),
        "the completion line records a duration"
    );
}
