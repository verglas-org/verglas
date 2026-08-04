//! Structured origin-fill-failure logging (#61), mapped to the acceptance
//! criterion "a fill that fails against a stub origin emits a structured event
//! carrying the trace ID and error". This is the visibility the #245/#233
//! postmortems were missing: a cold fill that fails against an unusable origin
//! must be loud, tagged with the request id the client saw, not silent.

use std::sync::{Arc, Mutex};

use serde_json::Value;
use tempfile::TempDir;
use tracing_subscriber::fmt::MakeWriter;
use verglas_cache::HybridCacheEngine;
use verglas_core::config::{ByteSize, Cache as CacheConfig};
use verglas_core::node::NodeId;
use verglas_core::peer::NoopPeerFetch;
use verglas_core::ring::RendezvousRing;
use verglas_core::trace::{self, RequestId};
use verglas_s3::{ObjectGet, ObjectMeta, ObjectRead, ReadError, ReadRange};

/// A backend whose every request fails as an unusable origin would (#233/#245):
/// no bytes, a clear error. Stands in for stale/rejected credentials or a hung
/// endpoint.
struct FailingOrigin;

/// The error text the stub origin returns, so the test can assert it survives
/// into the log event verbatim.
const ORIGIN_ERROR: &str = "origin unreachable: simulated credential failure";

impl ObjectRead for FailingOrigin {
    async fn get(
        &self,
        _key: &verglas_core::CacheKey,
        _range: ReadRange,
    ) -> Result<ObjectGet, ReadError> {
        Err(ReadError::Backend(ORIGIN_ERROR.to_owned()))
    }

    async fn head(&self, _key: &verglas_core::CacheKey) -> Result<ObjectMeta, ReadError> {
        Err(ReadError::Backend(ORIGIN_ERROR.to_owned()))
    }
}

/// A `MakeWriter` that appends every formatted log line to a shared buffer, so
/// the test can read back the JSON the daemon would emit.
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

/// Parses the captured buffer into one JSON object per emitted line.
fn captured_events(buf: &SharedBuf) -> Vec<Value> {
    let bytes = buf.0.lock().expect("buffer lock").clone();
    String::from_utf8(bytes)
        .expect("log output is utf8")
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str::<Value>(l).expect("each log line is one JSON object"))
        .collect()
}

/// A minimal cache config over a temp dir.
fn cache_config(dir: &TempDir) -> CacheConfig {
    CacheConfig {
        dir: dir.path().to_path_buf(),
        capacity_bytes: ByteSize(64 * 1024 * 1024),
        dram_bytes: ByteSize(128 * 1024 * 1024),
        data_block_bytes: ByteSize(8 * 1024 * 1024),
        ..CacheConfig::default()
    }
}

/// A cold GET against an unusable origin emits a structured `origin fill failed`
/// event that carries the scoped request id and the origin error — the trace-id
/// correlation the postmortems needed. Single-threaded runtime so the detached
/// fill task and the thread-local capturing subscriber share one thread.
#[tokio::test(flavor = "current_thread")]
async fn cold_fill_against_a_failing_origin_logs_a_structured_event_with_the_trace_id() {
    let buf = SharedBuf::default();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .flatten_event(true)
        .with_current_span(true)
        .with_span_list(false)
        .with_max_level(tracing::Level::DEBUG)
        .with_writer(buf.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let dir = TempDir::new().expect("temp dir");
    let node = NodeId::new("local");
    let engine = HybridCacheEngine::new(
        FailingOrigin,
        NoopPeerFetch,
        RendezvousRing::single(node.clone()),
        node,
        &cache_config(&dir),
    )
    .await
    .expect("engine builds");

    // Drive one request under a known id, exactly as the front-end would.
    let request_id = RequestId::generate();
    let key = verglas_core::CacheKey {
        bucket: "test-bucket".to_owned(),
        key: "cold/object.parquet".to_owned(),
    };
    let result = trace::scope(request_id.clone(), async {
        engine.get(&key, ReadRange::Full).await
    })
    .await;
    assert!(result.is_err(), "a failing origin cannot serve the object");

    let events = captured_events(&buf);
    let fill_failure = events
        .iter()
        .find(|e| e.get("message").and_then(Value::as_str) == Some("origin fill failed"))
        .unwrap_or_else(|| {
            panic!("expected an `origin fill failed` event, got: {events:#?}");
        });

    assert_eq!(
        fill_failure.get("request_id").and_then(Value::as_str),
        Some(request_id.as_str()),
        "the fill-failure event carries the request's trace id"
    );
    assert_eq!(
        fill_failure.get("level").and_then(Value::as_str),
        Some("WARN"),
        "a fill failure is a warning, not a silent debug line"
    );
    let error = fill_failure
        .get("error")
        .and_then(Value::as_str)
        .expect("the event carries the origin error");
    assert!(
        error.contains(ORIGIN_ERROR),
        "the origin error survives into the log: {error}"
    );
    // The raw object key is never logged, only a redacted hash (#61).
    let serialized = fill_failure.to_string();
    assert!(
        !serialized.contains("cold/object.parquet"),
        "the raw object key must never appear in the log: {serialized}"
    );
    assert_eq!(
        fill_failure.get("key_hash").and_then(Value::as_str),
        Some(trace::key_hash("test-bucket", "cold/object.parquet").as_str()),
        "the event carries the redacted key hash"
    );
}
