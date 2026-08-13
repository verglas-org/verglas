//! Contract tests for the isolated logical table write role.

use std::sync::{Arc, Mutex};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;
use verglas_api::table::CommitResponse;
use verglas_write_node::admin::{AppState, BatchCommitter, CommitPublisher, router};

#[derive(Default)]
struct FakeCommitter {
    calls: Mutex<Vec<(String, usize, Option<String>)>>,
    ingests: Mutex<Vec<IngestCall>>,
}

#[derive(Default)]
struct FakePublisher {
    commits: Mutex<Vec<(String, String)>>,
}

#[async_trait]
impl CommitPublisher for FakePublisher {
    async fn publish(&self, table: &str, snapshot_id: &str) -> Result<(), String> {
        self.commits
            .lock()
            .expect("published commits")
            .push((table.to_owned(), snapshot_id.to_owned()));
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq)]
struct IngestCall {
    table: String,
    mode: String,
    format: String,
    partition_by: Option<String>,
    body_len: usize,
    idempotency_key: Option<String>,
}

#[async_trait]
impl BatchCommitter for FakeCommitter {
    async fn commit(
        &self,
        table: &str,
        batches: Vec<RecordBatch>,
        idempotency_key: Option<String>,
    ) -> Result<CommitResponse, String> {
        let rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        self.calls
            .lock()
            .expect("calls")
            .push((table.to_owned(), rows, idempotency_key));
        Ok(CommitResponse {
            snapshot_id: "10".to_owned(),
            rows_committed: rows as u64,
            watermark: "10".to_owned(),
            idempotent: false,
        })
    }

    async fn ingest(
        &self,
        table: &str,
        mode: &str,
        format: &str,
        partition_by: Option<&str>,
        body: axum::body::Bytes,
        idempotency_key: Option<String>,
    ) -> Result<serde_json::Value, String> {
        self.ingests.lock().expect("ingests").push(IngestCall {
            table: table.to_owned(),
            mode: mode.to_owned(),
            format: format.to_owned(),
            partition_by: partition_by.map(str::to_owned),
            body_len: body.len(),
            idempotency_key,
        });
        Ok(serde_json::json!({"operation": "append"}))
    }
}

/// File ingestion is owned by the same isolated logical-write role.
#[tokio::test]
async fn source_file_ingest_is_dispatched_to_the_writer() {
    let committer = Arc::new(FakeCommitter::default());
    let app = router(AppState::new(
        committer.clone(),
        Arc::new(FakePublisher::default()),
    ));
    let response = app
        .oneshot(
            Request::post("/v1/ingest/sdk.events?mode=create&format=csv&partition_by=day")
                .header("idempotency-key", "source-1")
                .body(Body::from("day,id\n2026-08-04,1\n"))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        committer.ingests.lock().expect("ingests").as_slice(),
        &[IngestCall {
            table: "sdk.events".to_owned(),
            mode: "create".to_owned(),
            format: "csv".to_owned(),
            partition_by: Some("day".to_owned()),
            body_len: 20,
            idempotency_key: Some("source-1".to_owned()),
        }]
    );
}

/// One bounded Arrow stream becomes one idempotent Iceberg commit.
#[tokio::test]
async fn arrow_write_is_decoded_and_committed_once() {
    let committer = Arc::new(FakeCommitter::default());
    let publisher = Arc::new(FakePublisher::default());
    let app = router(AppState::new(committer.clone(), publisher.clone()));
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2]))])
        .expect("batch");
    let mut body = Vec::new();
    {
        let mut writer =
            arrow_ipc::writer::StreamWriter::try_new(&mut body, &schema).expect("Arrow writer");
        writer.write(&batch).expect("write batch");
        writer.finish().expect("finish stream");
    }
    let response = app
        .oneshot(
            Request::post("/v1/write/sdk.events")
                .header("content-type", "application/vnd.apache.arrow.stream")
                .header("idempotency-key", "run-1:0")
                .body(Body::from(body))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let response: CommitResponse = serde_json::from_slice(&body).expect("commit response");
    assert_eq!(response.rows_committed, 2);
    assert_eq!(
        committer.calls.lock().expect("calls").as_slice(),
        &[("sdk.events".to_owned(), 2, Some("run-1:0".to_owned()))]
    );
    assert_eq!(
        publisher
            .commits
            .lock()
            .expect("published commits")
            .as_slice(),
        &[("sdk.events".to_owned(), "10".to_owned())]
    );
}
