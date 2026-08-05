//! Dispatches bounded Arrow writes to an isolated `verglas-write` process.

use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use axum::body::Bytes;
use axum::response::Response;
use tokio::sync::Mutex;

use crate::query_worker::{relay_response, wait_for_port};

/// Monotonic suffix for per-request ports files.
static DISPATCH_COUNTER: AtomicU64 = AtomicU64::new(0);

/// On-demand logical write-role launcher.
pub struct WriteWorkerDispatcher {
    binary: PathBuf,
    config_path: PathBuf,
    lock: Arc<Mutex<()>>,
}

impl WriteWorkerDispatcher {
    /// Creates a dispatcher for a role binary and rendered role configuration.
    pub fn new(binary: PathBuf, config_path: PathBuf) -> Self {
        Self {
            binary,
            config_path,
            lock: Arc::new(Mutex::new(())),
        }
    }

    /// Sends one bounded Arrow commit to a fresh isolated writer.
    pub async fn dispatch(
        &self,
        table: &str,
        body: Bytes,
        idempotency_key: Option<String>,
    ) -> Result<Response, String> {
        self.dispatch_request(
            &format!("/v1/write/{table}"),
            body,
            verglas_sdk::ARROW_STREAM_CONTENT_TYPE,
            idempotency_key,
        )
        .await
    }

    /// Sends one bounded source-file ingest to an isolated writer.
    pub async fn dispatch_ingest(
        &self,
        table: &str,
        query: &str,
        body: Bytes,
        idempotency_key: Option<String>,
    ) -> Result<Response, String> {
        self.dispatch_request(
            &format!("/v1/ingest/{table}?{query}"),
            body,
            "application/octet-stream",
            idempotency_key,
        )
        .await
    }

    async fn dispatch_request(
        &self,
        path: &str,
        body: Bytes,
        content_type: &str,
        idempotency_key: Option<String>,
    ) -> Result<Response, String> {
        let guard = self.lock.clone().lock_owned().await;
        let dispatch_id = DISPATCH_COUNTER.fetch_add(1, Ordering::Relaxed);
        let ports_file = std::env::temp_dir().join(format!(
            "verglas-write-worker-{}-{dispatch_id}.ports",
            std::process::id()
        ));
        let mut child = tokio::process::Command::new(&self.binary)
            .arg("--config")
            .arg(&self.config_path)
            .arg("--ports-file")
            .arg(&ports_file)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .map_err(|error| format!("spawn {}: {error}", self.binary.display()))?;
        let port = wait_for_port(&mut child, &ports_file).await;
        let _ = std::fs::remove_file(&ports_file);
        let port = match port {
            Ok(port) => port,
            Err(error) => {
                let _ = child.start_kill();
                return Err(error);
            }
        };
        let mut request = reqwest::Client::new()
            .post(format!("http://127.0.0.1:{port}{path}"))
            .header(reqwest::header::CONTENT_TYPE, content_type)
            .body(body);
        if let Some(key) = idempotency_key {
            request = request.header("idempotency-key", key);
        }
        let response = request
            .send()
            .await
            .map_err(|error| format!("write worker request failed: {error}"));
        match response {
            Ok(response) => relay_response(response, (guard, child)),
            Err(error) => {
                let _ = child.start_kill();
                Err(error)
            }
        }
    }
}
