//! Acceptance tests for stateless Worker-pool host capability boundaries.

use std::sync::Arc;

use async_trait::async_trait;
use verglas_do_wasm::{DoRouter, HostError, Request, Response, WorkerPool};

/// Records the arguments supplied to a cross-component call.
struct RecordingRouter;

#[async_trait]
impl DoRouter for RecordingRouter {
    /// Returns a deterministic response for the pool host test.
    async fn do_fetch(
        &self,
        _binding: String,
        _object: String,
        _request: Request,
    ) -> Result<Response, HostError> {
        Ok(Response {
            status: 204,
            headers: Vec::new(),
            body: Vec::new(),
            accept_ws: None,
        })
    }
}

/// A hand-written worker component returns a constant successful response.
fn worker_component() -> Vec<u8> {
    wat::parse_str(
        r#"(component
            (core module $m
                (memory (export "memory") 1)
                (func (export "realloc") (param i32 i32 i32 i32) (result i32)
                    i32.const 32)
                (func (export "fetch")
                    (param i32 i32 i32 i32 i32 i32 i32 i32 i32 i64)
                    (result i32)
                    i32.const 64
                    i32.const 204
                    i32.store16 offset=8
                    i32.const 64)
                (func (export "init") (result i32)
                    i32.const 128)
                (func (export "alarm") (param i64) (result i32)
                    i32.const 128)
                (func (export "websocket-message")
                    (param i64 i32 i32) (result i32)
                    i32.const 128)
                (func (export "websocket-close")
                    (param i64 i32 i32 i32) (result i32)
                    i32.const 128))
            (core instance $m (instantiate $m))
            (type $request (record
                (field "method" string)
                (field "uri" string)
                (field "headers" (list (tuple string string)))
                (field "body" (list u8))
                (field "ws" (option u64))))
            (type $response (record
                (field "status" u16)
                (field "headers" (list (tuple string string)))
                (field "body" (list u8))
                (field "accept-ws" (option u64))))
            (type $error (record (field "message" string)))
            (type $result (result $response (error $error)))
            (type $fetch-type (func
                (param "request" $request)
                (result $result)))
            (type $unit-result (result (error $error)))
            (type $init-type (func (result $unit-result)))
            (type $alarm-type (func (param "scheduled-epoch-millis" u64) (result $unit-result)))
            (type $message-type (func
                (param "socket" u64)
                (param "message" (list u8))
                (result $unit-result)))
            (type $close-type (func
                (param "socket" u64)
                (param "code" u16)
                (param "reason" string)
                (result $unit-result)))
            (func $fetch (type $fetch-type)
                (canon lift
                    (core func $m "fetch")
                    (memory (core memory $m "memory"))
                    (realloc (func $m "realloc"))))
            (func $init (type $init-type)
                (canon lift (core func $m "init")
                    (memory (core memory $m "memory"))))
            (func $alarm (type $alarm-type)
                (canon lift (core func $m "alarm")
                    (memory (core memory $m "memory"))))
            (func $message (type $message-type)
                (canon lift (core func $m "websocket-message")
                    (memory (core memory $m "memory"))
                    (realloc (func $m "realloc"))))
            (func $close (type $close-type)
                (canon lift (core func $m "websocket-close")
                    (memory (core memory $m "memory"))
                    (realloc (func $m "realloc"))))
            (instance $worker
                (export "request" (type $request))
                (export "response" (type $response))
                (export "error" (type $error))
                (export "fetch" (func $fetch)))
            (export "verglas:do-worker/worker@0.1.0" (instance $worker))
            (instance $handler
                (export "request" (type $request))
                (export "response" (type $response))
                (export "error" (type $error))
                (export "init" (func $init))
                (export "fetch" (func $fetch))
                (export "alarm" (func $alarm))
                (export "websocket-message" (func $message))
                (export "websocket-close" (func $close)))
            (export "verglas:do-worker/handler@0.1.0" (instance $handler)))"#,
    )
    .expect("worker component WAT")
}

/// A pool can be constructed from component bytes and executes stateless fetch.
#[tokio::test]
async fn worker_pool_fetch_api_accepts_router() {
    let bytes = worker_component();
    let pool = WorkerPool::load(wasmtime::Config::new(), &bytes).expect("component compiles");
    let request = Request {
        method: "GET".to_owned(),
        uri: "/".to_owned(),
        headers: Vec::new(),
        body: Vec::new(),
        ws: None,
    };
    let response = pool
        .fetch(Arc::new(RecordingRouter), request)
        .await
        .expect("worker export returns a response");
    assert_eq!(response.status, 204);
    assert!(response.headers.is_empty());
    assert!(response.body.is_empty());
    assert_eq!(response.accept_ws, None);
}
