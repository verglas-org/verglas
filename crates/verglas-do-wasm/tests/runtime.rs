//! Acceptance tests for component loading and event dispatch boundaries.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;
use verglas_do_wasm::{
    EventGate, HostError, Request, Response, WorkerBindings, WorkerRuntime, WorkerSockets,
    WorkerStorage,
};

/// Storage stub used to prove that dispatch accepts per-event capabilities.
#[derive(Default)]
struct TestStorage;

#[async_trait]
impl WorkerStorage for TestStorage {
    /// Returns no value because the loading test does not invoke a handler.
    async fn get(&self, _key: String) -> Result<Option<Vec<u8>>, HostError> {
        Ok(None)
    }

    /// Accepts a staged write from the guest boundary.
    async fn put(&self, _key: String, _value: Vec<u8>) -> Result<(), HostError> {
        Ok(())
    }

    /// Reports that no key was deleted.
    async fn delete(&self, _key: String) -> Result<bool, HostError> {
        Ok(false)
    }

    /// Returns no keys for a bounded listing.
    async fn list(&self, _prefix: String, _limit: u32) -> Result<Vec<String>, HostError> {
        Ok(Vec::new())
    }

    /// Returns an empty SQL result.
    async fn sql(&self, _statement: String) -> Result<Vec<u8>, HostError> {
        Ok(Vec::new())
    }

    /// Returns an empty JSON row array.
    async fn sql_rows(&self, _statement: String) -> Result<String, HostError> {
        Ok("[]".to_owned())
    }

    /// Accepts an alarm deadline.
    async fn set_alarm(&self, _epoch_millis: u64) -> Result<(), HostError> {
        Ok(())
    }

    /// Reports that no alarm is armed.
    async fn get_alarm(&self) -> Result<Option<u64>, HostError> {
        Ok(None)
    }

    /// Accepts alarm removal.
    async fn delete_alarm(&self) -> Result<(), HostError> {
        Ok(())
    }
}

/// Binding stub used by runtime tests that do not invoke cross-object calls.
#[derive(Default)]
struct TestBindings;

#[async_trait]
impl WorkerBindings for TestBindings {
    /// Rejects calls because the runtime dispatch tests do not route objects.
    async fn do_fetch(
        &self,
        _binding: String,
        _object: String,
        _request: Request,
    ) -> Result<Response, HostError> {
        Err(HostError::Unsupported {
            operation: "test Durable Object binding",
        })
    }
}

/// Socket stub used as the output gate sink in runtime tests.
#[derive(Default)]
struct TestSockets {
    /// Records output only after an event permit commits.
    effects: Mutex<Vec<Vec<u8>>>,
}

#[async_trait]
impl WorkerSockets for TestSockets {
    /// Records a committed message.
    async fn send(&self, _socket: u64, message: Vec<u8>) -> Result<(), HostError> {
        self.effects.lock().await.push(message);
        Ok(())
    }

    /// Accepts a committed close.
    async fn close(&self, _socket: u64, _code: u16, _reason: String) -> Result<(), HostError> {
        Ok(())
    }

    /// Accepts an attachment write.
    async fn set_attachment(&self, _socket: u64, _value: Vec<u8>) -> Result<(), HostError> {
        Ok(())
    }

    /// Reports no attachment.
    async fn get_attachment(&self, _socket: u64) -> Result<Option<Vec<u8>>, HostError> {
        Ok(None)
    }

    /// Reports no attached sockets.
    async fn attached(&self) -> Result<Vec<u64>, HostError> {
        Ok(Vec::new())
    }
}

/// Garbage bytes are rejected before a runtime can be used for dispatch.
#[test]
fn runtime_load_rejects_non_component_bytes() {
    let config = wasmtime::Config::new();
    let error = match WorkerRuntime::load(config, b"not a component") {
        Ok(_) => panic!("garbage must fail closed"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("component"));
}

/// An empty component is accepted for startup-only artifact validation.
#[test]
fn runtime_load_accepts_empty_component() {
    let bytes = wat::parse_str("(component)").expect("empty component WAT");
    let runtime =
        WorkerRuntime::load(wasmtime::Config::new(), &bytes).expect("empty component compiles");
    let _ = runtime;
}

/// A failing init aborts its wake permit before any event can enter.
#[tokio::test]
async fn runtime_dispatch_init_rejects_component_without_init() {
    let bytes = wat::parse_str("(component)").expect("empty component WAT");
    let runtime =
        WorkerRuntime::load(wasmtime::Config::new(), &bytes).expect("empty component compiles");
    let sockets = Arc::new(TestSockets::default());
    let gate = EventGate::new(Arc::clone(&sockets) as Arc<dyn WorkerSockets>);
    let storage = Arc::new(TestStorage) as Arc<dyn WorkerStorage>;
    let event_sockets = Arc::new(TestSockets::default()) as Arc<dyn WorkerSockets>;
    let bindings = Arc::new(TestBindings) as Arc<dyn WorkerBindings>;

    let error = match runtime
        .dispatch_init(&gate, storage, event_sockets, bindings)
        .await
    {
        Ok(_) => panic!("component without init must fail at dispatch"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("init") || error.to_string().contains("export"));

    let permit = gate.begin_event().await;
    permit.abort();
}

/// A dispatch owns one gate permit and exposes a commit seam to its caller.
#[tokio::test]
async fn runtime_dispatch_rejects_missing_handler_without_releasing_gate_early() {
    let bytes = wat::parse_str("(component)").expect("empty component WAT");
    let runtime =
        WorkerRuntime::load(wasmtime::Config::new(), &bytes).expect("empty component compiles");
    let sockets = Arc::new(TestSockets::default());
    let gate = EventGate::new(Arc::clone(&sockets) as Arc<dyn WorkerSockets>);
    let storage = Arc::new(TestStorage) as Arc<dyn WorkerStorage>;
    let event_sockets = Arc::new(TestSockets::default()) as Arc<dyn WorkerSockets>;
    let bindings = Arc::new(TestBindings) as Arc<dyn WorkerBindings>;

    let error = match runtime
        .dispatch_alarm(&gate, storage, event_sockets, bindings, 42)
        .await
    {
        Ok(_) => panic!("component without handler must fail at dispatch"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("handler") || error.to_string().contains("export"));

    let permit = gate.begin_event().await;
    permit.abort();
}
