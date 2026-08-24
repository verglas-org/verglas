//! Wasmtime execution of one Durable Object Worker component.
//!
//! This module owns one compiled component and linker, while every dispatch
//! creates a fresh store and host capability set. A successful dispatch returns
//! a permit that the caller must commit to its durable transaction before
//! releasing staged socket output.

use std::sync::Arc;

use thiserror::Error;
use wasmtime::component::{Component, HasSelf, Linker, ResourceTable};
use wasmtime::{Config, Engine, Store};
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};

use crate::abi::{SocketId, WitHandlerError, WorkerHost, WorkerSockets, WorkerStorage, bindings};
use crate::artifact::{ArtifactError, ComponentDigest, CwasmCache};
use crate::gate::{EventGate, EventPermit};

/// Request record accepted by the Worker `fetch` export.
pub use bindings::verglas::do_worker::types::{Request, Response};

/// Errors raised while compiling, instantiating, or invoking a Worker.
#[derive(Debug, Error)]
pub enum RuntimeError {
    /// Reports failure to create the configured Wasmtime engine.
    #[error("failed to create Worker Wasmtime engine: {source}")]
    Engine {
        /// The Wasmtime configuration error.
        #[source]
        source: wasmtime::Error,
    },
    /// Reports component bytes that are not a valid Wasmtime component.
    #[error("failed to compile Worker component: {source}")]
    Component {
        /// The component compilation error.
        #[source]
        source: wasmtime::Error,
    },
    /// Reports failure to verify or use a component artifact or cache entry.
    #[error("Worker component artifact failed: {source}")]
    Artifact {
        /// The artifact failure.
        #[source]
        source: ArtifactError,
    },
    /// Reports failure to register the Worker host imports.
    #[error("failed to link Worker host imports: {source}")]
    Linker {
        /// The linker registration error.
        #[source]
        source: wasmtime::Error,
    },
    /// Reports failure to instantiate the component for one event.
    #[error("failed to instantiate Worker component: {source}")]
    Instantiation {
        /// The component instantiation error.
        #[source]
        source: wasmtime::Error,
    },
    /// Reports a trap or other failure during an event export call.
    #[error("Worker event invocation failed: {source}")]
    Invocation {
        /// The invocation error.
        #[source]
        source: wasmtime::Error,
    },
    /// Reports a handler-level error returned by the guest component.
    #[error("Worker handler failed: {message}")]
    Handler {
        /// The handler's stable error message.
        message: String,
    },
}

/// One successfully invoked event awaiting its caller's durable commit.
///
/// The caller must commit the event's storage transaction first and then call
/// [`EventPermit::commit`] on `permit`. Dropping the permit aborts the output
/// gate and releases no socket effect.
pub struct PendingEvent<T> {
    /// The event result returned by the guest handler.
    pub outcome: T,
    /// The input and output permit held until durable commit is complete.
    pub permit: EventPermit,
}

impl<T> PendingEvent<T> {
    /// Splits the outcome from the permit for an explicit commit sequence.
    pub fn into_parts(self) -> (T, EventPermit) {
        (self.outcome, self.permit)
    }
}

/// Per-event state shared by Worker host and WASI imports.
///
/// The context deliberately has no arguments, environment variables, preopens,
/// or network permissions. Standard output and error remain inherited so guest
/// diagnostics are visible to the resident `verglasd` process.
struct WorkerStore {
    /// Locked-down WASI capability context for this event.
    wasi: WasiCtx,
    /// Resource handles owned by the component instance.
    table: ResourceTable,
    /// Transactional Worker capabilities for this event.
    host: WorkerHost,
}

impl WorkerStore {
    /// Creates per-event state with only inherited standard output and error.
    fn new(host: WorkerHost) -> Self {
        let mut builder = WasiCtxBuilder::new();
        builder
            .inherit_stdout()
            .inherit_stderr()
            .allow_tcp(false)
            .allow_udp(false)
            .allow_ip_name_lookup(false);
        Self {
            wasi: builder.build(),
            table: ResourceTable::new(),
            host,
        }
    }
}

impl WasiView for WorkerStore {
    /// Exposes the event-owned WASI context and resource table to host imports.
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

/// One compiled Worker component with reusable host linker state.
pub struct WorkerRuntime {
    /// Shared Wasmtime compilation and execution engine.
    engine: Engine,
    /// Immutable tenant component compiled for `engine`.
    component: Component,
    /// Reusable linker containing Worker and locked-down WASI imports.
    linker: Linker<WorkerStore>,
}

impl WorkerRuntime {
    /// Compiles component bytes and prepares the host linker.
    ///
    /// Async execution is always enabled because every Worker host capability
    /// is asynchronous. The component is compiled here but instantiated only
    /// when an event is dispatched, so startup can validate an artifact without
    /// requiring an event export until the first invocation.
    pub fn load(engine_config: Config, component_bytes: &[u8]) -> Result<Self, RuntimeError> {
        Self::load_with_cache(engine_config, None, component_bytes)
    }

    /// Compiles or loads a verified component through an optional AOT cache.
    ///
    /// When a cache is supplied, its source digest is verified before cache use.
    /// Supplying `Some(cache)` makes cache filesystem and deserialization errors
    /// fatal; `None` compiles directly and does not access the filesystem.
    pub fn load_with_cache(
        mut engine_config: Config,
        cache: Option<(&CwasmCache, ComponentDigest)>,
        component_bytes: &[u8],
    ) -> Result<Self, RuntimeError> {
        enable_async_support(&mut engine_config);
        let engine =
            Engine::new(&engine_config).map_err(|source| RuntimeError::Engine { source })?;
        let component = match cache {
            Some((cache, digest)) => cache
                .load_or_compile(&engine, digest, component_bytes)
                .map_err(|source| RuntimeError::Artifact { source })?,
            None => Component::new(&engine, component_bytes)
                .map_err(|source| RuntimeError::Component { source })?,
        };
        Self::from_parts(engine, component)
    }

    /// Builds the reusable host linker around one engine-compatible component.
    fn from_parts(engine: Engine, component: Component) -> Result<Self, RuntimeError> {
        let mut linker = Linker::new(&engine);
        wasmtime_wasi::p2::add_to_linker_async(&mut linker)
            .map_err(|source| RuntimeError::Linker { source })?;
        bindings::DurableObject::add_to_linker::<_, HasSelf<_>>(&mut linker, worker_host)
            .map_err(|source| RuntimeError::Linker { source })?;
        Ok(Self {
            engine,
            component,
            linker,
        })
    }

    /// Runs the component `init` export for one gated wake.
    ///
    /// A failed initialization aborts the permit, so no event can observe an
    /// uninitialized component. The caller commits this initialization event
    /// before dispatching ordinary events.
    pub async fn dispatch_init(
        &self,
        gate: &EventGate,
        storage: Arc<dyn WorkerStorage>,
        sockets: Arc<dyn WorkerSockets>,
    ) -> Result<PendingEvent<()>, RuntimeError> {
        let (permit, mut store, instance) =
            self.instantiate_component(gate, storage, sockets).await?;
        let result = instance
            .verglas_do_worker_handler()
            .call_init(&mut store)
            .await;
        complete_event(permit, result)
    }

    /// Invokes the Worker `fetch` export for one gated request after its
    /// component-local initialization hook.
    ///
    /// The returned permit retains socket effects until the caller's storage
    /// transaction has committed. A handler or Wasmtime error aborts it before
    /// this method returns.
    pub async fn dispatch_fetch(
        &self,
        gate: &EventGate,
        storage: Arc<dyn WorkerStorage>,
        sockets: Arc<dyn WorkerSockets>,
        request: Request,
    ) -> Result<PendingEvent<Response>, RuntimeError> {
        let (permit, mut store, instance) = self.instantiate_event(gate, storage, sockets).await?;
        let result = instance
            .verglas_do_worker_handler()
            .call_fetch(&mut store, &request)
            .await;
        complete_event(permit, result)
    }

    /// Invokes the Worker `alarm` export for one scheduled alarm deadline.
    ///
    /// The returned permit retains socket effects until the caller's storage
    /// transaction has committed. A handler or Wasmtime error aborts it before
    /// this method returns.
    pub async fn dispatch_alarm(
        &self,
        gate: &EventGate,
        storage: Arc<dyn WorkerStorage>,
        sockets: Arc<dyn WorkerSockets>,
        scheduled_millis: u64,
    ) -> Result<PendingEvent<()>, RuntimeError> {
        let (permit, mut store, instance) = self.instantiate_event(gate, storage, sockets).await?;
        let result = instance
            .verglas_do_worker_handler()
            .call_alarm(&mut store, scheduled_millis)
            .await;
        complete_event(permit, result)
    }

    /// Invokes the Worker `websocket-message` export for one socket message.
    ///
    /// The returned permit retains socket effects until the caller's storage
    /// transaction has committed. A handler or Wasmtime error aborts it before
    /// this method returns.
    pub async fn dispatch_websocket_message(
        &self,
        gate: &EventGate,
        storage: Arc<dyn WorkerStorage>,
        sockets: Arc<dyn WorkerSockets>,
        socket: SocketId,
        message: Vec<u8>,
    ) -> Result<PendingEvent<()>, RuntimeError> {
        let (permit, mut store, instance) = self.instantiate_event(gate, storage, sockets).await?;
        let result = instance
            .verglas_do_worker_handler()
            .call_websocket_message(&mut store, socket, &message)
            .await;
        complete_event(permit, result)
    }

    /// Invokes the Worker `websocket-close` export for one socket close.
    ///
    /// The returned permit retains socket effects until the caller's storage
    /// transaction has committed. A handler or Wasmtime error aborts it before
    /// this method returns.
    pub async fn dispatch_websocket_close(
        &self,
        gate: &EventGate,
        storage: Arc<dyn WorkerStorage>,
        sockets: Arc<dyn WorkerSockets>,
        socket: SocketId,
        code: u16,
        reason: String,
    ) -> Result<PendingEvent<()>, RuntimeError> {
        let (permit, mut store, instance) = self.instantiate_event(gate, storage, sockets).await?;
        let result = instance
            .verglas_do_worker_handler()
            .call_websocket_close(&mut store, socket, code, &reason)
            .await;
        complete_event(permit, result)
    }

    /// Creates a fresh component instance and runs its initialization hook.
    async fn instantiate_event(
        &self,
        gate: &EventGate,
        storage: Arc<dyn WorkerStorage>,
        sockets: Arc<dyn WorkerSockets>,
    ) -> Result<(EventPermit, Store<WorkerStore>, bindings::DurableObject), RuntimeError> {
        let (permit, mut store, instance) =
            self.instantiate_component(gate, storage, sockets).await?;
        let result = instance
            .verglas_do_worker_handler()
            .call_init(&mut store)
            .await;
        match result {
            Ok(Ok(())) => Ok((permit, store, instance)),
            Ok(Err(error)) => {
                permit.abort();
                Err(RuntimeError::Handler {
                    message: error.message,
                })
            }
            Err(source) => {
                permit.abort();
                Err(RuntimeError::Invocation { source })
            }
        }
    }

    /// Creates a fresh store and component instance without running `init`.
    async fn instantiate_component(
        &self,
        gate: &EventGate,
        storage: Arc<dyn WorkerStorage>,
        sockets: Arc<dyn WorkerSockets>,
    ) -> Result<(EventPermit, Store<WorkerStore>, bindings::DurableObject), RuntimeError> {
        let permit = gate.begin_event().await;
        let event_sockets = permit.staging_sockets(sockets);
        let host = WorkerHost::new(storage, event_sockets);
        let mut store = Store::new(&self.engine, WorkerStore::new(host));
        let instance = match bindings::DurableObject::instantiate_async(
            &mut store,
            &self.component,
            &self.linker,
        )
        .await
        {
            Ok(instance) => instance,
            Err(source) => {
                permit.abort();
                return Err(RuntimeError::Instantiation { source });
            }
        };
        Ok((permit, store, instance))
    }
}

/// Enables asynchronous execution on Wasmtime configurations that expose the setting.
#[allow(deprecated)]
fn enable_async_support(config: &mut Config) {
    config.async_support(true);
}

/// Returns the Worker host state used by every generated import binding.
fn worker_host(store: &mut WorkerStore) -> &mut WorkerHost {
    &mut store.host
}

/// Converts one generated export result into a pending event or aborts it.
fn complete_event<T>(
    permit: EventPermit,
    result: Result<Result<T, WitHandlerError>, wasmtime::Error>,
) -> Result<PendingEvent<T>, RuntimeError> {
    match result {
        Ok(Ok(outcome)) => Ok(PendingEvent { outcome, permit }),
        Ok(Err(error)) => {
            permit.abort();
            Err(RuntimeError::Handler {
                message: error.message,
            })
        }
        Err(source) => {
            permit.abort();
            Err(RuntimeError::Invocation { source })
        }
    }
}

#[cfg(test)]
mod tests {
    //! Tests the WASI state shape used by component instantiation.

    use std::sync::Arc;

    use wasmtime::component::ResourceTable;
    use wasmtime::{Config, Store};
    use wasmtime_wasi::WasiCtx;

    use super::{WorkerRuntime, WorkerStore};
    use crate::abi::{WorkerHost, WorkerSockets, WorkerStorage};

    /// A no-op storage implementation for direct linker tests.
    struct EmptyStorage;

    #[async_trait::async_trait]
    impl WorkerStorage for EmptyStorage {
        /// Returns no value because the linker test does not invoke storage.
        async fn get(&self, _key: String) -> Result<Option<Vec<u8>>, crate::abi::HostError> {
            Ok(None)
        }

        /// Rejects no writes because the linker test does not invoke storage.
        async fn put(&self, _key: String, _value: Vec<u8>) -> Result<(), crate::abi::HostError> {
            Ok(())
        }

        /// Reports no deletion because the linker test does not invoke storage.
        async fn delete(&self, _key: String) -> Result<bool, crate::abi::HostError> {
            Ok(false)
        }

        /// Returns no keys because the linker test does not invoke storage.
        async fn list(
            &self,
            _prefix: String,
            _limit: u32,
        ) -> Result<Vec<String>, crate::abi::HostError> {
            Ok(Vec::new())
        }

        /// Returns no rows because the linker test does not invoke storage.
        async fn sql(&self, _statement: String) -> Result<Vec<u8>, crate::abi::HostError> {
            Ok(Vec::new())
        }

        /// Returns no JSON rows because the linker test does not invoke storage.
        async fn sql_rows(&self, _statement: String) -> Result<String, crate::abi::HostError> {
            Ok("[]".to_owned())
        }

        /// Accepts no alarm because the linker test does not invoke storage.
        async fn set_alarm(&self, _epoch_millis: u64) -> Result<(), crate::abi::HostError> {
            Ok(())
        }

        /// Reports no alarm because the linker test does not invoke storage.
        async fn get_alarm(&self) -> Result<Option<u64>, crate::abi::HostError> {
            Ok(None)
        }

        /// Clears no alarm because the linker test does not invoke storage.
        async fn delete_alarm(&self) -> Result<(), crate::abi::HostError> {
            Ok(())
        }
    }

    /// A no-op socket implementation for direct linker tests.
    struct EmptySockets;

    #[async_trait::async_trait]
    impl WorkerSockets for EmptySockets {
        /// Accepts no message because the linker test does not invoke sockets.
        async fn send(&self, _socket: u64, _message: Vec<u8>) -> Result<(), crate::abi::HostError> {
            Ok(())
        }

        /// Accepts no close because the linker test does not invoke sockets.
        async fn close(
            &self,
            _socket: u64,
            _code: u16,
            _reason: String,
        ) -> Result<(), crate::abi::HostError> {
            Ok(())
        }

        /// Accepts no attachment because the linker test does not invoke sockets.
        async fn set_attachment(
            &self,
            _socket: u64,
            _value: Vec<u8>,
        ) -> Result<(), crate::abi::HostError> {
            Ok(())
        }

        /// Returns no attachment because the linker test does not invoke sockets.
        async fn get_attachment(
            &self,
            _socket: u64,
        ) -> Result<Option<Vec<u8>>, crate::abi::HostError> {
            Ok(None)
        }

        /// Returns no sockets because the linker test does not invoke sockets.
        async fn attached(&self) -> Result<Vec<u64>, crate::abi::HostError> {
            Ok(Vec::new())
        }
    }

    /// Proves a component importing a WASI clock resolves through the runtime linker.
    #[tokio::test]
    async fn runtime_instantiates_wasi_clock_import() {
        let bytes = wat::parse_str(
            r#"(component
                (type $wall-clock (instance
                    (type (record
                        (field "seconds" u64)
                        (field "nanoseconds" u32)))
                    (export "datetime" (type (eq 0)))
                    (type (func (result 1)))
                    (export "now" (func (type 2)))
                    (export "resolution" (func (type 2)))))
                (import "wasi:clocks/wall-clock@0.2.0"
                    (instance $wall-import (type $wall-clock)))
            )"#,
        )
        .expect("WASI import component WAT");
        let runtime = WorkerRuntime::load(Config::new(), &bytes).expect("component compiles");
        let storage = Arc::new(EmptyStorage) as Arc<dyn WorkerStorage>;
        let sockets = Arc::new(EmptySockets) as Arc<dyn WorkerSockets>;
        let host = WorkerHost::new(storage, sockets);
        let mut store = Store::new(
            &runtime.engine,
            WorkerStore {
                wasi: WasiCtx::builder().build(),
                table: ResourceTable::new(),
                host,
            },
        );
        runtime
            .linker
            .instantiate_async(&mut store, &runtime.component)
            .await
            .expect("WASI clock import must instantiate");
    }
}
