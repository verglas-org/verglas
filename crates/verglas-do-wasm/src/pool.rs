//! Stateless Worker-tier execution with host-routed Durable Object bindings.
//!
//! A [`WorkerPool`] shares one compiled component and linker, but creates a
//! fresh Wasmtime store and component instance for every request. The pool
//! intentionally has no Durable Object engine, event gate, transaction, or
//! identity, and grants no storage or socket capability. Per-request
//! instantiation is the v0 extension point for future warm pools.

use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;
use wasmtime::component::{Component, HasSelf, Linker, ResourceTable};
use wasmtime::{Config, Engine, Store};
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};

use crate::abi::{HostError, Request, Response, WitHandlerError, WorkerBindings, bindings};
use crate::artifact::{ArtifactError, ComponentDigest, CwasmCache};
use crate::runtime::configure_worker_engine;

/// Routes a stateless Worker's flattened Durable Object stub call.
#[async_trait]
pub trait DoRouter: Send + Sync {
    /// Executes one binding call and returns the routed Durable Object response.
    async fn do_fetch(
        &self,
        binding: String,
        object: String,
        request: Request,
    ) -> Result<Response, HostError>;
}

/// Errors raised while compiling, linking, instantiating, or invoking a pool worker.
#[derive(Debug, Error)]
pub enum PoolError {
    /// Reports failure to create the configured Wasmtime engine.
    #[error("failed to create Worker pool Wasmtime engine: {source}")]
    Engine {
        /// The Wasmtime configuration error.
        #[source]
        source: wasmtime::Error,
    },
    /// Reports component bytes that are not a valid Wasmtime component.
    #[error("failed to compile Worker pool component: {source}")]
    Component {
        /// The component compilation error.
        #[source]
        source: wasmtime::Error,
    },
    /// Reports a verified AOT cache read or deserialization failure.
    #[error("failed to load Worker pool artifact: {source}")]
    Artifact {
        /// The cache or artifact error.
        #[source]
        source: ArtifactError,
    },
    /// Reports failure to register the pool host imports.
    #[error("failed to link Worker pool host imports: {source}")]
    Linker {
        /// The linker registration error.
        #[source]
        source: wasmtime::Error,
    },
    /// Reports failure to instantiate one fresh pool worker.
    #[error("failed to instantiate Worker pool component: {source}")]
    Instantiation {
        /// The component instantiation error.
        #[source]
        source: wasmtime::Error,
    },
    /// Reports a trap or other failure during a pool worker export call.
    #[error("Worker pool fetch invocation failed: {source}")]
    Invocation {
        /// The invocation error.
        #[source]
        source: wasmtime::Error,
    },
    /// Reports a handler-level error returned by a pool worker.
    #[error("Worker pool handler failed: {message}")]
    Handler {
        /// The handler's stable error message.
        message: String,
    },
}

/// Per-request state shared by pool host imports and locked-down WASI.
struct PoolStore {
    /// Locked-down WASI capability context for this request.
    wasi: WasiCtx,
    /// Resource handles owned by the component instance.
    table: ResourceTable,
    /// Stateless host capabilities for this request.
    host: PoolHost,
}

impl PoolStore {
    /// Creates request state with inherited diagnostics and no network access.
    fn new(host: PoolHost) -> Self {
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

impl WasiView for PoolStore {
    /// Exposes the request-owned WASI context and resource table.
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

/// Host state installed in each stateless pool store.
struct PoolHost {
    /// Router used for flattened Durable Object calls.
    router: Arc<dyn DoRouter>,
}

impl bindings::verglas::do_worker::types::Host for PoolHost {}

#[async_trait]
impl WorkerBindings for PoolHost {
    /// Delegates one binding call to the injected router.
    async fn do_fetch(
        &self,
        binding: String,
        object: String,
        request: Request,
    ) -> Result<Response, HostError> {
        self.router.do_fetch(binding, object, request).await
    }
}

impl bindings::verglas::do_worker::bindings::Host for PoolHost {
    /// Delegates a WIT binding call and converts host failures to handler errors.
    async fn do_fetch(
        &mut self,
        binding: String,
        object: String,
        request: Request,
    ) -> Result<Response, WitHandlerError> {
        WorkerBindings::do_fetch(self, binding, object, request)
            .await
            .map_err(to_handler_error)
    }
}

impl bindings::verglas::do_worker::storage::Host for PoolHost {
    /// Rejects a storage read because pool workers are stateless.
    async fn get(&mut self, _key: String) -> Result<Option<Vec<u8>>, WitHandlerError> {
        Err(stateless_storage_error())
    }

    /// Rejects a storage write because pool workers are stateless.
    async fn put(&mut self, _key: String, _value: Vec<u8>) -> Result<(), WitHandlerError> {
        Err(stateless_storage_error())
    }

    /// Rejects a storage deletion because pool workers are stateless.
    async fn delete(&mut self, _key: String) -> Result<bool, WitHandlerError> {
        Err(stateless_storage_error())
    }

    /// Rejects a storage listing because pool workers are stateless.
    async fn list(&mut self, _prefix: String, _limit: u32) -> Result<Vec<String>, WitHandlerError> {
        Err(stateless_storage_error())
    }

    /// Rejects row SQL because pool workers are stateless.
    async fn sql_rows(&mut self, _statement: String) -> Result<String, WitHandlerError> {
        Err(stateless_storage_error())
    }

    /// Rejects Stream publication because pool workers have no event transaction.
    async fn stream_send(
        &mut self,
        _stream_binding: String,
        _stream_name: String,
        _records: String,
    ) -> Result<(), WitHandlerError> {
        Err(stateless_storage_error())
    }

    /// Rejects alarm creation because pool workers are stateless.
    async fn set_alarm(&mut self, _epoch_millis: u64) -> Result<(), WitHandlerError> {
        Err(stateless_storage_error())
    }

    /// Rejects alarm reads because pool workers are stateless.
    async fn get_alarm(&mut self) -> Result<Option<u64>, WitHandlerError> {
        Err(stateless_storage_error())
    }

    /// Rejects alarm deletion because pool workers are stateless.
    async fn delete_alarm(&mut self) -> Result<(), WitHandlerError> {
        Err(stateless_storage_error())
    }
}

impl bindings::verglas::do_worker::sockets::Host for PoolHost {
    /// Rejects socket sends because pool workers have no socket capability.
    async fn send(&mut self, _socket: u64, _message: Vec<u8>) -> Result<(), WitHandlerError> {
        Err(stateless_socket_error())
    }

    /// Rejects socket closes because pool workers have no socket capability.
    async fn close(
        &mut self,
        _socket: u64,
        _code: u16,
        _reason: String,
    ) -> Result<(), WitHandlerError> {
        Err(stateless_socket_error())
    }

    /// Rejects attachment writes because pool workers have no socket capability.
    async fn set_attachment(
        &mut self,
        _socket: u64,
        _value: Vec<u8>,
    ) -> Result<(), WitHandlerError> {
        Err(stateless_socket_error())
    }

    /// Rejects attachment reads because pool workers have no socket capability.
    async fn get_attachment(&mut self, _socket: u64) -> Result<Option<Vec<u8>>, WitHandlerError> {
        Err(stateless_socket_error())
    }

    /// Rejects socket enumeration because pool workers have no socket capability.
    async fn attached(&mut self) -> Result<Vec<u64>, WitHandlerError> {
        Err(stateless_socket_error())
    }
}

/// Converts one host capability failure into the WIT handler-error record.
fn to_handler_error(error: HostError) -> WitHandlerError {
    WitHandlerError {
        message: error.to_string(),
    }
}

/// Creates the stable stateless-storage WIT error.
fn stateless_storage_error() -> WitHandlerError {
    to_handler_error(HostError::StatelessStorage)
}

/// Creates the stable stateless-sockets WIT error.
fn stateless_socket_error() -> WitHandlerError {
    to_handler_error(HostError::StatelessSockets)
}

/// A compiled, pre-linked stateless Worker component.
pub struct WorkerPool {
    /// Shared Wasmtime compilation and execution engine.
    engine: Engine,
    /// Immutable tenant component compiled for `engine`.
    component: Component,
    /// Reusable linker containing pool host and locked-down WASI imports.
    linker: Linker<PoolStore>,
}

impl WorkerPool {
    /// Compiles component bytes and prepares the stateless host linker.
    pub fn load(engine_config: Config, component_bytes: &[u8]) -> Result<Self, PoolError> {
        Self::load_with_cache(engine_config, None, component_bytes)
    }

    /// Loads a verified AOT component when a cache is configured.
    ///
    /// Supplying a cache makes cache failures fatal instead of silently
    /// compiling tenant code inside a newly started cell.
    pub fn load_with_cache(
        mut engine_config: Config,
        cache: Option<(&CwasmCache, ComponentDigest)>,
        component_bytes: &[u8],
    ) -> Result<Self, PoolError> {
        configure_worker_engine(&mut engine_config);
        let engine = Engine::new(&engine_config).map_err(|source| PoolError::Engine { source })?;
        let component = match cache {
            Some((cache, digest)) => cache
                .load_or_compile(&engine, digest, component_bytes)
                .map_err(|source| PoolError::Artifact { source })?,
            None => Component::new(&engine, component_bytes)
                .map_err(|source| PoolError::Component { source })?,
        };
        Self::from_parts(engine, component)
    }

    /// Builds a pool around one engine-compatible precompiled component.
    fn from_parts(engine: Engine, component: Component) -> Result<Self, PoolError> {
        let mut linker = Linker::new(&engine);
        wasmtime_wasi::p2::add_to_linker_async(&mut linker)
            .map_err(|source| PoolError::Linker { source })?;
        bindings::Service::add_to_linker::<_, HasSelf<_>>(&mut linker, pool_host)
            .map_err(|source| PoolError::Linker { source })?;
        Ok(Self {
            engine,
            component,
            linker,
        })
    }

    /// Executes one stateless Worker fetch with a fresh store and instance.
    pub async fn fetch(
        &self,
        router: Arc<dyn DoRouter>,
        request: Request,
    ) -> Result<Response, PoolError> {
        let host = PoolHost { router };
        let mut store = Store::new(&self.engine, PoolStore::new(host));
        let instance =
            bindings::Service::instantiate_async(&mut store, &self.component, &self.linker)
                .await
                .map_err(|source| PoolError::Instantiation { source })?;
        let result = instance
            .verglas_do_worker_worker()
            .call_fetch(&mut store, &request)
            .await;
        match result {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(error)) => Err(PoolError::Handler {
                message: error.message,
            }),
            Err(source) => Err(PoolError::Invocation { source }),
        }
    }

    /// Executes one stateless Worker fetch through the pool's public dispatch seam.
    pub async fn dispatch_fetch(
        &self,
        router: Arc<dyn DoRouter>,
        request: Request,
    ) -> Result<Response, PoolError> {
        self.fetch(router, request).await
    }

    /// Executes one stateless Worker scheduled event with a fresh store and instance.
    pub async fn scheduled(
        &self,
        router: Arc<dyn DoRouter>,
        scheduled_epoch_millis: u64,
        cron: String,
    ) -> Result<(), PoolError> {
        let host = PoolHost { router };
        let mut store = Store::new(&self.engine, PoolStore::new(host));
        let instance =
            bindings::Service::instantiate_async(&mut store, &self.component, &self.linker)
                .await
                .map_err(|source| PoolError::Instantiation { source })?;
        let result = instance
            .verglas_do_worker_worker()
            .call_scheduled(&mut store, scheduled_epoch_millis, &cron)
            .await;
        match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(PoolError::Handler {
                message: error.message,
            }),
            Err(source) => Err(PoolError::Invocation { source }),
        }
    }
}

/// Returns the pool host state used by every generated import binding.
fn pool_host(store: &mut PoolStore) -> &mut PoolHost {
    &mut store.host
}

#[cfg(test)]
mod tests {
    //! Tests stateless pool host capability rejection and router delegation.

    use std::sync::Arc;

    use async_trait::async_trait;

    use super::{DoRouter, PoolHost, stateless_socket_error, stateless_storage_error};
    use crate::abi::{HostError, Request, Response, bindings};

    /// Router fake that returns the request it received as a response body.
    struct EchoRouter;

    #[async_trait]
    impl DoRouter for EchoRouter {
        /// Encodes the binding, object, and URI to prove exact delegation.
        async fn do_fetch(
            &self,
            binding: String,
            object: String,
            request: Request,
        ) -> Result<Response, HostError> {
            Ok(Response {
                status: 200,
                headers: vec![
                    ("binding".to_owned(), binding),
                    ("object".to_owned(), object),
                ],
                body: request.uri.into_bytes(),
                accept_ws: None,
            })
        }
    }

    /// Proves a pool storage import returns the typed stateless error.
    #[tokio::test]
    async fn stateless_storage_import_returns_typed_error() {
        let mut host = PoolHost {
            router: Arc::new(EchoRouter),
        };
        let result = <PoolHost as bindings::verglas::do_worker::storage::Host>::get(
            &mut host,
            "key".to_owned(),
        )
        .await;
        let error = result.expect_err("storage must reject");
        assert_eq!(error.message, stateless_storage_error().message);
    }

    /// Proves a pool socket import returns a typed stateless error.
    #[tokio::test]
    async fn stateless_socket_import_returns_typed_error() {
        let mut host = PoolHost {
            router: Arc::new(EchoRouter),
        };
        let result = <PoolHost as bindings::verglas::do_worker::sockets::Host>::send(
            &mut host,
            3,
            b"hello".to_vec(),
        )
        .await;
        let error = result.expect_err("sockets must reject");
        assert_eq!(error.message, stateless_socket_error().message);
    }

    /// Proves a pool binding import delegates all call arguments to the router.
    #[tokio::test]
    async fn do_fetch_import_delegates_to_router() {
        let mut host = PoolHost {
            router: Arc::new(EchoRouter),
        };
        let request = Request {
            method: "POST".to_owned(),
            uri: "/items".to_owned(),
            headers: vec![("x-test".to_owned(), "yes".to_owned())],
            body: b"body".to_vec(),
            ws: None,
        };
        let response = <PoolHost as bindings::verglas::do_worker::bindings::Host>::do_fetch(
            &mut host,
            "NS".to_owned(),
            "alice".to_owned(),
            request,
        )
        .await
        .expect("router response");
        assert_eq!(response.status, 200);
        assert_eq!(response.headers[0], ("binding".to_owned(), "NS".to_owned()));
        assert_eq!(
            response.headers[1],
            ("object".to_owned(), "alice".to_owned())
        );
        assert_eq!(response.body, b"/items".to_vec());
    }
}
