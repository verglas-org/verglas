//! Wasmtime bindings and host-side delegation traits for the Durable Object ABI.
//!
//! The generated bindings are compiled directly from the checked-in WIT world;
//! this module owns only the adapter that maps those bindings onto host traits.

use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;

/// Bindings generated from the `verglas:do-worker/service` WIT world.
pub mod bindings {
    wasmtime::component::bindgen!({
        path: "wit",
        world: "service",
        imports: { default: async },
        exports: { default: async },
    });
}

/// Generated WIT error record used by imported host interfaces.
pub type WitHandlerError = bindings::verglas::do_worker::types::HandlerError;

/// Numeric identity of one accepted WebSocket connection.
pub type SocketId = u64;

/// Request record exchanged by Worker and Durable Object host calls.
pub type Request = bindings::verglas::do_worker::types::Request;

/// Response record exchanged by Worker and Durable Object host calls.
pub type Response = bindings::verglas::do_worker::types::Response;

/// Hard host-side limit for a connection attachment.
pub const MAX_ATTACHMENT_SIZE: usize = 16 * 1024;

/// Errors returned by host capability implementations.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum HostError {
    /// Reports an error from the storage or socket backend.
    #[error("worker backend error: {message}")]
    Backend {
        /// Stable human-readable backend failure description.
        message: String,
    },
    /// Reports a connection attachment that exceeds the ABI limit.
    #[error("attachment has {actual} bytes; maximum is {max} bytes")]
    AttachmentTooLarge {
        /// The rejected attachment's size.
        actual: usize,
        /// The ABI's hard bound.
        max: usize,
    },
    /// Reports a capability that has no safe host implementation yet.
    #[error("worker capability is not supported: {operation}")]
    Unsupported {
        /// Stable operation name exposed in the WIT error record.
        operation: &'static str,
    },
    /// Reports that a stateless Worker attempted to use Durable Object storage.
    #[error("stateless worker has no storage")]
    StatelessStorage,
    /// Reports that a stateless Worker attempted to use Durable Object sockets.
    #[error("stateless worker has no sockets")]
    StatelessSockets,
}

impl HostError {
    /// Creates a backend error from a stable human-readable message.
    pub fn backend(message: impl Into<String>) -> Self {
        Self::Backend {
            message: message.into(),
        }
    }
}

/// Object-safe asynchronous Durable Object binding calls granted to one Worker event.
#[async_trait]
pub trait WorkerBindings: Send + Sync {
    /// Routes a flattened stub fetch to the named Durable Object.
    async fn do_fetch(
        &self,
        binding: String,
        object: String,
        request: Request,
    ) -> Result<Response, HostError>;
}

/// Object-safe asynchronous storage capabilities granted to one Worker event.
#[async_trait]
pub trait WorkerStorage: Send + Sync {
    /// Reads one key at the event's storage snapshot.
    async fn get(&self, key: String) -> Result<Option<Vec<u8>>, HostError>;

    /// Stages one key/value write in the event transaction.
    async fn put(&self, key: String, value: Vec<u8>) -> Result<(), HostError>;

    /// Stages deletion of one key and reports whether it existed.
    async fn delete(&self, key: String) -> Result<bool, HostError>;

    /// Lists keys with a prefix subject to the caller's result bound.
    async fn list(&self, prefix: String, limit: u32) -> Result<Vec<String>, HostError>;

    /// Executes one SQL statement and returns its Arrow IPC result bytes.
    async fn sql(&self, statement: String) -> Result<Vec<u8>, HostError>;

    /// Executes one SQL statement and returns its rows as one JSON array.
    async fn sql_rows(&self, statement: String) -> Result<String, HostError>;

    /// Stages the event's single durable alarm deadline.
    async fn set_alarm(&self, epoch_millis: u64) -> Result<(), HostError>;

    /// Reads the currently armed durable alarm deadline, if any.
    async fn get_alarm(&self) -> Result<Option<u64>, HostError>;

    /// Stages removal of the event's durable alarm.
    async fn delete_alarm(&self) -> Result<(), HostError>;
}

/// Object-safe asynchronous socket capabilities granted to one Worker event.
#[async_trait]
pub trait WorkerSockets: Send + Sync {
    /// Sends one message to an accepted WebSocket.
    async fn send(&self, socket: SocketId, message: Vec<u8>) -> Result<(), HostError>;

    /// Closes one accepted WebSocket with a code and reason.
    async fn close(&self, socket: SocketId, code: u16, reason: String) -> Result<(), HostError>;

    /// Persists a bounded attachment for one accepted WebSocket.
    async fn set_attachment(&self, socket: SocketId, value: Vec<u8>) -> Result<(), HostError>;

    /// Reads the most recent attachment for one accepted WebSocket.
    async fn get_attachment(&self, socket: SocketId) -> Result<Option<Vec<u8>>, HostError>;

    /// Lists every WebSocket currently attached to this Durable Object.
    async fn attached(&self) -> Result<Vec<SocketId>, HostError>;
}

/// Host state that delegates generated WIT imports to storage and socket traits.
#[derive(Clone)]
pub struct WorkerHost {
    /// Transactional storage capability for the current event.
    storage: Arc<dyn WorkerStorage>,
    /// Gateway-held socket capability for the current event.
    sockets: Arc<dyn WorkerSockets>,
    /// Durable Object binding router for cross-component fetches.
    bindings: Arc<dyn WorkerBindings>,
}

impl WorkerHost {
    /// Creates host state with storage and sockets but no binding router.
    pub fn new(storage: Arc<dyn WorkerStorage>, sockets: Arc<dyn WorkerSockets>) -> Self {
        Self {
            storage,
            sockets,
            bindings: Arc::new(UnsupportedBindings),
        }
    }

    /// Creates host state with all capabilities for one Durable Object event.
    pub fn with_bindings(
        storage: Arc<dyn WorkerStorage>,
        sockets: Arc<dyn WorkerSockets>,
        bindings: Arc<dyn WorkerBindings>,
    ) -> Self {
        Self {
            storage,
            sockets,
            bindings,
        }
    }
}

/// Binding capability used when a caller has not supplied a router.
struct UnsupportedBindings;

#[async_trait]
impl WorkerBindings for UnsupportedBindings {
    /// Rejects cross-component calls when no router is configured.
    async fn do_fetch(
        &self,
        _binding: String,
        _object: String,
        _request: Request,
    ) -> Result<Response, HostError> {
        Err(HostError::Unsupported {
            operation: "Durable Object binding",
        })
    }
}

impl bindings::verglas::do_worker::types::Host for WorkerHost {}

impl bindings::verglas::do_worker::bindings::Host for WorkerHost {
    /// Delegates a flattened Durable Object fetch to the configured router.
    async fn do_fetch(
        &mut self,
        binding: String,
        object: String,
        request: Request,
    ) -> Result<Response, WitHandlerError> {
        self.bindings
            .do_fetch(binding, object, request)
            .await
            .map_err(to_handler_error)
    }
}

/// Checks the hard attachment bound before a value crosses the host boundary.
fn validate_attachment(value: &[u8]) -> Result<(), HostError> {
    if value.len() > MAX_ATTACHMENT_SIZE {
        return Err(HostError::AttachmentTooLarge {
            actual: value.len(),
            max: MAX_ATTACHMENT_SIZE,
        });
    }
    Ok(())
}

/// Converts a host error into the WIT error record returned to the component.
fn to_handler_error(error: HostError) -> WitHandlerError {
    WitHandlerError {
        message: error.to_string(),
    }
}

impl bindings::verglas::do_worker::storage::Host for WorkerHost {
    /// Delegates a WIT storage read to the configured storage capability.
    async fn get(&mut self, key: String) -> Result<Option<Vec<u8>>, WitHandlerError> {
        self.storage.get(key).await.map_err(to_handler_error)
    }

    /// Delegates a WIT storage write to the configured storage capability.
    async fn put(&mut self, key: String, value: Vec<u8>) -> Result<(), WitHandlerError> {
        self.storage.put(key, value).await.map_err(to_handler_error)
    }

    /// Delegates a WIT storage deletion to the configured storage capability.
    async fn delete(&mut self, key: String) -> Result<bool, WitHandlerError> {
        self.storage.delete(key).await.map_err(to_handler_error)
    }

    /// Delegates a bounded WIT key listing to the configured storage capability.
    async fn list(&mut self, prefix: String, limit: u32) -> Result<Vec<String>, WitHandlerError> {
        self.storage
            .list(prefix, limit)
            .await
            .map_err(to_handler_error)
    }

    /// Delegates a WIT SQL statement to the configured storage capability.
    async fn sql(&mut self, statement: String) -> Result<Vec<u8>, WitHandlerError> {
        self.storage.sql(statement).await.map_err(to_handler_error)
    }

    /// Delegates the WIT JSON-row SQL statement to the storage capability.
    async fn sql_rows(&mut self, statement: String) -> Result<String, WitHandlerError> {
        self.storage
            .sql_rows(statement)
            .await
            .map_err(to_handler_error)
    }

    /// Delegates arming the WIT durable alarm to the storage capability.
    async fn set_alarm(&mut self, epoch_millis: u64) -> Result<(), WitHandlerError> {
        self.storage
            .set_alarm(epoch_millis)
            .await
            .map_err(to_handler_error)
    }

    /// Delegates reading the WIT durable alarm to the storage capability.
    async fn get_alarm(&mut self) -> Result<Option<u64>, WitHandlerError> {
        self.storage.get_alarm().await.map_err(to_handler_error)
    }

    /// Delegates clearing the WIT durable alarm to the storage capability.
    async fn delete_alarm(&mut self) -> Result<(), WitHandlerError> {
        self.storage.delete_alarm().await.map_err(to_handler_error)
    }
}

impl bindings::verglas::do_worker::sockets::Host for WorkerHost {
    /// Delegates a WIT socket send to the configured socket capability.
    async fn send(&mut self, socket: SocketId, message: Vec<u8>) -> Result<(), WitHandlerError> {
        self.sockets
            .send(socket, message)
            .await
            .map_err(to_handler_error)
    }

    /// Delegates a WIT socket close to the configured socket capability.
    async fn close(
        &mut self,
        socket: SocketId,
        code: u16,
        reason: String,
    ) -> Result<(), WitHandlerError> {
        self.sockets
            .close(socket, code, reason)
            .await
            .map_err(to_handler_error)
    }

    /// Rejects oversized attachments before delegating the WIT attachment write.
    async fn set_attachment(
        &mut self,
        socket: SocketId,
        value: Vec<u8>,
    ) -> Result<(), WitHandlerError> {
        validate_attachment(&value).map_err(to_handler_error)?;
        self.sockets
            .set_attachment(socket, value)
            .await
            .map_err(to_handler_error)
    }

    /// Delegates a WIT attachment read and rejects an invalid backend value.
    async fn get_attachment(
        &mut self,
        socket: SocketId,
    ) -> Result<Option<Vec<u8>>, WitHandlerError> {
        let value = self
            .sockets
            .get_attachment(socket)
            .await
            .map_err(to_handler_error)?;
        if let Some(value) = value.as_ref() {
            validate_attachment(value).map_err(to_handler_error)?;
        }
        Ok(value)
    }

    /// Delegates listing attached WIT sockets to the configured capability.
    async fn attached(&mut self) -> Result<Vec<SocketId>, WitHandlerError> {
        self.sockets.attached().await.map_err(to_handler_error)
    }
}

/// Generated service-world instance containing both Worker and handler exports.
pub use bindings::Service;
