//! Error values shared by manifest-independent gateway request paths.
//!
//! Errors retain the distinction between client route failures, control-plane
//! failures, and event-socket protocol failures so callers can handle each one.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

/// A gateway operation failed before producing an HTTP or WebSocket result.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum GatewayError {
    /// No manifest binding has the requested URL name.
    #[error("unknown Durable Object binding: {binding}")]
    UnknownBinding {
        /// Binding name received from the route.
        binding: String,
    },
    /// A named object is not the identity declared by a system binding.
    #[error("unknown object {binding}/{name}")]
    UnknownObject {
        /// Binding that owns the object.
        binding: String,
        /// Object name that was not declared.
        name: String,
    },
    /// A route identity cannot be represented as a verglasd-safe child identity.
    #[error("invalid Durable Object route identity: {identity}")]
    InvalidIdentity {
        /// Rejected identity text.
        identity: String,
    },
    /// The verglasd control socket could not be reached or read.
    #[error("verglasd control socket failed during {operation}: {message}")]
    ControlIo {
        /// Control operation that failed.
        operation: &'static str,
        /// Operating-system detail.
        message: String,
    },
    /// Verglasd rejected a spawn command.
    #[error("verglasd rejected Durable Object spawn: {message}")]
    SpawnRejected {
        /// One-line control response detail.
        message: String,
    },
    /// The event socket could not be reached or written.
    #[error("DO event socket failed during {operation}: {message}")]
    EventIo {
        /// Event-socket operation that failed.
        operation: &'static str,
        /// Operating-system detail.
        message: String,
    },
    /// A worker frame violated the frozen NDJSON contract.
    #[error("invalid DO event frame: {message}")]
    Protocol {
        /// Stable protocol violation detail.
        message: String,
    },
    /// A Worker explicitly aborted one event.
    #[error("Durable Object event failed: {message}")]
    WorkerError {
        /// Worker-provided failure text.
        message: String,
    },
    /// The event socket closed while a request was pending.
    #[error("DO event socket closed")]
    Disconnected,
    /// A request or Worker response contained a value that cannot be represented by HTTP.
    #[error("invalid HTTP value: {message}")]
    InvalidHttp {
        /// Value validation detail.
        message: String,
    },
    /// A DO attempted to call itself while its serialized event gate was held.
    #[error(
        "self-call deadlock rejected: {source_binding}/{source_object} -> {target_binding}/{target_object}; v0 does not support Cloudflare reentrancy because the source gate is serialized"
    )]
    SelfCallDeadlock {
        /// Binding held by the source DO.
        source_binding: String,
        /// Object name held by the source DO.
        source_object: String,
        /// Binding targeted by the call.
        target_binding: String,
        /// Object name targeted by the call.
        target_object: String,
    },
    /// Worker-tier execution was not configured or could not be loaded.
    #[error("Worker pool unavailable: {message}")]
    WorkerUnavailable {
        /// Stable loading or configuration detail.
        message: String,
    },
    /// Worker-tier execution failed inside Wasmtime.
    #[error("Worker pool execution failed: {message}")]
    WorkerPool {
        /// Pool error detail.
        message: String,
    },
    /// A binding target in another Worker microVM could not be reached.
    #[error("remote Worker request failed: {message}")]
    RemoteWorker {
        /// Transport or response conversion detail.
        message: String,
    },
    /// Direct Fly ingress did not carry the control-plane mesh credential.
    #[error("unauthorized Worker ingress")]
    UnauthorizedIngress,
}

impl GatewayError {
    /// Maps one I/O failure into a stable gateway operation error.
    pub(crate) fn control_io(operation: &'static str, error: std::io::Error) -> Self {
        Self::ControlIo {
            operation,
            message: error.to_string(),
        }
    }

    /// Maps one event-socket I/O failure into a stable gateway operation error.
    pub(crate) fn event_io(operation: &'static str, error: std::io::Error) -> Self {
        Self::EventIo {
            operation,
            message: error.to_string(),
        }
    }

    /// Returns the stable frame error code used by DO do-call results.
    pub(crate) fn code(&self) -> &'static str {
        match self {
            Self::UnknownBinding { .. } => "unknown-binding",
            Self::UnknownObject { .. } => "unknown-object",
            Self::InvalidIdentity { .. } => "invalid-identity",
            Self::ControlIo { .. } => "control-io",
            Self::SpawnRejected { .. } => "spawn-rejected",
            Self::EventIo { .. } => "event-io",
            Self::Protocol { .. } => "protocol",
            Self::WorkerError { .. } => "worker-error",
            Self::Disconnected => "disconnected",
            Self::InvalidHttp { .. } => "invalid-http",
            Self::SelfCallDeadlock { .. } => "self-call-deadlock",
            Self::WorkerUnavailable { .. } => "worker-unavailable",
            Self::WorkerPool { .. } => "worker-pool",
            Self::RemoteWorker { .. } => "remote-worker",
            Self::UnauthorizedIngress => "unauthorized-ingress",
        }
    }
}

impl IntoResponse for GatewayError {
    /// Maps a typed gateway failure to a minimal HTTP response.
    fn into_response(self) -> Response {
        let status = match &self {
            Self::UnknownBinding { .. } | Self::UnknownObject { .. } => StatusCode::NOT_FOUND,
            Self::InvalidHttp { .. } => StatusCode::BAD_REQUEST,
            Self::UnauthorizedIngress => StatusCode::UNAUTHORIZED,
            Self::WorkerError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::ControlIo { .. }
            | Self::SpawnRejected { .. }
            | Self::EventIo { .. }
            | Self::Protocol { .. }
            | Self::Disconnected
            | Self::InvalidIdentity { .. }
            | Self::SelfCallDeadlock { .. }
            | Self::WorkerUnavailable { .. }
            | Self::WorkerPool { .. } => StatusCode::BAD_GATEWAY,
            Self::RemoteWorker { .. } => StatusCode::BAD_GATEWAY,
        };
        (status, self.to_string()).into_response()
    }
}
