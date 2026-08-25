//! NDJSON frame definitions for one gateway-to-`verglas-runtime` event socket.
//!
//! Binary fields stay base64 on the wire, while HTTP headers remain ordered
//! string pairs so duplicate header values survive the gateway boundary.

use serde::{Deserialize, Serialize};

/// A decoded HTTP response returned by one `fetch-result` frame.
#[derive(Debug, Eq, PartialEq)]
pub struct FetchResponse {
    /// HTTP status code selected by the Worker.
    pub status: u16,
    /// Ordered response header pairs.
    pub headers: Vec<(String, String)>,
    /// Raw response bytes after base64 decoding.
    pub body: Vec<u8>,
    /// Pending WebSocket identity accepted by the guest, when present.
    pub accept_ws: Option<u64>,
}

/// A decoded WebSocket effect released after an event commit.
#[derive(Debug, Eq, PartialEq)]
pub enum WsOutbound {
    /// A text or binary message addressed to one live client.
    Message {
        /// Whether the payload is UTF-8 text rather than binary bytes.
        text: bool,
        /// Decoded message bytes.
        data: Vec<u8>,
    },
    /// A close effect addressed to one live client.
    Close {
        /// WebSocket close code.
        code: u16,
        /// Human-readable close reason.
        reason: String,
    },
}

/// One typed failure returned from a cross-Durable-Object call.
#[derive(Debug, Serialize)]
pub(crate) struct DoCallError {
    /// Stable machine-readable failure code.
    pub code: String,
    /// Human-readable reason that names the violated invariant.
    pub message: String,
}

/// One frame emitted by the gateway into a DO event socket.
#[derive(Debug, Serialize)]
#[serde(tag = "type")]
pub(crate) enum GatewayFrame {
    /// Delivers one HTTP request event.
    #[serde(rename = "fetch")]
    Fetch {
        /// Per-connection event identity.
        id: u64,
        /// HTTP method text.
        method: String,
        /// Request path and query string.
        url: String,
        /// Ordered request header pairs.
        headers: Vec<(String, String)>,
        /// Base64-encoded request body.
        body_b64: String,
        /// Gateway-assigned pending WebSocket identity for a guest-driven upgrade.
        #[serde(skip_serializing_if = "Option::is_none")]
        ws: Option<u64>,
    },
    /// Answers one cross-Durable-Object call from the Worker.
    #[serde(rename = "do-call-result")]
    DoCallResult {
        /// Call identity echoed from the request frame.
        id: u64,
        /// Successful response status, omitted on an error.
        #[serde(skip_serializing_if = "Option::is_none")]
        status: Option<u16>,
        /// Successful ordered response headers.
        #[serde(skip_serializing_if = "Option::is_none")]
        headers: Option<Vec<(String, String)>>,
        /// Successful base64 response body.
        #[serde(skip_serializing_if = "Option::is_none")]
        body_b64: Option<String>,
        /// Accepted pending WebSocket identity, when present.
        #[serde(skip_serializing_if = "Option::is_none")]
        accept_ws: Option<u64>,
        /// Typed failure returned without entering the target gate.
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<DoCallError>,
    },
    /// Registers a gateway-accepted WebSocket identity.
    #[serde(rename = "ws-open")]
    WsOpen {
        /// Gateway-owned WebSocket identity.
        ws: u64,
    },
    /// Delivers one WebSocket message event.
    #[serde(rename = "ws-message")]
    WsMessage {
        /// Per-connection event identity.
        id: u64,
        /// Gateway-owned WebSocket identity.
        ws: u64,
        /// Whether the payload is text.
        text: bool,
        /// Base64-encoded message bytes.
        data_b64: String,
    },
    /// Delivers a client close event.
    #[serde(rename = "ws-close")]
    WsClose {
        /// Per-connection event identity.
        id: u64,
        /// Gateway-owned WebSocket identity.
        ws: u64,
        /// Client-provided close code.
        code: u16,
        /// Client-provided close reason.
        reason: String,
    },
}

/// One frame emitted by `verglas-runtime` into the gateway event socket.
#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub(crate) enum WorkerFrame {
    /// Releases one staged WebSocket send after commit.
    #[serde(rename = "ws-send")]
    WsSend {
        /// Gateway-owned WebSocket identity.
        ws: u64,
        /// Whether the payload is text.
        text: bool,
        /// Base64-encoded message bytes.
        data_b64: String,
    },
    /// Releases one staged WebSocket close after commit.
    #[serde(rename = "ws-close-out")]
    WsCloseOut {
        /// Gateway-owned WebSocket identity.
        ws: u64,
        /// Worker-selected close code.
        code: u16,
        /// Worker-selected close reason.
        reason: String,
    },
    /// Completes an HTTP fetch event.
    #[serde(rename = "fetch-result")]
    FetchResult {
        /// Event identity echoed from the request frame.
        id: u64,
        /// HTTP status code selected by the Worker.
        status: u16,
        /// Ordered response header pairs.
        headers: Vec<(String, String)>,
        /// Base64-encoded response body.
        body_b64: String,
        /// Pending WebSocket identity accepted by the guest.
        #[serde(default)]
        accept_ws: Option<u64>,
    },
    /// Requests one cross-Durable-Object fetch from the gateway.
    #[serde(rename = "do-call")]
    DoCall {
        /// Call identity echoed in the result frame.
        id: u64,
        /// Manifest binding to resolve.
        binding: String,
        /// Named object within the binding.
        object: String,
        /// HTTP method for the target fetch.
        method: String,
        /// Request URL path and query string.
        url: String,
        /// Ordered request headers.
        headers: Vec<(String, String)>,
        /// Base64-encoded request body.
        body_b64: String,
        /// Pending WebSocket identity propagated through the call.
        #[serde(default)]
        ws: Option<u64>,
    },
    /// Completes a WebSocket message or close event.
    #[serde(rename = "done")]
    Done {
        /// Event identity echoed from the request frame.
        id: u64,
    },
    /// Aborts one event without releasing staged effects.
    #[serde(rename = "error")]
    Error {
        /// Event identity echoed from the request frame.
        id: u64,
        /// Worker-provided error text.
        message: String,
    },
}
