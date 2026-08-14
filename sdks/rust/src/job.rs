//! Shared types the worker contract builds on: [`Row`], [`Logger`], and
//! [`JobError`].
//!
//! These used to sit beside the retired Source/Sink/MV Job traits. The worker
//! path (`worker` module) is the only deployment contract now; this module keeps
//! the small shared vocabulary so workers and harnesses do not redefine it.

use std::sync::Arc;

/// One row: a JSON object keyed by column name. Mirrors the TS SDK's
/// `Row = Record<string, unknown>`.
pub type Row = serde_json::Map<String, serde_json::Value>;

/// The structured log callback a worker context carries. The first argument is
/// the event name; the second is the optional metadata row. Never write secrets
/// through it.
pub type Logger = Arc<dyn Fn(&str, Option<Row>) + Send + Sync>;

/// A worker failure. The harness records it as an `error` run event; the message
/// is for the operator. Mirrors a TS worker throwing.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct JobError(pub String);
