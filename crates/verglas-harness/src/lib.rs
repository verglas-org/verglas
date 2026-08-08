//! Shared runtime the server's worker executor builds on (WHITEPAPER §7.2).
//!
//! The worker executor runs one deployment per trigger. The machinery around the
//! run is stable: invoke the code, commit under an idempotency key. There is no
//! deployment watermark in the loop — cron progress is the trigger's logical
//! interval. This crate owns that machinery, plus the worker subprocess
//! executor and the guard policy, in one place:
//!
//! - [`commit`]: the idempotent batch commit (a keyed append the table's own
//!   snapshot log dedupes).
//! - [`worker`]: the worker subprocess executor — spawn the deployment's code
//!   with the trigger's environment, read its result file back.
//! - [`cron`]: a Vixie-semantics cron matcher — the worker runtime's cron
//!   trigger.
//! - [`policy`]: the runaway-worker guard policy (single-flight, host-wide cap,
//!   backoff, child-marker suppression), composed from [`guard`].
//! - [`queue`]: the local durable queue (per-name JSONL segments with
//!   consumer-group offsets).
//!
//! Run-event logging and `_LOGS` retention are catalog-side lakekeeping, not
//! this crate's job.
//!
//! # Why a dedicated crate and not `verglas-sdk`
//!
//! `verglas-sdk` is the code-facing contract: the worker/trigger types a
//! generated deployment compiles against. It deliberately carries no engine or
//! catalog dependency. Idempotent commits and the queue need the Iceberg write
//! path, so folding them into the SDK would pull the engine into every
//! deployment's dependency graph. A separate harness-support crate keeps the
//! SDK thin and gives the worker executor one shared home.

pub mod commit;
pub mod cron;
pub mod follow;
pub mod guard;
pub mod policy;
pub mod queue;
pub mod worker;

pub use commit::{CommitOutcome, HarnessError};
pub use follow::{FollowEnd, FollowSource, follow_log_schema, follow_table_ident, run_follow};
pub use policy::{Guarded, Skipped, run_guarded};
pub use worker::{WorkerExec, WorkerOutcome, WorkerRun, run_worker};
