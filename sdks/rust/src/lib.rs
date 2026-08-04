//! The Rust SDK for the agent-data platform (issue #321, WHITEPAPER §7.2).
//!
//! One crate owns the contracts a worker and its harness share:
//!
//! - [`worker`]: the worker contract — the single deployment primitive (code +
//!   triggers), mirroring the TS `defineWorker`/`runWorker` model.
//! - [`job`]: shared [`Row`] / [`Logger`] / [`JobError`] vocabulary the worker
//!   context uses.
//! - [`tables_api`]: the commit/snapshot/rows/delta wire types shared by the
//!   engine, daemon routes, and SDK callers.
//! - [`report`]: the create/append/list/show/history/query report wire types,
//!   including compaction reports — what daemon routes serve and the CLI
//!   renders without linking the storage engine.
//! - [`graph`]: the `graph` verb-family wire types the daemon's
//!   `/v1/graphs/...` routes serve and the CLI and TypeScript SDK speak.
//! - [`grant`]: the memory grant contract every worker inherits through
//!   [`worker::WorkerContext`].
//! - [`client`] / [`daemon`]: the data-plane HTTP client and local daemon
//!   helpers the CLI and in-process callers use.

pub mod job;
pub mod worker;

pub mod client;
pub mod daemon;
pub mod grant;
pub mod graph;
pub mod report;
pub mod vector;

pub use client::{
    ARROW_STREAM_CONTENT_TYPE, AppendResult, Client, ClientError, ColumnSpec, ConnectOptions,
    EnsureTable, FollowStream, PartitionSpec, QueryStream, TableDefinition,
};
pub use grant::{GrantError, LocalGrantHost, MemoryGrant, MemoryGrantHost, MemoryGrantRequest};
pub use job::{JobError, Logger, Row};
pub use report::{CompactReport, CompactionReport};
/// Stable table request and response contracts from the dependency-leaf API crate.
pub use verglas_api::table as tables_api;
pub use worker::{
    Catchup, ChangeEvent, CronInterval, RunResult, TableRef, TriggerEvent, TriggerSpec, Worker,
    WorkerContext, WorkerResult,
};
