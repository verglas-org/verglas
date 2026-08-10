//! The Rust SDK for the agent-data platform (issue #321, WHITEPAPER §7.2).
//!
//! One crate owns the contracts a worker and its harness share:
//!
//! - [`worker`]: the worker contract — the single deployment primitive (code +
//!   triggers), mirroring the TS `defineWorker`/`runWorker` model.
//! - [`job`]: shared [`Row`] / [`Logger`] / [`JobError`] vocabulary the worker
//!   context uses.
//! - [`tables_api`]: the commit/snapshot/rows/delta wire types shared by the
//!   engine, server routes, and SDK callers.
//! - [`report`]: the create/append/list/show/history/query report wire types,
//!   including compaction reports — what server routes serve and the CLI
//!   renders without linking the storage engine.
//! - [`graph`]: the `graph` verb-family wire types the server's
//!   `/v1/graphs/...` routes serve and the CLI and TypeScript SDK speak.
//! - [`queue`]: the queue enqueue/poll/ack wire types the `/v1/queues/...`
//!   routes serve and both language SDKs speak.
//! - [`vector`]: the table/graph vector-index wire types the `/indexes`
//!   routes serve and both language SDKs speak.
//! - [`grant`]: the memory grant contract every worker inherits through
//!   [`worker::WorkerContext`].
//! - [`client`] / [`server`]: the data-plane HTTP client and local server
//!   helpers the CLI and in-process callers use.

pub mod job;
pub mod worker;

pub mod client;
pub mod grant;
pub mod graph;
pub mod queue;
pub mod report;
pub mod server;
pub mod token;
pub mod vector;

pub use client::{
    ARROW_STREAM_CONTENT_TYPE, AppendResult, Client, ClientError, ColumnSpec, ConnectOptions,
    Database, EnsureTable, Graph, GraphReadOptions, Kv, KvDeleteResult, KvListEntry, KvListPage,
    KvPutOptions, KvPutResult, KvReadTier, KvValue, Namespace, NamespaceManifest,
    NamespaceMethodManifest, NamespaceMethodMode, NamespaceStream, PartitionSpec, QueryStream,
    Queue, Table, TableDefinition,
};
pub use grant::{GrantError, LocalGrantHost, MemoryGrant, MemoryGrantHost, MemoryGrantRequest};
pub use job::{JobError, Logger, Row};
pub use queue::{QueueDelivery, QueueEnqueueResult, QueuePollResult, QueueReceipt};
pub use report::{CompactReport, CompactionReport};
pub use token::{
    AccessTokenCreateRequest, AccessTokenGrant, AccessTokenSummary, DatabaseConnectionToken,
    DatabaseConnectionTokenRequest, IssuedAccessToken,
};
/// Stable table request and response contracts from the dependency-leaf API crate.
pub use verglas_api::table as tables_api;
/// Universal authorization contracts used by access administration and checks.
pub use verglas_authz::{
    AccessCheck, AccessDecision, Action, Grant as AccessGrant, Principal, PrincipalKind, Resource,
    ResourceKind, ScopedTokenClaims,
};
pub use worker::{
    Catchup, ChangeEvent, CloudEvent, CronInterval, HttpCallback, RunResult, TriggerSpec, Worker,
    WorkerContext, WorkerResult,
};
