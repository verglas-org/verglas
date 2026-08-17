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
//! - [`grant`]: the memory grant contract every worker inherits through
//!   [`worker::WorkerContext`].
//! - [`client`] / [`server`]: the data-plane HTTP client and local server
//!   helpers the CLI and in-process callers use. [`client::DataClient`] is the
//!   separate /v0 client (events append + SQL) for the Tinybird-compatible
//!   surface on Verglas Cloud.
//! - [`ingest`]: client-side Iceberg table create/append (#145) — schema
//!   inference, REST catalog, and `FileIO` writes with catalog-vended
//!   credentials, no server round trip.
//!
//! The retired tenant-local access-service era (principal/resource/grant/token
//! CRUD, dynamic database CRUD, the Vessel container runtime) has no module
//! here — it called the old v1 access-token, v1 databases, and v1 vessels
//! routes, all dead server surfaces.

pub mod job;
pub mod worker;

pub mod client;
pub mod grant;
pub mod ingest;
pub mod report;
pub mod semantic;
pub mod semantic_types;
pub mod server;

pub use client::{
    Client, ClientError, ColumnSpec, ConnectOptions, DataClient, Database, DeliveryReceipt,
    EnsureTable, IngestResult, Namespace, NamespaceManifest, NamespaceMethodManifest,
    NamespaceMethodMode, NamespaceStream, PartitionSpec, TableChangeDelivery, TableChangeStream,
    TableDefinition, TableSubscriptionEvent,
};
pub use grant::{GrantError, LocalGrantHost, MemoryGrant, MemoryGrantHost, MemoryGrantRequest};
pub use job::{JobError, Logger, Row};
pub use report::{CompactReport, CompactionReport};
pub use semantic::{S3VectorsClient, SigV4Credentials, VerglasGraphsClient};
pub use semantic_types::*;
/// Stable table request and response contracts from the dependency-leaf API crate.
pub use verglas_api::table as tables_api;
pub use worker::{
    Catchup, ChangeEvent, CloudEvent, CronInterval, HttpCallback, RunResult, TriggerSpec, Worker,
    WorkerContext, WorkerResult,
};
