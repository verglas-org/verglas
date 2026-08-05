// @verglas/sdk — the thin client a worker imports to read and write Verglas
// Iceberg tables. Runs in any fetch-capable runtime and in Node 18+.
//
// The public surface is exactly two layers:
//   1. Data-plane verbs — connect() and the client objects (Table, Queue, Graph),
//      plus the change feed (client.follow) and change-driven row follow
//      (client.followRows).
//   2. The worker contract — defineWorker() + the trigger types.
// Internals the runner uses on the author's behalf (run logging, reference
// examples) are NOT re-exported here; reach them explicitly via the
// `@verglas/sdk/logging` and `@verglas/sdk/examples` subpaths.

export { connect, VerglasClient, Table, Queue, Graph, Kv } from "./client";
export { VerglasHttpError } from "./http";

export { defineWorker, runWorker } from "./contracts";
export type {
  WorkerContext,
  WorkerHandler,
  WorkerDefinition,
  WorkerResult,
  RunWorkerOptions,
  // CloudEvents runtime payload on ctx.trigger
  CloudEvent,
  // Trigger specs (deployment config the SDK types)
  TriggerSpec,
  CronTriggerSpec,
  WebhookTriggerSpec,
  EventTriggerSpec,
} from "./contracts";

export type {
  Row,
  Watermark,
  ConnectOptions,
  ScanOptions,
  ScanResult,
  DeltaResult,
  Snapshot,
  FollowRowsOptions,
  FollowHandler,
  ChangeEvent,
  ChangeHandler,
  FollowFeedOptions,
  FeedSubscription,
  CommitOptions,
  CommitResult,
  ColumnSpec,
  PartitionSpec,
  TableDefinition,
  CreateTableResult,
  EnsureTableResult,
  QueryResult,
  DashboardCreateOptions,
  DashboardInfo,
  DashboardList,
  DashboardDeleted,
  QueueEnqueueResult,
  QueueRecord,
  QueuePollResult,
  GraphDirection,
  GraphNodeInput,
  GraphEdgeInput,
  GraphReadOptions,
  GraphKHopOptions,
  GraphPathsOptions,
  GraphBackend,
  GraphNeighbor,
  GraphReached,
  GraphTriplet,
  GraphPath,
  GraphCreateResult,
  GraphInsertResult,
  GraphIndexResult,
  GraphShowResult,
  AddIndexOptions,
  IndexReport,
  SearchHit,
  SearchIndexOptions,
  SearchResult,
  IndexInfo,
  KvDeleteResult,
  KvListEntry,
  KvListOptions,
  KvListPage,
  KvPutOptions,
  KvPutResult,
  KvValue,
} from "./types";
