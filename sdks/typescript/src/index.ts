// @verglas/sdk — the thin client for Verglas catalog and semantic services.
// Runs in any fetch-capable runtime and in Node 18+.
//
// The public surface contains the catalog/table client, catalog change feed,
// reflected Integration calls, and direct S3 Vectors and Graph clients.

export { connect, VerglasClient, Table } from "./client";
export { VerglasHttpError } from "./http";
export { S3VectorsClient, VerglasGraphsClient } from "./semantic";
export { Graph, graphFromEnv } from "./graph-handle";
export type { GraphEdgeInput, GraphFromEnvOptions, GraphNodeInput } from "./graph-handle";
export type { SemanticDocument, SigV4Credentials } from "./semantic";
export type * from "./semantic-types";
export {
  connectScheduler,
  extractWorkerSource,
  VerglasSchedulerClient,
} from "./control";
export type {
  ControlConnectOptions,
  WorkerRow,
  WorkerSpec,
} from "./control";

export { createDataClient } from "./data";
export type { DataClient, DataClientOptions, IngestResult } from "./data";

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
  QueryAt,
  DynamicNamespaceRegistry,
  NamespaceBindings,
  NamespaceCall,
  NamespaceJsonSchema,
  NamespaceManifest,
  NamespaceMethod,
  NamespaceMethodManifest,
  NamespaceMethodMode,
  NamespaceRegistry,
} from "./types";

export {
  WorkersManagementClient,
  WorkersManagementError,
  buildScriptFormData,
  buildWorkerScriptFormData,
} from "./management";
export type {
  ManagementFetch,
  WorkerModuleSource,
  WorkerScript,
  WorkerScriptMetadata,
  WorkerScriptUpload,
  WorkerScriptUploadParts,
  WorkersManagementMessage,
} from "./management";

