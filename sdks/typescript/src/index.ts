// @verglas/sdk — the thin client a worker imports to read and write Verglas
// Iceberg tables. Runs in any fetch-capable runtime and in Node 18+.
//
// The public surface has three layers:
//   1. Data-plane verbs — connect() and the client objects (Table),
//      plus the change feed (client.follow) and change-driven row follow
//      (client.followRows).
//   2. The worker contract — defineWorker() + the trigger types.
//   3. The Cloudflare Durable Objects model and local worker host.
// Internals the runner uses on the author's behalf (run logging, reference
// examples) are NOT re-exported here; reach them explicitly via the
// `@verglas/sdk/logging` and `@verglas/sdk/examples` subpaths.

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
  DurableObject,
  DurableObjectId,
  DurableObjectNamespace,
  DurableObjectState,
  DurableObjectStorage,
  DurableObjectStub,
  DurableObjectTransaction,
  SqlStorageCursor,
  StorageBridge,
  LocalExecutionContext,
  WorkerRuntime,
  createDurableObjectRuntime,
  createUnixSocketTransport,
  createWorkerRuntime,
} from "./durable-objects";
export type {
  DurableObjectConstructor,
  DurableObjectNamespaceBinding,
  DurableObjectNamespaceOptions,
  DurableObjectStorageGetOptions,
  DurableObjectStorageListOptions,
  DurableObjectStorageOptions,
  DurableObjectStoragePutOptions,
  DurableObjectStubRpc,
  DurableObjectTransport,
  DurableObjectTransportFactory,
  ExecutionContext,
  SqlStorage,
  SqlStorageResult,
  SqlStorageValue,
  WorkerEntrypoint,
  WorkerModule,
  WorkerRuntimeOptions,
} from "./durable-objects";
export { DurableObjectRuntime } from "./durable-objects";

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
  WorkersDurableObject,
  WorkersDurableObjectBinding,
  WorkersDurableObjectNamespace,
  WorkersManagementMessage,
  WorkersObjectCreateOptions,
} from "./management";
