// @verglas/sdk — the thin client for Verglas Catalog and Worker management.
// Runs in any fetch-capable runtime and in Node 18+.

export { connect, VerglasClient, Table } from "./client";
export { VerglasHttpError } from "./http";

export type {
  Row,
  Watermark,
  ConnectOptions,
  ScanOptions,
  ScanResult,
  DeltaResult,
  Snapshot,
  CommitOptions,
  CommitResult,
  ColumnSpec,
  PartitionSpec,
  TableDefinition,
  CreateTableResult,
  EnsureTableResult,
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
  WorkersDurableObjectBinding,
  WorkersManagementMessage,
} from "./management";
