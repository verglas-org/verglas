//! `verglas` command-line interface definition.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};

use crate::credentials::{CredentialsError, credentials_path};

/// Global flags shared by every subcommand.
#[derive(Debug, Parser)]
#[command(name = "verglas", version, about = "Verglas operator CLI")]
pub struct Cli {
    /// Emit machine-readable JSON.
    #[arg(long, global = true)]
    pub json: bool,

    #[command(subcommand)]
    pub command: Command,
}

/// Top-level `verglas` subcommands.
///
/// Every command operates against the selected server and Iceberg catalog.
/// Workers are the single scheduled/event-driven compute primitive; there are
/// no source, MV, or sink command groups.
///
/// The CLI's own version is a flag (`-V`/`--version`), not a subcommand; the
/// running server's version is reported by `verglas status`.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Authenticate with Verglas Cloud.
    ///
    /// Writes the shared, secret-free connection profile used by
    /// integrations.
    Login(LoginArgs),
    /// Sign out and remove the stored connection profile.
    Logout,
    /// Probe the configured server.
    Status,
    /// Create, append to, inspect, and drop agent-managed Iceberg tables.
    #[command(subcommand)]
    Table(TableCommand),
    /// Manage json-render dashboards on Verglas Cloud.
    ///
    /// Dashboards are declared from `--file` specs. The OSS stack does not
    /// host dashboards.
    #[command(subcommand)]
    Dashboard(DashboardCommand),
    /// Manage workers on Verglas Cloud.
    ///
    /// `list`/`get` read them; `create`/`delete` manage them from a spec
    /// file; `run` dispatches one now. The OSS stack does not host workers.
    #[command(subcommand)]
    Workers(WorkersCommand),
    /// Create Lakekeeper-managed lakehouses.
    #[command(subcommand)]
    Lakehouse(LakehouseCommand),
    /// Create scoped credentials.
    ///
    /// Values are read from stdin and never appear in argv.
    #[command(subcommand)]
    Secret(SecretCommand),
    /// Manage scoped access tokens.
    #[command(subcommand)]
    Token(TokenCommand),
    /// Property graphs: ingest nodes and edges, traverse, search precedents.
    #[command(subcommand)]
    Graph(GraphCommand),
    /// Vector buckets: put vectors, build indexes, nearest-neighbor search.
    #[command(subcommand)]
    Vector(VectorCommand),
}

impl Cli {
    /// Resolves the credential-file location: `VERGLAS_CREDENTIALS_FILE`, then
    /// the default owner-only path.
    pub fn resolved_credentials_path(&self) -> Result<PathBuf, CredentialsError> {
        let overridden = std::env::var_os("VERGLAS_CREDENTIALS_FILE").map(PathBuf::from);
        credentials_path(overridden.as_deref())
    }

    /// Resolves the tenant access API URL: `VERGLAS_ACCESS_ENDPOINT`, then the
    /// connection profile's control plane, then Verglas Cloud.
    pub fn access_endpoint() -> String {
        if let Ok(url) = std::env::var("VERGLAS_ACCESS_ENDPOINT")
            && !url.trim().is_empty()
        {
            return url;
        }
        crate::connection_profile::control_plane_url()
            .unwrap_or_else(|| "https://api.verglas.dev".to_owned())
    }
}

/// `verglas token` operations against the authorization service.
#[derive(Debug, Subcommand)]
pub enum TokenCommand {
    /// Mint a delegated token and retain its one-time plaintext value locally.
    Create(TokenCreateArgs),
    /// List token metadata visible to the authenticated principal.
    List,
    /// Revoke one token and remove an exactly matching local credential.
    Revoke(TokenRevokeArgs),
}

/// Arguments for `verglas token create`.
#[derive(Debug, Args)]
pub struct TokenCreateArgs {
    /// Human-readable label for the new token.
    pub name: String,
    /// Runtime audience that may present the token.
    #[arg(long, default_value = "verglas-cli")]
    pub audience: String,
    /// Token lifetime in seconds. Omit only for deliberately non-expiring local credentials.
    #[arg(long)]
    pub expires_in_seconds: Option<u64>,
    /// Delegated resource actions as RESOURCE=action,action. Repeat for multiple resources.
    #[arg(long = "grant")]
    pub grants: Vec<String>,
}

/// Arguments for `verglas token revoke`.
#[derive(Debug, Args)]
pub struct TokenRevokeArgs {
    /// Token identifier returned by `verglas token list`.
    pub id: String,
}

/// Arguments for the shared connection-profile login flow.
#[derive(Debug, Args)]
pub struct LoginArgs {
    /// Automation API key; omit for an interactive WorkOS device sign-in.
    #[arg(long)]
    pub api_key: Option<String>,
    /// Never open a browser; print the verification URI to sign in manually.
    #[arg(long)]
    pub no_browser: bool,
}

/// `verglas lakehouse` operations against the local database resource API.
#[derive(Debug, Subcommand)]
pub enum LakehouseCommand {
    /// Create one Lakekeeper-managed lakehouse.
    Create(LakehouseCreateArgs),
}

/// Arguments for `verglas lakehouse create`.
#[derive(Debug, Args)]
pub struct LakehouseCreateArgs {
    /// Stable lakehouse name.
    pub name: String,
    /// S3 prefix for a lakehouse that uses an authorized scoped secret.
    #[arg(long)]
    pub data_path: Option<String>,
    /// External Iceberg REST catalog URI.
    #[arg(long, requires = "warehouse")]
    pub catalog: Option<String>,
    /// Warehouse selected from the external catalog.
    #[arg(long, requires = "catalog")]
    pub warehouse: Option<String>,
}

/// `verglas secret` operations against the local access service.
#[derive(Debug, Subcommand)]
pub enum SecretCommand {
    /// Create a typed URI-scoped secret, reading its value from standard input.
    Create(SecretCreateArgs),
}

/// Arguments for `verglas secret create`.
#[derive(Debug, Args)]
pub struct SecretCreateArgs {
    /// Stable secret name.
    pub name: String,
    /// Credential type used when resolving resource bindings.
    #[arg(long = "type", value_enum)]
    pub secret_type: SecretType,
    /// URI prefix to which this secret grants access.
    #[arg(long)]
    pub scope: String,
}

/// Secret types accepted by the local access service.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum SecretType {
    /// S3-compatible object-storage credentials.
    S3,
    /// Iceberg REST catalog credentials.
    #[value(name = "iceberg-rest")]
    IcebergRest,
}

/// `verglas dashboard` subcommands for Verglas Cloud json-render dashboards.
#[derive(Debug, Subcommand)]
pub enum DashboardCommand {
    /// Create or update a dashboard from a json-render spec file (`--file`).
    Create(DashboardCreateArgs),
    /// List dashboards on Verglas Cloud.
    List(DashboardTenantArgs),
    /// Show one dashboard and its hosted URL.
    Get(DashboardNameArgs),
    /// Delete a dashboard by name.
    Delete(DashboardNameArgs),
}

/// Arguments for `verglas dashboard create`.
#[derive(Debug, Args)]
pub struct DashboardCreateArgs {
    /// Path to a json-render dashboard spec (JSON or TOML).
    #[arg(long)]
    pub file: std::path::PathBuf,
    /// Tenant deployment id. Required when the Cloud credential is not
    /// already scoped to one tenant.
    #[arg(long)]
    pub tenant_id: Option<String>,
}

/// Optional tenant filter for list.
#[derive(Debug, Args)]
pub struct DashboardTenantArgs {
    /// Tenant deployment id.
    #[arg(long)]
    pub tenant_id: Option<String>,
}

/// A dashboard referenced by its stable name.
#[derive(Debug, Args)]
pub struct DashboardNameArgs {
    /// Dashboard name returned by create or list.
    pub name: String,
    /// Tenant deployment id.
    #[arg(long)]
    pub tenant_id: Option<String>,
}

/// `verglas workers` subcommands against Verglas Cloud (`/v1/workers`).
#[derive(Debug, Subcommand)]
pub enum WorkersCommand {
    /// List every active worker on Verglas Cloud.
    List,
    /// Show one worker's full detail by name.
    Get(WorkerRefArgs),
    /// Register a worker from a portable spec file (`--file`, JSON or TOML).
    ///
    /// `--name`/`--schedule` override the matching spec fields.
    Create(WorkerCreateArgs),
    /// Archive a worker (lifecycle state transition).
    ///
    /// Accepts the worker's name.
    Delete(WorkerRefArgs),
    /// Dispatch a manual run of the worker now.
    ///
    /// Accepts the worker's name.
    Run(WorkerRefArgs),
    /// Not supported.
    ///
    /// Workers run on Verglas Cloud; the OSS stack has no follow runtime.
    Follow(WorkerFollowArgs),
}

/// A worker referenced by its registered name.
#[derive(Debug, Args)]
pub struct WorkerRefArgs {
    /// The worker's name.
    pub worker: String,
}

/// Arguments for `verglas workers create`.
#[derive(Debug, Args)]
pub struct WorkerCreateArgs {
    /// The portable worker spec, a JSON (`.json`) or TOML (`.toml`) object.
    #[arg(long)]
    pub file: PathBuf,
    /// Override the spec's `name`.
    #[arg(long)]
    pub name: Option<String>,
    /// Override the spec's cron schedule (a cron-triggered spec only).
    #[arg(long)]
    pub schedule: Option<String>,
}

/// Arguments for `verglas workers follow`.
#[derive(Debug, Args)]
pub struct WorkerFollowArgs {
    /// Tail this file instead of wrapping a command. Mutually exclusive with a
    /// trailing `-- <command...>`.
    #[arg(long, conflicts_with = "command")]
    pub file: Option<PathBuf>,
    /// The target table captured lines are appended to (`namespace.table`).
    /// Defaults to `follow.<name>`.
    #[arg(long)]
    pub table: Option<String>,
    /// A name for the follow worker. Defaults to a generated throwaway name.
    #[arg(long)]
    pub name: Option<String>,
    /// Register the worker durably instead of tearing it down when you exit.
    #[arg(long)]
    pub keep: bool,
    /// The command to follow, after `--`. It is run and its stdout and stderr are
    /// captured as rows.
    #[arg(last = true)]
    pub command: Vec<String>,
}

/// `verglas table` subcommands. Every verb takes `--output json` via the global
/// `--json` flag; the JSON shape is documented in each verb's long help.
#[derive(Debug, Subcommand)]
pub enum TableCommand {
    /// Create a table from a source file.
    ///
    /// CSV, Parquet, and JSONL infer their schema from the extension. JSON
    /// (`--json`): {"table","operation":"create","schema":[{"id","name",
    /// "type","required"}],"partition_by":[...],"records_added","data_files_added",
    /// "snapshot_id"}.
    Create(TableCreateArgs),
    /// Append a source file to an existing table.
    ///
    /// Same formats as `create`. JSON (`--json`):
    /// {"table","operation":"append","records_added",
    /// "data_files_added","snapshot_id"}. A schema mismatch fails naming
    /// the column, with a nonzero exit code, and leaves the table
    /// unchanged.
    Append(TableAppendArgs),
    /// List tables, optionally within one namespace.
    ///
    /// JSON (`--json`): {"tables":[{"namespace","name"}]}.
    List(TableListArgs),
    /// Show a table's schema, partitioning, and current-snapshot counters.
    ///
    /// JSON (`--json`):
    /// {"table","schema":[...],"partition_by":[...],"row_count",
    /// "file_count","size_bytes","current_snapshot_id"}.
    Show(TableInspectArgs),
    /// Show a table's snapshot history (ids, times, operations, summaries).
    ///
    /// JSON (`--json`):
    /// {"table","snapshots":[{"snapshot_id","parent_snapshot_id",
    /// "timestamp_ms","timestamp","operation","summary":{...}}]}.
    History(TableInspectArgs),
    /// Drop a table.
    ///
    /// Removes the table's catalog entry. Requires `--yes` or an
    /// interactive confirmation.
    Delete(TableDeleteArgs),
}

/// Arguments for `verglas table delete`.
#[derive(Debug, Args)]
pub struct TableDeleteArgs {
    /// The table to drop, as `namespace.name`.
    pub table: String,
    /// Drop without the interactive confirmation prompt. Required for scripts and
    /// any non-interactive session.
    #[arg(long)]
    pub yes: bool,
}

/// Arguments for `verglas table create`.
#[derive(Debug, Args)]
pub struct TableCreateArgs {
    /// The table to create, as `namespace.name`.
    pub table: String,
    /// The source file. `.csv`, `.parquet`, and `.jsonl` are recognised by
    /// extension.
    pub source: PathBuf,
    /// Add an identity partition on this column.
    #[arg(long)]
    pub partition_by: Option<String>,
}

/// Arguments for `verglas table append`.
#[derive(Debug, Args)]
pub struct TableAppendArgs {
    /// The table to append to, as `namespace.name`.
    pub table: String,
    /// The source file. `.csv`, `.parquet`, and `.jsonl` are recognised by
    /// extension.
    pub source: PathBuf,
}

/// Arguments for `verglas table list`.
#[derive(Debug, Args)]
pub struct TableListArgs {
    /// Restrict the listing to this dotted namespace. Omit to list every one.
    pub namespace: Option<String>,
}

/// Arguments for `verglas table show` and `verglas table history`.
#[derive(Debug, Args)]
pub struct TableInspectArgs {
    /// The table, as `namespace.name`.
    pub table: String,
}

/// `verglas graph` operations against the S3 semantic listener (#144). Every
/// verb wraps `verglas_sdk::semantic::VerglasGraphsClient`; the CLI signs
/// nothing itself.
#[derive(Debug, Subcommand)]
pub enum GraphCommand {
    /// Create an empty graph.
    Create(GraphNameArgs),
    /// Add or update nodes from a JSON array (a file path, or `-` for stdin).
    AddNode(GraphInputArgs),
    /// Add or update edges from a JSON array (a file path, or `-` for stdin).
    AddEdge(GraphInputArgs),
    /// List one node's immediate neighbors.
    Neighbors(GraphNeighborsArgs),
    /// Build the graph's traversal index.
    Index(GraphNameArgs),
    /// Show one graph's metadata.
    Show(GraphNameArgs),
    /// Delete a graph.
    Delete(GraphNameArgs),
    /// List every graph.
    List,
    /// Traverse up to `--hops` edges out from one node.
    #[command(name = "k-hop")]
    KHop(GraphKHopArgs),
    /// Find paths between two nodes within `--max-hops`.
    Paths(GraphPathsArgs),
    /// Rank Decision nodes against a lexical query, with an optional entity structural boost.
    Precedents(GraphPrecedentsArgs),
}

/// A graph referenced by its stable name.
#[derive(Debug, Args)]
pub struct GraphNameArgs {
    /// The graph's name.
    pub name: String,
}

/// Arguments for `verglas graph add-node`/`add-edge`.
#[derive(Debug, Args)]
pub struct GraphInputArgs {
    /// The graph's name.
    pub name: String,
    /// A JSON array file path, or `-` to read from stdin.
    pub input: String,
}

/// Arguments for `verglas graph neighbors`.
#[derive(Debug, Args)]
pub struct GraphNeighborsArgs {
    /// The graph's name.
    pub name: String,
    /// The node whose neighbors are listed.
    pub node: String,
}

/// Arguments for `verglas graph k-hop`.
#[derive(Debug, Args)]
pub struct GraphKHopArgs {
    /// The graph's name.
    pub name: String,
    /// The starting node.
    pub node: String,
    /// Maximum traversal depth.
    #[arg(long)]
    pub hops: i64,
}

/// Arguments for `verglas graph paths`.
#[derive(Debug, Args)]
pub struct GraphPathsArgs {
    /// The graph's name.
    pub name: String,
    /// The source node.
    pub source: String,
    /// The target node.
    pub target: String,
    /// Maximum path length in hops.
    #[arg(long = "max-hops")]
    pub max_hops: i64,
}

/// Arguments for `verglas graph precedents`.
#[derive(Debug, Args)]
pub struct GraphPrecedentsArgs {
    /// The graph's name.
    pub name: String,
    /// The lexical query text, BM25-ranked against each Decision node's
    /// `objective` property.
    #[arg(long)]
    pub query: String,
    /// Caps the number of ranked precedents returned.
    #[arg(long)]
    pub limit: Option<i32>,
    /// An entity node id that boosts a decision sharing it as a direct
    /// neighbor. Repeatable.
    #[arg(long = "entity")]
    pub entities: Vec<String>,
}

/// `verglas vector` operations against the S3 Vectors semantic listener
/// (#144). Every verb wraps `verglas_sdk::semantic::S3VectorsClient`; the CLI
/// signs nothing itself.
#[derive(Debug, Subcommand)]
pub enum VectorCommand {
    /// Create a vector bucket.
    CreateBucket(VectorBucketArgs),
    /// Create a vector index inside a bucket.
    CreateIndex(VectorCreateIndexArgs),
    /// Put vectors from a JSON array (a file path, or `-` for stdin).
    Put(VectorInputArgs),
    /// Query the nearest vectors to `--query-vector`.
    Query(VectorQueryArgs),
    /// List vectors in an index.
    List(VectorIndexArgs),
    /// Get vectors by key.
    Get(VectorKeysArgs),
    /// Delete vectors by key.
    Delete(VectorKeysArgs),
    /// Delete a vector index.
    DeleteIndex(VectorIndexArgs),
    /// Delete a vector bucket.
    DeleteBucket(VectorBucketArgs),
    /// List every vector bucket.
    ListBuckets,
    /// List every index in a bucket.
    ListIndexes(VectorBucketArgs),
}

/// A vector bucket referenced by its stable name.
#[derive(Debug, Args)]
pub struct VectorBucketArgs {
    /// The bucket's name.
    pub bucket: String,
}

/// A bucket + index pair.
#[derive(Debug, Args)]
pub struct VectorIndexArgs {
    /// The bucket's name.
    pub bucket: String,
    /// The index's name.
    pub index: String,
}

/// Arguments for `verglas vector create-index`.
#[derive(Debug, Args)]
pub struct VectorCreateIndexArgs {
    /// The bucket's name.
    pub bucket: String,
    /// The index's name.
    pub index: String,
    /// Vector dimension.
    #[arg(long)]
    pub dimension: i32,
    /// Distance metric used for nearest-neighbor queries.
    #[arg(long, value_enum, default_value_t = VectorMetric::Cosine)]
    pub metric: VectorMetric,
}

/// Distance metrics accepted by `verglas vector create-index`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum VectorMetric {
    /// Cosine distance.
    Cosine,
    /// Euclidean distance.
    Euclidean,
}

/// Arguments for `verglas vector put`.
#[derive(Debug, Args)]
pub struct VectorInputArgs {
    /// The bucket's name.
    pub bucket: String,
    /// The index's name.
    pub index: String,
    /// A JSON array file path, or `-` to read from stdin.
    pub input: String,
}

/// Arguments for `verglas vector query`.
#[derive(Debug, Args)]
pub struct VectorQueryArgs {
    /// The bucket's name.
    pub bucket: String,
    /// The index's name.
    pub index: String,
    /// Number of nearest results to return.
    #[arg(long = "top-k")]
    pub top_k: i32,
    /// The query vector, as a JSON float array (e.g. `[0.1,0.2,0.3]`).
    #[arg(long = "query-vector")]
    pub query_vector: String,
}

/// A bucket + index + key set, for `get`/`delete`.
#[derive(Debug, Args)]
pub struct VectorKeysArgs {
    /// The bucket's name.
    pub bucket: String,
    /// The index's name.
    pub index: String,
    /// One or more vector keys.
    #[arg(required = true)]
    pub keys: Vec<String>,
}
