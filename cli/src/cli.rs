//! `verglas` command-line interface definition.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};
use verglas_core::admin::{DEFAULT_ENDPOINT, ENDPOINT_ENV};

use crate::credentials::{CredentialsError, credentials_path, load_token};

/// Global flags shared by every subcommand.
#[derive(Debug, Parser)]
#[command(name = "verglas", version, about = "Verglas operator CLI")]
pub struct Cli {
    /// Admin API base URL for the target server (`VERGLAS_ENDPOINT`).
    #[arg(
        id = "server_endpoint",
        long = "server-endpoint",
        env = ENDPOINT_ENV,
        default_value = DEFAULT_ENDPOINT,
        global = true
    )]
    pub endpoint: String,

    /// Tenant access/database API (`VERGLAS_ACCESS_ENDPOINT`).
    #[arg(
        long = "access-endpoint",
        env = "VERGLAS_ACCESS_ENDPOINT",
        default_value = "http://127.0.0.1:8345",
        global = true
    )]
    pub access_endpoint: String,

    /// Bearer token for authenticated server APIs (`VERGLAS_TOKEN`).
    #[arg(long, env = "VERGLAS_TOKEN", global = true)]
    pub token: Option<String>,

    /// Owner-only file that stores locally minted access tokens.
    #[arg(long, env = "VERGLAS_CREDENTIALS_FILE", global = true)]
    pub credentials_file: Option<PathBuf>,

    /// Emit machine-readable JSON instead of human-readable tables.
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
    /// Drain this node: stop taking new cache ownership, donate warmth to
    /// peers, then exit.
    Drain(DrainArgs),
    /// Probe the server at `--server-endpoint` (health, version, cache warmth).
    Status,
    /// Create, append to, inspect, and drop agent-managed Iceberg tables, and
    /// read their local per-table cache metrics.
    #[command(subcommand)]
    Table(TableCommand),
    /// Create and manage Rill dashboards over catalog-resolved Iceberg tables.
    /// Available when the self-hosted Compose analytics profile is running.
    #[command(subcommand)]
    Dashboard(DashboardCommand),
    /// Scheduled or event-driven workers on the local server registry.
    /// `list`/`get` read them; `create`/`delete` manage them from a spec file;
    /// `run` dispatches one now; `follow` streams a local process or file into
    /// a table.
    #[command(subcommand)]
    Workers(WorkersCommand),
    /// Set and get small raw values in the built-in persistent KV engine.
    #[command(subcommand)]
    Kv(KvCommand),
    /// Manage and call long-lived local HTTP services in the Docker runtime.
    #[command(subcommand)]
    Vessel(VesselCommand),
    /// Create independently bound Lakehouse and Postgres databases.
    #[command(subcommand)]
    Db(DbCommand),
    /// Create and manage independently scalable PostgreSQL-backed queues.
    #[command(subcommand)]
    Queue(QueueCommand),
    /// Create scoped credentials without exposing their values in argv.
    #[command(subcommand)]
    Secret(SecretCommand),
    /// Mint, inspect, and revoke scoped access tokens.
    #[command(subcommand)]
    Token(TokenCommand),
}

impl Cli {
    /// Resolves an explicit or environment bearer token before the active local credential.
    pub fn resolved_token(&self) -> Result<Option<String>, CredentialsError> {
        if let Some(token) = self
            .token
            .as_deref()
            .filter(|token| !token.trim().is_empty())
        {
            return Ok(Some(token.to_owned()));
        }
        let path = credentials_path(self.credentials_file.as_deref())?;
        Ok(load_token(&path, &self.access_endpoint)?.map(|stored| stored.token))
    }

    /// Resolves the credential-file location used for token lifecycle commands.
    pub fn resolved_credentials_path(&self) -> Result<PathBuf, CredentialsError> {
        credentials_path(self.credentials_file.as_deref())
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

/// `verglas db` operations against the local database resource API.
#[derive(Debug, Subcommand)]
pub enum DbCommand {
    /// Create one managed or externally bound database.
    Create(DbCreateArgs),
    /// Mint a short-lived PostgreSQL password token for one managed Postgres database.
    Token(DbTokenArgs),
}

/// `verglas queue` resource lifecycle operations.
#[derive(Debug, Subcommand)]
pub enum QueueCommand {
    /// Provision a dedicated Neon database and queue service container.
    Create(QueueNameArgs),
    /// List explicitly declared queues.
    List,
    /// Show one queue and both managed deployment identities.
    Show(QueueNameArgs),
    /// Delete the queue container and its dedicated Neon database.
    Delete(QueueNameArgs),
}

/// Stable tenant-local queue name.
#[derive(Debug, Args)]
pub struct QueueNameArgs {
    /// Queue resource name.
    pub name: String,
}

/// Arguments for `verglas db create`.
#[derive(Debug, Args)]
pub struct DbCreateArgs {
    /// Stable database name.
    pub name: String,
    /// Database engine and resource composition.
    #[arg(long = "type", value_enum)]
    pub database_type: DatabaseType,
    /// S3 prefix for a Lakehouse that uses an authorized scoped secret.
    #[arg(long)]
    pub data_path: Option<String>,
    /// External Iceberg REST catalog URI for a Lakehouse.
    #[arg(long)]
    pub catalog: Option<String>,
    /// Warehouse selected from the external catalog.
    #[arg(long, requires = "catalog")]
    pub warehouse: Option<String>,
}

/// Arguments for `verglas db token`.
#[derive(Debug, Args)]
pub struct DbTokenArgs {
    /// Managed Postgres database identifier.
    pub database: String,
    /// Requested connection-token lifetime in seconds, from 1 through 900.
    #[arg(long = "expires-in", default_value_t = 900)]
    pub expires_in_seconds: u64,
    /// Print only the JWT suitable for `PGPASSWORD`; omit to keep it in local secure storage.
    #[arg(long)]
    pub print_password: bool,
}

/// Database types accepted by the local resource API.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum DatabaseType {
    /// Iceberg storage with managed or externally bound services.
    Lakehouse,
    /// A managed Neon Postgres database.
    Postgres,
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

/// `verglas vessel` operations against the local Docker runtime manager.
#[derive(Debug, Subcommand)]
pub enum VesselCommand {
    /// Create or replace a Vessel from a YAML or JSON declaration.
    Add(VesselAddArgs),
    /// List desired Vessels and their observed state.
    List,
    /// Show one Vessel by name.
    Get(VesselNameArgs),
    /// Remove one Vessel and its owned container.
    Remove(VesselNameArgs),
    /// Send an HTTP GET to a path exposed by a Vessel.
    Curl(VesselCurlArgs),
    /// POST a JSON operation to the Vessel's `/v1/query` endpoint.
    Query(VesselQueryArgs),
}

/// Arguments for creating or replacing a Vessel.
#[derive(Debug, Args)]
pub struct VesselAddArgs {
    /// YAML or JSON Vessel declaration.
    #[arg(long)]
    pub file: PathBuf,
}

/// A Vessel referenced by its stable local name.
#[derive(Debug, Args)]
pub struct VesselNameArgs {
    /// Stable Vessel name.
    pub name: String,
}

/// Arguments for a direct Vessel HTTP call.
#[derive(Debug, Args)]
pub struct VesselCurlArgs {
    /// Stable Vessel name.
    pub name: String,
    /// Origin-relative HTTP path exposed by the Vessel.
    pub path: String,
    /// HTTP method sent through the runtime proxy.
    #[arg(long, default_value = "GET")]
    pub method: String,
    /// JSON request body. Prefer `--data-stdin` for credentials.
    #[arg(long, conflicts_with = "data_stdin")]
    pub data: Option<String>,
    /// Read a JSON request body from stdin so secrets avoid shell history.
    #[arg(long, conflicts_with = "data")]
    pub data_stdin: bool,
}

/// Arguments for a semantic Vessel query.
#[derive(Debug, Args)]
pub struct VesselQueryArgs {
    /// Stable Vessel name.
    pub name: String,
    /// JSON request body sent to `/v1/query`.
    pub request: String,
}

/// `verglas kv` operations.
#[derive(Debug, Subcommand)]
pub enum KvCommand {
    /// Set a raw UTF-8 value, optionally with a TTL in seconds.
    Set(KvSetArgs),
    /// Get a raw value.
    Get(KvKeyArgs),
}

/// Arguments for `verglas kv set`.
#[derive(Debug, Args)]
pub struct KvSetArgs {
    /// Tenant-scoped application namespace.
    pub namespace: String,
    /// Key within the namespace.
    pub key: String,
    /// Raw UTF-8 value.
    pub value: String,
    /// Lifetime in seconds.
    #[arg(long)]
    pub ttl: Option<u64>,
}

/// Arguments for `verglas kv get`.
#[derive(Debug, Args)]
pub struct KvKeyArgs {
    /// Tenant-scoped application namespace.
    pub namespace: String,
    /// Key within the namespace.
    pub key: String,
}

/// `verglas dashboard` subcommands for the optional on-prem Rill integration.
#[derive(Debug, Subcommand)]
pub enum DashboardCommand {
    /// Create or refresh an Explore dashboard for an Iceberg table.
    Create(DashboardCreateArgs),
    /// List dashboards managed by Verglas in the configured Rill project.
    List,
    /// Show one dashboard and its browser URL.
    Show(DashboardNameArgs),
    /// Delete the Rill resources owned by one Verglas dashboard.
    Delete(DashboardNameArgs),
}

/// Arguments for `verglas dashboard create`.
#[derive(Debug, Args)]
pub struct DashboardCreateArgs {
    /// Dotted Iceberg table identifier, such as `sales.orders`.
    pub table: String,
    /// Stable Rill resource name. Defaults to the table identifier with dots
    /// replaced by underscores.
    #[arg(long)]
    pub name: Option<String>,
}

/// A dashboard referenced by its Rill resource name.
#[derive(Debug, Args)]
pub struct DashboardNameArgs {
    /// Dashboard resource name returned by create or list.
    pub name: String,
}

/// `verglas workers` subcommands against the local server registry
/// (`/v1/workers`). Every verb takes `--json` for a machine-readable shape.
#[derive(Debug, Subcommand)]
pub enum WorkersCommand {
    /// List every active worker on the local server.
    List,
    /// Show one worker's full detail by name.
    Get(WorkerRefArgs),
    /// Register a worker from a portable spec file (`--file`, JSON or TOML).
    /// `--name`/`--schedule` override the matching spec fields.
    Create(WorkerCreateArgs),
    /// Archive a worker (lifecycle state transition). Accepts the worker's name.
    Delete(WorkerRefArgs),
    /// Dispatch a manual run of the worker now. Accepts the worker's name.
    Run(WorkerRefArgs),
    /// Follow a local process or file and stream every captured line into a table
    /// as rows. Wraps a command after `--`, or tails `--file <path>`. Streams
    /// until Ctrl-C, then tears the worker down; `--keep` leaves it registered.
    Follow(WorkerFollowArgs),
}

/// A worker referenced by its registered name.
#[derive(Debug, Args)]
pub struct WorkerRefArgs {
    /// The worker's name in the local registry.
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
    /// Create a table from a source file. CSV, Parquet, and JSONL infer their
    /// schema from the extension.
    ///
    /// JSON (`--json`): {"table","operation":"create","schema":[{"id","name",
    /// "type","required"}],"partition_by":[...],"records_added","data_files_added",
    /// "snapshot_id"}.
    Create(TableCreateArgs),
    /// Append a source file to an existing table. Same formats as `create`.
    ///
    /// JSON (`--json`): {"table","operation":"append","records_added",
    /// "data_files_added","snapshot_id"}. A schema mismatch fails naming the
    /// column, with a nonzero exit code, and leaves the table unchanged.
    Append(TableAppendArgs),
    /// List tables, optionally within one namespace.
    ///
    /// JSON (`--json`): {"tables":[{"namespace","name"}]}.
    List(TableListArgs),
    /// Show a table's schema, partitioning, and current-snapshot counters.
    ///
    /// JSON (`--json`): {"table","schema":[...],"partition_by":[...],"row_count",
    /// "file_count","size_bytes","current_snapshot_id"}.
    Show(TableInspectArgs),
    /// Show a table's snapshot history (ids, times, operations, summaries).
    ///
    /// JSON (`--json`): {"table","snapshots":[{"snapshot_id","parent_snapshot_id",
    /// "timestamp_ms","timestamp","operation","summary":{...}}]}.
    History(TableInspectArgs),
    /// Compact tables now: rewrite accumulated small data files into fewer,
    /// larger ones and commit the result. A one-shot manual pass over every
    /// table — the server runs no compaction on its own. Progress ratchets one
    /// commit per group and the pass is time-bounded, so on a large backlog it
    /// may stop partway; run it again to continue.
    ///
    /// JSON (`--json`): {"tables_scanned","groups_committed",
    /// "undersized_remaining","budget_bounded","compacted":[{"table",
    /// "groups_committed","input_data_files","output_data_files",
    /// "undersized_remaining","budget_bounded","snapshot_id",...}],
    /// "failures":[["table","message"]]}.
    Compact,
    /// Drop a table via the tenant's Iceberg REST catalog. Removes the table's
    /// catalog entry; requires `--yes` or an interactive confirmation. Uses the
    /// `[catalog]` uri and bearer from `~/.verglas/config.toml`.
    ///
    /// JSON (`--json`): {"table","dropped":true}.
    Delete(TableDeleteArgs),
    /// Per-table cache metrics from the local server (hit rate, cached bytes,
    /// backend requests avoided, and a dollar-savings ESTIMATE at published S3 GET
    /// list pricing).
    Metrics,
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

/// Arguments for `verglas drain`: drain the LOCAL server. The CLI takes no
/// target — it POSTs `/admin/drain` on this machine's admin endpoint (the
/// loopback default, `VERGLAS_ENDPOINT` / `--server-endpoint` override), never
/// resolving or addressing other nodes.
#[derive(Debug, Args)]
pub struct DrainArgs {
    /// Maximum time to keep serving as a donor before exiting, e.g. `10m`,
    /// `30s`, `1h`, or a plain seconds count. Omit to take the server's
    /// configured drain timeout.
    #[arg(long)]
    pub timeout: Option<String>,
}
