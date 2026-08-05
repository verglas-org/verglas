//! `verglas` command-line interface definition.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};
use verglas_core::admin::{DEFAULT_ENDPOINT, ENDPOINT_ENV};

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

    /// Emit machine-readable JSON instead of human-readable tables.
    #[arg(long, global = true)]
    pub json: bool,

    #[command(subcommand)]
    pub command: Command,
}

/// Top-level `verglas` subcommands.
///
/// Local commands operate against the selected server and Iceberg catalog.
/// Logged-in commands manage the tenant's container and worker deployments
/// through the control plane. Workers are the single scheduled/event-driven
/// compute primitive; there are no source, MV, or sink command groups.
///
/// The CLI's own version is a flag (`-V`/`--version`), not a subcommand; the
/// running server's version is reported by `verglas status`.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Log in to Verglas Cloud (browser by default). Stores the API key locally
    /// (mode 0600) and the control-plane URL in config. Local commands keep
    /// working without it; re-run to refresh credentials.
    Login(LoginArgs),
    /// Drain this node: stop taking new cache ownership, donate warmth to
    /// peers, then exit.
    Drain(DrainArgs),
    /// Probe the server at `--server-endpoint` (health, version, cache warmth).
    Status,
    /// Create, append to, inspect, and drop agent-managed Iceberg tables, and
    /// read their local per-table cache metrics.
    #[command(subcommand)]
    Table(TableCommand),
    /// Create and traverse property graphs. A graph is a namespace holding two
    /// plain Iceberg tables (nodes and edges) plus a snapshot-bound adjacency
    /// index; the verbs here parallel `table`.
    #[command(subcommand)]
    Graph(GraphCommand),
    /// Run an embedded SQL query over Iceberg tables.
    Query(QueryArgs),
    /// Table-scoped vector (ANN) indexes. `list <table>` discovers attachments
    /// from the current snapshot; `add` builds one and `search` queries it.
    #[command(subcommand)]
    Index(IndexCommand),
    /// Cloud workers — scheduled or event-driven container executions on the
    /// Verglas cloud or a registered node. `list`/`get` read them;
    /// `create`/`update`/`delete` manage them from a spec file; `run` dispatches
    /// one now; `logs` tails its runtime output. Requires `verglas login`.
    #[command(subcommand)]
    Workers(WorkersCommand),
    /// Cloud containers — long-lived per-tenant container deployments on the
    /// control plane. `list`/`get` read them; `create`/`update`/`delete` manage
    /// them from a spec file; `scale`/`stop`/`resume` control their running
    /// instances. Requires `verglas login`.
    #[command(subcommand)]
    Containers(ContainersCommand),
    /// Cloud databases — the tenant's managed serverless Postgres databases on
    /// the control plane. `list` shows them; `create` provisions one and prints
    /// its one-time connection credentials; `delete` tears one down. Requires
    /// `verglas login`.
    #[command(subcommand)]
    Db(DbCommand),
    /// Cloud block volumes — the tenant's durable, cache-served block volumes on
    /// the control plane. `list`/`get` read them; `create` reserves one at a given
    /// size; `resize` grows one (grow only); `delete` removes one (refused while it
    /// is attached to a deployment). Requires `verglas login`.
    #[command(subcommand)]
    Volumes(VolumesCommand),
    /// Cloud secrets — the tenant's named worker secrets on the control plane. A
    /// cloud deployment references a secret by name (`@secret:NAME` in its
    /// config); at dispatch the value is sealed to the box that runs the worker,
    /// so only that box can open it. `list` shows the names (values are never
    /// returned); `set` stores a value; `delete` removes one. Requires
    /// `verglas login`.
    #[command(subcommand)]
    Secrets(SecretsCommand),
}

/// `verglas secrets` subcommands. Secrets are control-plane resources; every verb
/// calls the control plane and takes `--json`. A stored value is NEVER returned
/// by the control plane and never printed by the CLI.
#[derive(Debug, Subcommand)]
pub enum SecretsCommand {
    /// List the names of the tenant's secrets. Names only — the control plane
    /// never returns a stored value.
    List,
    /// Store a secret value under `NAME`. The value comes from `--value`, from
    /// `--file <path>`, or (the default) is read from stdin so it never lands in
    /// your shell history. An empty value is refused. A deployment references the
    /// secret by name as `@secret:NAME` in its config.
    Set(SecretSetArgs),
    /// Delete a secret by name.
    Delete(SecretNameArgs),
}

/// Arguments for `verglas secrets set`.
#[derive(Debug, Args)]
pub struct SecretSetArgs {
    /// The secret name (e.g. `EXAMPLE_API_KEY`), referenced from a deployment as
    /// `@secret:NAME`.
    pub name: String,
    /// The secret value on the command line. Convenient for scripts, but it lands
    /// in your shell history — prefer stdin (the default) or `--file`. Mutually
    /// exclusive with `--file`.
    #[arg(long, conflicts_with = "file")]
    pub value: Option<String>,
    /// Read the secret value from this file. Mutually exclusive with `--value`.
    #[arg(long, conflicts_with = "value")]
    pub file: Option<PathBuf>,
}

/// A secret referenced by its name.
#[derive(Debug, Args)]
pub struct SecretNameArgs {
    /// The secret name.
    pub name: String,
}

/// `verglas workers` subcommands. Workers are control-plane deployments; every
/// verb calls the control plane and takes `--json` for a machine-readable shape.
#[derive(Debug, Subcommand)]
pub enum WorkersCommand {
    /// List every worker (cloud and node deployments) the control plane knows for
    /// this tenant.
    List,
    /// Show one worker's full detail, including its code and config. Accepts the
    /// worker's control-plane id or its name.
    Get(WorkerRefArgs),
    /// Register a worker from a portable spec file (`--file`, JSON or TOML). The
    /// same file registers on the cloud (the default) or, with `--local`, on the
    /// local server. `--name`/`--schedule` override the matching spec fields.
    Create(WorkerCreateArgs),
    /// Update a worker from a spec file (`--file`) and/or the common overrides
    /// (`--schedule`, `--status`). Accepts the worker's id or name.
    Update(WorkerUpdateArgs),
    /// Delete a worker (undeploys a cloud worker). Accepts the worker's id or name.
    Delete(WorkerRefArgs),
    /// Dispatch a manual run of the worker now. Accepts the worker's id or name.
    Run(WorkerRefArgs),
    /// Tail a worker's runtime logs. Accepts the worker's id or name.
    Logs(WorkerRefArgs),
    /// Follow a local process or file and stream every captured line into a table
    /// as rows. Wraps a command after `--`, or tails `--file <path>`. Streams
    /// until Ctrl-C, then tears the worker down; `--keep` leaves it registered.
    /// When the server is logged in, the rows land in your cloud lakehouse.
    Follow(WorkerFollowArgs),
    /// Push a locally-registered worker (its spec and bundled files) to the cloud
    /// as a deployment. Secrets never ride along — a missing `@secret:` reference
    /// is reported so you can set it in the cloud. Accepts the worker's name.
    Push(WorkerPushArgs),
    /// Pull a cloud worker down to a local portable spec file (the reverse of
    /// push). Accepts the worker's id or name.
    Pull(WorkerPullArgs),
}

/// A worker referenced by its control-plane id or its name.
#[derive(Debug, Args)]
pub struct WorkerRefArgs {
    /// The worker's control-plane id, or its name (resolved via the worker list).
    pub worker: String,
}

/// Arguments for `verglas workers create`.
#[derive(Debug, Args)]
pub struct WorkerCreateArgs {
    /// The portable worker spec, a JSON (`.json`) or TOML (`.toml`) object.
    #[arg(long)]
    pub file: PathBuf,
    /// Register on the LOCAL server instead of the cloud. The same spec file works
    /// for both — develop and test locally, then push (or create) to the cloud.
    #[arg(long)]
    pub local: bool,
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

/// Arguments for `verglas workers push`.
#[derive(Debug, Args)]
pub struct WorkerPushArgs {
    /// The name of a locally-registered worker to push to the cloud.
    pub worker: String,
    /// Place the pushed worker on the bare-metal fleet instead of the default
    /// cloud runtime.
    #[arg(long)]
    pub fleet: bool,
}

/// Arguments for `verglas workers pull`.
#[derive(Debug, Args)]
pub struct WorkerPullArgs {
    /// The cloud worker's id, or its name.
    pub worker: String,
    /// Write the portable spec to this file (TOML) instead of printing it.
    #[arg(long)]
    pub file: Option<PathBuf>,
}

/// Arguments for `verglas workers update`.
#[derive(Debug, Args)]
pub struct WorkerUpdateArgs {
    /// The worker's control-plane id, or its name (resolved via the worker list).
    pub worker: String,
    /// A spec file (JSON or TOML) whose fields become the update body. Omit to
    /// send only the `--schedule`/`--status` overrides.
    #[arg(long)]
    pub file: Option<PathBuf>,
    /// Set the worker's `schedule`.
    #[arg(long)]
    pub schedule: Option<String>,
    /// Set the worker's `status` (e.g. `active`, `paused`).
    #[arg(long)]
    pub status: Option<String>,
}

/// `verglas containers` subcommands. Containers are control-plane resources;
/// every verb calls the control plane and takes `--json`.
#[derive(Debug, Subcommand)]
pub enum ContainersCommand {
    /// List every container the control plane knows for this tenant.
    List,
    /// Show one container's full detail. Accepts the container's id.
    Get(ContainerRefArgs),
    /// Create a container from a spec file (`--file`, JSON or TOML) carrying its
    /// config (`image`, `mode`, `min_instances`, `max_instances`, `schedule`,
    /// `resources`, `data`). `--name` overrides the spec's name.
    Create(ContainerCreateArgs),
    /// Update a container from a spec file (`--file`). Accepts the container's id.
    Update(ContainerUpdateArgs),
    /// Delete a container. Accepts the container's id.
    Delete(ContainerRefArgs),
    /// Scale a container to `--instances` running instances. Accepts its id.
    Scale(ContainerScaleArgs),
    /// Stop a container (scale to zero). Accepts its id.
    Stop(ContainerRefArgs),
    /// Resume a stopped container. Accepts its id.
    Resume(ContainerRefArgs),
    /// List the curated catalog apps you can deploy (id, description, whether each
    /// serves a web UI, speaks MCP, and is deployed to every tenant by default).
    Catalog,
    /// Deploy a curated catalog app as one of this tenant's containers. Idempotent:
    /// an app already deployed is reported and left untouched. Prints the container
    /// id, the UI hostname when the app has one, and the MCP endpoint when declared.
    Deploy(ContainerDeployArgs),
    /// Show or set a curated container's configuration. With no flags it shows the
    /// config schema and current mode/values (secrets are shown only as set/unset).
    /// With `--set`/`--mode` it writes the config and relaunches the container.
    Config(ContainerConfigArgs),
    /// Push a container image into your tenant registry so the cloud can run it —
    /// the same portability story as workers, over the bring-your-own-image path.
    /// Give the image reference the cloud pulls (`docker://…`); the fleet converts
    /// it to a bootable rootfs. Local container execution is out of scope.
    Push(ContainerPushArgs),
}

/// Arguments for `verglas containers push`.
#[derive(Debug, Args)]
pub struct ContainerPushArgs {
    /// The image reference to push, e.g. `docker://ghcr.io/acme/app:1.2`. The
    /// cloud pulls and converts it; a purely-local image must first be pushed to a
    /// registry the cloud can reach.
    pub image: String,
    /// The name to register the image under in your tenant registry. Defaults to
    /// the image's repository name.
    #[arg(long)]
    pub name: Option<String>,
    /// The tag to register. Defaults to the image reference's tag, or `latest`.
    #[arg(long)]
    pub tag: Option<String>,
}

/// Arguments for `verglas containers deploy`.
#[derive(Debug, Args)]
pub struct ContainerDeployArgs {
    /// The catalog app id to deploy (see `verglas containers catalog`).
    pub catalog_id: String,
}

/// Arguments for `verglas containers config`.
#[derive(Debug, Args)]
pub struct ContainerConfigArgs {
    /// The container's control-plane id.
    pub container: String,
    /// Set a config field as `KEY=VALUE`, repeatable. Use `KEY=-` to read the
    /// value from stdin (one line per `-`, in the order given) — the right way to
    /// pass a secret, keeping it out of your shell history.
    #[arg(long = "set", value_name = "KEY=VALUE")]
    pub set: Vec<String>,
    /// The config mode to select (e.g. `default` or `custom`). When omitted while
    /// setting fields, the container's current mode is kept.
    #[arg(long)]
    pub mode: Option<String>,
}

/// A container referenced by its control-plane id.
#[derive(Debug, Args)]
pub struct ContainerRefArgs {
    /// The container's control-plane id.
    pub container: String,
}

/// Arguments for `verglas containers create`.
#[derive(Debug, Args)]
pub struct ContainerCreateArgs {
    /// The container spec, a JSON (`.json`) or TOML (`.toml`) object carrying the
    /// full container config. This is the whole request body.
    #[arg(long)]
    pub file: PathBuf,
    /// Override the spec's `name`.
    #[arg(long)]
    pub name: Option<String>,
}

/// Arguments for `verglas containers update`.
#[derive(Debug, Args)]
pub struct ContainerUpdateArgs {
    /// The container's control-plane id.
    pub container: String,
    /// A spec file (JSON or TOML) whose fields become the update body.
    #[arg(long)]
    pub file: PathBuf,
}

/// Arguments for `verglas containers scale`.
#[derive(Debug, Args)]
pub struct ContainerScaleArgs {
    /// The container's control-plane id.
    pub container: String,
    /// The number of running instances to scale to.
    #[arg(long)]
    pub instances: u32,
}

/// `verglas db` subcommands. Databases are control-plane resources; every verb
/// calls the control plane and takes `--json`.
#[derive(Debug, Subcommand)]
pub enum DbCommand {
    /// List every managed database the control plane knows for this tenant.
    List,
    /// Provision a database and print its one-time connection credentials. The
    /// password is shown once and never stored by the CLI.
    Create(DbCreateArgs),
    /// Delete a database by name.
    Delete(DbNameArgs),
}

/// A database referenced by its name.
#[derive(Debug, Args)]
pub struct DbNameArgs {
    /// The database name.
    pub name: String,
}

/// Arguments for `verglas db create`.
#[derive(Debug, Args)]
pub struct DbCreateArgs {
    /// The database name.
    pub name: String,
    /// The database engine: `postgres` (default), `mysql`, or `clickhouse`. Every
    /// database is its own serverless deployment (own VM, own storage, scales to
    /// zero); the control plane validates the value and returns the engine's
    /// connection endpoint.
    #[arg(long = "type", default_value = "postgres")]
    pub db_type: String,
}

/// `verglas volumes` subcommands. Volumes are control-plane resources; every verb
/// calls the control plane and takes `--json`.
#[derive(Debug, Subcommand)]
pub enum VolumesCommand {
    /// List every block volume the control plane knows for this tenant.
    List,
    /// Show one volume's detail: size, state, attachment, and its durable id.
    Get(VolumeNameArgs),
    /// Reserve a block volume of the given size. Its durable disk is created on
    /// first attach. `--size` accepts a byte count or a suffixed size (e.g. `10GiB`,
    /// `500MB`).
    Create(VolumeCreateArgs),
    /// Grow a volume to a larger size (grow only — a shrink is refused). `--size`
    /// accepts a byte count or a suffixed size (e.g. `20GiB`).
    Resize(VolumeResizeArgs),
    /// Delete a volume by name. Refused while the volume is attached to a deployment.
    Delete(VolumeNameArgs),
}

/// A volume referenced by its name.
#[derive(Debug, Args)]
pub struct VolumeNameArgs {
    /// The volume name.
    pub name: String,
}

/// Arguments for `verglas volumes create`.
#[derive(Debug, Args)]
pub struct VolumeCreateArgs {
    /// The volume name.
    pub name: String,
    /// The volume size: a byte count (e.g. `10737418240`) or a suffixed size
    /// (`10GiB`, `500MB`, `2T`). Binary suffixes (KiB/MiB/GiB/TiB) are powers of
    /// 1024; decimal (KB/MB/GB/TB) are powers of 1000.
    #[arg(long)]
    pub size: String,
}

/// Arguments for `verglas volumes resize`.
#[derive(Debug, Args)]
pub struct VolumeResizeArgs {
    /// The volume name.
    pub name: String,
    /// The new, larger size (grow only). Same format as `create --size`.
    #[arg(long)]
    pub size: String,
}

/// `verglas index` subcommands for table-scoped vector (ANN) indexes.
#[derive(Debug, Subcommand)]
pub enum IndexCommand {
    /// List vector indexes attached to a table's current snapshot.
    ///
    /// JSON (`--json`):
    /// {"indexes":[{"target","field","metric","reflectedSnapshot","liveCount"}]}.
    List(IndexListArgs),
    /// Declare a vector (ANN) index on a table's embedding field and run the
    /// initial build.
    ///
    /// JSON (`--json`): {"target","field","metric","reflectedSnapshot","fullBuild",
    /// "inserts","deletes","consolidated","liveCount","tombstones","blobLocation",
    /// "blobBytes"}.
    Add(IndexAddArgs),
    /// Search an indexed field for the nearest neighbors of a query vector.
    /// The table's current snapshot must have an attached index.
    ///
    /// JSON (`--json`): {"source","neighbors":[{"id","distance"}]}.
    Search(IndexSearchArgs),
}

/// Arguments for `verglas index list`.
#[derive(Debug, Args)]
pub struct IndexListArgs {
    /// The table (`namespace.name`) whose current-snapshot indexes are listed.
    pub table: String,
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

/// Arguments for `verglas index add`.
#[derive(Debug, Args)]
pub struct IndexAddArgs {
    /// The table, as `namespace.name`.
    pub table: String,
    /// The embedding column to index.
    #[arg(long)]
    pub field: String,
    /// The distance metric: `cosine` (default) or `l2`.
    #[arg(long, default_value = "cosine")]
    pub metric: String,
    /// The identity column keying vectors (default `id`).
    #[arg(long)]
    pub id_field: Option<String>,
    /// Vamana max out-degree R (default 64).
    #[arg(long)]
    pub r: Option<usize>,
    /// Vamana build/insert candidate-list size L (default 100).
    #[arg(long = "build-list")]
    pub l: Option<usize>,
    /// Vamana pruning parameter alpha, >= 1 (default 1.2).
    #[arg(long)]
    pub alpha: Option<f32>,
}

/// Arguments for `verglas index search`.
#[derive(Debug, Args)]
pub struct IndexSearchArgs {
    /// The table, as `namespace.name`.
    pub table: String,
    /// The indexed embedding field.
    pub field: String,
    /// The query vector, as comma-separated floats (e.g. `0.1,0.2,0.3`).
    #[arg(long)]
    pub vector: String,
    /// The number of neighbors to return.
    #[arg(long, short = 'k', default_value_t = 10)]
    pub k: usize,
    /// The search candidate-list size L (defaults to max(k, R)).
    #[arg(long = "search-list")]
    pub l: Option<usize>,
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

/// `verglas graph` subcommands. A graph lives in one namespace; its data is two
/// plain Iceberg tables (`<namespace>.nodes` and `<namespace>.edges`) plus a
/// snapshot-bound adjacency index. Every verb calls the server; every verb takes
/// `--json` for a stable machine-readable shape.
#[derive(Debug, Subcommand)]
pub enum GraphCommand {
    /// Create a graph: ensure its nodes and edges tables exist. Idempotent.
    ///
    /// JSON (`--json`): {"namespace","nodesTable","edgesTable"}.
    Create(GraphNameArgs),
    /// Insert a batch of nodes from a JSON file (or stdin when the path is
    /// omitted or `-`). The input is a JSON array of node objects, each
    /// `{"id", "labels"?, "properties"?, "agentId"?, "namespace"?}`.
    ///
    /// JSON (`--json`): {"snapshotId","count"}.
    AddNode(GraphInsertArgs),
    /// Insert a batch of edges from a JSON file (or stdin when the path is
    /// omitted or `-`). The input is a JSON array of edge objects, each
    /// `{"srcId","predicate","dstId","provenance", "confidence"?, "edgeId"?,
    /// "supersedes"?, "validFrom"?, "properties"?}`.
    ///
    /// JSON (`--json`): {"snapshotId","count"}.
    AddEdge(GraphInsertArgs),
    /// Show a node's direct neighbors.
    ///
    /// JSON (`--json`): {"op":"neighbors","backend","snapshotId","neighbors":[...]}.
    Neighbors(GraphNeighborsArgs),
    /// Expand K hops from a node, returning every node reached with its hop
    /// distance and path confidence.
    ///
    /// JSON (`--json`): {"op":"kHop","backend","snapshotId","reached":[...]}.
    KHop(GraphKHopArgs),
    /// Find the shortest path between two nodes within a hop bound.
    ///
    /// JSON (`--json`): {"op":"paths","backend","snapshotId","paths":[...]}.
    Paths(GraphPathsArgs),
    /// Build or refresh the adjacency index for the graph.
    ///
    /// JSON (`--json`): {"built","snapshotId","nodeCount","edgeCount",
    /// "blobPath","blobBytes","mode"}.
    Index(GraphNameArgs),
    /// Show the graph's backing tables, live counts, and whether an index is
    /// bound to the current edge snapshot.
    ///
    /// JSON (`--json`): {"namespace","nodesTable","edgesTable","nodeCount",
    /// "edgeCount","indexed","snapshotId"}.
    Show(GraphNameArgs),
}

/// Arguments naming just a graph namespace (`create`, `index`, `show`).
#[derive(Debug, Args)]
pub struct GraphNameArgs {
    /// The graph namespace.
    pub namespace: String,
}

/// Arguments for `verglas graph add-node` / `add-edge`: the namespace and an
/// optional JSON input path (stdin when omitted or `-`).
#[derive(Debug, Args)]
pub struct GraphInsertArgs {
    /// The graph namespace.
    pub namespace: String,
    /// A JSON file holding an array of node/edge objects. Omit (or pass `-`) to
    /// read the array from stdin, mirroring how `table append` takes rows.
    pub input: Option<PathBuf>,
}

/// Shared traversal filter flags (`--predicate`, `--min-confidence`,
/// `--direction`).
#[derive(Debug, Args)]
pub struct GraphTraversalOpts {
    /// Only follow edges with this predicate.
    #[arg(long)]
    pub predicate: Option<String>,
    /// Only follow edges whose confidence is at least this.
    #[arg(long)]
    pub min_confidence: Option<f64>,
    /// The direction to follow edges: `out` (default), `in`, or `both`.
    #[arg(long, default_value = "out")]
    pub direction: String,
}

/// Arguments for `verglas graph neighbors`.
#[derive(Debug, Args)]
pub struct GraphNeighborsArgs {
    /// The graph namespace.
    pub namespace: String,
    /// The node to read neighbors of.
    pub node: String,
    #[command(flatten)]
    pub opts: GraphTraversalOpts,
}

/// Arguments for `verglas graph k-hop`.
#[derive(Debug, Args)]
pub struct GraphKHopArgs {
    /// The graph namespace.
    pub namespace: String,
    /// The node to expand from.
    pub node: String,
    /// The number of hops to expand.
    #[arg(long)]
    pub hops: u32,
    #[command(flatten)]
    pub opts: GraphTraversalOpts,
}

/// Arguments for `verglas graph paths`.
#[derive(Debug, Args)]
pub struct GraphPathsArgs {
    /// The graph namespace.
    pub namespace: String,
    /// The source node.
    pub src: String,
    /// The destination node.
    pub dst: String,
    /// The maximum number of hops to search.
    #[arg(long)]
    pub max_hops: u32,
    #[command(flatten)]
    pub opts: GraphTraversalOpts,
}

/// Arguments for `verglas query`.
///
/// JSON (`--json`): {"columns":[...],"rows":[{col:value,...}],"row_count"}.
#[derive(Debug, Args)]
pub struct QueryArgs {
    /// The SQL to run. Tables are referenced as `namespace.name`.
    pub sql: String,
    /// Time travel: pin a table to a snapshot for this query, as
    /// `--at <snapshot-id|timestamp> <namespace.table>`.
    #[arg(long, num_args = 2, value_names = ["REF", "TABLE"])]
    pub at: Option<Vec<String>>,
}

/// Arguments for `verglas login`.
///
/// With no mode flag, `verglas login` runs the browser flow: it opens your
/// browser to authorize and completes over a loopback redirect. `--device` runs
/// the headless device-code flow (authorize on any device from a printed code).
/// `--api-key` takes a long-lived key (positional or piped on stdin) for CI and
/// automation.
#[derive(Debug, Args)]
pub struct LoginArgs {
    /// Override the control plane URL. Default is Verglas Cloud
    /// (`https://api.verglas.dev`); omit for a normal login.
    #[arg(long)]
    pub url: Option<String>,

    /// Run the OAuth device-code flow: print a short code and a URL to open on any
    /// device, then wait for you to authorize. Suits headless machines.
    #[arg(long, conflicts_with = "api_key_mode")]
    pub device: bool,

    /// Authenticate with a long-lived API key instead of a browser flow — the
    /// automation path. The key is the positional argument or read from stdin.
    #[arg(long = "api-key")]
    pub api_key_mode: bool,

    /// The API key, used only with `--api-key`. Omit to read it from stdin, so it
    /// is not recorded in your shell history.
    pub api_key: Option<String>,
}

/// Arguments for `verglas drain` (issue, local-only since): drain the
/// LOCAL server. The CLI takes no target — it POSTs `/admin/drain` on this
/// machine's admin endpoint (the loopback default, `VERGLAS_ENDPOINT` /
/// `--server-endpoint` override), never resolving or addressing other nodes.
#[derive(Debug, Args)]
pub struct DrainArgs {
    /// Maximum time to keep serving as a donor before exiting, e.g. `10m`,
    /// `30s`, `1h`, or a plain seconds count. Omit to take the server's
    /// configured drain timeout.
    #[arg(long)]
    pub timeout: Option<String>,
}
