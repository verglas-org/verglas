//! `verglas graph` — create and traverse property graphs through the daemon.
//!
//! A graph is a namespace holding two plain Iceberg tables (`<ns>.nodes` and
//! `<ns>.edges`) plus a snapshot-bound adjacency index; the verbs here parallel
//! `table`. Every verb calls the local daemon's HTTP API (`/v1/graphs/...`) —
//! the daemon is the only local write authority, so the CLI embeds no engine and
//! has no daemon-less path. `add-node`/`add-edge` read a JSON array of
//! node/edge objects from a file or stdin, mirroring how `table append` takes
//! rows. Results render as `--output json` (the stable shape) or a human
//! summary; the reader falls back to a table scan when no index is built, so a
//! query answers the same with or without one.

use std::error::Error;
use std::io::Read;

use verglas_sdk::graph::{
    BuildIndexRequest, EdgeInput, GraphBackend, GraphCreateReport, GraphDirection, GraphFilter,
    GraphOp, GraphQueryRequest, GraphQueryResponse, GraphShowReport, IndexReport,
    InsertEdgesRequest, InsertNodesRequest, InsertReport, NodeInput,
};

use crate::cli::{
    GraphCommand, GraphInsertArgs, GraphKHopArgs, GraphNameArgs, GraphNeighborsArgs,
    GraphPathsArgs, GraphTraversalOpts,
};
use verglas_sdk::daemon::DaemonClient;

/// Dispatches a `verglas graph` subcommand.
pub async fn run(
    command: GraphCommand,
    daemon_endpoint: &str,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let client = crate::backend::daemon(daemon_endpoint)?;
    match command {
        GraphCommand::Create(args) => create(&client, args, json).await,
        GraphCommand::AddNode(args) => add_node(&client, args, json).await,
        GraphCommand::AddEdge(args) => add_edge(&client, args, json).await,
        GraphCommand::Neighbors(args) => neighbors(&client, args, json).await,
        GraphCommand::KHop(args) => k_hop(&client, args, json).await,
        GraphCommand::Paths(args) => paths(&client, args, json).await,
        GraphCommand::Index(args) => index(&client, args, json).await,
        GraphCommand::Show(args) => show(&client, args, json).await,
    }
}

/// `graph create <ns>`.
async fn create(
    client: &DaemonClient,
    args: GraphNameArgs,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let report: GraphCreateReport = client
        .post_json(
            &format!("/v1/graphs/{}", args.namespace),
            &serde_json::json!({}),
        )
        .await?;
    emit(&report, json, render_create)
}

/// `graph add-node <ns> [FILE]`.
async fn add_node(
    client: &DaemonClient,
    args: GraphInsertArgs,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let nodes: Vec<NodeInput> = read_json_array(args.input.as_deref())?;
    let body = serde_json::to_value(InsertNodesRequest { nodes })?;
    let report: InsertReport = client
        .post_json(&format!("/v1/graphs/{}/nodes", args.namespace), &body)
        .await?;
    emit(&report, json, render_insert)
}

/// `graph add-edge <ns> [FILE]`.
async fn add_edge(
    client: &DaemonClient,
    args: GraphInsertArgs,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let edges: Vec<EdgeInput> = read_json_array(args.input.as_deref())?;
    let body = serde_json::to_value(InsertEdgesRequest { edges })?;
    let report: InsertReport = client
        .post_json(&format!("/v1/graphs/{}/edges", args.namespace), &body)
        .await?;
    emit(&report, json, render_insert)
}

/// `graph neighbors <ns> <node>`.
async fn neighbors(
    client: &DaemonClient,
    args: GraphNeighborsArgs,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let request = GraphQueryRequest {
        op: GraphOp::Neighbors,
        start: args.node,
        dst: None,
        direction: parse_direction(&args.opts.direction)?,
        k: None,
        max_hops: None,
        filter: filter_of(&args.opts),
        as_of: None,
    };
    let report = query(client, &args.namespace, &request).await?;
    emit(&report, json, render_neighbors)
}

/// `graph k-hop <ns> <node> --hops N`.
async fn k_hop(
    client: &DaemonClient,
    args: GraphKHopArgs,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let request = GraphQueryRequest {
        op: GraphOp::KHop,
        start: args.node,
        dst: None,
        direction: parse_direction(&args.opts.direction)?,
        k: Some(args.hops),
        max_hops: None,
        filter: filter_of(&args.opts),
        as_of: None,
    };
    let report = query(client, &args.namespace, &request).await?;
    emit(&report, json, render_khop)
}

/// `graph paths <ns> <src> <dst> --max-hops N`.
async fn paths(
    client: &DaemonClient,
    args: GraphPathsArgs,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let request = GraphQueryRequest {
        op: GraphOp::Paths,
        start: args.src,
        dst: Some(args.dst),
        direction: parse_direction(&args.opts.direction)?,
        k: None,
        max_hops: Some(args.max_hops),
        filter: filter_of(&args.opts),
        as_of: None,
    };
    let report = query(client, &args.namespace, &request).await?;
    emit(&report, json, render_paths)
}

/// `graph index <ns>`.
async fn index(
    client: &DaemonClient,
    args: GraphNameArgs,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let body = serde_json::to_value(BuildIndexRequest { at: None })?;
    let report: IndexReport = client
        .post_json(&format!("/v1/graphs/{}/index", args.namespace), &body)
        .await?;
    emit(&report, json, render_index)
}

/// `graph show <ns>`.
async fn show(
    client: &DaemonClient,
    args: GraphNameArgs,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let report: GraphShowReport = client
        .get(&format!("/v1/graphs/{}", args.namespace))
        .await?;
    emit(&report, json, render_show)
}

/// Posts a traversal request to the graph query route.
async fn query(
    client: &DaemonClient,
    namespace: &str,
    request: &GraphQueryRequest,
) -> Result<GraphQueryResponse, Box<dyn Error>> {
    let body = serde_json::to_value(request)?;
    let report: GraphQueryResponse = client
        .post_json(&format!("/v1/graphs/{namespace}/query"), &body)
        .await?;
    Ok(report)
}

/// Builds the traversal filter from the shared traversal flags.
fn filter_of(opts: &GraphTraversalOpts) -> GraphFilter {
    GraphFilter {
        predicate: opts.predicate.clone(),
        min_confidence: opts.min_confidence,
    }
}

/// Parses a `--direction` value into the wire direction, rejecting anything but
/// `out`, `in`, or `both`.
fn parse_direction(value: &str) -> Result<GraphDirection, Box<dyn Error>> {
    match value {
        "out" => Ok(GraphDirection::Out),
        "in" => Ok(GraphDirection::In),
        "both" => Ok(GraphDirection::Both),
        other => Err(format!("invalid --direction `{other}`: expected out, in, or both").into()),
    }
}

/// Reads a JSON array of `T` from a file, or from stdin when the path is absent
/// or `-`. The batch-insert input, mirroring how `table append` takes rows.
fn read_json_array<T: serde::de::DeserializeOwned>(
    path: Option<&std::path::Path>,
) -> Result<Vec<T>, Box<dyn Error>> {
    let raw = match path {
        Some(p) if p != std::path::Path::new("-") => std::fs::read_to_string(p)
            .map_err(|e| format!("failed to read `{}`: {e}", p.display()))?,
        _ => {
            let mut buf = String::new();
            std::io::stdin()
                .read_to_string(&mut buf)
                .map_err(|e| format!("failed to read stdin: {e}"))?;
            buf
        }
    };
    let items: Vec<T> = serde_json::from_str(&raw)
        .map_err(|e| format!("input is not a JSON array of the expected objects: {e}"))?;
    Ok(items)
}

/// Serializes `report` as pretty JSON when `json` is set, else calls `render`.
fn emit<T, F>(report: &T, json: bool, render: F) -> Result<(), Box<dyn Error>>
where
    T: serde::Serialize,
    F: FnOnce(&T),
{
    if json {
        println!("{}", serde_json::to_string_pretty(report)?);
    } else {
        render(report);
    }
    Ok(())
}

/// Renders an optional snapshot id, or `-` when absent.
fn snapshot_label(snapshot_id: Option<i64>) -> String {
    snapshot_id
        .map(|s| s.to_string())
        .unwrap_or_else(|| "-".to_owned())
}

/// The human label for which path served a query.
fn backend_label(backend: GraphBackend) -> &'static str {
    match backend {
        GraphBackend::Index => "index",
        GraphBackend::Scan => "scan",
    }
}

/// Human summary for `create`.
fn render_create(report: &GraphCreateReport) {
    println!(
        "created graph {} (tables {}, {})",
        report.namespace, report.nodes_table, report.edges_table
    );
}

/// Human summary for `add-node` / `add-edge`.
fn render_insert(report: &InsertReport) {
    println!(
        "inserted {} row(s); snapshot {}",
        report.count,
        snapshot_label(report.snapshot_id)
    );
}

/// Human summary for `neighbors`.
fn render_neighbors(report: &GraphQueryResponse) {
    let neighbors = report.neighbors.as_deref().unwrap_or(&[]);
    println!(
        "{} neighbor(s) [{}]",
        neighbors.len(),
        backend_label(report.backend)
    );
    for neighbor in neighbors {
        println!(
            "  --{}--> {}  (conf {:.3}, {})",
            neighbor.predicate, neighbor.node_id, neighbor.confidence, neighbor.provenance
        );
    }
}

/// Human summary for `k-hop`.
fn render_khop(report: &GraphQueryResponse) {
    let reached = report.reached.as_deref().unwrap_or(&[]);
    println!(
        "{} node(s) reached [{}]",
        reached.len(),
        backend_label(report.backend)
    );
    for node in reached {
        println!(
            "  {} (hops {}, conf {:.3})",
            node.node_id, node.hops, node.path_confidence
        );
    }
}

/// Human summary for `paths`.
fn render_paths(report: &GraphQueryResponse) {
    let paths = report.paths.as_deref().unwrap_or(&[]);
    if paths.is_empty() {
        println!("no path [{}]", backend_label(report.backend));
        return;
    }
    for path in paths {
        println!(
            "  {}  (conf {:.3}) [{}]",
            path.nodes.join(" -> "),
            path.confidence,
            backend_label(report.backend)
        );
    }
}

/// Human summary for `index`.
fn render_index(report: &IndexReport) {
    if !report.built {
        println!("no edges to index (graph is empty)");
        return;
    }
    println!(
        "index built: {} node(s), {} edge(s); snapshot {}; mode {}",
        report.node_count,
        report.edge_count,
        snapshot_label(report.snapshot_id),
        report.mode.as_deref().unwrap_or("-")
    );
}

/// Human summary for `show`.
fn render_show(report: &GraphShowReport) {
    println!("{}", report.namespace);
    println!("  nodes table: {}", report.nodes_table);
    println!("  edges table: {}", report.edges_table);
    let n = |v: Option<u64>| v.map(|x| x.to_string()).unwrap_or_else(|| "-".to_owned());
    println!(
        "  nodes: {}   edges: {}",
        n(report.node_count),
        n(report.edge_count)
    );
    println!(
        "  index bound: {}   edge snapshot: {}",
        if report.indexed { "yes" } else { "no" },
        snapshot_label(report.snapshot_id)
    );
}
