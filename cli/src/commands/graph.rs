//! `verglas graph` — property-graph node/edge ingestion and traversal over
//! the S3 semantic listener (issue #144). Every verb wraps the pre-signed
//! `verglas_sdk::semantic::VerglasGraphsClient`; this module builds typed DTOs
//! from parsed CLI/stdin JSON and renders the response. It signs nothing.

use std::error::Error;

use verglas_sdk::{
    AddEdgesInput, AddNodesInput, BuildIndexInput, CreateGraphInput, DeleteGraphInput,
    DescribeGraphInput, Edge, ListGraphsInput, Node, RetrieveNeighborsInput, SearchKHopInput,
    SearchPathsInput, SearchPrecedentsInput,
};

use crate::cli::{
    GraphCommand, GraphInputArgs, GraphKHopArgs, GraphNameArgs, GraphNeighborsArgs, GraphPathsArgs,
    GraphPrecedentsArgs,
};
use crate::commands::semantic::{graph_client, read_json_input};

/// Dispatches a `verglas graph` subcommand against the S3 semantic listener.
pub async fn run(
    command: GraphCommand,
    s3_endpoint: &str,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let client = graph_client(s3_endpoint)?;
    match command {
        GraphCommand::Create(args) => create(&client, args, json).await,
        GraphCommand::AddNode(args) => add_node(&client, args, json).await,
        GraphCommand::AddEdge(args) => add_edge(&client, args, json).await,
        GraphCommand::Neighbors(args) => neighbors(&client, args, json).await,
        GraphCommand::Index(args) => index(&client, args, json).await,
        GraphCommand::Show(args) => show(&client, args, json).await,
        GraphCommand::Delete(args) => delete(&client, args, json).await,
        GraphCommand::List => list(&client, json).await,
        GraphCommand::KHop(args) => k_hop(&client, args, json).await,
        GraphCommand::Paths(args) => paths(&client, args, json).await,
        GraphCommand::Precedents(args) => precedents(&client, args, json).await,
    }
}

/// Creates an empty graph.
async fn create(
    client: &verglas_sdk::VerglasGraphsClient,
    args: GraphNameArgs,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let output = client
        .create_graph(CreateGraphInput {
            graph_name: args.name.clone(),
        })
        .await?;
    emit(json, &output, |_| println!("created graph {}", args.name))
}

/// Reads a JSON array of nodes (file or stdin) and puts them into the graph.
async fn add_node(
    client: &verglas_sdk::VerglasGraphsClient,
    args: GraphInputArgs,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let nodes: Vec<Node> = serde_json::from_str(&read_json_input(&args.input)?)?;
    let count = nodes.len();
    let output = client
        .add_nodes(AddNodesInput {
            graph_name: args.name.clone(),
            nodes,
        })
        .await?;
    emit(json, &output, |_| {
        println!("added {count} node(s) to {}", args.name)
    })
}

/// Reads a JSON array of edges (file or stdin) and puts them into the graph.
async fn add_edge(
    client: &verglas_sdk::VerglasGraphsClient,
    args: GraphInputArgs,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let edges: Vec<Edge> = serde_json::from_str(&read_json_input(&args.input)?)?;
    let count = edges.len();
    let output = client
        .add_edges(AddEdgesInput {
            graph_name: args.name.clone(),
            edges,
        })
        .await?;
    emit(json, &output, |_| {
        println!("added {count} edge(s) to {}", args.name)
    })
}

/// Lists one node's immediate neighbors.
async fn neighbors(
    client: &verglas_sdk::VerglasGraphsClient,
    args: GraphNeighborsArgs,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let output = client
        .retrieve_neighbors(RetrieveNeighborsInput {
            direction: None,
            filter: None,
            graph_name: args.name.clone(),
            node_id: args.node.clone(),
        })
        .await?;
    emit(json, &output, |output| {
        if output.neighbors.is_empty() {
            println!("(no neighbors)");
        } else {
            for neighbor in &output.neighbors {
                println!(
                    "  {} -{}-> {} (confidence {})",
                    args.node, neighbor.predicate, neighbor.node_id, neighbor.confidence
                );
            }
        }
    })
}

/// Builds the graph's traversal index.
async fn index(
    client: &verglas_sdk::VerglasGraphsClient,
    args: GraphNameArgs,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let output = client
        .build_index(BuildIndexInput {
            graph_name: args.name.clone(),
        })
        .await?;
    emit(json, &output, |output| {
        println!("index built for {}: {}", args.name, output.index)
    })
}

/// Shows one graph's metadata.
async fn show(
    client: &verglas_sdk::VerglasGraphsClient,
    args: GraphNameArgs,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let output = client
        .describe_graph(DescribeGraphInput {
            graph_name: args.name.clone(),
        })
        .await?;
    emit(json, &output, |output| {
        println!(
            "{}  edges_snapshot_id={}",
            output.graph_name,
            output
                .edges_snapshot_id
                .map(|id| id.to_string())
                .unwrap_or_else(|| "-".to_owned())
        )
    })
}

/// Deletes a graph.
async fn delete(
    client: &verglas_sdk::VerglasGraphsClient,
    args: GraphNameArgs,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let output = client
        .delete_graph(DeleteGraphInput {
            graph_name: args.name.clone(),
        })
        .await?;
    emit(json, &output, |_| println!("deleted graph {}", args.name))
}

/// Lists every graph.
async fn list(client: &verglas_sdk::VerglasGraphsClient, json: bool) -> Result<(), Box<dyn Error>> {
    let output = client
        .list_graphs(ListGraphsInput {
            max_results: None,
            next_token: None,
        })
        .await?;
    emit(json, &output, |output| {
        if output.graphs.is_empty() {
            println!("(no graphs)");
        } else {
            for graph in &output.graphs {
                println!("{}", graph.graph_name);
            }
        }
    })
}

/// Traverses up to `--hops` edges out from one node.
async fn k_hop(
    client: &verglas_sdk::VerglasGraphsClient,
    args: GraphKHopArgs,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let output = client
        .search_k_hop(SearchKHopInput {
            direction: None,
            filter: None,
            graph_name: args.name.clone(),
            k: args.hops,
            node_id: args.node.clone(),
        })
        .await?;
    emit(json, &output, |output| {
        if output.nodes.is_empty() {
            println!("(no reachable nodes)");
        } else {
            for node in &output.nodes {
                println!(
                    "  {} (hops={}, confidence={})",
                    node.node_id, node.hops, node.path_confidence
                );
            }
        }
    })
}

/// Finds paths between two nodes within `--max-hops`.
async fn paths(
    client: &verglas_sdk::VerglasGraphsClient,
    args: GraphPathsArgs,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let output = client
        .search_paths(SearchPathsInput {
            direction: None,
            filter: None,
            graph_name: args.name.clone(),
            max_hops: args.max_hops,
            source_id: args.source.clone(),
            target_id: args.target.clone(),
        })
        .await?;
    emit(json, &output, |output| {
        if output.paths.is_empty() {
            println!("(no paths)");
        } else {
            for path in &output.paths {
                println!(
                    "  {} (confidence={})",
                    path.nodes.join(" -> "),
                    path.confidence
                );
            }
        }
    })
}

/// Ranks Decision nodes by lexical match to `--query`, boosted by any
/// `--entity` overlap with a decision's direct neighbors.
async fn precedents(
    client: &verglas_sdk::VerglasGraphsClient,
    args: GraphPrecedentsArgs,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let output = client
        .search_precedents(SearchPrecedentsInput {
            graph_name: args.name.clone(),
            query: args.query.clone(),
            limit: args.limit,
            entities: (!args.entities.is_empty()).then_some(args.entities.clone()),
        })
        .await?;
    emit(json, &output, |output| {
        if output.precedents.is_empty() {
            println!("(no precedents)");
        } else {
            for precedent in &output.precedents {
                println!(
                    "  {} score={} (lexical={}, structural={}) shared={:?} :: {}",
                    precedent.decision_id,
                    precedent.score,
                    precedent.lexical_score,
                    precedent.structural_score,
                    precedent.shared_entities,
                    precedent.objective
                );
            }
        }
    })
}

/// Serializes `output` as pretty JSON when `json` is set, else calls `render`.
fn emit<T, F>(json: bool, output: &T, render: F) -> Result<(), Box<dyn Error>>
where
    T: serde::Serialize,
    F: FnOnce(&T),
{
    if json {
        println!("{}", serde_json::to_string_pretty(output)?);
    } else {
        render(output);
    }
    Ok(())
}
