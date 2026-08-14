//! Contract tests for the native S3 Vectors and Graph REST-JSON SDK clients.

use serde_json::Value;
use verglas_sdk::{S3VectorsClient, SigV4Credentials, VerglasGraphsClient};

/// Keeps every checked-in S3 Vectors operation in the public Rust client surface.
#[allow(clippy::too_many_lines)]
#[test]
fn s3_vectors_client_exposes_every_modelled_operation() {
    let _ = S3VectorsClient::create_index;
    let _ = S3VectorsClient::create_vector_bucket;
    let _ = S3VectorsClient::delete_index;
    let _ = S3VectorsClient::delete_vector_bucket;
    let _ = S3VectorsClient::delete_vector_bucket_policy;
    let _ = S3VectorsClient::delete_vectors;
    let _ = S3VectorsClient::get_index;
    let _ = S3VectorsClient::get_vector_bucket;
    let _ = S3VectorsClient::get_vector_bucket_policy;
    let _ = S3VectorsClient::get_vectors;
    let _ = S3VectorsClient::list_indexes;
    let _ = S3VectorsClient::list_tags_for_resource;
    let _ = S3VectorsClient::list_vector_buckets;
    let _ = S3VectorsClient::list_vectors;
    let _ = S3VectorsClient::put_vector_bucket_policy;
    let _ = S3VectorsClient::put_vectors;
    let _ = S3VectorsClient::query_vectors;
    let _ = S3VectorsClient::tag_resource;
    let _ = S3VectorsClient::untag_resource;
    let _: Value = serde_json::json!({});
}

/// Keeps every checked-in Graph operation in the public Rust client surface.
#[test]
fn graph_client_exposes_every_modelled_operation() {
    let _ = VerglasGraphsClient::build_graph_index;
    let _ = VerglasGraphsClient::create_graph;
    let _ = VerglasGraphsClient::delete_graph;
    let _ = VerglasGraphsClient::get_graph;
    let _ = VerglasGraphsClient::get_neighbors;
    let _ = VerglasGraphsClient::list_graphs;
    let _ = VerglasGraphsClient::put_edges;
    let _ = VerglasGraphsClient::put_nodes;
    let _ = VerglasGraphsClient::query_k_hop;
    let _ = VerglasGraphsClient::query_neighborhood;
    let _ = VerglasGraphsClient::query_paths;
}

/// The clients use the services the cache listener verifies, not bearer routes.
#[test]
fn semantic_clients_select_the_listener_sigv4_services() {
    let credentials = SigV4Credentials::new("key", "secret");
    assert_eq!(
        S3VectorsClient::new("http://127.0.0.1:8333", credentials.clone()).signing_name(),
        "s3vectors"
    );
    assert_eq!(
        VerglasGraphsClient::new("http://127.0.0.1:8333", credentials).signing_name(),
        "verglasgraphs"
    );
}
