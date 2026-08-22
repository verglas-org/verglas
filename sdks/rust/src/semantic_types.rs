//! Field-level DTOs generated from the checked-in semantic service models.
//! Do not edit by hand; run scripts/generate-semantic-dtos.mjs.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccessDeniedException {
    pub message: std::string::String,
}

pub type Boolean = bool;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BuildIndexInput {
    pub graph_name: std::string::String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BuildIndexOutput {
    pub index: bool,
}

pub type Confidence = f64;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConflictException {
    pub message: std::string::String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateGraphInput {
    pub graph_name: std::string::String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateGraphOutput {}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateIndexInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_bucket_name: Option<std::string::String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_bucket_arn: Option<std::string::String>,
    pub index_name: std::string::String,
    pub data_type: DataType,
    pub dimension: i32,
    pub distance_metric: DistanceMetric,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata_configuration: Option<MetadataConfiguration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encryption_configuration: Option<EncryptionConfiguration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<std::collections::BTreeMap<String, std::string::String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateIndexOutput {
    pub index_arn: std::string::String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateVectorBucketInput {
    pub vector_bucket_name: std::string::String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encryption_configuration: Option<EncryptionConfiguration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<std::collections::BTreeMap<String, std::string::String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateVectorBucketOutput {
    pub vector_bucket_arn: std::string::String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DataType {
    #[serde(rename = "float32")]
    Float32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteGraphInput {
    pub graph_name: std::string::String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteGraphOutput {}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteIndexInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_bucket_name: Option<std::string::String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_name: Option<std::string::String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_arn: Option<std::string::String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteIndexOutput {}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteVectorBucketInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_bucket_name: Option<std::string::String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_bucket_arn: Option<std::string::String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteVectorBucketOutput {}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteVectorBucketPolicyInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_bucket_name: Option<std::string::String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_bucket_arn: Option<std::string::String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteVectorBucketPolicyOutput {}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteVectorsInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_bucket_name: Option<std::string::String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_name: Option<std::string::String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_arn: Option<std::string::String>,
    pub keys: Vec<std::string::String>,
}

pub type DeleteVectorsInputList = Vec<std::string::String>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteVectorsOutput {}

pub type Dimension = i32;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Direction {
    #[serde(rename = "out")]
    Out,
    #[serde(rename = "in")]
    In,
    #[serde(rename = "both")]
    Both,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DistanceMetric {
    #[serde(rename = "euclidean")]
    Euclidean,
    #[serde(rename = "cosine")]
    Cosine,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Edge {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub edge_id: Option<std::string::String>,
    pub predicate: std::string::String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<std::collections::BTreeMap<String, serde_json::Value>>,
    pub provenance: std::string::String,
    pub source_id: std::string::String,
    pub target_id: std::string::String,
}

pub type EdgeId = std::string::String;

pub type EdgeList = Vec<Edge>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EncryptionConfiguration {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sse_type: Option<SseType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kms_key_arn: Option<std::string::String>,
}

pub type ErrorMessage = std::string::String;

pub type ExceptionMessage = std::string::String;

pub type Float = f32;

pub type Float32VectorData = Vec<f32>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DescribeGraphInput {
    pub graph_name: std::string::String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DescribeGraphOutput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub edges_snapshot_id: Option<i64>,
    pub graph_name: std::string::String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetIndexInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_bucket_name: Option<std::string::String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_name: Option<std::string::String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_arn: Option<std::string::String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetIndexOutput {
    pub index: Index,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetrieveNeighborsInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub direction: Option<Direction>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<TraversalFilter>,
    pub graph_name: std::string::String,
    pub node_id: std::string::String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetrieveNeighborsOutput {
    pub neighbors: Vec<Neighbor>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetOutputVector {
    pub key: std::string::String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<VectorData>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetVectorBucketInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_bucket_name: Option<std::string::String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_bucket_arn: Option<std::string::String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetVectorBucketOutput {
    pub vector_bucket: VectorBucket,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetVectorBucketPolicyInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_bucket_name: Option<std::string::String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_bucket_arn: Option<std::string::String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetVectorBucketPolicyOutput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy: Option<std::string::String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetVectorsInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_bucket_name: Option<std::string::String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_name: Option<std::string::String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_arn: Option<std::string::String>,
    pub keys: Vec<std::string::String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_data: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_metadata: Option<bool>,
}

pub type GetVectorsInputList = Vec<std::string::String>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetVectorsOutput {
    pub vectors: Vec<GetOutputVector>,
}

pub type GetVectorsOutputList = Vec<GetOutputVector>;

pub type GraphName = std::string::String;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GraphSummary {
    pub graph_name: std::string::String,
}

pub type GraphSummaryList = Vec<GraphSummary>;

pub type HopCount = i64;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Index {
    pub vector_bucket_name: std::string::String,
    pub index_name: std::string::String,
    pub index_arn: std::string::String,
    /// Unix epoch seconds, as the cache node's semantic listener actually
    /// serializes it (not an ISO-8601 string).
    pub creation_time: i64,
    pub data_type: DataType,
    pub dimension: i32,
    pub distance_metric: DistanceMetric,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata_configuration: Option<MetadataConfiguration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encryption_configuration: Option<EncryptionConfiguration>,
}

pub type IndexArn = std::string::String;

pub type IndexBuilt = bool;

pub type IndexName = std::string::String;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexSummary {
    pub vector_bucket_name: std::string::String,
    pub index_name: std::string::String,
    pub index_arn: std::string::String,
    /// Unix epoch seconds, as the cache node's semantic listener actually
    /// serializes it (not an ISO-8601 string).
    pub creation_time: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InternalServerException {
    pub message: std::string::String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KmsDisabledException {
    pub message: std::string::String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KmsInvalidKeyUsageException {
    pub message: std::string::String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KmsInvalidStateException {
    pub message: std::string::String,
}

pub type KmsKeyArn = std::string::String;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KmsNotFoundException {
    pub message: std::string::String,
}

pub type Label = std::string::String;

pub type LabelList = Vec<std::string::String>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListGraphsInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_results: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_token: Option<std::string::String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListGraphsOutput {
    pub graphs: Vec<GraphSummary>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_token: Option<std::string::String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListIndexesInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_bucket_name: Option<std::string::String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_bucket_arn: Option<std::string::String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_results: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_token: Option<std::string::String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix: Option<std::string::String>,
}

pub type ListIndexesMaxResults = i32;

pub type ListIndexesNextToken = std::string::String;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListIndexesOutput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_token: Option<std::string::String>,
    pub indexes: Vec<IndexSummary>,
}

pub type ListIndexesOutputList = Vec<IndexSummary>;

pub type ListIndexesPrefix = std::string::String;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListOutputVector {
    pub key: std::string::String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<VectorData>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListTagsForResourceInput {
    pub resource_arn: std::string::String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListTagsForResourceOutput {
    pub tags: std::collections::BTreeMap<String, std::string::String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListVectorBucketsInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_results: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_token: Option<std::string::String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix: Option<std::string::String>,
}

pub type ListVectorBucketsMaxResults = i32;

pub type ListVectorBucketsNextToken = std::string::String;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListVectorBucketsOutput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_token: Option<std::string::String>,
    pub vector_buckets: Vec<VectorBucketSummary>,
}

pub type ListVectorBucketsOutputList = Vec<VectorBucketSummary>;

pub type ListVectorBucketsPrefix = std::string::String;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListVectorsInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_bucket_name: Option<std::string::String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_name: Option<std::string::String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_arn: Option<std::string::String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_results: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_token: Option<std::string::String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub segment_count: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub segment_index: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_data: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_metadata: Option<bool>,
}

pub type ListVectorsMaxResults = i32;

pub type ListVectorsNextToken = std::string::String;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListVectorsOutput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_token: Option<std::string::String>,
    pub vectors: Vec<ListOutputVector>,
}

pub type ListVectorsOutputList = Vec<ListOutputVector>;

pub type ListVectorsSegmentCount = i32;

pub type ListVectorsSegmentIndex = i32;

pub type MaxResults = i32;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetadataConfiguration {
    pub non_filterable_metadata_keys: Vec<std::string::String>,
}

pub type MetadataKey = std::string::String;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Neighbor {
    pub confidence: f64,
    pub direction: Direction,
    pub edge_id: std::string::String,
    pub node_id: std::string::String,
    pub predicate: std::string::String,
    pub provenance: std::string::String,
}

pub type NeighborList = Vec<Neighbor>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Node {
    pub id: std::string::String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub labels: Option<Vec<std::string::String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<std::collections::BTreeMap<String, serde_json::Value>>,
}

pub type NodeId = std::string::String;

pub type NodeIdList = Vec<std::string::String>;

pub type NodeList = Vec<Node>;

pub type NonFilterableMetadataKeys = Vec<std::string::String>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NotFoundException {
    pub message: std::string::String,
}

pub type PaginationToken = std::string::String;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Path {
    pub confidence: f64,
    pub edges: Vec<TripletReceipt>,
    pub nodes: Vec<std::string::String>,
}

pub type PathList = Vec<Path>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Precedent {
    pub decision_id: std::string::String,
    pub score: f64,
    pub lexical_score: f64,
    pub structural_score: f64,
    pub shared_entities: Vec<std::string::String>,
    pub objective: std::string::String,
}

pub type PrecedentList = Vec<Precedent>;

pub type Predicate = std::string::String;

pub type Properties = std::collections::BTreeMap<String, serde_json::Value>;

pub type PropertyName = std::string::String;

pub type Provenance = std::string::String;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AddEdgesInput {
    pub edges: Vec<Edge>,
    pub graph_name: std::string::String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AddEdgesOutput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PutInputVector {
    pub key: std::string::String,
    pub data: VectorData,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AddNodesInput {
    pub graph_name: std::string::String,
    pub nodes: Vec<Node>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AddNodesOutput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PutVectorBucketPolicyInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_bucket_name: Option<std::string::String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_bucket_arn: Option<std::string::String>,
    pub policy: std::string::String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PutVectorBucketPolicyOutput {}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PutVectorsInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_bucket_name: Option<std::string::String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_name: Option<std::string::String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_arn: Option<std::string::String>,
    pub vectors: Vec<PutInputVector>,
}

pub type PutVectorsInputList = Vec<PutInputVector>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PutVectorsOutput {}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchKHopInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub direction: Option<Direction>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<TraversalFilter>,
    pub graph_name: std::string::String,
    pub k: i64,
    pub node_id: std::string::String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchKHopOutput {
    pub nodes: Vec<ReachedNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchNeighborhoodInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub direction: Option<Direction>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<TraversalFilter>,
    pub graph_name: std::string::String,
    pub k: i64,
    pub node_id: std::string::String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchNeighborhoodOutput {
    pub neighborhood: Subgraph,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryOutputVector {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub distance: Option<f32>,
    pub key: std::string::String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchPathsInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub direction: Option<Direction>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<TraversalFilter>,
    pub graph_name: std::string::String,
    pub max_hops: i64,
    pub source_id: std::string::String,
    pub target_id: std::string::String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchPathsOutput {
    pub paths: Vec<Path>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchPrecedentsInput {
    pub graph_name: std::string::String,
    pub query: std::string::String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entities: Option<Vec<std::string::String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchPrecedentsOutput {
    pub precedents: Vec<Precedent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryVectorsInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_bucket_name: Option<std::string::String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_name: Option<std::string::String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_arn: Option<std::string::String>,
    pub top_k: i32,
    pub query_vector: VectorData,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_metadata: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_distance: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_token: Option<std::string::String>,
}

pub type QueryVectorsNextToken = std::string::String;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryVectorsOutput {
    pub vectors: Vec<QueryOutputVector>,
    pub distance_metric: DistanceMetric,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_token: Option<std::string::String>,
}

pub type QueryVectorsOutputList = Vec<QueryOutputVector>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReachedNode {
    pub hops: i64,
    pub node_id: std::string::String,
    pub path_confidence: f64,
}

pub type ReachedNodeList = Vec<ReachedNode>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestTimeoutException {
    pub message: std::string::String,
}

pub type ResourceARN = std::string::String;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceQuotaExceededException {
    pub message: std::string::String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceUnavailableException {
    pub message: std::string::String,
}

pub type SnapshotId = i64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SseType {
    #[serde(rename = "AES256")]
    AES256,
    #[serde(rename = "aws:kms")]
    AwsKms,
}

pub type String = std::string::String;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Subgraph {
    pub edges: Vec<TripletReceipt>,
    pub nodes: Vec<ReachedNode>,
}

pub type TagKey = std::string::String;

pub type TagKeyList = Vec<std::string::String>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TagResourceInput {
    pub resource_arn: std::string::String,
    pub tags: std::collections::BTreeMap<String, std::string::String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TagResourceOutput {}

pub type TagsMap = std::collections::BTreeMap<String, std::string::String>;

pub type TagValue = std::string::String;

pub type Timestamp = String;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TooManyRequestsException {
    pub message: std::string::String,
}

pub type TopK = i32;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraversalFilter {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_confidence: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub predicate: Option<std::string::String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TripletReceipt {
    pub confidence: f64,
    pub edge_id: std::string::String,
    pub predicate: std::string::String,
    pub provenance: std::string::String,
    pub source_id: std::string::String,
    pub target_id: std::string::String,
}

pub type TripletReceiptList = Vec<TripletReceipt>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UntagResourceInput {
    pub resource_arn: std::string::String,
    pub tag_keys: Vec<std::string::String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UntagResourceOutput {}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ValidationException {
    pub message: std::string::String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field_list: Option<Vec<ValidationExceptionField>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ValidationExceptionField {
    pub path: std::string::String,
    pub message: std::string::String,
}

pub type ValidationExceptionFieldList = Vec<ValidationExceptionField>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VectorBucket {
    pub vector_bucket_name: std::string::String,
    pub vector_bucket_arn: std::string::String,
    /// Unix epoch seconds, as the cache node's semantic listener actually
    /// serializes it (not an ISO-8601 string).
    pub creation_time: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encryption_configuration: Option<EncryptionConfiguration>,
}

pub type VectorBucketArn = std::string::String;

pub type VectorBucketName = std::string::String;

pub type VectorBucketPolicy = std::string::String;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VectorBucketSummary {
    pub vector_bucket_name: std::string::String,
    pub vector_bucket_arn: std::string::String,
    /// Unix epoch seconds, as the cache node's semantic listener actually
    /// serializes it (not an ISO-8601 string).
    pub creation_time: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum VectorData {
    Float32 { float32: Vec<f32> },
}

pub type VectorKey = std::string::String;
