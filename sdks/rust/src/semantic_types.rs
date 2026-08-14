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
pub struct BuildGraphIndexInput {
    pub graph_name: std::string::String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BuildGraphIndexOutput {
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
    pub vector_bucket_name: Option<std::string::String>,
    pub vector_bucket_arn: Option<std::string::String>,
    pub index_name: std::string::String,
    pub data_type: DataType,
    pub dimension: i32,
    pub distance_metric: DistanceMetric,
    pub metadata_configuration: Option<MetadataConfiguration>,
    pub encryption_configuration: Option<EncryptionConfiguration>,
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
    pub encryption_configuration: Option<EncryptionConfiguration>,
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
    pub vector_bucket_name: Option<std::string::String>,
    pub index_name: Option<std::string::String>,
    pub index_arn: Option<std::string::String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteIndexOutput {}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteVectorBucketInput {
    pub vector_bucket_name: Option<std::string::String>,
    pub vector_bucket_arn: Option<std::string::String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteVectorBucketOutput {}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteVectorBucketPolicyInput {
    pub vector_bucket_name: Option<std::string::String>,
    pub vector_bucket_arn: Option<std::string::String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteVectorBucketPolicyOutput {}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteVectorsInput {
    pub vector_bucket_name: Option<std::string::String>,
    pub index_name: Option<std::string::String>,
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
    pub confidence: Option<f64>,
    pub edge_id: Option<std::string::String>,
    pub predicate: std::string::String,
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
    pub sse_type: Option<SseType>,
    pub kms_key_arn: Option<std::string::String>,
}

pub type ErrorMessage = std::string::String;

pub type ExceptionMessage = std::string::String;

pub type Float = f32;

pub type Float32VectorData = Vec<f32>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetGraphInput {
    pub graph_name: std::string::String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetGraphOutput {
    pub edges_snapshot_id: Option<i64>,
    pub graph_name: std::string::String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetIndexInput {
    pub vector_bucket_name: Option<std::string::String>,
    pub index_name: Option<std::string::String>,
    pub index_arn: Option<std::string::String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetIndexOutput {
    pub index: Index,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetNeighborsInput {
    pub direction: Option<Direction>,
    pub filter: Option<TraversalFilter>,
    pub graph_name: std::string::String,
    pub node_id: std::string::String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetNeighborsOutput {
    pub neighbors: Vec<Neighbor>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetOutputVector {
    pub key: std::string::String,
    pub data: Option<VectorData>,
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetVectorBucketInput {
    pub vector_bucket_name: Option<std::string::String>,
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
    pub vector_bucket_name: Option<std::string::String>,
    pub vector_bucket_arn: Option<std::string::String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetVectorBucketPolicyOutput {
    pub policy: Option<std::string::String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetVectorsInput {
    pub vector_bucket_name: Option<std::string::String>,
    pub index_name: Option<std::string::String>,
    pub index_arn: Option<std::string::String>,
    pub keys: Vec<std::string::String>,
    pub return_data: Option<bool>,
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
    pub creation_time: String,
    pub data_type: DataType,
    pub dimension: i32,
    pub distance_metric: DistanceMetric,
    pub metadata_configuration: Option<MetadataConfiguration>,
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
    pub creation_time: String,
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
    pub max_results: Option<i32>,
    pub next_token: Option<std::string::String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListGraphsOutput {
    pub graphs: Vec<GraphSummary>,
    pub next_token: Option<std::string::String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListIndexesInput {
    pub vector_bucket_name: Option<std::string::String>,
    pub vector_bucket_arn: Option<std::string::String>,
    pub max_results: Option<i32>,
    pub next_token: Option<std::string::String>,
    pub prefix: Option<std::string::String>,
}

pub type ListIndexesMaxResults = i32;

pub type ListIndexesNextToken = std::string::String;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListIndexesOutput {
    pub next_token: Option<std::string::String>,
    pub indexes: Vec<IndexSummary>,
}

pub type ListIndexesOutputList = Vec<IndexSummary>;

pub type ListIndexesPrefix = std::string::String;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListOutputVector {
    pub key: std::string::String,
    pub data: Option<VectorData>,
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
    pub max_results: Option<i32>,
    pub next_token: Option<std::string::String>,
    pub prefix: Option<std::string::String>,
}

pub type ListVectorBucketsMaxResults = i32;

pub type ListVectorBucketsNextToken = std::string::String;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListVectorBucketsOutput {
    pub next_token: Option<std::string::String>,
    pub vector_buckets: Vec<VectorBucketSummary>,
}

pub type ListVectorBucketsOutputList = Vec<VectorBucketSummary>;

pub type ListVectorBucketsPrefix = std::string::String;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListVectorsInput {
    pub vector_bucket_name: Option<std::string::String>,
    pub index_name: Option<std::string::String>,
    pub index_arn: Option<std::string::String>,
    pub max_results: Option<i32>,
    pub next_token: Option<std::string::String>,
    pub segment_count: Option<i32>,
    pub segment_index: Option<i32>,
    pub return_data: Option<bool>,
    pub return_metadata: Option<bool>,
}

pub type ListVectorsMaxResults = i32;

pub type ListVectorsNextToken = std::string::String;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListVectorsOutput {
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
    pub labels: Option<Vec<std::string::String>>,
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

pub type Predicate = std::string::String;

pub type Properties = std::collections::BTreeMap<String, serde_json::Value>;

pub type PropertyName = std::string::String;

pub type Provenance = std::string::String;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PutEdgesInput {
    pub edges: Vec<Edge>,
    pub graph_name: std::string::String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PutEdgesOutput {
    pub snapshot_id: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PutInputVector {
    pub key: std::string::String,
    pub data: VectorData,
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PutNodesInput {
    pub graph_name: std::string::String,
    pub nodes: Vec<Node>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PutNodesOutput {
    pub snapshot_id: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PutVectorBucketPolicyInput {
    pub vector_bucket_name: Option<std::string::String>,
    pub vector_bucket_arn: Option<std::string::String>,
    pub policy: std::string::String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PutVectorBucketPolicyOutput {}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PutVectorsInput {
    pub vector_bucket_name: Option<std::string::String>,
    pub index_name: Option<std::string::String>,
    pub index_arn: Option<std::string::String>,
    pub vectors: Vec<PutInputVector>,
}

pub type PutVectorsInputList = Vec<PutInputVector>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PutVectorsOutput {}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryKHopInput {
    pub direction: Option<Direction>,
    pub filter: Option<TraversalFilter>,
    pub graph_name: std::string::String,
    pub k: i64,
    pub node_id: std::string::String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryKHopOutput {
    pub nodes: Vec<ReachedNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryNeighborhoodInput {
    pub direction: Option<Direction>,
    pub filter: Option<TraversalFilter>,
    pub graph_name: std::string::String,
    pub k: i64,
    pub node_id: std::string::String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryNeighborhoodOutput {
    pub neighborhood: Subgraph,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryOutputVector {
    pub distance: Option<f32>,
    pub key: std::string::String,
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryPathsInput {
    pub direction: Option<Direction>,
    pub filter: Option<TraversalFilter>,
    pub graph_name: std::string::String,
    pub max_hops: i64,
    pub source_id: std::string::String,
    pub target_id: std::string::String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryPathsOutput {
    pub paths: Vec<Path>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryVectorsInput {
    pub vector_bucket_name: Option<std::string::String>,
    pub index_name: Option<std::string::String>,
    pub index_arn: Option<std::string::String>,
    pub top_k: i32,
    pub query_vector: VectorData,
    pub filter: Option<serde_json::Value>,
    pub return_metadata: Option<bool>,
    pub return_distance: Option<bool>,
    pub next_token: Option<std::string::String>,
}

pub type QueryVectorsNextToken = std::string::String;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryVectorsOutput {
    pub vectors: Vec<QueryOutputVector>,
    pub distance_metric: DistanceMetric,
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
    pub min_confidence: Option<f64>,
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
    pub creation_time: String,
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
    pub creation_time: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum VectorData {
    Float32 { float32: Vec<f32> },
}

pub type VectorKey = std::string::String;
