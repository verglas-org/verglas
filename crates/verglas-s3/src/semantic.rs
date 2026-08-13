//! Semantic REST-JSON dispatch on the cache node's S3 listener.
//!
//! This module owns only wire routing and error rendering. A [`SemanticApi`]
//! implementation owns all meaning and must use Iceberg tables as its source of
//! truth; Puffin is an optional, snapshot-bound acceleration artifact.

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use axum::{
    Json, Router,
    extract::State,
    http::{Method, StatusCode, Uri},
    response::IntoResponse,
    routing::{get, post},
};
use iceberg::{Catalog, NamespaceIdent, TableCommit, TableRequirement, TableUpdate};
use serde_json::{Value, json};
use verglas_graph::{Direction, Edge, Graph, Node, TraversalFilter};
use verglas_iceberg::{parse_table_ident, tables_api};

/// The operations in the checked-in AWS S3 Vectors and Verglas Graph contracts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SemanticOperation {
    /// An AWS S3 Vectors REST-JSON operation.
    S3Vectors(&'static str),
    /// A Verglas Graph REST-JSON operation.
    Graph(&'static str),
}

/// Error returned by a semantic engine while serving a protocol operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SemanticError {
    /// HTTP status returned to the client.
    pub status: StatusCode,
    /// Stable REST-JSON error code.
    pub code: &'static str,
    /// Safe operator/client-facing explanation.
    pub message: String,
}

impl SemanticError {
    /// Builds a validation error without exposing internal state.
    pub fn validation(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: "ValidationException",
            message: message.into(),
        }
    }

    /// Builds an unavailable error for a node without a configured Iceberg catalog.
    pub fn unavailable(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "ServiceUnavailableException",
            message: message.into(),
        }
    }
}

/// Durable semantic implementation selected by the cache-node runtime.
///
/// Implementations must not keep bucket, index, vector, or graph membership in
/// process memory. A restart must rehydrate solely from the customer-owned
/// Iceberg snapshot and its Puffin attachments.
#[async_trait]
pub trait SemanticApi: Send + Sync + 'static {
    /// Executes one exact REST-JSON operation and returns its response object.
    async fn call(
        &self,
        operation: SemanticOperation,
        input: Value,
    ) -> Result<Value, SemanticError>;
}

/// Router state shared by all semantic endpoints.
#[derive(Clone)]
struct SemanticState {
    /// The explicitly wired durable semantic implementation.
    api: Arc<dyn SemanticApi>,
}

/// Builds the semantic routes that take precedence over the ordinary S3 fallback.
///
/// Every route is intentionally declared from the two checked-in contracts.
/// This is an extension seam for future wire-format evolution; no negotiation
/// or compatibility path exists in this prototype.
pub fn router(api: Arc<dyn SemanticApi>) -> Router {
    let state = SemanticState { api };
    Router::new()
        .route("/CreateIndex", post(dispatch_s3))
        .route("/CreateVectorBucket", post(dispatch_s3))
        .route("/DeleteIndex", post(dispatch_s3))
        .route("/DeleteVectorBucket", post(dispatch_s3))
        .route("/DeleteVectorBucketPolicy", post(dispatch_s3))
        .route("/DeleteVectors", post(dispatch_s3))
        .route("/GetIndex", post(dispatch_s3))
        .route("/GetVectorBucket", post(dispatch_s3))
        .route("/GetVectorBucketPolicy", post(dispatch_s3))
        .route("/GetVectors", post(dispatch_s3))
        .route("/ListIndexes", post(dispatch_s3))
        .route("/ListVectorBuckets", post(dispatch_s3))
        .route("/ListVectors", post(dispatch_s3))
        .route("/PutVectorBucketPolicy", post(dispatch_s3))
        .route("/PutVectors", post(dispatch_s3))
        .route("/QueryVectors", post(dispatch_s3))
        .route(
            "/tags/{resourceArn}",
            get(dispatch_s3).post(dispatch_s3).delete(dispatch_s3),
        )
        .route("/CreateGraph", post(dispatch_graph))
        .route("/DeleteGraph", post(dispatch_graph))
        .route("/GetGraph", post(dispatch_graph))
        .route("/ListGraphs", post(dispatch_graph))
        .route("/PutNodes", post(dispatch_graph))
        .route("/PutEdges", post(dispatch_graph))
        .route("/GetNeighbors", post(dispatch_graph))
        .route("/QueryKHop", post(dispatch_graph))
        .route("/QueryNeighborhood", post(dispatch_graph))
        .route("/QueryPaths", post(dispatch_graph))
        .route("/BuildGraphIndex", post(dispatch_graph))
        .with_state(state)
}

/// Resolves and runs an AWS operation from its exact request URI.
async fn dispatch_s3(
    State(state): State<SemanticState>,
    method: Method,
    uri: Uri,
    body: Option<Json<Value>>,
) -> impl IntoResponse {
    let Some(operation) = s3_operation(uri.path(), &method) else {
        return not_found();
    };
    let mut input = body.map_or(Value::Object(Default::default()), |body| body.0);
    if let Some(resource) = uri.path().strip_prefix("/tags/") {
        let Ok(resource) = percent_encoding::percent_decode_str(resource).decode_utf8() else {
            return not_found();
        };
        input["resourceArn"] = Value::String(resource.into_owned());
    }
    dispatch(state, operation, input).await
}

/// Resolves and runs a graph operation from its exact request URI.
async fn dispatch_graph(
    State(state): State<SemanticState>,
    uri: Uri,
    body: Option<Json<Value>>,
) -> impl IntoResponse {
    let Some(operation) = graph_operation(uri.path()) else {
        return not_found();
    };
    dispatch(
        state,
        operation,
        body.map_or(Value::Object(Default::default()), |body| body.0),
    )
    .await
}

/// Calls the durable adapter and maps its result to REST-JSON.
async fn dispatch(
    state: SemanticState,
    operation: SemanticOperation,
    body: Value,
) -> axum::response::Response {
    match state.api.call(operation, body).await {
        Ok(output) => (StatusCode::OK, Json(output)).into_response(),
        Err(error) => (
            error.status,
            Json(json!({"code": error.code, "message": error.message})),
        )
            .into_response(),
    }
}

/// Returns the protocol's standard unknown-operation response.
fn not_found() -> axum::response::Response {
    (
        StatusCode::NOT_FOUND,
        Json(json!({"code": "NotFoundException", "message": "unknown semantic operation"})),
    )
        .into_response()
}

/// Maps each exact S3 Vectors URI to its operation name.
fn s3_operation(path: &str, method: &Method) -> Option<SemanticOperation> {
    if path.starts_with("/tags/") {
        return match *method {
            Method::GET => Some(SemanticOperation::S3Vectors("ListTagsForResource")),
            Method::POST => Some(SemanticOperation::S3Vectors("TagResource")),
            Method::DELETE => Some(SemanticOperation::S3Vectors("UntagResource")),
            _ => None,
        };
    }
    const OPERATIONS: [&str; 16] = [
        "CreateIndex",
        "CreateVectorBucket",
        "DeleteIndex",
        "DeleteVectorBucket",
        "DeleteVectorBucketPolicy",
        "DeleteVectors",
        "GetIndex",
        "GetVectorBucket",
        "GetVectorBucketPolicy",
        "GetVectors",
        "ListIndexes",
        "ListVectorBuckets",
        "ListVectors",
        "PutVectorBucketPolicy",
        "PutVectors",
        "QueryVectors",
    ];
    let operation = path.strip_prefix('/')?;
    OPERATIONS
        .into_iter()
        .find(|candidate| *candidate == operation)
        .map(SemanticOperation::S3Vectors)
}

/// Maps each exact Graph URI to its operation name.
fn graph_operation(path: &str) -> Option<SemanticOperation> {
    const OPERATIONS: [&str; 11] = [
        "CreateGraph",
        "DeleteGraph",
        "GetGraph",
        "ListGraphs",
        "PutNodes",
        "PutEdges",
        "GetNeighbors",
        "QueryKHop",
        "QueryNeighborhood",
        "QueryPaths",
        "BuildGraphIndex",
    ];
    let operation = path.strip_prefix('/')?;
    OPERATIONS
        .into_iter()
        .find(|candidate| *candidate == operation)
        .map(SemanticOperation::Graph)
}

/// A deliberately fail-closed implementation used until the cache node has a
/// native Iceberg catalog runtime. It stores nothing and cannot fabricate data.
pub struct UnavailableSemanticApi;

#[async_trait]
impl SemanticApi for UnavailableSemanticApi {
    /// Rejects requests rather than constructing an authoritative local registry.
    async fn call(
        &self,
        _operation: SemanticOperation,
        _input: Value,
    ) -> Result<Value, SemanticError> {
        Err(SemanticError::unavailable(
            "semantic APIs require a configured native Iceberg catalog",
        ))
    }
}

/// Iceberg-backed Graph adapter used by the cache-node runtime.
///
/// It has no registry: a graph name is an Iceberg namespace and [`Graph`] is
/// reopened for every operation. Consequently the graph's node/edge tables and
/// snapshot-bound Puffin attachment remain the only durable state.
pub struct IcebergCatalogSemanticStore {
    /// The catalog that owns all graph tables and Puffin statistics files.
    catalog: Arc<dyn Catalog>,
}

impl IcebergCatalogSemanticStore {
    /// Creates an adapter over one already-open Iceberg catalog.
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        Self { catalog }
    }

    /// Opens the named graph directly from its Iceberg namespace.
    fn graph(&self, input: &Value) -> Result<Graph, SemanticError> {
        let graph = input
            .get("graphName")
            .and_then(Value::as_str)
            .ok_or_else(|| SemanticError::validation("graphName is required"))?;
        Graph::open(self.catalog.clone(), graph).map_err(graph_error)
    }
}

#[async_trait]
impl SemanticApi for IcebergCatalogSemanticStore {
    /// Runs semantic operations against customer-owned Iceberg tables.
    async fn call(
        &self,
        operation: SemanticOperation,
        input: Value,
    ) -> Result<Value, SemanticError> {
        if let SemanticOperation::S3Vectors(operation) = operation {
            return self.vector_call(operation, input).await;
        }
        let SemanticOperation::Graph(operation) = operation else {
            unreachable!("semantic operation is vector or graph")
        };
        if operation == "ListGraphs" {
            let graphs = self
                .catalog
                .list_namespaces(None)
                .await
                .map_err(iceberg_error)?
                .into_iter()
                .filter_map(|namespace| namespace.first().cloned())
                .map(|name| json!({"graphName": name}))
                .collect::<Vec<_>>();
            return Ok(json!({"graphs": graphs}));
        }
        let graph = self.graph(&input)?;
        match operation {
            "CreateGraph" => {
                graph.ensure_tables().await.map_err(graph_error)?;
                Ok(json!({}))
            }
            "DeleteGraph" => {
                self.catalog
                    .drop_table(graph.nodes_ident())
                    .await
                    .map_err(iceberg_error)?;
                self.catalog
                    .drop_table(graph.edges_ident())
                    .await
                    .map_err(iceberg_error)?;
                self.catalog
                    .drop_namespace(&NamespaceIdent::new(
                        required_string(&input, "graphName")?.to_owned(),
                    ))
                    .await
                    .map_err(iceberg_error)?;
                Ok(json!({}))
            }
            "PutNodes" => {
                let rows = input
                    .get("nodes")
                    .and_then(Value::as_array)
                    .ok_or_else(|| SemanticError::validation("nodes is required"))?;
                let nodes = rows
                    .iter()
                    .map(node_from_json)
                    .collect::<Result<Vec<_>, _>>()?;
                let snapshot = graph.insert_nodes(&nodes).await.map_err(graph_error)?;
                Ok(json!({"snapshotId": snapshot}))
            }
            "PutEdges" => {
                let rows = input
                    .get("edges")
                    .and_then(Value::as_array)
                    .ok_or_else(|| SemanticError::validation("edges is required"))?;
                let edges = rows
                    .iter()
                    .map(edge_from_json)
                    .collect::<Result<Vec<_>, _>>()?;
                let snapshot = graph.insert_edges(&edges).await.map_err(graph_error)?;
                Ok(json!({"snapshotId": snapshot}))
            }
            "BuildGraphIndex" => {
                Ok(json!({"index": graph.build_index(None).await.map_err(graph_error)?.is_some()}))
            }
            "GetNeighbors" => {
                let reader = graph.reader(None).await.map_err(graph_error)?;
                let node = required_string(&input, "nodeId")?;
                Ok(
                    json!({"neighbors": reader.get_neighbors(node, direction(&input)?, &TraversalFilter::default())}),
                )
            }
            "QueryKHop" => {
                let reader = graph.reader(None).await.map_err(graph_error)?;
                let node = required_string(&input, "nodeId")?;
                let hops = required_u32(&input, "k")?;
                Ok(
                    json!({"nodes": reader.k_hop(node, hops, direction(&input)?, &TraversalFilter::default())}),
                )
            }
            "QueryNeighborhood" => {
                let reader = graph.reader(None).await.map_err(graph_error)?;
                let node = required_string(&input, "nodeId")?;
                let hops = required_u32(&input, "k")?;
                Ok(
                    json!({"neighborhood": reader.neighborhood(node, hops, direction(&input)?, &TraversalFilter::default())}),
                )
            }
            "QueryPaths" => {
                let reader = graph.reader(None).await.map_err(graph_error)?;
                Ok(
                    json!({"paths": reader.paths(required_string(&input, "sourceId")?, required_string(&input, "targetId")?, required_u32(&input, "maxHops")?, direction(&input)?, &TraversalFilter::default())}),
                )
            }
            "GetGraph" => Ok(
                json!({"graphName": required_string(&input, "graphName")?, "edgesSnapshotId": graph.current_edges_snapshot().await.map_err(graph_error)?}),
            ),
            _ => Err(SemanticError::validation("unknown graph operation")),
        }
    }
}

impl IcebergCatalogSemanticStore {
    /// Executes the vector operations whose durable state is an Iceberg table.
    async fn vector_call(&self, operation: &str, input: Value) -> Result<Value, SemanticError> {
        let input = normalize_vector_arn(input)?;
        if operation == "CreateVectorBucket" {
            let namespace = bucket_namespace(&input)?;
            self.catalog
                .create_namespace(
                    &namespace,
                    HashMap::from([
                        ("verglas.s3vectors.kind".to_owned(), "bucket".to_owned()),
                        (
                            "verglas.s3vectors.created".to_owned(),
                            now_millis().to_string(),
                        ),
                        ("verglas.s3vectors.policy".to_owned(), "null".to_owned()),
                        ("verglas.s3vectors.tags".to_owned(), "{}".to_owned()),
                    ]),
                )
                .await
                .map_err(iceberg_error)?;
            return Ok(json!({"vectorBucketArn": bucket_arn(&input)?}));
        }
        if operation == "DeleteVectorBucket" {
            self.catalog
                .drop_namespace(&bucket_namespace(&input)?)
                .await
                .map_err(iceberg_error)?;
            return Ok(json!({}));
        }
        if operation == "GetVectorBucket" {
            let namespace = bucket_namespace(&input)?;
            let state = self
                .catalog
                .get_namespace(&namespace)
                .await
                .map_err(iceberg_error)?;
            return Ok(
                json!({"vectorBucket": {"vectorBucketName": required_string(&input, "vectorBucketName")?, "vectorBucketArn": bucket_arn(&input)?, "creationTime": state.properties().get("verglas.s3vectors.created").cloned().unwrap_or_else(|| "0".to_owned())}}),
            );
        }
        if operation == "ListVectorBuckets" {
            let mut buckets = Vec::new();
            for namespace in self
                .catalog
                .list_namespaces(None)
                .await
                .map_err(iceberg_error)?
            {
                let state = self
                    .catalog
                    .get_namespace(&namespace)
                    .await
                    .map_err(iceberg_error)?;
                if state
                    .properties()
                    .get("verglas.s3vectors.kind")
                    .map(String::as_str)
                    != Some("bucket")
                {
                    continue;
                }
                let name = namespace.first().cloned().ok_or_else(|| {
                    SemanticError::validation("managed bucket namespace is empty")
                })?;
                buckets.push(json!({"vectorBucketName": name, "vectorBucketArn": format!("arn:aws:s3vectors:us-east-1:000000000000:bucket/{name}"), "creationTime": state.properties().get("verglas.s3vectors.created").cloned().ok_or_else(|| SemanticError::validation("bucket creation metadata is absent"))?}));
            }
            if let Some(prefix) = input.get("prefix").and_then(Value::as_str) {
                buckets.retain(|value| {
                    value["vectorBucketName"]
                        .as_str()
                        .is_some_and(|name| name.starts_with(prefix))
                });
            }
            buckets.sort_by_key(|value| {
                value["vectorBucketName"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned()
            });
            let (vector_buckets, next_token) = page(&input, buckets)?;
            return Ok(json!({"vectorBuckets": vector_buckets, "nextToken": next_token}));
        }
        if matches!(
            operation,
            "PutVectorBucketPolicy" | "DeleteVectorBucketPolicy" | "GetVectorBucketPolicy"
        ) {
            let namespace = bucket_namespace(&input)?;
            let state = self
                .catalog
                .get_namespace(&namespace)
                .await
                .map_err(iceberg_error)?;
            let mut properties = state.properties().clone();
            if operation == "GetVectorBucketPolicy" {
                return Ok(
                    json!({"policy": properties.get("verglas.s3vectors.policy").cloned().ok_or_else(|| SemanticError::validation("vector bucket policy does not exist"))?}),
                );
            }
            if operation == "PutVectorBucketPolicy" {
                properties.insert(
                    "verglas.s3vectors.policy".to_owned(),
                    input
                        .get("policy")
                        .and_then(Value::as_str)
                        .ok_or_else(|| SemanticError::validation("policy is required"))?
                        .to_owned(),
                );
            } else {
                properties.remove("verglas.s3vectors.policy");
            }
            self.catalog
                .update_namespace(&namespace, properties)
                .await
                .map_err(iceberg_error)?;
            return Ok(json!({}));
        }
        if operation == "ListIndexes" {
            let namespace = bucket_namespace(&input)?;
            let bucket = required_string(&input, "vectorBucketName")?;
            let mut indexes = Vec::new();
            for ident in self
                .catalog
                .list_tables(&namespace)
                .await
                .map_err(iceberg_error)?
            {
                let name = ident.name().to_owned();
                let table = self
                    .catalog
                    .load_table(&ident)
                    .await
                    .map_err(iceberg_error)?;
                let Some(text) = table.metadata().properties().get("verglas.s3vectors.index")
                else {
                    continue;
                };
                let metadata: Value = serde_json::from_str(text)
                    .map_err(|_| SemanticError::validation("index metadata is corrupt"))?;
                indexes.push(json!({"vectorBucketName": bucket, "indexName": name, "indexArn": format!("arn:aws:s3vectors:us-east-1:000000000000:bucket/{bucket}/index/{name}"), "creationTime": metadata["creationTime"]}));
            }
            if let Some(prefix) = input.get("prefix").and_then(Value::as_str) {
                indexes.retain(|value| {
                    value["indexName"]
                        .as_str()
                        .is_some_and(|name| name.starts_with(prefix))
                });
            }
            indexes.sort_by_key(|value| value["indexName"].as_str().unwrap_or_default().to_owned());
            let (indexes, next_token) = page(&input, indexes)?;
            return Ok(json!({"indexes": indexes, "nextToken": next_token}));
        }
        if matches!(
            operation,
            "TagResource" | "UntagResource" | "ListTagsForResource"
        ) {
            let ident = input
                .get("indexName")
                .is_some()
                .then(|| vector_ident(&input))
                .transpose()?;
            return self.tags_call(operation, &input, ident.as_ref()).await;
        }
        let ident = vector_ident(&input)?;
        match operation {
            "CreateIndex" => {
                let definition = tables_api::CreateTableRequest {
                    schema: vec![
                        tables_api::ColumnSpec::required("key", "string"),
                        tables_api::ColumnSpec::nullable("data", "list<float32>"),
                        tables_api::ColumnSpec::nullable("metadata", "string"),
                        tables_api::ColumnSpec::required("deleted", "boolean"),
                    ],
                    partitions: Vec::new(),
                };
                tables_api::create_table(self.catalog.as_ref(), &ident, definition)
                    .await
                    .map_err(iceberg_error)?;
                let table = self
                    .catalog
                    .load_table(&ident)
                    .await
                    .map_err(iceberg_error)?;
                let metadata = json!({"creationTime": now_millis(), "dataType": required_string(&input, "dataType")?, "dimension": input.get("dimension").cloned().ok_or_else(|| SemanticError::validation("dimension is required"))?, "distanceMetric": required_string(&input, "distanceMetric")?, "metadataConfiguration": input.get("metadataConfiguration").cloned().unwrap_or(Value::Null), "encryptionConfiguration": input.get("encryptionConfiguration").cloned().unwrap_or(Value::Null)}).to_string();
                self.catalog
                    .update_table(TableCommit::from_parts(
                        ident.clone(),
                        vec![TableRequirement::UuidMatch {
                            uuid: table.metadata().uuid(),
                        }],
                        vec![TableUpdate::SetProperties {
                            updates: HashMap::from([(
                                "verglas.s3vectors.index".to_owned(),
                                metadata,
                            )]),
                        }],
                    ))
                    .await
                    .map_err(iceberg_error)?;
                Ok(json!({"indexArn": vector_arn(&input)?}))
            }
            "DeleteIndex" => {
                self.catalog
                    .drop_table(&ident)
                    .await
                    .map_err(iceberg_error)?;
                Ok(json!({}))
            }
            "GetIndex" => {
                let table = self
                    .catalog
                    .load_table(&ident)
                    .await
                    .map_err(iceberg_error)?;
                let value = table
                    .metadata()
                    .properties()
                    .get("verglas.s3vectors.index")
                    .ok_or_else(|| SemanticError::validation("index metadata is absent"))?;
                let metadata: Value = serde_json::from_str(value)
                    .map_err(|_| SemanticError::validation("index metadata is corrupt"))?;
                Ok(
                    json!({"index": {"vectorBucketName": required_string(&input, "vectorBucketName")?, "indexName": required_string(&input, "indexName")?, "indexArn": vector_arn(&input)?, "creationTime": metadata["creationTime"], "dataType": metadata["dataType"], "dimension": metadata["dimension"], "distanceMetric": metadata["distanceMetric"], "metadataConfiguration": metadata["metadataConfiguration"], "encryptionConfiguration": metadata["encryptionConfiguration"]}}),
                )
            }
            "PutVectors" | "DeleteVectors" => {
                let values = if operation == "PutVectors" {
                    input.get("vectors")
                } else {
                    input.get("keys")
                }
                .and_then(Value::as_array)
                .ok_or_else(|| SemanticError::validation("vectors or keys is required"))?;
                let rows = values
                    .iter()
                    .map(|value| vector_row(value, operation == "DeleteVectors"))
                    .collect::<Result<Vec<_>, _>>()?;
                tables_api::commit(
                    self.catalog.as_ref(),
                    &ident,
                    tables_api::CommitRequest {
                        rows,
                        idempotency_key: None,
                    },
                )
                .await
                .map_err(iceberg_error)?;
                Ok(json!({}))
            }
            "GetVectors" => {
                let wanted = input
                    .get("keys")
                    .and_then(Value::as_array)
                    .ok_or_else(|| SemanticError::validation("keys is required"))?
                    .iter()
                    .filter_map(Value::as_str)
                    .collect::<std::collections::HashSet<_>>();
                let vectors = live_vectors(self.catalog.as_ref(), &ident)
                    .await?
                    .into_iter()
                    .filter(|vector| wanted.contains(vector.key.as_str()))
                    .map(|vector| vector.output(&input))
                    .collect::<Vec<_>>();
                Ok(json!({"vectors": vectors}))
            }
            "ListVectors" => {
                let mut vectors = live_vectors(self.catalog.as_ref(), &ident)
                    .await?
                    .into_iter()
                    .map(|vector| vector.output(&input))
                    .collect::<Vec<_>>();
                if let (Some(count), Some(index)) = (
                    input.get("segmentCount").and_then(Value::as_u64),
                    input.get("segmentIndex").and_then(Value::as_u64),
                ) {
                    if count == 0 || index >= count {
                        return Err(SemanticError::validation(
                            "segmentIndex must be less than segmentCount",
                        ));
                    }
                    vectors.retain(|value| {
                        stable_segment(value["key"].as_str().unwrap_or_default(), count) == index
                    });
                } else if input.get("segmentCount").is_some() || input.get("segmentIndex").is_some()
                {
                    return Err(SemanticError::validation(
                        "segmentCount and segmentIndex must be provided together",
                    ));
                }
                let (vectors, next_token) = page(&input, vectors)?;
                Ok(json!({"vectors": vectors, "nextToken": next_token}))
            }
            "QueryVectors" => {
                let query = input
                    .get("queryVector")
                    .and_then(Value::as_array)
                    .ok_or_else(|| SemanticError::validation("queryVector is required"))?
                    .iter()
                    .map(|value| {
                        value.as_f64().map(|number| number as f32).ok_or_else(|| {
                            SemanticError::validation("queryVector must contain numbers")
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let mut vectors = live_vectors(self.catalog.as_ref(), &ident).await?;
                vectors.retain(|vector| vector.data.len() == query.len());
                vectors.sort_by(|left, right| {
                    squared_distance(&left.data, &query)
                        .total_cmp(&squared_distance(&right.data, &query))
                });
                let vectors = vectors.into_iter().take(required_u32(&input, "topK")? as usize).map(|vector| json!({"key": vector.key, "distance": squared_distance(&vector.data, &query), "metadata": vector.metadata})).collect::<Vec<_>>();
                Ok(json!({"vectors": vectors}))
            }
            _ => Err(SemanticError::validation("unknown S3 Vectors operation")),
        }
    }

    /// Persists tags on the bucket namespace or the index table selected by ARN.
    async fn tags_call(
        &self,
        operation: &str,
        input: &Value,
        ident: Option<&iceberg::TableIdent>,
    ) -> Result<Value, SemanticError> {
        let _resource = required_string(input, "resourceArn")?;
        let key = "verglas.s3vectors.tags";
        if let Some(ident) = ident {
            let table = self
                .catalog
                .load_table(ident)
                .await
                .map_err(iceberg_error)?;
            let mut tags = table
                .metadata()
                .properties()
                .get(key)
                .and_then(|text| serde_json::from_str::<serde_json::Map<String, Value>>(text).ok())
                .unwrap_or_default();
            mutate_tags(operation, input, &mut tags)?;
            if operation != "ListTagsForResource" {
                self.catalog
                    .update_table(TableCommit::from_parts(
                        ident.clone(),
                        vec![TableRequirement::UuidMatch {
                            uuid: table.metadata().uuid(),
                        }],
                        vec![TableUpdate::SetProperties {
                            updates: HashMap::from([(
                                key.to_owned(),
                                Value::Object(tags.clone()).to_string(),
                            )]),
                        }],
                    ))
                    .await
                    .map_err(iceberg_error)?;
            }
            return Ok(json!({"tags": tags}));
        }
        let namespace = bucket_namespace(input)?;
        let state = self
            .catalog
            .get_namespace(&namespace)
            .await
            .map_err(iceberg_error)?;
        let mut properties = state.properties().clone();
        let mut tags = properties
            .get(key)
            .and_then(|text| serde_json::from_str::<serde_json::Map<String, Value>>(text).ok())
            .unwrap_or_default();
        mutate_tags(operation, input, &mut tags)?;
        if operation != "ListTagsForResource" {
            properties.insert(key.to_owned(), Value::Object(tags.clone()).to_string());
            self.catalog
                .update_namespace(&namespace, properties)
                .await
                .map_err(iceberg_error)?;
        }
        Ok(json!({"tags": tags}))
    }
}

/// Normalizes modeled ARN selectors to the name fields the Iceberg adapter owns.
fn normalize_vector_arn(mut input: Value) -> Result<Value, SemanticError> {
    for field in ["vectorBucketArn", "indexArn", "resourceArn"] {
        let Some(arn) = input.get(field).and_then(Value::as_str).map(str::to_owned) else {
            continue;
        };
        if let Some((bucket, index)) = arn_parts(&arn)? {
            input["vectorBucketName"] = Value::String(bucket);
            if let Some(index) = index {
                input["indexName"] = Value::String(index);
            }
        }
    }
    Ok(input)
}

/// Parses local S3 Vector bucket and index ARNs without guessing other ARN forms.
fn arn_parts(arn: &str) -> Result<Option<(String, Option<String>)>, SemanticError> {
    let mut parts = arn.splitn(6, ':');
    if parts.next() != Some("arn")
        || parts.next() != Some("aws")
        || parts.next() != Some("s3vectors")
    {
        return Ok(None);
    };
    let _region = parts
        .next()
        .ok_or_else(|| SemanticError::validation("invalid S3 Vectors ARN"))?;
    let account = parts
        .next()
        .ok_or_else(|| SemanticError::validation("invalid S3 Vectors ARN"))?;
    let resource = parts
        .next()
        .ok_or_else(|| SemanticError::validation("invalid S3 Vectors ARN"))?;
    if account.len() != 12 || !account.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(SemanticError::validation("invalid S3 Vectors ARN account"));
    }
    let Some(resource) = resource.strip_prefix("bucket/") else {
        return Err(SemanticError::validation("invalid S3 Vectors ARN resource"));
    };
    let (bucket, index) = match resource.split_once("/index/") {
        Some((bucket, index)) => (bucket, Some(index)),
        None => (resource, None),
    };
    if bucket.is_empty() || index.is_some_and(str::is_empty) {
        return Err(SemanticError::validation("invalid S3 Vectors ARN"));
    }
    Ok(Some((bucket.to_owned(), index.map(str::to_owned))))
}

/// Applies one tag mutation to an authoritative JSON property map.
fn mutate_tags(
    operation: &str,
    input: &Value,
    tags: &mut serde_json::Map<String, Value>,
) -> Result<(), SemanticError> {
    if operation == "TagResource" {
        for (name, value) in input
            .get("tags")
            .and_then(Value::as_object)
            .ok_or_else(|| SemanticError::validation("tags is required"))?
        {
            tags.insert(name.clone(), value.clone());
        }
    }
    if operation == "UntagResource" {
        for name in input
            .get("tagKeys")
            .and_then(Value::as_array)
            .ok_or_else(|| SemanticError::validation("tagKeys is required"))?
            .iter()
            .filter_map(Value::as_str)
        {
            tags.remove(name);
        }
    }
    Ok(())
}

/// Resolves a bucket name to the customer-owned Iceberg namespace.
fn bucket_namespace(input: &Value) -> Result<NamespaceIdent, SemanticError> {
    Ok(NamespaceIdent::new(
        required_string(input, "vectorBucketName")?.to_owned(),
    ))
}

/// Builds the stable ARN for a bucket.
fn bucket_arn(input: &Value) -> Result<String, SemanticError> {
    Ok(format!(
        "arn:aws:s3vectors:us-east-1:000000000000:bucket/{}",
        required_string(input, "vectorBucketName")?
    ))
}

/// Returns a durable creation timestamp recorded with each catalog resource.
fn now_millis() -> i64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => match i64::try_from(duration.as_millis()) {
            Ok(value) => value,
            Err(_) => i64::MAX,
        },
        Err(_) => 0,
    }
}

/// Names the catalog namespace property carrying one index's exact definition.
/// Applies the AWS cursor/max-results contract to an already stably sorted list.
fn page(input: &Value, values: Vec<Value>) -> Result<(Vec<Value>, Option<String>), SemanticError> {
    let binding = format!(
        "{}|{}|{}",
        input
            .get("vectorBucketName")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        input
            .get("indexName")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        input
            .get("prefix")
            .and_then(Value::as_str)
            .unwrap_or_default()
    );
    let last = input
        .get("nextToken")
        .and_then(Value::as_str)
        .map(|token| decode_cursor(token, &binding))
        .transpose()?;
    let offset = last
        .as_deref()
        .map(|last| {
            values
                .iter()
                .position(|value| listing_key(value) > last)
                .unwrap_or(values.len())
        })
        .unwrap_or(0);
    let limit = input
        .get("maxResults")
        .and_then(Value::as_u64)
        .map(|value| {
            usize::try_from(value).map_err(|_| SemanticError::validation("maxResults is too large"))
        })
        .transpose()?
        .unwrap_or(500);
    let end = offset.saturating_add(limit).min(values.len());
    let next = (end < values.len()).then(|| encode_cursor(&binding, listing_key(&values[end - 1])));
    Ok((values[offset..end].to_vec(), next))
}

/// Returns a stable key for an already sorted list response.
fn listing_key(value: &Value) -> &str {
    ["vectorBucketName", "indexName", "key"]
        .into_iter()
        .find_map(|field| value.get(field).and_then(Value::as_str))
        .unwrap_or_default()
}

/// Encodes a resource- and filter-bound last-key cursor.
fn encode_cursor(binding: &str, last: &str) -> String {
    base64::Engine::encode(
        &base64::engine::general_purpose::URL_SAFE_NO_PAD,
        format!("v1\n{binding}\n{last}"),
    )
}

/// Rejects cursors reused with a different resource or filter.
fn decode_cursor(token: &str, binding: &str) -> Result<String, SemanticError> {
    let text = String::from_utf8(
        base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, token)
            .map_err(|_| SemanticError::validation("nextToken is invalid"))?,
    )
    .map_err(|_| SemanticError::validation("nextToken is invalid"))?;
    let mut parts = text.splitn(3, '\n');
    if parts.next() != Some("v1") || parts.next() != Some(binding) {
        return Err(SemanticError::validation(
            "nextToken does not match this listing",
        ));
    }
    parts
        .next()
        .map(str::to_owned)
        .ok_or_else(|| SemanticError::validation("nextToken is invalid"))
}

/// Assigns a key to a deterministic list segment without process-random hashing.
fn stable_segment(key: &str, count: u64) -> u64 {
    key.bytes().fold(0_u64, |hash, byte| {
        hash.wrapping_mul(1099511628211)
            .wrapping_add(u64::from(byte))
    }) % count
}

/// Resolves a vector bucket/index name to its customer-owned Iceberg table.
fn vector_ident(input: &Value) -> Result<iceberg::TableIdent, SemanticError> {
    parse_table_ident(&format!(
        "{}.{}",
        required_string(input, "vectorBucketName")?,
        required_string(input, "indexName")?
    ))
    .map_err(iceberg_error)
}

/// Builds the stable local ARN returned for a newly created vector index.
fn vector_arn(input: &Value) -> Result<String, SemanticError> {
    Ok(format!(
        "arn:aws:s3vectors:us-east-1:000000000000:bucket/{}/index/{}",
        required_string(input, "vectorBucketName")?,
        required_string(input, "indexName")?
    ))
}

/// Converts an S3 Vector write/delete item into an append-only Iceberg row.
fn vector_row(value: &Value, deleted: bool) -> Result<Value, SemanticError> {
    let key = if deleted {
        value
            .as_str()
            .ok_or_else(|| SemanticError::validation("key must be a string"))?
    } else {
        required_string(value, "key")?
    };
    let data = if deleted {
        Value::Null
    } else {
        value
            .get("data")
            .cloned()
            .ok_or_else(|| SemanticError::validation("vector data is required"))?
    };
    if !deleted
        && !data
            .as_array()
            .is_some_and(|items| items.iter().all(Value::is_number))
    {
        return Err(SemanticError::validation(
            "vector data must contain numbers",
        ));
    }
    Ok(
        json!({"key": key, "data": data, "metadata": if deleted { None } else { value.get("metadata").map(Value::to_string) }, "deleted": deleted}),
    )
}

/// A source-table vector with its exact customer-provided key preserved.
struct LiveVector {
    key: String,
    data: Vec<f32>,
    metadata: Option<Value>,
}

impl LiveVector {
    /// Renders Get/List output without exposing the implementation's scan state.
    fn output(&self, input: &Value) -> Value {
        let mut output = json!({"key": self.key});
        if input
            .get("returnData")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            output["data"] = json!(self.data);
        }
        if input
            .get("returnMetadata")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            output["metadata"] = self.metadata.clone().unwrap_or(Value::Null);
        }
        output
    }
}

/// Scans the current source snapshot; this is the correct, slower turn-off path.
async fn live_vectors(
    catalog: &dyn Catalog,
    ident: &iceberg::TableIdent,
) -> Result<Vec<LiveVector>, SemanticError> {
    let rows = tables_api::rows(catalog, ident, None, None)
        .await
        .map_err(iceberg_error)?
        .rows;
    let mut current = std::collections::BTreeMap::new();
    for row in rows {
        let Some(key) = row.get("key").and_then(Value::as_str) else {
            continue;
        };
        if row.get("deleted").and_then(Value::as_bool).unwrap_or(false) {
            current.remove(key);
            continue;
        }
        let data = row
            .get("data")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(Value::as_f64)
                    .map(|number| number as f32)
                    .collect()
            })
            .unwrap_or_default();
        let metadata = row
            .get("metadata")
            .and_then(Value::as_str)
            .and_then(|text| serde_json::from_str(text).ok());
        current.insert(
            key.to_owned(),
            LiveVector {
                key: key.to_owned(),
                data,
                metadata,
            },
        );
    }
    Ok(current.into_values().collect())
}

/// Computes exact squared L2 distance for the no-Puffin query path.
fn squared_distance(left: &[f32], right: &[f32]) -> f32 {
    left.iter()
        .zip(right)
        .map(|(a, b)| {
            let delta = a - b;
            delta * delta
        })
        .sum()
}

/// Converts an Iceberg error to a safe service failure.
fn iceberg_error(error: impl std::fmt::Display) -> SemanticError {
    SemanticError::unavailable(error.to_string())
}

/// Converts a REST-JSON node object to the graph engine's durable row model.
fn node_from_json(value: &Value) -> Result<Node, SemanticError> {
    let mut node = Node::new(required_string(value, "id")?);
    node.labels = value
        .get("labels")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();
    node.properties = value
        .get("properties")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    Ok(node)
}

/// Converts a REST-JSON edge object to the graph engine's append-only model.
fn edge_from_json(value: &Value) -> Result<Edge, SemanticError> {
    let mut edge = Edge::new(
        required_string(value, "sourceId")?,
        required_string(value, "predicate")?,
        required_string(value, "targetId")?,
        required_string(value, "provenance")?,
    );
    if let Some(id) = value.get("edgeId").and_then(Value::as_str) {
        edge.edge_id = id.to_owned();
    }
    if let Some(confidence) = value.get("confidence").and_then(Value::as_f64) {
        edge.confidence = confidence;
    }
    edge.properties = value
        .get("properties")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    Ok(edge)
}

/// Reads one required string field from an operation input.
fn required_string<'a>(value: &'a Value, field: &str) -> Result<&'a str, SemanticError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| SemanticError::validation(format!("{field} is required")))
}

/// Reads one required non-negative hop count from an operation input.
fn required_u32(value: &Value, field: &str) -> Result<u32, SemanticError> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .and_then(|number| u32::try_from(number).ok())
        .ok_or_else(|| SemanticError::validation(format!("{field} is required")))
}

/// Parses the optional traversal direction, defaulting to outgoing edges.
fn direction(input: &Value) -> Result<Direction, SemanticError> {
    match input
        .get("direction")
        .and_then(Value::as_str)
        .unwrap_or("out")
    {
        "out" => Ok(Direction::Out),
        "in" => Ok(Direction::In),
        "both" => Ok(Direction::Both),
        _ => Err(SemanticError::validation(
            "direction must be out, in, or both",
        )),
    }
}

/// Converts graph-engine errors to a safe REST-JSON service failure.
fn graph_error(error: verglas_graph::GraphError) -> SemanticError {
    SemanticError::unavailable(error.to_string())
}
