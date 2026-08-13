//! Thin DuckDB table functions for the database-scoped Verglas HTTP API.
//! Credentials are read only from the process environment and never enter SQL values.

use arrow::{
    array::{
        ArrayRef, Float32Array, Float64Array, Int32Array, Int64Array, ListBuilder, StringArray,
        StringBuilder,
    },
    datatypes::{DataType, Field, Schema},
    ipc::reader::StreamReader,
    record_batch::RecordBatch,
};
use duckdb::{
    Connection, Result,
    core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId},
    duckdb_entrypoint_c_api,
    vtab::{
        BindInfo, InitInfo, TableFunctionInfo, VTab,
        arrow::{record_batch_to_duckdb_data_chunk, to_duckdb_logical_type_for_field},
    },
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    error::Error,
    io::Read,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

const ARROW: &str = "application/vnd.apache.arrow.stream";
const ERROR_LIMIT: usize = 8 * 1024;
const JSON_LIMIT: usize = 64 * 1024 * 1024;

/// The environment-only connection settings required by every request.
#[derive(Clone)]
struct Settings {
    endpoint: url::Url,
    database: String,
    token: String,
}
impl Settings {
    /// Reads and validates the required configuration without exposing its token.
    fn read() -> Result<Self, Box<dyn Error>> {
        let required = |name: &str| std::env::var(name).map_err(|_| format!("{name} is required"));
        let endpoint = url::Url::parse(&required("VERGLAS_ENDPOINT")?)?;
        if endpoint.scheme() != "http" && endpoint.scheme() != "https" {
            return Err("VERGLAS_ENDPOINT must be an HTTP URL".into());
        }
        if !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(
                "VERGLAS_ENDPOINT must not contain credentials, a query, or a fragment".into(),
            );
        }
        let database = required("VERGLAS_DATABASE")?;
        let token = required("VERGLAS_TOKEN")?;
        if database.is_empty() {
            return Err("VERGLAS_DATABASE must not be empty".into());
        }
        if token.is_empty() {
            return Err("VERGLAS_TOKEN must not be empty".into());
        }
        Ok(Self {
            endpoint,
            database,
            token,
        })
    }
    /// Builds a database-scoped API URL with parser-owned path-segment encoding.
    fn url(&self, tail: &[&str]) -> Result<url::Url, Box<dyn Error>> {
        let mut url = self.endpoint.clone();
        let mut segments = url
            .path_segments_mut()
            .map_err(|_| "VERGLAS_ENDPOINT cannot be a base URL")?;
        segments.pop_if_empty();
        segments.extend(["v1", "databases", self.database.as_str()]);
        segments.extend(tail.iter().copied());
        drop(segments);
        Ok(url)
    }
}

/// Sends JSON with bearer authorization, retaining only a bounded diagnostic body.
fn post(
    settings: &Settings,
    tail: &[&str],
    body: &impl Serialize,
    accept: &str,
) -> Result<Vec<u8>, Box<dyn Error>> {
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .build()
        .into();
    let url = settings.url(tail)?;
    let response = agent
        .post(url.as_str())
        .header("authorization", &format!("Bearer {}", settings.token))
        .header("accept", accept)
        .send_json(body);
    match response {
        Ok(mut response) if response.status().is_success() => {
            let mut bytes = Vec::new();
            response
                .body_mut()
                .as_reader()
                .take((JSON_LIMIT + 1) as u64)
                .read_to_end(&mut bytes)?;
            if bytes.len() > JSON_LIMIT {
                return Err(format!("Verglas JSON response exceeds {JSON_LIMIT} bytes").into());
            }
            Ok(bytes)
        }
        Ok(mut response) => Err(response_error(&mut response)?),
        Err(error) => Err(format!("Verglas transport error: {error}").into()),
    }
}

/// Builds a bounded server diagnostic without reading or exposing request credentials.
fn response_error(
    response: &mut http::Response<ureq::Body>,
) -> Result<Box<dyn Error>, Box<dyn Error>> {
    let code = response.status().as_u16();
    let mut bytes = Vec::with_capacity(ERROR_LIMIT);
    response
        .body_mut()
        .as_reader()
        .take(ERROR_LIMIT as u64)
        .read_to_end(&mut bytes)?;
    Ok(format!(
        "Verglas request failed with HTTP {code}: {}",
        String::from_utf8_lossy(&bytes)
    )
    .into())
}

/// Holds an open Arrow IPC reader, consuming only batches DuckDB requests.
struct ArrowData {
    reader: Mutex<StreamReader<ureq::BodyReader<'static>>>,
}
struct QueryScan {
    state: Mutex<(Option<RecordBatch>, usize)>,
    vector_size: usize,
}
impl VTab for ArrowData {
    type BindData = Self;
    type InitData = QueryScan;
    fn bind(bind: &BindInfo) -> Result<Self, Box<dyn Error>> {
        let reader = query_reader(&bind.get_parameter(0).to_string())?;
        for field in reader.schema().fields() {
            bind.add_result_column(field.name(), to_duckdb_logical_type_for_field(field)?);
        }
        Ok(Self {
            reader: Mutex::new(reader),
        })
    }
    fn init(_: &InitInfo) -> Result<QueryScan, Box<dyn Error>> {
        Ok(QueryScan {
            state: Mutex::new((None, 0)),
            vector_size: unsafe { duckdb::ffi::duckdb_vector_size() as usize },
        })
    }
    fn func(
        info: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        let mut state = info
            .get_init_data()
            .state
            .lock()
            .map_err(|_| "Arrow scan lock poisoned")?;
        loop {
            if let Some(batch) = state.0.as_ref()
                && state.1 < batch.num_rows()
            {
                let length = (batch.num_rows() - state.1).min(info.get_init_data().vector_size);
                record_batch_to_duckdb_data_chunk(&batch.slice(state.1, length), output)?;
                state.1 += length;
                return Ok(());
            }
            let mut reader = info
                .get_bind_data()
                .reader
                .lock()
                .map_err(|_| "Arrow reader lock poisoned")?;
            state.0 = reader.next().transpose()?;
            state.1 = 0;
            if state.0.is_none() {
                output.set_len(0);
                return Ok(());
            }
        }
    }
    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeId::Varchar.into()])
    }
}

struct ArrowScan {
    offset: AtomicUsize,
    vector_size: usize,
}
/// Opens the Arrow IPC response and retains its network reader for incremental scan calls.
fn query_reader(sql: &str) -> Result<StreamReader<ureq::BodyReader<'static>>, Box<dyn Error>> {
    let settings = Settings::read()?;
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .build()
        .into();
    let url = settings.url(&["query"])?;
    let mut response = agent
        .post(url.as_str())
        .header("authorization", &format!("Bearer {}", settings.token))
        .header("accept", ARROW)
        .send_json(json!({"sql": sql}))?;
    if !response.status().is_success() {
        return Err(response_error(&mut response)?);
    }
    let (_, body) = response.into_parts();
    Ok(StreamReader::try_new(body.into_reader(), None)?)
}

/// Stores one JSON response as a typed Arrow batch.
struct JsonData {
    batch: RecordBatch,
}

macro_rules! json_table {
    ($name:ident, $kind:literal, [$($type:expr),* $(,)?]) => {
        struct $name;

        impl VTab for $name {
            type BindData = JsonData;
            type InitData = ArrowScan;

            fn bind(bind: &BindInfo) -> Result<JsonData, Box<dyn Error>> {
                let batch = json_batch($kind, bind)?;
                for field in batch.schema().fields() {
                    bind.add_result_column(field.name(), to_duckdb_logical_type_for_field(field)?);
                }
                Ok(JsonData { batch })
            }

            fn init(_: &InitInfo) -> Result<ArrowScan, Box<dyn Error>> {
                Ok(ArrowScan {
                    offset: AtomicUsize::new(0),
                    vector_size: unsafe { duckdb::ffi::duckdb_vector_size() as usize },
                })
            }

            fn func(info: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
                let start = info
                    .get_init_data()
                    .offset
                    .fetch_add(info.get_init_data().vector_size, Ordering::Relaxed);
                let batch = &info.get_bind_data().batch;
                if start >= batch.num_rows() {
                    output.set_len(0);
                } else {
                    record_batch_to_duckdb_data_chunk(
                        &batch.slice(
                            start,
                            (batch.num_rows() - start).min(info.get_init_data().vector_size),
                        ),
                        output,
                    )?;
                }
                Ok(())
            }

            fn parameters() -> Option<Vec<LogicalTypeHandle>> {
                Some(vec![$($type),*])
            }
        }
    };
}

json_table!(
    NeighborsTable,
    "neighbors",
    [
        LogicalTypeId::Varchar.into(),
        LogicalTypeId::Varchar.into(),
        LogicalTypeId::Varchar.into()
    ]
);
json_table!(
    KHopTable,
    "kHop",
    [
        LogicalTypeId::Varchar.into(),
        LogicalTypeId::Varchar.into(),
        LogicalTypeId::Integer.into(),
        LogicalTypeId::Varchar.into()
    ]
);
json_table!(
    PathsTable,
    "paths",
    [
        LogicalTypeId::Varchar.into(),
        LogicalTypeId::Varchar.into(),
        LogicalTypeId::Varchar.into(),
        LogicalTypeId::Integer.into(),
        LogicalTypeId::Varchar.into()
    ]
);
json_table!(
    VectorTable,
    "vector",
    [
        LogicalTypeId::Varchar.into(),
        LogicalTypeId::Varchar.into(),
        LogicalTypeId::Varchar.into(),
        LogicalTypeId::Any.into(),
        LogicalTypeId::Integer.into()
    ]
);

/// Canonical response for a graph neighbor traversal.
#[derive(Deserialize)]
struct NeighborsResponse {
    backend: String,
    #[serde(rename = "snapshotId")]
    snapshot_id: i64,
    neighbors: Vec<NeighborRow>,
}

/// One typed graph neighbor returned by the service.
#[derive(Deserialize)]
struct NeighborRow {
    #[serde(rename = "nodeId")]
    node_id: String,
    predicate: String,
    confidence: f64,
    #[serde(rename = "edgeId")]
    edge_id: String,
    provenance: String,
    direction: String,
}

/// Canonical response for a graph k-hop traversal.
#[derive(Deserialize)]
struct KHopResponse {
    backend: String,
    #[serde(rename = "snapshotId")]
    snapshot_id: i64,
    reached: Vec<ReachedRow>,
}

/// One reached node and its distance from the traversal start.
#[derive(Deserialize)]
struct ReachedRow {
    #[serde(rename = "nodeId")]
    node_id: String,
    hops: i32,
    #[serde(rename = "pathConfidence")]
    path_confidence: f64,
}

/// Canonical response for bounded graph paths.
#[derive(Deserialize)]
struct PathsResponse {
    backend: String,
    #[serde(rename = "snapshotId")]
    snapshot_id: i64,
    paths: Vec<PathRow>,
}

/// One path with its ordered nodes and edges.
#[derive(Deserialize)]
struct PathRow {
    nodes: Vec<String>,
    edges: Vec<PathEdge>,
    confidence: f64,
}

/// One typed edge in a returned graph path.
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct PathEdge {
    src_id: String,
    predicate: String,
    dst_id: String,
    confidence: f64,
    edge_id: String,
    provenance: String,
}

/// Canonical response for a vector-index search.
#[derive(Deserialize)]
struct VectorResponse {
    source: String,
    neighbors: Vec<VectorRow>,
}

/// One vector-search neighbor in server order.
#[derive(Deserialize)]
struct VectorRow {
    id: i64,
    distance: f32,
}

/// A request body whose variant preserves native f32 JSON serialization.
#[derive(Serialize)]
#[serde(untagged)]
enum ApiRequest {
    Json(Value),
    Vector(VectorRequest),
}

/// Canonical vector-search request body.
#[derive(Serialize)]
struct VectorRequest {
    vector: Vec<f32>,
    k: i32,
}

/// Maps the explicit vector resource kind to its canonical API collection.
fn vector_collection(kind: &str) -> Result<&'static str, Box<dyn Error>> {
    match kind {
        "table" => Ok("tables"),
        "graph" => Ok("graphs"),
        _ => Err("vector resource kind must be 'table' or 'graph'".into()),
    }
}

/// Converts canonical graph/vector JSON into the relational result contracts.
fn json_batch(kind: &str, bind: &BindInfo) -> Result<RecordBatch, Box<dyn Error>> {
    let p = |i| bind.get_parameter(i).to_string();
    let s = Settings::read()?;
    let (tail, body): (Vec<String>, ApiRequest) = match kind {
        "neighbors" => (
            vec!["graphs".into(), p(0), "query".into()],
            ApiRequest::Json(json!({"op":"neighbors","start":p(1),"direction":p(2),"filter":{}})),
        ),
        "kHop" => (
            vec!["graphs".into(), p(0), "query".into()],
            ApiRequest::Json(
                json!({"op":"kHop","start":p(1),"direction":p(3),"k":positive_parameter(&bind.get_parameter(2), "k")?,"filter":{}}),
            ),
        ),
        "paths" => (
            vec!["graphs".into(), p(0), "query".into()],
            ApiRequest::Json(
                json!({"op":"paths","start":p(1),"dst":p(2),"maxHops":positive_parameter(&bind.get_parameter(3), "max_hops")?,"direction":p(4),"filter":{}}),
            ),
        ),
        _ => {
            let kind = p(0);
            let resource = vector_collection(&kind)?;
            (
                vec![
                    resource.into(),
                    p(1),
                    "indexes".into(),
                    p(2),
                    "search".into(),
                ],
                ApiRequest::Vector(VectorRequest {
                    vector: vector_parameter(&bind.get_parameter(3))?,
                    k: positive_parameter(&bind.get_parameter(4), "k")?,
                }),
            )
        }
    };
    let refs: Vec<&str> = tail.iter().map(String::as_str).collect();
    let bytes = post(&s, &refs, &body, "application/json")?;
    match kind {
        "neighbors" => rows_neighbors(&serde_json::from_slice(&bytes)?),
        "kHop" => rows_khop(&serde_json::from_slice(&bytes)?),
        "paths" => rows_paths(&serde_json::from_slice(&bytes)?),
        _ => rows_vector(&serde_json::from_slice(&bytes)?),
    }
}
/// Reads a native DuckDB FLOAT list without reparsing its display representation.
fn vector_parameter(value: &duckdb::vtab::Value) -> Result<Vec<f32>, Box<dyn Error>> {
    let values = value
        .to_list()
        .ok_or("vector must be a non-null FLOAT list")?;
    let mut vector = Vec::with_capacity(values.len());
    for value in values {
        if value.logical_type_id() != LogicalTypeId::Float {
            return Err("vector must contain FLOAT values".into());
        }
        let component = value.to_float();
        if !component.is_finite() {
            return Err("vector values must be finite".into());
        }
        vector.push(component);
    }
    if vector.is_empty() {
        return Err("vector must not be empty".into());
    }
    Ok(vector)
}

/// Reads a positive integer after DuckDB has applied the declared INTEGER argument type.
fn positive_parameter(value: &duckdb::vtab::Value, name: &str) -> Result<i32, Box<dyn Error>> {
    let value = value.to_int32();
    if value <= 0 {
        return Err(format!("{name} must be positive").into());
    }
    Ok(value)
}
/// Builds a record batch from named primitive arrays.
fn batch(fields: Vec<(&str, DataType, ArrayRef)>) -> Result<RecordBatch, Box<dyn Error>> {
    let schema = Schema::new(
        fields
            .iter()
            .map(|(n, t, _)| Field::new(*n, t.clone(), false))
            .collect::<Vec<_>>(),
    );
    Ok(RecordBatch::try_new(
        std::sync::Arc::new(schema),
        fields.into_iter().map(|(_, _, a)| a).collect(),
    )?)
}
/// Converts neighbors to their stable relational columns.
fn rows_neighbors(response: &NeighborsResponse) -> Result<RecordBatch, Box<dyn Error>> {
    let rows = &response.neighbors;
    batch(vec![
        (
            "node_id",
            DataType::Utf8,
            std::sync::Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.node_id.as_str())
                    .collect::<Vec<_>>(),
            )),
        ),
        (
            "predicate",
            DataType::Utf8,
            std::sync::Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.predicate.as_str())
                    .collect::<Vec<_>>(),
            )),
        ),
        (
            "confidence",
            DataType::Float64,
            std::sync::Arc::new(arrow::array::Float64Array::from(
                rows.iter().map(|row| row.confidence).collect::<Vec<_>>(),
            )),
        ),
        (
            "edge_id",
            DataType::Utf8,
            std::sync::Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.edge_id.as_str())
                    .collect::<Vec<_>>(),
            )),
        ),
        (
            "provenance",
            DataType::Utf8,
            std::sync::Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.provenance.as_str())
                    .collect::<Vec<_>>(),
            )),
        ),
        (
            "direction",
            DataType::Utf8,
            std::sync::Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.direction.as_str())
                    .collect::<Vec<_>>(),
            )),
        ),
        (
            "backend",
            DataType::Utf8,
            std::sync::Arc::new(StringArray::from(vec![
                response.backend.as_str();
                rows.len()
            ])),
        ),
        (
            "snapshot_id",
            DataType::Int64,
            std::sync::Arc::new(Int64Array::from(vec![response.snapshot_id; rows.len()])),
        ),
    ])
}
/// Converts k-hop rows to their stable relational columns.
fn rows_khop(response: &KHopResponse) -> Result<RecordBatch, Box<dyn Error>> {
    let rows = &response.reached;
    batch(vec![
        (
            "node_id",
            DataType::Utf8,
            std::sync::Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.node_id.as_str())
                    .collect::<Vec<_>>(),
            )),
        ),
        (
            "hops",
            DataType::Int32,
            std::sync::Arc::new(Int32Array::from(
                rows.iter().map(|row| row.hops).collect::<Vec<_>>(),
            )),
        ),
        (
            "path_confidence",
            DataType::Float64,
            std::sync::Arc::new(Float64Array::from(
                rows.iter()
                    .map(|row| row.path_confidence)
                    .collect::<Vec<_>>(),
            )),
        ),
        (
            "backend",
            DataType::Utf8,
            std::sync::Arc::new(StringArray::from(vec![
                response.backend.as_str();
                rows.len()
            ])),
        ),
        (
            "snapshot_id",
            DataType::Int64,
            std::sync::Arc::new(Int64Array::from(vec![response.snapshot_id; rows.len()])),
        ),
    ])
}
/// Converts path rows without dropping their ordered edge payloads.
fn rows_paths(response: &PathsResponse) -> Result<RecordBatch, Box<dyn Error>> {
    let rows = &response.paths;
    let mut nodes = ListBuilder::new(StringBuilder::new());
    for path in rows {
        for node in &path.nodes {
            nodes.values().append_value(node);
        }
        nodes.append(true);
    }
    batch(vec![
        (
            "nodes",
            DataType::List(std::sync::Arc::new(Field::new(
                "item",
                DataType::Utf8,
                true,
            ))),
            std::sync::Arc::new(nodes.finish()),
        ),
        (
            "edges_json",
            DataType::Utf8,
            std::sync::Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| serde_json::to_string(&row.edges))
                    .collect::<Result<Vec<_>, _>>()?,
            )),
        ),
        (
            "confidence",
            DataType::Float64,
            std::sync::Arc::new(Float64Array::from(
                rows.iter().map(|row| row.confidence).collect::<Vec<_>>(),
            )),
        ),
        (
            "backend",
            DataType::Utf8,
            std::sync::Arc::new(StringArray::from(vec![
                response.backend.as_str();
                rows.len()
            ])),
        ),
        (
            "snapshot_id",
            DataType::Int64,
            std::sync::Arc::new(Int64Array::from(vec![response.snapshot_id; rows.len()])),
        ),
    ])
}
/// Converts vector neighbors to their stable relational columns.
fn rows_vector(response: &VectorResponse) -> Result<RecordBatch, Box<dyn Error>> {
    let rows = &response.neighbors;
    batch(vec![
        (
            "id",
            DataType::Int64,
            std::sync::Arc::new(Int64Array::from(
                rows.iter().map(|row| row.id).collect::<Vec<_>>(),
            )),
        ),
        (
            "distance",
            DataType::Float32,
            std::sync::Arc::new(Float32Array::from(
                rows.iter().map(|row| row.distance).collect::<Vec<_>>(),
            )),
        ),
        (
            "source",
            DataType::Utf8,
            std::sync::Arc::new(StringArray::from(vec![
                response.source.as_str();
                rows.len()
            ])),
        ),
    ])
}

/// Registers the five public table functions.
#[duckdb_entrypoint_c_api]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<ArrowData>("verglas_query")?;
    con.register_table_function::<NeighborsTable>("verglas_graph_neighbors")?;
    con.register_table_function::<KHopTable>("verglas_graph_k_hop")?;
    con.register_table_function::<PathsTable>("verglas_graph_paths")?;
    con.register_table_function::<VectorTable>("verglas_vector_search")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verifies dynamic route segments cannot escape their database resource boundary.
    #[test]
    fn path_segments_use_url_path_encoding() -> Result<(), Box<dyn Error>> {
        let settings = Settings {
            endpoint: url::Url::parse("https://example.com/base/")?,
            database: "a b/c?d".to_owned(),
            token: "secret".to_owned(),
        };
        assert_eq!(
            settings.url(&["graphs", "knowledge graph"])?.as_str(),
            "https://example.com/base/v1/databases/a%20b%2Fc%3Fd/graphs/knowledge%20graph"
        );
        Ok(())
    }

    /// Verifies malformed graph responses fail instead of producing guessed null columns.
    #[test]
    fn graph_response_requires_every_canonical_field() {
        let missing_edge_id = br#"{
          "backend":"index","snapshotId":9,
          "neighbors":[{"nodeId":"b","predicate":"knows","confidence":0.9,
            "provenance":"m1","direction":"out"}]
        }"#;
        assert!(serde_json::from_slice::<NeighborsResponse>(missing_edge_id).is_err());
    }

    /// Verifies path edges survive conversion even though DuckDB exposes them as JSON text.
    #[test]
    fn path_rows_preserve_ordered_edges() -> Result<(), Box<dyn Error>> {
        let response: PathsResponse = serde_json::from_str(
            r#"{
              "backend":"scan","snapshotId":33,
              "paths":[{"nodes":["a","z"],"edges":[{
                "srcId":"a","predicate":"knows","dstId":"z","confidence":0.6,
                "edgeId":"e1","provenance":"source"
              }],"confidence":0.6}]
            }"#,
        )?;
        let batch = rows_paths(&response)?;
        let edges = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("edges_json must be a string array")?;
        assert_eq!(
            edges.value(0),
            r#"[{"srcId":"a","predicate":"knows","dstId":"z","confidence":0.6,"edgeId":"e1","provenance":"source"}]"#
        );
        Ok(())
    }

    /// Verifies vector response numbers keep their declared integer and float types.
    #[test]
    fn vector_rows_are_strictly_typed() -> Result<(), Box<dyn Error>> {
        let response: VectorResponse =
            serde_json::from_str(r#"{"source":"index","neighbors":[{"id":7,"distance":0.125}]}"#)?;
        let batch = rows_vector(&response)?;
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Int64);
        assert_eq!(batch.schema().field(1).data_type(), &DataType::Float32);
        assert!(
            serde_json::from_str::<VectorResponse>(
                r#"{"source":"index","neighbors":[{"id":"7","distance":0.125}]}"#
            )
            .is_err()
        );
        Ok(())
    }

    /// Verifies vector search distinguishes table and graph index routes explicitly.
    #[test]
    fn vector_resource_kind_selects_canonical_collection() -> Result<(), Box<dyn Error>> {
        assert_eq!(vector_collection("table")?, "tables");
        assert_eq!(vector_collection("graph")?, "graphs");
        assert!(vector_collection("memory").is_err());
        Ok(())
    }
}
