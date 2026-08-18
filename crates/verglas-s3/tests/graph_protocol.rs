//! Endpoint tests for the Verglas Graph REST-JSON contract.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use tower::ServiceExt;
use verglas_s3::semantic::{SemanticApi, SemanticError, SemanticOperation, router};

/// Loads the checked-in Graph REST-JSON service model.
fn graph_contract() -> Value {
    serde_json::from_str(include_str!("../models/verglas-graphs-service-1.json"))
        .expect("Graph contract is valid JSON")
}

/// Resolves a named shape from the service model.
fn shape<'a>(contract: &'a Value, name: &str) -> &'a Value {
    &contract["shapes"][name]
}

/// Checks that every nested shape reference resolves to a declared shape.
fn assert_shape_references_resolve(contract: &Value, definition: &Value) {
    match definition["type"].as_str().expect("shape type") {
        "structure" => {
            for member in definition["members"]
                .as_object()
                .expect("structure members")
                .values()
            {
                let referenced = shape(
                    contract,
                    member["shape"].as_str().expect("member shape reference"),
                );
                assert!(referenced.is_object(), "declared member shape");
                assert_shape_references_resolve(contract, referenced);
            }
        }
        "list" => {
            let referenced = shape(
                contract,
                definition["member"]["shape"]
                    .as_str()
                    .expect("list member shape reference"),
            );
            assert!(referenced.is_object(), "declared list member shape");
            assert_shape_references_resolve(contract, referenced);
        }
        "map" => {
            let key = shape(
                contract,
                definition["key"]["shape"]
                    .as_str()
                    .expect("map key shape reference"),
            );
            assert!(key.is_object(), "declared map key shape");
            assert_shape_references_resolve(contract, key);
            let value = shape(
                contract,
                definition["value"]["shape"]
                    .as_str()
                    .expect("map value shape reference"),
            );
            assert!(value.is_object(), "declared map value shape");
            assert_shape_references_resolve(contract, value);
        }
        "string" | "integer" | "long" | "double" | "boolean" | "document" => {}
        other => panic!("unsupported contract shape type: {other}"),
    }
}

/// Validates the JSON subset used by the checked-in botocore-style model.
///
/// The service model is deliberately data, not generated Rust. This small test
/// validator makes representative payloads prove their names, required fields,
/// container members, enums, and numeric bounds against that data.
fn assert_matches_shape(contract: &Value, shape_name: &str, value: &Value) {
    let definition = shape(contract, shape_name);
    match definition["type"].as_str().expect("shape type") {
        "structure" => {
            let object = value.as_object().expect("structure value");
            for required in definition["required"].as_array().into_iter().flatten() {
                let member = required.as_str().expect("required member name");
                assert!(
                    object.contains_key(member),
                    "{shape_name}.{member} is required"
                );
            }
            for (member, member_definition) in definition["members"]
                .as_object()
                .expect("structure members")
            {
                if let Some(member_value) = object.get(member) {
                    assert_matches_shape(
                        contract,
                        member_definition["shape"].as_str().expect("member shape"),
                        member_value,
                    );
                }
            }
        }
        "list" => {
            for item in value.as_array().expect("list value") {
                assert_matches_shape(
                    contract,
                    definition["member"]["shape"]
                        .as_str()
                        .expect("list member shape"),
                    item,
                );
            }
        }
        "map" => {
            for (key, item) in value.as_object().expect("map value") {
                assert_matches_shape(
                    contract,
                    definition["key"]["shape"].as_str().expect("map key shape"),
                    &Value::String(key.to_owned()),
                );
                assert_matches_shape(
                    contract,
                    definition["value"]["shape"]
                        .as_str()
                        .expect("map value shape"),
                    item,
                );
            }
        }
        "document" => {}
        "string" => {
            let string = value.as_str().expect("string value");
            if let Some(minimum) = definition["min"].as_u64() {
                assert!(string.len() >= minimum as usize, "string minimum");
            }
            if let Some(values) = definition["enum"].as_array() {
                assert!(
                    values
                        .iter()
                        .any(|candidate| candidate.as_str() == Some(string)),
                    "string enum"
                );
            }
        }
        "integer" | "long" => {
            let number = value.as_i64().expect("integer value");
            if let Some(minimum) = definition["min"].as_i64() {
                assert!(number >= minimum, "integer minimum");
            }
        }
        "double" => {
            let number = value.as_f64().expect("double value");
            if let Some(minimum) = definition["min"].as_f64() {
                assert!(number >= minimum, "double minimum");
            }
            if let Some(maximum) = definition["max"].as_f64() {
                assert!(number <= maximum, "double maximum");
            }
        }
        "boolean" => assert!(value.is_boolean(), "boolean value"),
        other => panic!("unsupported contract shape type: {other}"),
    }
}

/// Every public endpoint has botocore-style shape and error bindings.
#[test]
fn graph_contract_is_complete_and_self_consistent() {
    let contract = graph_contract();
    let metadata = &contract["metadata"];
    assert_eq!(metadata["apiVersion"], "2026-08-13");
    assert_eq!(metadata["endpointPrefix"], "verglas-graphs");
    assert_eq!(metadata["protocol"], "rest-json");
    assert_eq!(metadata["serviceFullName"], "Verglas Graphs");
    assert_eq!(metadata["serviceId"], "VerglasGraphs");
    assert_eq!(metadata["signatureVersion"], "v4");
    assert_eq!(metadata["signingName"], "verglasgraphs");

    let operations = contract["operations"]
        .as_object()
        .expect("operations object");
    assert_eq!(operations.len(), 12);
    for (operation_name, operation) in operations {
        assert_eq!(operation["http"]["method"], "POST", "{operation_name}");
        assert_eq!(
            operation["http"]["requestUri"],
            format!("/{operation_name}")
        );
        assert_eq!(operation["http"]["responseCode"], 200, "{operation_name}");
        assert!(
            shape(
                &contract,
                operation["input"]["shape"].as_str().expect("input shape")
            )
            .is_object(),
            "{operation_name} input shape"
        );
        assert!(
            shape(
                &contract,
                operation["output"]["shape"].as_str().expect("output shape")
            )
            .is_object(),
            "{operation_name} output shape"
        );
        assert!(
            operation["errors"]
                .as_array()
                .is_some_and(|errors| !errors.is_empty()),
            "{operation_name} errors"
        );
    }
    for definition in contract["shapes"]
        .as_object()
        .expect("shapes object")
        .values()
    {
        assert_shape_references_resolve(&contract, definition);
    }

    for shape_name in [
        "GraphName",
        "Node",
        "Edge",
        "TraversalFilter",
        "Direction",
        "Neighbor",
        "ReachedNode",
        "Subgraph",
        "Path",
        "PaginationToken",
        "ValidationException",
        "ServiceUnavailableException",
    ] {
        assert!(
            shape(&contract, shape_name).is_object(),
            "{shape_name} shape"
        );
    }
    assert_eq!(
        shape(&contract, "Direction")["enum"],
        json!(["out", "in", "both"])
    );
    assert_eq!(shape(&contract, "Confidence")["min"], 0.0);
    assert_eq!(shape(&contract, "Confidence")["max"], 1.0);
    assert_eq!(
        shape(&contract, "ListGraphsInput")["members"]["maxResults"]["shape"],
        "MaxResults"
    );
    assert_eq!(
        shape(&contract, "ListGraphsInput")["members"]["nextToken"]["shape"],
        "PaginationToken"
    );
    assert_eq!(
        shape(&contract, "ListGraphsOutput")["members"]["nextToken"]["shape"],
        "PaginationToken"
    );
}

/// Representative wire payloads round-trip through JSON and conform to their
/// declared request or response shape.
#[test]
fn graph_contract_representative_payloads_round_trip() {
    let contract = graph_contract();
    let samples = [
        (
            "PutNodesInput",
            json!({
                "graphName": "knowledge",
                "nodes": [{"id": "alice", "labels": ["Person"], "properties": {"age": 42}}]
            }),
        ),
        (
            "PutEdgesInput",
            json!({
                "graphName": "knowledge",
                "edges": [{
                    "sourceId": "alice", "predicate": "knows", "targetId": "bob",
                    "provenance": "memory-17", "edgeId": "edge-17", "confidence": 0.8,
                    "properties": {"since": 2024}
                }]
            }),
        ),
        (
            "QueryNeighborhoodInput",
            json!({
                "graphName": "knowledge", "nodeId": "alice", "k": 2, "direction": "both",
                "filter": {"predicate": "knows", "minConfidence": 0.5}
            }),
        ),
        (
            "QueryNeighborhoodOutput",
            json!({
                "neighborhood": {
                    "nodes": [{"nodeId": "alice", "hops": 0, "pathConfidence": 1.0}],
                    "edges": [{
                        "sourceId": "alice", "predicate": "knows", "targetId": "bob",
                        "confidence": 0.8, "edgeId": "edge-17", "provenance": "memory-17"
                    }]
                }
            }),
        ),
        (
            "ListGraphsOutput",
            json!({"graphs": [{"graphName": "knowledge"}], "nextToken": "page-2"}),
        ),
    ];

    for (shape_name, payload) in samples {
        let round_tripped: Value = serde_json::from_str(
            &serde_json::to_string(&payload).expect("serialize representative payload"),
        )
        .expect("deserialize representative payload");
        assert_eq!(round_tripped, payload, "{shape_name}");
        assert_matches_shape(&contract, shape_name, &round_tripped);
    }
}

/// Captures graph requests at the engine boundary.
struct RecordingGraphApi(Mutex<Vec<SemanticOperation>>);

#[async_trait]
impl SemanticApi for RecordingGraphApi {
    /// Rejects accidental S3-vector dispatches and records only Graph operations.
    async fn call(
        &self,
        operation: SemanticOperation,
        _input: Value,
    ) -> Result<Value, SemanticError> {
        if !matches!(operation, SemanticOperation::Graph(_)) {
            return Err(SemanticError::validation("expected graph operation"));
        }
        self.0
            .lock()
            .map_err(|error| SemanticError::unavailable(error.to_string()))?
            .push(operation);
        Ok(json!({"ok": true}))
    }
}

/// The public Graph verbs all dispatch through one REST-JSON adapter.
#[tokio::test]
async fn graph_contract_operations_reach_the_engine_adapter() {
    let api = Arc::new(RecordingGraphApi(Mutex::new(Vec::new())));
    let app = router(api.clone());
    for operation in [
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
        "QueryPrecedents",
        "BuildGraphIndex",
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::post(format!("/{operation}"))
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .expect("request"),
            )
            .await
            .expect("route response");
        assert_eq!(response.status(), StatusCode::OK, "{operation}");
    }
    assert_eq!(api.0.lock().expect("recording lock").len(), 12);
}
