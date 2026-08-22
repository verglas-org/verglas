//! In-process acceptance for `SearchPrecedents` (#148): BM25 lexical ranking of
//! `Decision` nodes plus an entity-overlap structural boost, driven through
//! the real Graph REST-JSON router against an Iceberg `MemoryCatalog`.

use std::{collections::HashMap, sync::Arc};

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::{Catalog, CatalogBuilder};
use serde_json::{Value, json};
use tower::ServiceExt;
use verglas_s3::semantic::{IcebergCatalogSemanticStore, router};

/// Opens a fresh, empty Iceberg memory catalog backed by a temp warehouse.
async fn catalog() -> Arc<dyn Catalog> {
    let warehouse = tempfile::tempdir().expect("warehouse").keep();
    Arc::new(
        MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_owned(),
                    format!("file://{}", warehouse.display()),
                )]),
            )
            .await
            .expect("catalog"),
    )
}

/// Builds the real semantic router over a fresh in-process catalog.
async fn app() -> axum::Router {
    router(Arc::new(IcebergCatalogSemanticStore::new(catalog().await)))
}

/// Posts one Graph REST-JSON operation through the router and returns the
/// decoded JSON response, asserting a 200.
async fn post(app: &axum::Router, operation: &str, body: Value) -> Value {
    let response = app
        .clone()
        .oneshot(
            Request::post(format!("/{operation}"))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .expect("request"),
        )
        .await
        .expect("route response");
    assert_eq!(response.status(), StatusCode::OK, "{operation}");
    let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("response body");
    serde_json::from_slice(&bytes).expect("response json")
}

/// Posts one Graph REST-JSON operation and returns the raw response bytes, for
/// byte-identical comparisons across repeated calls.
async fn post_raw(app: &axum::Router, operation: &str, body: Value) -> Vec<u8> {
    let response = app
        .clone()
        .oneshot(
            Request::post(format!("/{operation}"))
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .expect("request"),
        )
        .await
        .expect("route response");
    assert_eq!(response.status(), StatusCode::OK, "{operation}");
    axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("response body")
        .to_vec()
}

/// Seeds three Decision nodes with distinct objectives and two entity nodes,
/// with `decision:alpha` and `decision:beta` each owned by a different entity.
async fn seed_ranking_graph(app: &axum::Router, graph_name: &str) {
    post(app, "CreateGraph", json!({"graphName": graph_name})).await;
    post(
        app,
        "AddNodes",
        json!({
            "graphName": graph_name,
            "nodes": [
                {"id": "decision:alpha", "labels": ["Decision"], "properties": {"objective": "adopt vector search for retrieval"}},
                {"id": "decision:beta", "labels": ["Decision"], "properties": {"objective": "roll back the cache eviction policy"}},
                {"id": "decision:gamma", "labels": ["Decision"], "properties": {"objective": "improve retrieval latency for the search index"}},
                {"id": "entity:team-search", "labels": ["Team"]},
                {"id": "entity:team-cache", "labels": ["Team"]}
            ]
        }),
    )
    .await;
    post(
        app,
        "AddEdges",
        json!({
            "graphName": graph_name,
            "edges": [
                {"sourceId": "decision:alpha", "predicate": "owned_by", "targetId": "entity:team-search", "provenance": "test"},
                {"sourceId": "decision:beta", "predicate": "owned_by", "targetId": "entity:team-cache", "provenance": "test"}
            ]
        }),
    )
    .await;
}

/// (a) Ranking: a query that shares all three of `decision:alpha`'s objective
/// terms, two of `decision:gamma`'s, and none of `decision:beta`'s must return
/// exactly that order and expose the underlying scores.
#[tokio::test]
async fn ranking_orders_decisions_by_lexical_match_to_the_query() {
    let app = app().await;
    seed_ranking_graph(&app, "rankings").await;

    let output = post(
        &app,
        "SearchPrecedents",
        json!({"graphName": "rankings", "query": "vector search retrieval"}),
    )
    .await;
    let precedents = output["precedents"].as_array().expect("precedents array");
    let ids: Vec<&str> = precedents
        .iter()
        .map(|p| p["decisionId"].as_str().expect("decisionId"))
        .collect();
    assert_eq!(
        ids,
        vec!["decision:alpha", "decision:gamma", "decision:beta"],
        "unexpected ranking order: {precedents:#?}"
    );

    let alpha = &precedents[0];
    assert!(alpha["score"].as_f64().expect("score") > 0.0);
    assert_eq!(
        alpha["score"], alpha["lexicalScore"],
        "no entities means score is purely lexical"
    );
    assert_eq!(alpha["structuralScore"], 0.0);
    assert_eq!(alpha["objective"], "adopt vector search for retrieval");
    assert!(
        alpha["sharedEntities"]
            .as_array()
            .expect("sharedEntities")
            .is_empty()
    );

    let beta = &precedents[2];
    assert_eq!(
        beta["lexicalScore"], 0.0,
        "no query terms overlap beta's objective"
    );
}

/// Seeds two Decision nodes with identical objectives (so lexical scores tie
/// and the ordering falls back to `decisionId` ascending) but different
/// entity neighbors, so supplying `entities` can flip which one leads.
async fn seed_tie_graph(app: &axum::Router, graph_name: &str) {
    post(app, "CreateGraph", json!({"graphName": graph_name})).await;
    post(
        app,
        "AddNodes",
        json!({
            "graphName": graph_name,
            "nodes": [
                {"id": "decision:a", "labels": ["Decision"], "properties": {"objective": "migrate the billing pipeline"}},
                {"id": "decision:b", "labels": ["Decision"], "properties": {"objective": "migrate the billing pipeline"}},
                {"id": "entity:x", "labels": ["Team"]},
                {"id": "entity:y", "labels": ["Team"]}
            ]
        }),
    )
    .await;
    post(
        app,
        "AddEdges",
        json!({
            "graphName": graph_name,
            "edges": [
                {"sourceId": "decision:a", "predicate": "owned_by", "targetId": "entity:y", "provenance": "test"},
                {"sourceId": "decision:b", "predicate": "owned_by", "targetId": "entity:x", "provenance": "test"}
            ]
        }),
    )
    .await;
}

/// (b) Entity boost: with tied lexical scores the tie-break (decisionId
/// ascending) puts `decision:a` first; supplying `entities: ["entity:x"]`
/// (a neighbor only of `decision:b`) must deterministically flip the order.
#[tokio::test]
async fn supplying_entities_flips_the_ranking_deterministically() {
    let app = app().await;
    seed_tie_graph(&app, "tie-break").await;

    let unboosted = post(
        &app,
        "SearchPrecedents",
        json!({"graphName": "tie-break", "query": "migrate billing pipeline"}),
    )
    .await;
    let unboosted_ids: Vec<&str> = unboosted["precedents"]
        .as_array()
        .expect("precedents array")
        .iter()
        .map(|p| p["decisionId"].as_str().expect("decisionId"))
        .collect();
    assert_eq!(
        unboosted_ids,
        vec!["decision:a", "decision:b"],
        "tied lexical scores must break by decisionId ascending"
    );

    let boosted = post(
        &app,
        "SearchPrecedents",
        json!({"graphName": "tie-break", "query": "migrate billing pipeline", "entities": ["entity:x"]}),
    )
    .await;
    let boosted_precedents = boosted["precedents"].as_array().expect("precedents array");
    let boosted_ids: Vec<&str> = boosted_precedents
        .iter()
        .map(|p| p["decisionId"].as_str().expect("decisionId"))
        .collect();
    assert_eq!(
        boosted_ids,
        vec!["decision:b", "decision:a"],
        "the entity-boosted decision must now lead: {boosted_precedents:#?}"
    );
    assert_eq!(boosted_precedents[0]["structuralScore"], 1.0);
    assert_eq!(boosted_precedents[0]["sharedEntities"], json!(["entity:x"]));
    assert_eq!(boosted_precedents[1]["structuralScore"], 0.0);
}

/// (c) Determinism: two identical `SearchPrecedents` calls against unchanged
/// graph state return byte-identical response bodies.
#[tokio::test]
async fn identical_requests_against_unchanged_state_return_byte_identical_responses() {
    let app = app().await;
    seed_ranking_graph(&app, "determinism").await;

    let request =
        json!({"graphName": "determinism", "query": "vector search retrieval", "limit": 2});
    let first = post_raw(&app, "SearchPrecedents", request.clone()).await;
    let second = post_raw(&app, "SearchPrecedents", request).await;
    assert_eq!(
        first, second,
        "identical requests must return byte-identical bodies"
    );

    let decoded: Value = serde_json::from_slice(&first).expect("response json");
    assert_eq!(
        decoded["precedents"]
            .as_array()
            .expect("precedents array")
            .len(),
        2,
        "limit must cap the returned precedent count"
    );
}

/// A node without the `Decision` label, or missing the `objective` property,
/// is excluded from precedent search rather than erroring the whole request.
#[tokio::test]
async fn non_decision_and_objective_less_nodes_are_excluded() {
    let app = app().await;
    let graph_name = "precedents-filtering";
    post(&app, "CreateGraph", json!({"graphName": graph_name})).await;
    post(
        &app,
        "AddNodes",
        json!({
            "graphName": graph_name,
            "nodes": [
                {"id": "decision:only-one", "labels": ["Decision"], "properties": {"objective": "cache warming"}},
                {"id": "decision:no-objective", "labels": ["Decision"], "properties": {}},
                {"id": "entity:not-a-decision", "labels": ["Entity"], "properties": {"objective": "cache warming"}}
            ]
        }),
    )
    .await;

    let output = post(
        &app,
        "SearchPrecedents",
        json!({"graphName": graph_name, "query": "cache warming"}),
    )
    .await;
    let precedents = output["precedents"].as_array().expect("precedents array");
    assert_eq!(precedents.len(), 1, "{precedents:?}");
    assert_eq!(precedents[0]["decisionId"], "decision:only-one");
}
