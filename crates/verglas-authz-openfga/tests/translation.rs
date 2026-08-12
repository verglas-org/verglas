//! OpenFGA wire translation tests against a bounded mock service.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use axum::extract::{Path, State};
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use verglas_authz::{AccessCheck, Action, Grant, PolicyEngine};
use verglas_authz_openfga::{OpenFgaConfig, OpenFgaPolicyEngine};

type CapturedRequests = Arc<Mutex<Vec<(String, Value)>>>;

#[tokio::test]
async fn expands_grants_and_pins_check_model() {
    let requests = Arc::new(Mutex::new(Vec::<(String, Value)>::new()));
    let app = Router::new()
        .route(
            "/stores/{store}/{operation}",
            post(
                |State(requests): State<CapturedRequests>,
                 Path((_store, operation)): Path<(String, String)>,
                 Json(body): Json<Value>| async move {
                    requests
                        .lock()
                        .expect("request lock")
                        .push((operation.clone(), body));
                    Json(json!({"allowed": true}))
                },
            ),
        )
        .with_state(requests.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let endpoint = format!("http://{}", listener.local_addr().expect("address"));
    tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });

    let engine = OpenFgaPolicyEngine::new(
        OpenFgaConfig::new(endpoint, "store-1", "model-1", "secret").expect("config"),
    )
    .expect("engine");
    engine
        .write_grant(&Grant::new(
            "grant-1",
            "tenant-a",
            "job-1",
            "db-1",
            BTreeSet::from([Action::Query]),
        ))
        .await
        .expect("write");
    assert!(
        engine
            .check(&AccessCheck::new(
                "tenant-a",
                "job-1",
                "db-1",
                Action::Query,
            ))
            .await
            .expect("check")
    );

    let captured = requests.lock().expect("request lock");
    assert_eq!(captured[0].0, "write");
    let tuples = captured[0].1["writes"]["tuple_keys"]
        .as_array()
        .expect("tuples");
    assert!(tuples.iter().any(|tuple| tuple["relation"] == "query"));
    assert!(tuples.iter().any(|tuple| tuple["relation"] == "describe"));
    assert_eq!(captured[1].1["authorization_model_id"], "model-1");
}

#[tokio::test]
async fn translates_connect_grants_and_checks_to_connect_relation() {
    let requests = Arc::new(Mutex::new(Vec::<(String, Value)>::new()));
    let app = Router::new()
        .route(
            "/stores/{store}/{operation}",
            post(
                |State(requests): State<CapturedRequests>,
                 Path((_store, operation)): Path<(String, String)>,
                 Json(body): Json<Value>| async move {
                    requests
                        .lock()
                        .expect("request lock")
                        .push((operation, body));
                    Json(json!({"allowed": true}))
                },
            ),
        )
        .with_state(requests.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let endpoint = format!("http://{}", listener.local_addr().expect("address"));
    tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });

    let engine = OpenFgaPolicyEngine::new(
        OpenFgaConfig::new(endpoint, "store-1", "model-1", "secret").expect("config"),
    )
    .expect("engine");
    engine
        .write_grant(&Grant::new(
            "grant-1",
            "tenant-a",
            "user-1",
            "database-1",
            BTreeSet::from([Action::Connect]),
        ))
        .await
        .expect("write");
    assert!(
        engine
            .check(&AccessCheck::new(
                "tenant-a",
                "user-1",
                "database-1",
                Action::Connect,
            ))
            .await
            .expect("check")
    );

    let captured = requests.lock().expect("request lock");
    let tuples = captured[0].1["writes"]["tuple_keys"]
        .as_array()
        .expect("tuples");
    assert!(tuples.iter().any(|tuple| tuple["relation"] == "connect"));
    assert_eq!(captured[1].1["tuple_key"]["relation"], "connect");
}
