//! End-to-end HTTP smoke of the vector-index routes.
//!
//! The vector routes are catalog-gated, and wiring a *process* server's loopback
//! catalog needs a real Iceberg REST catalog + S3 backend (the same infra
//! `verglas dev` needs) — not hermetic. So this drives the REAL `admin::router`
//! (with a filled `VectorSlot` over an in-process `MemoryCatalog`) over REAL
//! HTTP on a kernel-assigned high port, the same technique the server's other
//! HTTP tests use. Nothing but this test's own server task is served on that
//! port; no OS process, no installed server, is touched.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use arrow_array::builder::{Float32Builder, ListBuilder};
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};

use verglas_server::VERSION;
use verglas_server::admin::{self, Health, Slots, VectorRuntime, VectorSlot};

fn table_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "embedding",
            DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
            true,
        ),
    ]))
}

fn batch(rows: &[(i64, Vec<f32>)]) -> RecordBatch {
    let ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
    let mut lb = ListBuilder::new(Float32Builder::new());
    for (_, v) in rows {
        for x in v {
            lb.values().append_value(*x);
        }
        lb.append(true);
    }
    RecordBatch::try_new(
        table_schema(),
        vec![Arc::new(Int64Array::from(ids)), Arc::new(lb.finish())],
    )
    .expect("batch")
}

#[tokio::test]
async fn declare_then_search_over_http() {
    // A catalog + a table with a clear 2-D corpus: each point sits on the unit
    // grid so the nearest neighbor of a query is obvious.
    let dir = tempfile::tempdir().expect("tempdir");
    let catalog: Arc<dyn Catalog> = Arc::new(
        MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    format!("file://{}", dir.path().display()),
                )]),
            )
            .await
            .expect("catalog"),
    );
    catalog
        .create_namespace(&NamespaceIdent::new("default".into()), HashMap::new())
        .await
        .expect("ns");
    let ident = TableIdent::from_strs(["default", "docs"]).expect("ident");
    verglas_iceberg::write::create_table_from_schema(
        catalog.as_ref(),
        &ident,
        &table_schema(),
        None,
    )
    .await
    .expect("create");
    let corpus = vec![
        (1i64, vec![0.0, 0.0]),
        (2, vec![1.0, 0.0]),
        (3, vec![0.0, 1.0]),
        (4, vec![1.0, 1.0]),
        (5, vec![0.5, 0.5]),
    ];
    verglas_iceberg::write::append_batches(
        catalog.as_ref(),
        &ident,
        vec![batch(&corpus)],
        HashMap::new(),
    )
    .await
    .expect("append");

    // Fill the vector slot with the real attachment service and build the admin
    // router.
    let service = Arc::new(verglas_vector::service::VectorService::new());
    let slot: VectorSlot = Arc::new(OnceLock::new());
    slot.set(Arc::new(VectorRuntime {
        catalog: catalog.clone(),
        service,
    }))
    .ok();
    let app = admin::router(
        VERSION,
        Health::ready(),
        Slots {
            vector: Some(slot),
            ..Slots::default()
        },
    );

    // Serve on a kernel-assigned high port.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    let base = format!("http://{addr}");
    let http = reqwest::Client::new();

    // No compatibility path: without an exact attachment the search fails.
    let missing = http
        .post(format!(
            "{base}/v1/tables/default.docs/indexes/embedding/search"
        ))
        .json(&serde_json::json!({ "vector": [0.1, 0.9], "k": 1 }))
        .send()
        .await
        .expect("missing send");
    assert_eq!(missing.status(), reqwest::StatusCode::NOT_FOUND);

    // Declare an index on the embedding field.
    let declare: serde_json::Value = http
        .post(format!("{base}/v1/tables/default.docs/indexes"))
        .json(&serde_json::json!({ "field": "embedding", "metric": "l2" }))
        .send()
        .await
        .expect("declare send")
        .json()
        .await
        .expect("declare json");
    assert_eq!(declare["field"], "embedding");
    assert_eq!(declare["metric"], "l2");
    assert_eq!(declare["liveCount"], 5);
    assert_eq!(declare["fullBuild"], true);

    // Now served from the index. Search near (0.9, 0.1): nearest is id 2 at
    // (1, 0).
    let search: serde_json::Value = http
        .post(format!(
            "{base}/v1/tables/default.docs/indexes/embedding/search"
        ))
        .json(&serde_json::json!({ "vector": [0.9, 0.1], "k": 3 }))
        .send()
        .await
        .expect("search send")
        .json()
        .await
        .expect("search json");
    assert_eq!(search["source"], "index");
    let neighbors = search["neighbors"].as_array().expect("neighbors");
    assert_eq!(neighbors.len(), 3);
    assert_eq!(neighbors[0]["id"], 2, "nearest to (0.9,0.1) should be id 2");
    let d0 = neighbors[0]["distance"].as_f64().expect("d0");
    let d1 = neighbors[1]["distance"].as_f64().expect("d1");
    assert!(d0 <= d1, "distances are nearest-first");

    // The index appears in the list.
    let list: serde_json::Value = http
        .get(format!("{base}/v1/tables/default.docs/indexes"))
        .send()
        .await
        .expect("list send")
        .json()
        .await
        .expect("list json");
    assert_eq!(list["indexes"][0]["field"], "embedding");
    assert_eq!(list["indexes"][0]["liveCount"], 5);

    server.abort();
}

/// Builds a table with a fixed 2-D corpus in a fresh catalog, returning the
/// catalog and the ident.
async fn corpus_table(dir: &std::path::Path) -> (Arc<dyn Catalog>, TableIdent) {
    let catalog: Arc<dyn Catalog> = Arc::new(
        MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_string(),
                    format!("file://{}", dir.display()),
                )]),
            )
            .await
            .expect("catalog"),
    );
    catalog
        .create_namespace(&NamespaceIdent::new("default".into()), HashMap::new())
        .await
        .expect("ns");
    let ident = TableIdent::from_strs(["default", "docs"]).expect("ident");
    verglas_iceberg::write::create_table_from_schema(
        catalog.as_ref(),
        &ident,
        &table_schema(),
        None,
    )
    .await
    .expect("create");
    let corpus = vec![
        (1i64, vec![0.0, 0.0]),
        (2, vec![1.0, 0.0]),
        (3, vec![0.0, 1.0]),
        (4, vec![1.0, 1.0]),
        (5, vec![0.5, 0.5]),
    ];
    verglas_iceberg::write::append_batches(
        catalog.as_ref(),
        &ident,
        vec![batch(&corpus)],
        HashMap::new(),
    )
    .await
    .expect("append");
    (catalog, ident)
}

/// Serves a router for `runtime` on a kernel-assigned high port, returning the
/// base URL and the server task handle (only this test's task uses the port).
fn serve(runtime: Arc<VectorRuntime>) -> (String, tokio::task::JoinHandle<()>) {
    let slot: VectorSlot = Arc::new(OnceLock::new());
    slot.set(runtime).ok();
    let app = admin::router(
        VERSION,
        Health::ready(),
        Slots {
            vector: Some(slot),
            ..Slots::default()
        },
    );
    let std_listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    std_listener.set_nonblocking(true).expect("nonblocking");
    let addr = std_listener.local_addr().expect("addr");
    let handle = tokio::spawn(async move {
        let listener = tokio::net::TcpListener::from_std(std_listener).expect("tokio listener");
        let _ = axum::serve(listener, app).await;
    });
    (format!("http://{addr}"), handle)
}

/// The full durable round-trip: a fresh service discovers the table's attached
/// Puffin file from Iceberg metadata and serves it without a side registry.
#[tokio::test]
async fn declare_survives_reboot_via_snapshot_attachment() {
    let cat_dir = tempfile::tempdir().expect("cat dir");
    let (catalog, _ident) = corpus_table(cat_dir.path()).await;

    // First boot: declare over HTTP. The build attaches the Puffin file.
    let runtime1 = Arc::new(VectorRuntime {
        catalog: catalog.clone(),
        service: Arc::new(verglas_vector::service::VectorService::new()),
    });
    let (base1, server1) = serve(runtime1);
    let http = reqwest::Client::new();
    let declare: serde_json::Value = http
        .post(format!("{base1}/v1/tables/default.docs/indexes"))
        .json(&serde_json::json!({ "field": "embedding", "metric": "l2" }))
        .send()
        .await
        .expect("declare send")
        .json()
        .await
        .expect("declare json");
    assert_eq!(declare["liveCount"], 5);

    server1.abort();

    // Reboot: a brand-new disposable cache over the same catalog.
    let runtime2 = Arc::new(VectorRuntime {
        catalog: catalog.clone(),
        service: Arc::new(verglas_vector::service::VectorService::new()),
    });

    // The rebooted runtime discovers and serves the same attached index.
    let (base2, server2) = serve(runtime2);
    let search: serde_json::Value = http
        .post(format!(
            "{base2}/v1/tables/default.docs/indexes/embedding/search"
        ))
        .json(&serde_json::json!({ "vector": [0.9, 0.1], "k": 3 }))
        .send()
        .await
        .expect("search send")
        .json()
        .await
        .expect("search json");
    assert_eq!(search["source"], "index", "served from the attached index");
    assert_eq!(
        search["neighbors"][0]["id"], 2,
        "same nearest as before reboot"
    );

    server2.abort();
}
