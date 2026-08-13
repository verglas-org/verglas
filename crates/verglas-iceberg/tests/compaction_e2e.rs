//! End-to-end compaction against the REAL Cloudflare catalog service
//! (apps/catalog) in `wrangler dev` mode plus a throwaway MinIO — the same
//! iceberg-rust client verglas-server uses. Proves the executor's REPLACE commit is
//! accepted by the production catalog's commit/CAS path (not just MemoryCatalog),
//! that file count drops while content is preserved, that time travel to the
//! pre-compaction snapshot still works, and that an interleaved append conflicts
//! cleanly.
//!
//! This test is inert unless `VG_COMPACTION_E2E=1` is set (it needs external
//! infra). Bring the infra up with, from apps/catalog:
//!   minio server /tmp/vg-compact-m0 --address 127.0.0.1:19010 \
//!     --console-address 127.0.0.1:19011   # (or a docker container on those ports)
//!   npx wrangler dev -c wrangler.dev.jsonc --port 18799
//! then:
//!   VG_COMPACTION_E2E=1 cargo test -p verglas-iceberg --test compaction_e2e \
//!     -- --ignored --nocapture

use verglas_iceberg::conn::Connection;
use verglas_iceberg::{
    CompactionOptions, TimeTravel, catalog, compact_table, compact_table_once, inspect,
    parse_table_ident, query, write,
};

/// Reads an env var, or a default.
fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_owned())
}

/// Writes `contents` to a `.csv` under `dir` and returns its path.
fn write_csv(dir: &std::path::Path, name: &str, contents: &str) -> std::path::PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, contents).expect("write csv");
    path
}

/// count(*) over a table via the query engine.
async fn count(catalog: std::sync::Arc<dyn iceberg::Catalog>, dotted: &str) -> i64 {
    let report = query::query(
        catalog,
        &format!("SELECT count(*) AS n FROM {dotted}"),
        None,
    )
    .await
    .expect("count");
    report.rows[0]["n"].as_i64().expect("int")
}

#[tokio::test]
#[ignore = "needs wrangler dev + MinIO; run with VG_COMPACTION_E2E=1 -- --ignored"]
async fn compaction_e2e_against_catalog_service() {
    if std::env::var("VG_COMPACTION_E2E").is_err() {
        eprintln!("VG_COMPACTION_E2E unset; skipping compaction e2e");
        return;
    }

    let connection = Connection {
        catalog_uri: env_or("VG_COMPACTION_CATALOG_URI", "http://127.0.0.1:18799"),
        token: None, // dev-auth bypass (CATALOG_DEV_TENANT) — no bearer needed.
        warehouse: Some(env_or("VG_COMPACTION_WAREHOUSE", "warehouse")),
        s3_endpoint: Some(env_or(
            "VG_COMPACTION_S3_ENDPOINT",
            "http://127.0.0.1:19010",
        )),
        region: env_or("VG_COMPACTION_REGION", "us-east-1"),
        access_key_id: Some(env_or("VG_COMPACTION_ACCESS_KEY_ID", "minioadmin")),
        secret_access_key: Some(env_or("VG_COMPACTION_SECRET_ACCESS_KEY", "minioadmin")),
    };
    let catalog = catalog::open_catalog(&connection)
        .await
        .expect("open catalog against the catalog service");

    // A table name unique per run so repeated runs don't collide.
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis();
    let dotted = format!("compaction.events_{suffix}");
    let ident = parse_table_ident(&dotted).expect("ident");
    let dir = tempfile::tempdir().expect("tempdir");

    // Create + 11 more small appends => 12 small data files.
    let seed = write_csv(dir.path(), "seed.csv", "k,v\n0,zero\n");
    write::create_table(catalog.as_ref(), &ident, &seed, None)
        .await
        .expect("create");
    for i in 1..=11 {
        let f = write_csv(dir.path(), "more.csv", &format!("k,v\n{i},row{i}\n"));
        write::append(catalog.as_ref(), &ident, &f)
            .await
            .expect("append");
    }

    let files_before = inspect::show(catalog.as_ref(), &ident)
        .await
        .expect("show")
        .file_count
        .expect("file count");
    assert_eq!(files_before, 12, "12 small files before compaction");
    let count_before = count(catalog.clone(), &dotted).await;
    let pre_snapshot = inspect::show(catalog.as_ref(), &ident)
        .await
        .expect("show")
        .current_snapshot_id
        .expect("snapshot");

    // Compact through the real catalog.
    let report = compact_table(catalog.as_ref(), &ident, CompactionOptions::default())
        .await
        .expect("compact");
    assert_eq!(report.input_data_files, 12);
    assert!(report.output_data_files < 12, "file count dropped");
    assert_eq!(report.input_records, report.output_records);
    assert!(report.snapshot_id.is_some());

    let files_after = inspect::show(catalog.as_ref(), &ident)
        .await
        .expect("show")
        .file_count
        .expect("file count");
    assert!(files_after < files_before, "{files_after} < {files_before}");
    assert_eq!(count(catalog.clone(), &dotted).await, count_before);

    // The new snapshot is a REPLACE.
    let history = inspect::history(catalog.as_ref(), &ident)
        .await
        .expect("history");
    assert_eq!(history.snapshots.last().expect("snap").operation, "replace");

    // Time travel to the pre-compaction snapshot still resolves the original rows.
    let travelled = query::query(
        catalog.clone(),
        &format!("SELECT count(*) AS n FROM \"events_{suffix}\""),
        Some(TimeTravel {
            reference: pre_snapshot.to_string(),
            table: dotted.clone(),
        }),
    )
    .await
    .expect("time travel");
    assert_eq!(
        travelled.rows[0]["n"].as_i64().expect("int"),
        count_before,
        "time travel to pre-compaction snapshot works"
    );

    // Conflict case: a REPLACE against a stale handle conflicts with an
    // interleaved append and is rejected, never overwriting it.
    let contended = format!("compaction.contended_{suffix}");
    let cident = parse_table_ident(&contended).expect("ident");
    let cseed = write_csv(dir.path(), "cseed.csv", "k,v\n0,zero\n");
    write::create_table(catalog.as_ref(), &cident, &cseed, None)
        .await
        .expect("create contended");
    for i in 1..=3 {
        let f = write_csv(dir.path(), "cmore.csv", &format!("k,v\n{i},row{i}\n"));
        write::append(catalog.as_ref(), &cident, &f)
            .await
            .expect("append contended");
    }
    let stale = catalog.load_table(&cident).await.expect("load stale");
    let interleaved = write_csv(dir.path(), "int.csv", "k,v\n99,interleaved\n");
    write::append(catalog.as_ref(), &cident, &interleaved)
        .await
        .expect("interleaved append");
    let err = compact_table_once(catalog.as_ref(), &stale, CompactionOptions::default())
        .await
        .expect_err("stale REPLACE must conflict");
    match err {
        verglas_iceberg::AgentError::Iceberg(e) => assert_eq!(
            e.kind(),
            iceberg::ErrorKind::CatalogCommitConflicts,
            "stale REPLACE conflicts"
        ),
        other => panic!("expected commit conflict, got {other}"),
    }
    // The append survived; the retrying path then succeeds with all 5 rows.
    assert_eq!(count(catalog.clone(), &contended).await, 5);
    compact_table(catalog.as_ref(), &cident, CompactionOptions::default())
        .await
        .expect("retried compaction");
    assert_eq!(count(catalog.clone(), &contended).await, 5);

    eprintln!("compaction e2e OK: {files_before} -> {files_after} files, content preserved");
}
