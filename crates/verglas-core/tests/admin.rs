//! Integration tests for shared admin API wire types.

use verglas_core::admin::{
    ACCESS_PATH, CacheConfigInfo, CountersInfo, HealthzInfo, LocalAccess, STATS_PATH, StatsInfo,
    VersionInfo,
};

#[test]
fn local_access_carries_the_discovery_fields_but_never_the_secret() {
    // The zero-config CLI verbs (#287) parse this off `/admin/access`, so its
    // shape is a wire contract. It carries only the non-secret discovery fields;
    // the secret access key must never appear, because the admin socket is
    // unauthenticated and host-scoped (see the security fix).
    let access = LocalAccess {
        s3_endpoint: "http://127.0.0.1:8333".to_owned(),
        query_uri: "http://127.0.0.1:8334".to_owned(),
        catalog_uri: Some("https://catalog.example.test".to_owned()),
        warehouse: Some("s3://warehouse/tenant".to_owned()),
        region: "us-east-1".to_owned(),
        bucket: Some("warehouse".to_owned()),
        access_key_id: Some("VGKEY".to_owned()),
    };
    let encoded = serde_json::to_string(&access).expect("encode");
    let decoded: LocalAccess = serde_json::from_str(&encoded).expect("decode");
    assert_eq!(decoded, access);

    let value: serde_json::Value = serde_json::from_str(&encoded).expect("json");
    for key in [
        "s3_endpoint",
        "query_uri",
        "catalog_uri",
        "warehouse",
        "region",
        "bucket",
        "access_key_id",
    ] {
        assert!(value.get(key).is_some(), "access JSON must carry `{key}`");
    }
    // The secret must not be part of the served shape at all — not blanked,
    // absent — so it can never leak over the loopback admin socket.
    assert!(
        value.get("secret_access_key").is_none(),
        "access JSON must not carry a secret_access_key field"
    );
    // The probe path is a stable admin route.
    assert_eq!(ACCESS_PATH, "/admin/access");
}

#[test]
fn local_access_without_a_catalog_or_keys_still_decodes() {
    // A server with no catalog or auth keypair reports nulls; the CLI must
    // still decode the snapshot.
    let json = r#"{"s3_endpoint":"http://127.0.0.1:8333","query_uri":"http://127.0.0.1:8334","catalog_uri":null,"warehouse":null,"region":"us-east-1","bucket":null,"access_key_id":null}"#;
    let decoded: LocalAccess = serde_json::from_str(json).expect("decode");
    assert_eq!(decoded.query_uri, "http://127.0.0.1:8334");
    assert!(decoded.catalog_uri.is_none());
    assert!(decoded.warehouse.is_none());
    assert!(decoded.access_key_id.is_none());
}

#[test]
fn version_info_round_trips_through_json() {
    let info = VersionInfo::for_server("0.1.0-test");
    let encoded = serde_json::to_string(&info).expect("encode");
    let decoded: VersionInfo = serde_json::from_str(&encoded).expect("decode");

    assert_eq!(decoded, info);
}

#[test]
fn healthz_info_round_trips_through_json() {
    let info = HealthzInfo::ok();
    let encoded = serde_json::to_string(&info).expect("encode");
    let decoded: HealthzInfo = serde_json::from_str(&encoded).expect("decode");

    assert_eq!(decoded, info);
}

#[test]
fn stats_info_round_trips_and_exposes_config_and_counters() {
    assert_eq!(STATS_PATH, "/admin/stats");
    let info = StatsInfo {
        cache: CacheConfigInfo {
            dir: "/var/lib/verglas".to_owned(),
            capacity_bytes: 20 * 1024 * 1024 * 1024,
            dram_bytes: 80 * 1024 * 1024,
        },
        counters: CountersInfo {
            dram_hits: 3,
            dram_misses: 40,
            disk_hits: 37,
            disk_misses: 3,
            peer_hits: 0,
            peer_misses: 0,
            peer_errors: 0,
            peer_served_blocks: 0,
            peer_served_bytes: 0,
            dram_bytes_served: 24 * 1024 * 1024,
            disk_bytes_served: 296 * 1024 * 1024,
            peer_bytes_served: 0,
            backend_bytes_served: 24 * 1024 * 1024,
            backend_fills: 40,
            backend_fill_bytes: 320 * 1024 * 1024,
            backend_heads: 8,
            non_cacheable_passthroughs: 0,
            meta_hits: 12,
            meta_misses: 1,
            meta_bytes_served: 2 * 1024 * 1024,
            retired_bytes_pending: 4096,
            retired_bytes_reclaimed: 8192,
            retired_files_reclaimed: 2,
        },
        dram_usage_bytes: 16 * 1024 * 1024,
        dram_live_bytes: 10 * 1024 * 1024,
        dram_reclaimable_bytes: 6 * 1024 * 1024,
        warming: Some(verglas_core::admin::WarmingInfo {
            tables_started: 1,
            tables_completed: 1,
            files_seen: 4,
            parquet_files: 4,
            block_objects_warmed: 3,
            footers_warmed: 4,
            footer_bytes_warmed: 8192,
            footer_gets: 4,
            footer_refetches: 0,
            skipped_non_parquet: 0,
            skipped_over_budget: 0,
            budget_alerts: 0,
        }),
        writeback: Some(verglas_core::admin::WritebackStatsInfo {
            acked_via_quorum: 5,
            acked_via_write_through: 2,
            mode_transitions: 1,
            propagated: 4,
            propagation_failures: 0,
            fragments_repaired: 0,
            fragments_scrubbed: 12,
            corrupt_fragments_found: 1,
        }),
    };
    let encoded = serde_json::to_string(&info).expect("encode");
    let decoded: StatsInfo = serde_json::from_str(&encoded).expect("decode");
    assert_eq!(decoded, info);
    assert_eq!(decoded.warming.expect("warming present").footers_warmed, 4);
    assert_eq!(
        decoded
            .writeback
            .expect("writeback present")
            .acked_via_quorum,
        5
    );

    // A stats body without the warming field (older server) still decodes.
    let no_warming = r#"{"cache":{"dir":"/c","capacity_bytes":1,"dram_bytes":1},"counters":{"dram_hits":0,"dram_misses":0,"disk_hits":0,"disk_misses":0,"peer_hits":0,"peer_misses":0,"peer_errors":0,"peer_served_blocks":0,"peer_served_bytes":0,"dram_bytes_served":0,"disk_bytes_served":0,"peer_bytes_served":0,"backend_bytes_served":0,"backend_fills":0,"backend_fill_bytes":0,"backend_heads":0,"non_cacheable_passthroughs":0,"meta_hits":0,"meta_misses":0,"meta_bytes_served":0,"retired_bytes_pending":0,"retired_bytes_reclaimed":0,"retired_files_reclaimed":0},"dram_usage_bytes":0}"#;
    let decoded: StatsInfo = serde_json::from_str(no_warming).expect("decode without warming");
    assert!(decoded.warming.is_none());

    // The tier context a report stamps on its numbers is machine-readable.
    let value: serde_json::Value = serde_json::from_str(&encoded).expect("json");
    assert_eq!(value["cache"]["dram_bytes"], 80 * 1024 * 1024);
    assert_eq!(value["cache"]["capacity_bytes"], 20u64 * 1024 * 1024 * 1024);
    // Disk served the bulk here despite a tiny DRAM tier — the nvme-resident
    // signal the profile checks for.
    assert_eq!(
        value["counters"]["disk_bytes_served"].as_u64(),
        Some(296 * 1024 * 1024)
    );
    assert_eq!(value["dram_usage_bytes"].as_u64(), Some(16 * 1024 * 1024));
    // The live/reclaimable split (#178) is on the wire and sums to the total.
    assert_eq!(value["dram_live_bytes"].as_u64(), Some(10 * 1024 * 1024));
    assert_eq!(
        value["dram_reclaimable_bytes"].as_u64(),
        Some(6 * 1024 * 1024)
    );
}
