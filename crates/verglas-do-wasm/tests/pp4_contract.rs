//! Frozen PP4 ABI contract tests for transactional Stream publication and SQL.
//!
//! The WIT contract exposes only JSON-row SQL and makes Stream publication a
//! storage mutation, so guest code cannot issue an irreversible append.

/// The WIT world removes the Arrow SQL import and exposes transactional Stream sends.
#[test]
fn wit_exposes_only_transactional_stream_and_json_sql() {
    let world = include_str!("../wit/world.wit");
    assert!(
        !world.contains("sql: func"),
        "Arrow IPC SQL must be removed from WIT"
    );
    assert!(world.contains("sql-rows: func"));
    assert!(world.contains("stream-send: func"));
}
