//! Reserved Turso schema declarations and validation.
//!
//! These are the only tables owned by the Durable Object host. Tenant SQL is
//! rejected from all reserved names and Turso/SQLite implementation prefixes.

use std::collections::BTreeMap;

use turso::{Connection, Value};

use crate::error::{Error, Result};

/// Reserved byte KV table name.
pub const WORKER_KV_TABLE: &str = "__worker_kv";
/// Reserved single-alarm table name.
pub const WORKER_ALARM_TABLE: &str = "__worker_alarm";
/// Reserved WebSocket attachment table name.
pub const WORKER_ATTACHMENTS_TABLE: &str = "__worker_attachments";
/// Reserved event-sequence table name.
pub const EVENT_SEQUENCE_TABLE: &str = "__verglas_event_sequence";
/// Reserved publication outbox table name.
pub const OUTBOX_TABLE: &str = "__verglas_outbox";

/// Expected column names and declared SQL types for every reserved table.
pub fn reserved_schema() -> BTreeMap<&'static str, Vec<(&'static str, &'static str)>> {
    BTreeMap::from([
        (WORKER_KV_TABLE, vec![("key", "TEXT"), ("value", "BLOB")]),
        (
            WORKER_ALARM_TABLE,
            vec![("id", "INTEGER"), ("deadline_ms", "INTEGER")],
        ),
        (
            WORKER_ATTACHMENTS_TABLE,
            vec![("socket", "INTEGER"), ("value", "BLOB")],
        ),
        (
            EVENT_SEQUENCE_TABLE,
            vec![("id", "INTEGER"), ("next_sequence", "INTEGER")],
        ),
        (
            OUTBOX_TABLE,
            vec![
                ("stream_binding", "TEXT"),
                ("stream_name", "TEXT"),
                ("source_do_id", "TEXT"),
                ("event_sequence", "INTEGER"),
                ("record_index", "INTEGER"),
                ("event_id", "TEXT"),
                ("payload", "TEXT"),
                ("state", "TEXT"),
                ("lease_owner", "TEXT"),
                ("lease_expires_at", "INTEGER"),
                ("delivered_at", "INTEGER"),
            ],
        ),
    ])
}

/// Creates all reserved tables without replacing an existing table.
pub async fn create_reserved_tables(connection: &Connection) -> Result<()> {
    connection
        .execute(
            "CREATE TABLE IF NOT EXISTS __worker_kv (key TEXT PRIMARY KEY NOT NULL, value BLOB)",
            (),
        )
        .await?;
    connection
        .execute(
            "CREATE TABLE IF NOT EXISTS __worker_alarm (id INTEGER PRIMARY KEY CHECK (id = 1), deadline_ms INTEGER)",
            (),
        )
        .await?;
    connection
        .execute(
            "CREATE TABLE IF NOT EXISTS __worker_attachments (socket INTEGER PRIMARY KEY NOT NULL, value BLOB)",
            (),
        )
        .await?;
    connection
        .execute(
            "CREATE TABLE IF NOT EXISTS __verglas_event_sequence (id INTEGER PRIMARY KEY CHECK (id = 1), next_sequence INTEGER NOT NULL)",
            (),
        )
        .await?;
    connection
        .execute(
            "INSERT OR IGNORE INTO __verglas_event_sequence (id, next_sequence) VALUES (1, 0)",
            (),
        )
        .await?;
    connection
        .execute(
            "CREATE TABLE IF NOT EXISTS __verglas_outbox (
                stream_binding TEXT NOT NULL,
                stream_name TEXT NOT NULL,
                source_do_id TEXT NOT NULL,
                event_sequence INTEGER NOT NULL,
                record_index INTEGER NOT NULL,
                event_id TEXT NOT NULL,
                payload TEXT NOT NULL,
                state TEXT NOT NULL,
                lease_owner TEXT,
                lease_expires_at INTEGER,
                delivered_at INTEGER,
                PRIMARY KEY (source_do_id, event_sequence, record_index),
                UNIQUE (event_id)
            )",
            (),
        )
        .await?;
    Ok(())
}

/// Validates every reserved table's visible columns before serving events.
pub async fn validate_reserved_tables(connection: &Connection) -> Result<()> {
    for (table, expected) in reserved_schema() {
        let statement = format!("PRAGMA table_info({table})");
        let mut rows = connection.query(statement, ()).await?;
        let mut actual = Vec::new();
        while let Some(row) = rows.next().await? {
            let name = match row.get_value(1)? {
                Value::Text(value) => value,
                other => {
                    return Err(Error::InvalidSchema(format!(
                        "{table} column name has unexpected value {other:?}"
                    )));
                }
            };
            let declared_type = match row.get_value(2)? {
                Value::Text(value) => value.to_ascii_uppercase(),
                Value::Null => String::new(),
                other => {
                    return Err(Error::InvalidSchema(format!(
                        "{table} column type has unexpected value {other:?}"
                    )));
                }
            };
            actual.push((name, declared_type));
        }
        let expected = expected
            .into_iter()
            .map(|(name, declared_type)| (name.to_owned(), declared_type.to_owned()))
            .collect::<Vec<_>>();
        if actual.len() != expected.len()
            || actual.iter().zip(expected.iter()).any(
                |((actual_name, actual_type), (expected_name, expected_type))| {
                    actual_name != expected_name || !actual_type.contains(expected_type)
                },
            )
        {
            return Err(Error::InvalidSchema(format!(
                "{table} columns are {actual:?}, expected {expected:?}"
            )));
        }
    }
    Ok(())
}

/// Rejects tenant SQL that could touch host or engine implementation tables.
pub fn validate_tenant_sql(statement: &str) -> Result<()> {
    let normalized = statement.to_ascii_lowercase();
    let forbidden = [
        WORKER_KV_TABLE,
        WORKER_ALARM_TABLE,
        WORKER_ATTACHMENTS_TABLE,
        EVENT_SEQUENCE_TABLE,
        OUTBOX_TABLE,
        "turso_",
        "__turso_",
        "sqlite_",
    ];
    for token in forbidden {
        if normalized.contains(token) {
            return Err(Error::InvalidSql(format!(
                "reserved or internal name `{token}`"
            )));
        }
    }
    Ok(())
}
