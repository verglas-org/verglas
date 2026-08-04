//! The drain-tick control flow.
//!
//! One tick: ensure the publication and slot, resync every published table if
//! the slot was missing (Neon recycles compute and slots are not durable across
//! a recycle — the documented honest alternative), then drain the slot
//! incrementally, decode pgoutput, build change-row batches, create/evolve
//! tables, append, and only then advance the slot. The slot is advanced strictly
//! after the Iceberg append commits (commit-before-advance): an append failure
//! propagates and the slot is left where it was, so the change is redelivered on
//! the next tick.
//!
//! All live IO sits behind two traits — [`PgSource`] and [`Sink`] — so the
//! control flow (resync-on-missing-slot, advance-after-append, parse-error
//! accounting) is exercised with in-memory fakes and needs no Postgres or
//! Iceberg to test. The live implementations ([`PgConn`], [`IcebergSink`]) are
//! compile-checked here and exercised only against a real endpoint.

use std::collections::HashMap;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, SchemaRef};
use async_trait::async_trait;

use crate::iceberg_sink::TableState;
use crate::pgoutput::{Message, Relation, TupleCol, decode};
use crate::rows::{ChangeRow, Op, build_batch};
use crate::schema::{ColumnDiff, change_row_schema, diff_columns};
use crate::status::{CdcJobStatus, CdcTableStatus, state};
use crate::{CdcError, Result};

/// The runner configuration: the slot and publication names and the per-tick row
/// bound. These are fixed platform names, not tuning knobs — the slot and
/// publication are both `verglas_cdc`.
#[derive(Debug, Clone)]
pub struct RunnerConfig {
    /// The replication slot name (`verglas_cdc`).
    pub slot: String,
    /// The publication name (`verglas_cdc`).
    pub publication: String,
    /// The maximum change rows drained per tick.
    pub max_rows_per_tick: usize,
}

impl Default for RunnerConfig {
    fn default() -> Self {
        RunnerConfig {
            slot: "verglas_cdc".to_owned(),
            publication: "verglas_cdc".to_owned(),
            max_rows_per_tick: 10_000,
        }
    }
}

/// A published table's descriptor: enough to build its change-row schema and
/// resync it. Carries the pgoutput [`Relation`] shape so the resync path and the
/// incremental path type columns identically.
#[derive(Debug, Clone)]
pub struct PublishedTable {
    /// The relation descriptor (schema, name, columns, replica identity).
    pub relation: Relation,
}

/// One raw change row from `pg_logical_slot_peek_binary_changes`.
#[derive(Debug, Clone)]
pub struct RawChange {
    /// The change's WAL LSN, as a numeric offset.
    pub lsn: i64,
    /// The transaction id, when the row carries one.
    pub xid: Option<i64>,
    /// The pgoutput message bytes.
    pub data: Vec<u8>,
}

/// The Postgres side of the runner, behind a trait so the drain logic is
/// testable with a fake.
#[async_trait]
pub trait PgSource {
    /// Ensure the `verglas_cdc` publication exists (idempotent).
    async fn ensure_publication(&self) -> Result<()>;
    /// Whether the `verglas_cdc` replication slot exists.
    async fn slot_exists(&self) -> Result<bool>;
    /// Create the `verglas_cdc` pgoutput slot (called only when missing).
    async fn ensure_slot(&self) -> Result<()>;
    /// List the tables the publication covers.
    async fn list_published_tables(&self) -> Result<Vec<PublishedTable>>;
    /// Full-table snapshot for a resync: every row's column values as tuples,
    /// aligned to the relation's columns.
    async fn snapshot_table(&self, table: &PublishedTable) -> Result<Vec<Vec<TupleCol>>>;
    /// Peek up to `max_rows` changes from the slot without consuming them.
    async fn peek_changes(&self, max_rows: usize) -> Result<Vec<RawChange>>;
    /// Advance the slot's confirmed position to `end_lsn` (consuming up to it).
    async fn advance_slot(&self, end_lsn: i64) -> Result<()>;
}

/// The Iceberg side of the runner, behind a trait so the drain logic is testable
/// with a fake.
#[async_trait]
pub trait Sink {
    /// Ensure the change-log table for a PG table exists with `schema`, returning
    /// its current data columns for the evolution diff.
    async fn ensure_table(
        &self,
        pg_schema: &str,
        pg_table: &str,
        schema: &SchemaRef,
    ) -> Result<TableState>;
    /// Append a change-row batch, stamping `end_lsn` and the slot on the
    /// snapshot; returns the rows appended.
    async fn append(
        &self,
        pg_schema: &str,
        pg_table: &str,
        batch: RecordBatch,
        end_lsn: i64,
    ) -> Result<u64>;
    /// Add a nullable column to a change-log table.
    async fn evolve_add_column(
        &self,
        pg_schema: &str,
        pg_table: &str,
        name: &str,
        data_type: &DataType,
    ) -> Result<()>;
}

/// The decoded, grouped result of one peek: the relations seen, the change rows
/// grouped by relation oid, and the highest LSN observed.
struct Grouped {
    relations: HashMap<u32, Relation>,
    by_rel: HashMap<u32, Vec<ChangeRow>>,
    order: Vec<u32>,
    end_lsn: i64,
}

/// Decodes and groups a peek's raw changes. Relation messages define schemas;
/// Begin carries the transaction's commit timestamp and xid, which the row
/// changes inherit. A data change referencing a relation not seen in this peek is
/// an error (pgoutput re-emits Relation at the start of each peek, so this should
/// not happen) rather than a silent drop.
fn decode_and_group(raw: &[RawChange], seq_start: i64) -> Result<Grouped> {
    let mut relations: HashMap<u32, Relation> = HashMap::new();
    let mut by_rel: HashMap<u32, Vec<ChangeRow>> = HashMap::new();
    let mut order: Vec<u32> = Vec::new();
    let mut end_lsn = 0i64;
    let mut seq = seq_start;
    let mut cur_ts = 0i64;
    let mut cur_xid: Option<i64> = None;

    let push = |rel_oid: u32,
                row: ChangeRow,
                by_rel: &mut HashMap<u32, Vec<ChangeRow>>,
                order: &mut Vec<u32>| {
        if !by_rel.contains_key(&rel_oid) {
            order.push(rel_oid);
        }
        by_rel.entry(rel_oid).or_default().push(row);
    };

    for change in raw {
        end_lsn = end_lsn.max(change.lsn);
        match decode(&change.data)? {
            Message::Begin { commit_ts, xid, .. } => {
                cur_ts = commit_ts;
                cur_xid = Some(i64::from(xid));
            }
            Message::Commit { .. } | Message::Origin { .. } | Message::Type { .. } => {}
            Message::Relation(rel) => {
                relations.insert(rel.rel_oid, rel);
            }
            Message::Insert { rel_oid, tuple } => {
                let row = ChangeRow {
                    op: Op::Insert,
                    lsn: change.lsn,
                    seq,
                    ts: cur_ts,
                    xid: cur_xid,
                    cols: tuple,
                };
                seq += 1;
                push(rel_oid, row, &mut by_rel, &mut order);
            }
            Message::Update {
                rel_oid, new_tuple, ..
            } => {
                let row = ChangeRow {
                    op: Op::Update,
                    lsn: change.lsn,
                    seq,
                    ts: cur_ts,
                    xid: cur_xid,
                    cols: new_tuple,
                };
                seq += 1;
                push(rel_oid, row, &mut by_rel, &mut order);
            }
            Message::Delete { rel_oid, old_tuple } => {
                let row = ChangeRow {
                    op: Op::Delete,
                    lsn: change.lsn,
                    seq,
                    ts: cur_ts,
                    xid: cur_xid,
                    cols: old_tuple,
                };
                seq += 1;
                push(rel_oid, row, &mut by_rel, &mut order);
            }
            Message::Truncate { rel_oids, .. } => {
                for rel_oid in rel_oids {
                    let ncols = relations
                        .get(&rel_oid)
                        .map(|r| r.columns.len())
                        .unwrap_or(0);
                    let row = ChangeRow {
                        op: Op::Truncate,
                        lsn: change.lsn,
                        seq,
                        ts: cur_ts,
                        xid: cur_xid,
                        cols: vec![TupleCol::Null; ncols],
                    };
                    seq += 1;
                    push(rel_oid, row, &mut by_rel, &mut order);
                }
            }
        }
    }

    // Every relation referenced by a change must have a descriptor.
    for rel_oid in &order {
        if !relations.contains_key(rel_oid) {
            return Err(CdcError::Message(format!(
                "change references relation oid {rel_oid} with no Relation message in this peek"
            )));
        }
    }

    Ok(Grouped {
        relations,
        by_rel,
        order,
        end_lsn,
    })
}

/// Runs one drain tick and returns the job status.
pub async fn drain_tick<P, S>(pg: &P, sink: &S, cfg: &RunnerConfig) -> Result<CdcJobStatus>
where
    P: PgSource + Sync,
    S: Sink + Sync,
{
    pg.ensure_publication().await?;
    let existed = pg.slot_exists().await?;
    let resynced = !existed;

    // Per-table status, keyed by the pg_analytics table name.
    let mut statuses: HashMap<String, CdcTableStatus> = HashMap::new();
    let mut seq = 0i64;

    // A fresh slot: create it and full-resync every published table before
    // streaming forward.
    if !existed {
        pg.ensure_slot().await?;
        for pt in pg.list_published_tables().await? {
            let rel = &pt.relation;
            let schema = change_row_schema(rel);
            sink.ensure_table(&rel.namespace, &rel.rel_name, &schema)
                .await?;
            let snap = pg.snapshot_table(&pt).await?;
            let table_name = format!("{}.{}_{}", "pg_analytics", rel.namespace, rel.rel_name);
            if snap.is_empty() {
                statuses.insert(
                    table_name.clone(),
                    CdcTableStatus {
                        state: state::RESYNC.to_owned(),
                        ..CdcTableStatus::streaming(table_name)
                    },
                );
                continue;
            }
            let change_rows: Vec<ChangeRow> = snap
                .into_iter()
                .map(|cols| {
                    let row = ChangeRow {
                        op: Op::Resync,
                        lsn: 0,
                        seq,
                        ts: 0,
                        xid: None,
                        cols,
                    };
                    seq += 1;
                    row
                })
                .collect();
            let built = build_batch(&schema, &change_rows)?;
            let rows = sink
                .append(&rel.namespace, &rel.rel_name, built.batch, 0)
                .await?;
            statuses.insert(
                table_name.clone(),
                CdcTableStatus {
                    table: table_name,
                    last_lsn: 0,
                    last_committed_at: String::new(),
                    rows_appended: rows,
                    parse_errors: built.parse_errors,
                    state: state::RESYNC.to_owned(),
                    error: None,
                },
            );
        }
    }

    // Incremental drain.
    let raw = pg.peek_changes(cfg.max_rows_per_tick).await?;
    let grouped = decode_and_group(&raw, seq)?;
    let end_lsn = grouped.end_lsn;
    let mut appended_any = false;

    for rel_oid in &grouped.order {
        let rel = grouped
            .relations
            .get(rel_oid)
            .expect("relation presence checked in decode_and_group");
        let rows = grouped
            .by_rel
            .get(rel_oid)
            .expect("grouped rel has rows")
            .clone();
        let schema = change_row_schema(rel);
        let table_name = format!("pg_analytics.{}_{}", rel.namespace, rel.rel_name);

        let table_state = sink
            .ensure_table(&rel.namespace, &rel.rel_name, &schema)
            .await?;
        let diff = diff_columns(&table_state.data_columns, rel);

        // An incompatible type change blocks the table: mark schema_pending and
        // do not append to it (never drop or guess the column).
        if let Some((col, old, new)) = diff.iter().find_map(|(name, d)| match d {
            ColumnDiff::TypeChanged { old, new } => Some((name.clone(), old.clone(), new.clone())),
            _ => None,
        }) {
            statuses.insert(
                table_name.clone(),
                CdcTableStatus {
                    table: table_name,
                    last_lsn: 0,
                    last_committed_at: String::new(),
                    rows_appended: 0,
                    parse_errors: 0,
                    state: state::SCHEMA_PENDING.to_owned(),
                    error: Some(format!(
                        "column {col} type changed {old:?} -> {new:?}; append blocked"
                    )),
                },
            );
            continue;
        }

        // Evolve added columns before appending, so the append coerces cleanly.
        for (name, d) in &diff {
            if matches!(d, ColumnDiff::Added) {
                let dt = rel
                    .columns
                    .iter()
                    .find(|c| &c.name == name)
                    .map(|c| crate::pgtype::pg_type_to_arrow(c.type_oid, c.type_mod))
                    .expect("added column is in the relation");
                sink.evolve_add_column(&rel.namespace, &rel.rel_name, name, &dt)
                    .await?;
            }
        }

        let built = build_batch(&schema, &rows)?;
        let appended = sink
            .append(&rel.namespace, &rel.rel_name, built.batch, end_lsn)
            .await?;
        appended_any = true;
        let last_lsn = rows.iter().map(|r| r.lsn).max().unwrap_or(0);
        let entry = statuses
            .entry(table_name.clone())
            .or_insert_with(|| CdcTableStatus::streaming(table_name.clone()));
        entry.state = state::STREAMING.to_owned();
        entry.last_lsn = last_lsn;
        entry.rows_appended += appended;
        entry.parse_errors += built.parse_errors;
    }

    // Advance the slot only after every append committed. `?` above means an
    // append failure returns before this point, leaving the slot in place.
    if appended_any && end_lsn > 0 {
        pg.advance_slot(end_lsn).await?;
    }

    let mut tables: Vec<CdcTableStatus> = statuses.into_values().collect();
    tables.sort_by(|a, b| a.table.cmp(&b.table));
    Ok(CdcJobStatus {
        slot: cfg.slot.clone(),
        publication: cfg.publication.clone(),
        confirmed_lsn: end_lsn,
        resynced,
        tables,
    })
}

// ---------------------------------------------------------------------------
// Live implementations. Compile-checked here; exercised only against a real
// endpoint (no live infra in the crate's test run).
// ---------------------------------------------------------------------------

/// The live Postgres source over a `sqlx` pool.
pub struct PgConn {
    /// The connection pool.
    pub pool: sqlx::PgPool,
    /// The runner config (slot/publication names).
    pub cfg: RunnerConfig,
}

/// Renders an LSN offset (`i64`) to Postgres `pg_lsn` text (`X/Y`, upper-hex).
pub fn lsn_to_text(lsn: i64) -> String {
    let lsn = lsn as u64;
    format!("{:X}/{:X}", lsn >> 32, lsn & 0xFFFF_FFFF)
}

/// Parses Postgres `pg_lsn` text (`X/Y`) into an LSN offset (`i64`).
pub fn lsn_from_text(text: &str) -> Option<i64> {
    let (hi, lo) = text.split_once('/')?;
    let hi = u64::from_str_radix(hi.trim(), 16).ok()?;
    let lo = u64::from_str_radix(lo.trim(), 16).ok()?;
    Some((((hi << 32) | lo) as i64).max(0))
}

#[async_trait]
impl PgSource for PgConn {
    async fn ensure_publication(&self) -> Result<()> {
        let exists: Option<(i32,)> =
            sqlx::query_as("SELECT 1 FROM pg_publication WHERE pubname = $1")
                .bind(&self.cfg.publication)
                .fetch_optional(&self.pool)
                .await?;
        if exists.is_none() {
            // FOR ALL TABLES: the zero-ETL contract publishes the whole database.
            let stmt = format!(
                "CREATE PUBLICATION {} FOR ALL TABLES",
                quote_ident(&self.cfg.publication)
            );
            sqlx::query(&stmt).execute(&self.pool).await?;
        }
        Ok(())
    }

    async fn slot_exists(&self) -> Result<bool> {
        let row: Option<(i32,)> =
            sqlx::query_as("SELECT 1 FROM pg_replication_slots WHERE slot_name = $1")
                .bind(&self.cfg.slot)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.is_some())
    }

    async fn ensure_slot(&self) -> Result<()> {
        sqlx::query("SELECT pg_create_logical_replication_slot($1, 'pgoutput')")
            .bind(&self.cfg.slot)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn list_published_tables(&self) -> Result<Vec<PublishedTable>> {
        use sqlx::Row;
        let rows = sqlx::query(
            "SELECT c.oid::int8 AS rel_oid, n.nspname AS namespace, c.relname AS rel_name, \
                    c.relreplident::text AS replident, a.attname AS col_name, \
                    a.atttypid::int8 AS type_oid, a.atttypmod::int4 AS type_mod \
             FROM pg_publication_tables pt \
             JOIN pg_namespace n ON n.nspname = pt.schemaname \
             JOIN pg_class c ON c.relname = pt.tablename AND c.relnamespace = n.oid \
             JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped \
             WHERE pt.pubname = $1 \
             ORDER BY c.oid, a.attnum",
        )
        .bind(&self.cfg.publication)
        .fetch_all(&self.pool)
        .await?;

        let mut by_oid: HashMap<i64, PublishedTable> = HashMap::new();
        let mut order: Vec<i64> = Vec::new();
        for row in rows {
            let rel_oid: i64 = row.try_get("rel_oid")?;
            let namespace: String = row.try_get("namespace")?;
            let rel_name: String = row.try_get("rel_name")?;
            let replident: String = row.try_get("replident")?;
            let col_name: String = row.try_get("col_name")?;
            let type_oid: i64 = row.try_get("type_oid")?;
            let type_mod: i32 = row.try_get("type_mod")?;
            let entry = by_oid.entry(rel_oid).or_insert_with(|| {
                order.push(rel_oid);
                PublishedTable {
                    relation: Relation {
                        rel_oid: rel_oid as u32,
                        namespace,
                        rel_name,
                        replica_identity: replident.bytes().next().unwrap_or(b'd'),
                        columns: Vec::new(),
                    },
                }
            });
            entry
                .relation
                .columns
                .push(crate::pgoutput::RelationColumn {
                    flags: 0,
                    name: col_name,
                    type_oid: type_oid as u32,
                    type_mod,
                });
        }
        Ok(order
            .into_iter()
            .filter_map(|o| by_oid.remove(&o))
            .collect())
    }

    async fn snapshot_table(&self, table: &PublishedTable) -> Result<Vec<Vec<TupleCol>>> {
        use sqlx::Row;
        let rel = &table.relation;
        let cols: Vec<String> = rel
            .columns
            .iter()
            .map(|c| format!("{}::text", quote_ident(&c.name)))
            .collect();
        let stmt = format!(
            "SELECT {} FROM {}.{}",
            cols.join(", "),
            quote_ident(&rel.namespace),
            quote_ident(&rel.rel_name)
        );
        let rows = sqlx::query(&stmt).fetch_all(&self.pool).await?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let mut tuple = Vec::with_capacity(rel.columns.len());
            for i in 0..rel.columns.len() {
                let value: Option<String> = row.try_get(i)?;
                tuple.push(match value {
                    Some(s) => TupleCol::Text(s),
                    None => TupleCol::Null,
                });
            }
            out.push(tuple);
        }
        Ok(out)
    }

    async fn peek_changes(&self, max_rows: usize) -> Result<Vec<RawChange>> {
        use sqlx::Row;
        let rows = sqlx::query(
            "SELECT lsn::text AS lsn, xid::text AS xid, data \
             FROM pg_logical_slot_peek_binary_changes($1, NULL, $2, \
                  'proto_version', '1', 'publication_names', $3)",
        )
        .bind(&self.cfg.slot)
        .bind(max_rows as i32)
        .bind(&self.cfg.publication)
        .fetch_all(&self.pool)
        .await?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let lsn_text: String = row.try_get("lsn")?;
            let xid_text: Option<String> = row.try_get("xid")?;
            let data: Vec<u8> = row.try_get("data")?;
            out.push(RawChange {
                lsn: lsn_from_text(&lsn_text).unwrap_or(0),
                xid: xid_text.and_then(|s| s.trim().parse::<i64>().ok()),
                data,
            });
        }
        Ok(out)
    }

    async fn advance_slot(&self, end_lsn: i64) -> Result<()> {
        sqlx::query("SELECT pg_replication_slot_advance($1, $2::pg_lsn)")
            .bind(&self.cfg.slot)
            .bind(lsn_to_text(end_lsn))
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

/// Double-quotes a SQL identifier, escaping embedded quotes. Used for the
/// dynamic snapshot SELECT and the publication DDL.
fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// The live Iceberg sink over an opened catalog.
pub struct IcebergSink {
    /// The opened REST catalog.
    pub catalog: std::sync::Arc<dyn iceberg::Catalog>,
    /// The slot name, stamped on every append's snapshot summary.
    pub slot: String,
}

#[async_trait]
impl Sink for IcebergSink {
    async fn ensure_table(
        &self,
        pg_schema: &str,
        pg_table: &str,
        schema: &SchemaRef,
    ) -> Result<TableState> {
        let ident = crate::iceberg_sink::table_ident(pg_schema, pg_table)?;
        crate::iceberg_sink::ensure_table(self.catalog.as_ref(), &ident, schema).await
    }

    async fn append(
        &self,
        pg_schema: &str,
        pg_table: &str,
        batch: RecordBatch,
        end_lsn: i64,
    ) -> Result<u64> {
        let ident = crate::iceberg_sink::table_ident(pg_schema, pg_table)?;
        crate::iceberg_sink::append(
            self.catalog.as_ref(),
            &ident,
            vec![batch],
            end_lsn,
            &self.slot,
        )
        .await
    }

    async fn evolve_add_column(
        &self,
        pg_schema: &str,
        pg_table: &str,
        name: &str,
        data_type: &DataType,
    ) -> Result<()> {
        let ident = crate::iceberg_sink::table_ident(pg_schema, pg_table)?;
        crate::iceberg_sink::evolve_add_column(self.catalog.as_ref(), &ident, name, data_type).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pgoutput::RelationColumn;
    use crate::pgtype::oid;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};

    // -- pgoutput fixture encoders (mirror the wire layout) -----------------
    fn be16(v: u16) -> Vec<u8> {
        v.to_be_bytes().to_vec()
    }
    fn be32(v: u32) -> Vec<u8> {
        v.to_be_bytes().to_vec()
    }
    fn cstr(s: &str) -> Vec<u8> {
        let mut b = s.as_bytes().to_vec();
        b.push(0);
        b
    }
    fn cat(parts: &[Vec<u8>]) -> Vec<u8> {
        parts.iter().flatten().copied().collect()
    }

    fn relation_msg(rel_oid: u32) -> Vec<u8> {
        cat(&[
            vec![b'R'],
            be32(rel_oid),
            cstr("public"),
            cstr("orders"),
            vec![b'd'],
            be16(1),
            vec![0u8],
            cstr("id"),
            be32(oid::INT4),
            (-1i32).to_be_bytes().to_vec(),
        ])
    }

    fn insert_msg(rel_oid: u32, value: &str) -> Vec<u8> {
        let mut m = cat(&[vec![b'I'], be32(rel_oid), vec![b'N'], be16(1), vec![b't']]);
        m.extend((value.len() as i32).to_be_bytes());
        m.extend_from_slice(value.as_bytes());
        m
    }

    fn published_orders(rel_oid: u32) -> PublishedTable {
        PublishedTable {
            relation: Relation {
                rel_oid,
                namespace: "public".to_owned(),
                rel_name: "orders".to_owned(),
                replica_identity: b'd',
                columns: vec![RelationColumn {
                    flags: 0,
                    name: "id".to_owned(),
                    type_oid: oid::INT4,
                    type_mod: -1,
                }],
            },
        }
    }

    // -- fakes --------------------------------------------------------------
    struct FakePg {
        slot_exists: bool,
        ensure_slot_called: AtomicBool,
        snapshot_called: AtomicBool,
        advance_calls: Mutex<Vec<i64>>,
        peek: Vec<RawChange>,
        published: Vec<PublishedTable>,
        snapshot_rows: Vec<Vec<TupleCol>>,
    }

    impl FakePg {
        fn new(slot_exists: bool) -> Self {
            FakePg {
                slot_exists,
                ensure_slot_called: AtomicBool::new(false),
                snapshot_called: AtomicBool::new(false),
                advance_calls: Mutex::new(Vec::new()),
                peek: Vec::new(),
                published: Vec::new(),
                snapshot_rows: Vec::new(),
            }
        }
    }

    #[async_trait]
    impl PgSource for FakePg {
        async fn ensure_publication(&self) -> Result<()> {
            Ok(())
        }
        async fn slot_exists(&self) -> Result<bool> {
            Ok(self.slot_exists)
        }
        async fn ensure_slot(&self) -> Result<()> {
            self.ensure_slot_called.store(true, Ordering::SeqCst);
            Ok(())
        }
        async fn list_published_tables(&self) -> Result<Vec<PublishedTable>> {
            Ok(self.published.clone())
        }
        async fn snapshot_table(&self, _t: &PublishedTable) -> Result<Vec<Vec<TupleCol>>> {
            self.snapshot_called.store(true, Ordering::SeqCst);
            Ok(self.snapshot_rows.clone())
        }
        async fn peek_changes(&self, _max: usize) -> Result<Vec<RawChange>> {
            Ok(self.peek.clone())
        }
        async fn advance_slot(&self, end_lsn: i64) -> Result<()> {
            self.advance_calls.lock().expect("lock").push(end_lsn);
            Ok(())
        }
    }

    struct FakeSink {
        existing_columns: Vec<(String, DataType)>,
        fail_append: bool,
        append_calls: AtomicUsize,
        evolve_calls: Mutex<Vec<String>>,
        last_append_lsn: AtomicI64,
    }

    impl FakeSink {
        fn new() -> Self {
            FakeSink {
                existing_columns: Vec::new(),
                fail_append: false,
                append_calls: AtomicUsize::new(0),
                evolve_calls: Mutex::new(Vec::new()),
                last_append_lsn: AtomicI64::new(-1),
            }
        }
    }

    #[async_trait]
    impl Sink for FakeSink {
        async fn ensure_table(
            &self,
            _s: &str,
            _t: &str,
            _schema: &SchemaRef,
        ) -> Result<TableState> {
            Ok(TableState {
                existed: !self.existing_columns.is_empty(),
                data_columns: self.existing_columns.clone(),
            })
        }
        async fn append(
            &self,
            _s: &str,
            _t: &str,
            _batch: RecordBatch,
            end_lsn: i64,
        ) -> Result<u64> {
            if self.fail_append {
                return Err(CdcError::Message("append boom".to_owned()));
            }
            self.append_calls.fetch_add(1, Ordering::SeqCst);
            self.last_append_lsn.store(end_lsn, Ordering::SeqCst);
            Ok(1)
        }
        async fn evolve_add_column(
            &self,
            _s: &str,
            _t: &str,
            name: &str,
            _dt: &DataType,
        ) -> Result<()> {
            self.evolve_calls
                .lock()
                .expect("lock")
                .push(name.to_owned());
            Ok(())
        }
    }

    #[tokio::test]
    async fn missing_slot_triggers_resync() {
        let mut pg = FakePg::new(false);
        pg.published = vec![published_orders(16384)];
        pg.snapshot_rows = vec![
            vec![TupleCol::Text("1".into())],
            vec![TupleCol::Text("2".into())],
        ];
        let sink = FakeSink::new();
        let status = drain_tick(&pg, &sink, &RunnerConfig::default())
            .await
            .expect("tick");

        assert!(pg.ensure_slot_called.load(Ordering::SeqCst), "slot created");
        assert!(
            pg.snapshot_called.load(Ordering::SeqCst),
            "resync snapshot ran"
        );
        assert!(status.resynced, "status marks resync");
        assert_eq!(
            sink.append_calls.load(Ordering::SeqCst),
            1,
            "resync appended"
        );
        assert_eq!(status.tables.len(), 1);
        assert_eq!(status.tables[0].state, state::RESYNC);
    }

    #[tokio::test]
    async fn present_slot_streams_incrementally_only() {
        let mut pg = FakePg::new(true);
        pg.peek = vec![
            RawChange {
                lsn: 5,
                xid: Some(1),
                data: relation_msg(16384),
            },
            RawChange {
                lsn: 6,
                xid: Some(1),
                data: insert_msg(16384, "42"),
            },
        ];
        let sink = FakeSink::new();
        let status = drain_tick(&pg, &sink, &RunnerConfig::default())
            .await
            .expect("tick");

        assert!(
            !pg.ensure_slot_called.load(Ordering::SeqCst),
            "no slot create"
        );
        assert!(!pg.snapshot_called.load(Ordering::SeqCst), "no resync");
        assert!(!status.resynced);
        assert_eq!(sink.append_calls.load(Ordering::SeqCst), 1);
        assert_eq!(status.tables[0].state, state::STREAMING);
    }

    #[tokio::test]
    async fn advance_runs_after_a_successful_append() {
        let mut pg = FakePg::new(true);
        pg.peek = vec![
            RawChange {
                lsn: 5,
                xid: Some(1),
                data: relation_msg(16384),
            },
            RawChange {
                lsn: 9,
                xid: Some(1),
                data: insert_msg(16384, "42"),
            },
        ];
        let sink = FakeSink::new();
        let status = drain_tick(&pg, &sink, &RunnerConfig::default())
            .await
            .expect("tick");

        let advances = pg.advance_calls.lock().expect("lock").clone();
        assert_eq!(
            advances,
            vec![9],
            "advanced to the highest LSN after append"
        );
        assert_eq!(status.confirmed_lsn, 9);
        assert_eq!(sink.last_append_lsn.load(Ordering::SeqCst), 9);
    }

    #[tokio::test]
    async fn append_failure_leaves_the_slot_unadvanced() {
        let mut pg = FakePg::new(true);
        pg.peek = vec![
            RawChange {
                lsn: 5,
                xid: Some(1),
                data: relation_msg(16384),
            },
            RawChange {
                lsn: 9,
                xid: Some(1),
                data: insert_msg(16384, "42"),
            },
        ];
        let mut sink = FakeSink::new();
        sink.fail_append = true;
        let result = drain_tick(&pg, &sink, &RunnerConfig::default()).await;

        assert!(result.is_err(), "append failure surfaces");
        assert!(
            pg.advance_calls.lock().expect("lock").is_empty(),
            "slot NOT advanced on append failure"
        );
    }

    #[tokio::test]
    async fn added_column_evolves_before_append() {
        let mut pg = FakePg::new(true);
        // Relation carries id + name; the table currently has only id.
        let rel_msg = cat(&[
            vec![b'R'],
            be32(16384),
            cstr("public"),
            cstr("orders"),
            vec![b'd'],
            be16(2),
            vec![0u8],
            cstr("id"),
            be32(oid::INT4),
            (-1i32).to_be_bytes().to_vec(),
            vec![0u8],
            cstr("name"),
            be32(oid::TEXT),
            (-1i32).to_be_bytes().to_vec(),
        ]);
        let mut ins = cat(&[vec![b'I'], be32(16384), vec![b'N'], be16(2)]);
        ins.extend(vec![b't']);
        ins.extend((1i32).to_be_bytes());
        ins.extend_from_slice(b"7");
        ins.extend(vec![b't']);
        ins.extend((3i32).to_be_bytes());
        ins.extend_from_slice(b"abc");
        pg.peek = vec![
            RawChange {
                lsn: 1,
                xid: Some(1),
                data: rel_msg,
            },
            RawChange {
                lsn: 2,
                xid: Some(1),
                data: ins,
            },
        ];
        let mut sink = FakeSink::new();
        sink.existing_columns = vec![("id".to_owned(), DataType::Int32)];
        let status = drain_tick(&pg, &sink, &RunnerConfig::default())
            .await
            .expect("tick");

        let evolved = sink.evolve_calls.lock().expect("lock").clone();
        assert_eq!(evolved, vec!["name".to_owned()], "added column evolved");
        assert_eq!(status.tables[0].state, state::STREAMING);
    }

    #[tokio::test]
    async fn type_change_marks_schema_pending_and_skips_append() {
        let mut pg = FakePg::new(true);
        pg.peek = vec![
            RawChange {
                lsn: 1,
                xid: Some(1),
                data: relation_msg(16384),
            },
            RawChange {
                lsn: 2,
                xid: Some(1),
                data: insert_msg(16384, "42"),
            },
        ];
        let mut sink = FakeSink::new();
        // Table has id as Utf8; relation maps id (int4) to Int32 -> TypeChanged.
        sink.existing_columns = vec![("id".to_owned(), DataType::Utf8)];
        let status = drain_tick(&pg, &sink, &RunnerConfig::default())
            .await
            .expect("tick");

        assert_eq!(sink.append_calls.load(Ordering::SeqCst), 0, "no append");
        assert_eq!(status.tables[0].state, state::SCHEMA_PENDING);
        assert!(status.tables[0].error.is_some());
        assert!(
            pg.advance_calls.lock().expect("lock").is_empty(),
            "no advance"
        );
    }

    #[test]
    fn lsn_text_round_trips() {
        assert_eq!(lsn_to_text(0x1_0000_0008), "1/8");
        assert_eq!(lsn_from_text("1/8"), Some(0x1_0000_0008));
        assert_eq!(lsn_from_text("0/0"), Some(0));
    }
}
