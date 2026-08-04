//! Building an Arrow [`RecordBatch`] of change rows from decoded pgoutput
//! tuples.
//!
//! A batch is a run of [`ChangeRow`]s that all belong to one relation (so they
//! share the change-row schema from [`crate::schema`]). Each row carries its
//! metadata (op, lsn, seq, ts, xid) and one [`TupleCol`] per relation column.
//! The reserved metadata columns are built directly from the row metadata; each
//! data column is parsed from pgoutput's text form into the column's Arrow type.
//!
//! Parsing is total and non-panicking: a `Null` or `UnchangedToast` cell becomes
//! null, and a `Text` cell that fails to parse into its target type also becomes
//! null and bumps the returned `parse_errors` count. A malformed value never
//! aborts the batch or corrupts a neighbouring column — the change row lands with
//! that one cell null and the error is counted for the status surface.

use std::sync::Arc;

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, RecordBatch, StringArray, Time64MicrosecondArray,
    TimestampMicrosecondArray,
};
use arrow_schema::{ArrowError, DataType, SchemaRef, TimeUnit};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Timelike};

use crate::pgoutput::TupleCol;
use crate::schema::RESERVED_COLUMN_COUNT;

/// The change operation a row records. The string form is the `_vg_op` value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    /// An inserted row.
    Insert,
    /// An updated row (new image).
    Update,
    /// A deleted row (replica-identity columns).
    Delete,
    /// A truncate marker for the relation.
    Truncate,
    /// A full-table resync row (initial snapshot).
    Resync,
}

impl Op {
    /// The single-character `_vg_op` value.
    pub fn as_str(&self) -> &'static str {
        match self {
            Op::Insert => "I",
            Op::Update => "U",
            Op::Delete => "D",
            Op::Truncate => "T",
            Op::Resync => "R",
        }
    }
}

/// One change row to write: its metadata and one column value per relation
/// column, aligned to the data columns of the change-row schema.
#[derive(Debug, Clone)]
pub struct ChangeRow {
    /// The change operation.
    pub op: Op,
    /// The WAL LSN of the change.
    pub lsn: i64,
    /// The per-drain monotonic sequence (tiebreak within an LSN).
    pub seq: i64,
    /// The commit timestamp, unix micros.
    pub ts: i64,
    /// The transaction id, when known (a resync row has none).
    pub xid: Option<i64>,
    /// The column values, one per relation column, in schema data-column order.
    pub cols: Vec<TupleCol>,
}

/// A built batch and the count of values that failed to parse into their target
/// type (and were written as null).
#[derive(Debug)]
pub struct BuiltBatch {
    /// The change-row record batch.
    pub batch: RecordBatch,
    /// How many `Text` cells failed to parse and were nulled.
    pub parse_errors: u64,
}

/// Builds a change-row [`RecordBatch`] matching `schema` from `rows`. Returns the
/// batch and the parse-error count. The only error returned is a structural
/// Arrow error (a schema/column-count mismatch), never a per-value parse failure
/// — those are counted, not raised.
pub fn build_batch(schema: &SchemaRef, rows: &[ChangeRow]) -> Result<BuiltBatch, ArrowError> {
    let mut parse_errors: u64 = 0;
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    // Reserved metadata columns, built directly from the row metadata.
    columns.push(Arc::new(StringArray::from_iter_values(
        rows.iter().map(|r| r.op.as_str()),
    )));
    columns.push(Arc::new(Int64Array::from_iter_values(
        rows.iter().map(|r| r.lsn),
    )));
    columns.push(Arc::new(Int64Array::from_iter_values(
        rows.iter().map(|r| r.seq),
    )));
    columns.push(Arc::new(
        TimestampMicrosecondArray::from_iter_values(rows.iter().map(|r| r.ts)).with_timezone("UTC"),
    ));
    columns.push(Arc::new(Int64Array::from(
        rows.iter().map(|r| r.xid).collect::<Vec<_>>(),
    )));

    // Data columns: parse each cell into the field's Arrow type.
    for (data_index, field) in schema
        .fields()
        .iter()
        .enumerate()
        .skip(RESERVED_COLUMN_COUNT)
    {
        let col_index = data_index - RESERVED_COLUMN_COUNT;
        let cells = rows
            .iter()
            .map(|r| r.cols.get(col_index).unwrap_or(&TupleCol::Null));
        let array = build_data_column(field.data_type(), cells, &mut parse_errors)?;
        columns.push(array);
    }

    let batch = RecordBatch::try_new(schema.clone(), columns)?;
    Ok(BuiltBatch {
        batch,
        parse_errors,
    })
}

/// Builds one typed data column from the cells, counting parse failures.
fn build_data_column<'a>(
    data_type: &DataType,
    cells: impl Iterator<Item = &'a TupleCol>,
    parse_errors: &mut u64,
) -> Result<ArrayRef, ArrowError> {
    // Each cell yields Option<parsed>; a Text that fails parsing yields None and
    // bumps parse_errors. `text_to` centralizes that accounting.
    macro_rules! collect_with {
        ($parse:expr) => {{
            cells
                .map(|cell| text_to(cell, parse_errors, $parse))
                .collect::<Vec<_>>()
        }};
    }

    let array: ArrayRef = match data_type {
        DataType::Boolean => Arc::new(BooleanArray::from(collect_with!(parse_bool))),
        DataType::Int16 => Arc::new(Int16Array::from(collect_with!(|s: &str| s
            .trim()
            .parse::<i16>()
            .ok()))),
        DataType::Int32 => Arc::new(Int32Array::from(collect_with!(|s: &str| s
            .trim()
            .parse::<i32>()
            .ok()))),
        DataType::Int64 => Arc::new(Int64Array::from(collect_with!(|s: &str| s
            .trim()
            .parse::<i64>()
            .ok()))),
        DataType::Float32 => Arc::new(Float32Array::from(collect_with!(|s: &str| s
            .trim()
            .parse::<f32>()
            .ok()))),
        DataType::Float64 => Arc::new(Float64Array::from(collect_with!(|s: &str| s
            .trim()
            .parse::<f64>()
            .ok()))),
        DataType::Utf8 => {
            // A textual column: the value is carried verbatim; it cannot fail to
            // parse, so parse_errors is untouched.
            Arc::new(StringArray::from(
                cells
                    .map(|cell| match cell {
                        TupleCol::Text(s) => Some(s.clone()),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            ))
        }
        DataType::Binary => {
            let values: Vec<Option<Vec<u8>>> = collect_with!(parse_bytea);
            Arc::new(BinaryArray::from(
                values
                    .iter()
                    .map(|o| o.as_deref())
                    .collect::<Vec<Option<&[u8]>>>(),
            ))
        }
        DataType::Date32 => Arc::new(Date32Array::from(collect_with!(parse_date32))),
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let values: Vec<Option<i64>> = if tz.is_some() {
                collect_with!(parse_timestamptz_micros)
            } else {
                collect_with!(parse_timestamp_micros)
            };
            let array = TimestampMicrosecondArray::from(values);
            match tz {
                Some(zone) => Arc::new(array.with_timezone(zone.as_ref())),
                None => Arc::new(array),
            }
        }
        DataType::Time64(TimeUnit::Microsecond) => Arc::new(Time64MicrosecondArray::from(
            collect_with!(parse_time64_micros),
        )),
        DataType::Decimal128(precision, scale) => {
            let s = *scale;
            let values: Vec<Option<i128>> = collect_with!(move |t: &str| parse_decimal_i128(t, s));
            let array = Decimal128Array::from(values)
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| ArrowError::CastError(e.to_string()))?;
            Arc::new(array)
        }
        // pgtype never maps to another Arrow type, but be total: carry as text
        // form so nothing is silently dropped.
        _ => Arc::new(StringArray::from(
            cells
                .map(|cell| match cell {
                    TupleCol::Text(s) => Some(s.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        )),
    };
    Ok(array)
}

/// Applies `parse` to a `Text` cell, counting a failure; a null / unchanged-toast
/// cell is null without counting.
fn text_to<T>(
    cell: &TupleCol,
    parse_errors: &mut u64,
    parse: impl FnOnce(&str) -> Option<T>,
) -> Option<T> {
    match cell {
        TupleCol::Null | TupleCol::UnchangedToast => None,
        TupleCol::Text(s) => match parse(s) {
            Some(v) => Some(v),
            None => {
                *parse_errors += 1;
                None
            }
        },
    }
}

/// Parses pgoutput's boolean text (`t`/`f`, with `true`/`false`/`1`/`0`
/// tolerated).
fn parse_bool(s: &str) -> Option<bool> {
    match s.trim() {
        "t" | "true" | "1" => Some(true),
        "f" | "false" | "0" => Some(false),
        _ => None,
    }
}

/// Decodes PostgreSQL's `bytea` text output (`\x` followed by hex) into bytes.
fn parse_bytea(s: &str) -> Option<Vec<u8>> {
    let hex = s.strip_prefix("\\x")?;
    if hex.len() % 2 != 0 {
        return None;
    }
    let mut out = Vec::with_capacity(hex.len() / 2);
    let bytes = hex.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let hi = (bytes[i] as char).to_digit(16)?;
        let lo = (bytes[i + 1] as char).to_digit(16)?;
        out.push(((hi << 4) | lo) as u8);
        i += 2;
    }
    Some(out)
}

/// The unix epoch date, for Date32 (days since 1970-01-01).
fn unix_epoch_date() -> NaiveDate {
    NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid epoch date")
}

/// Parses a `date` text (`YYYY-MM-DD`) into a Date32 (days since epoch).
fn parse_date32(s: &str) -> Option<i32> {
    let date = NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d").ok()?;
    Some((date - unix_epoch_date()).num_days() as i32)
}

/// Parses a `timestamp` (no zone) text into unix micros.
fn parse_timestamp_micros(s: &str) -> Option<i64> {
    let s = s.trim();
    let dt = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S"))
        .ok()?;
    Some(dt.and_utc().timestamp_micros())
}

/// Parses a `timestamptz` text (with a `+HH`/`+HH:MM` offset) into unix micros.
fn parse_timestamptz_micros(s: &str) -> Option<i64> {
    let s = s.trim();
    // PostgreSQL prints offsets like `+00`, `-05`, or `+05:30`. chrono's `%#z`
    // accepts the short forms; try a fractional and a whole-second layout.
    let parsed = chrono::DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%#z")
        .or_else(|_| chrono::DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%#z"));
    Some(parsed.ok()?.timestamp_micros())
}

/// Parses a `time` text (`HH:MM:SS[.ffffff]`) into micros since midnight.
fn parse_time64_micros(s: &str) -> Option<i64> {
    let t = NaiveTime::parse_from_str(s.trim(), "%H:%M:%S%.f")
        .or_else(|_| NaiveTime::parse_from_str(s.trim(), "%H:%M:%S"))
        .ok()?;
    let secs = t.num_seconds_from_midnight() as i64;
    let micros = (t.nanosecond() as i64) / 1_000;
    Some(secs * 1_000_000 + micros)
}

/// Parses a decimal string into an unscaled `i128` at the target `scale`. A value
/// with more fractional digits than `scale` is rejected (loss would corrupt the
/// stored value); fewer are zero-padded. Rejects non-numeric input.
fn parse_decimal_i128(s: &str, scale: i8) -> Option<i128> {
    if scale < 0 {
        return None;
    }
    let scale = scale as usize;
    let s = s.trim();
    let (neg, digits) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let (int_part, frac_part) = match digits.split_once('.') {
        Some((i, f)) => (i, f),
        None => (digits, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return None;
    }
    if !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    if frac_part.len() > scale {
        return None;
    }
    let mut unscaled = String::with_capacity(int_part.len() + scale);
    unscaled.push_str(int_part);
    unscaled.push_str(frac_part);
    for _ in 0..(scale - frac_part.len()) {
        unscaled.push('0');
    }
    let value: i128 = unscaled.parse().ok()?;
    Some(if neg { -value } else { value })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pgoutput::{Relation, RelationColumn};
    use crate::pgtype::oid;
    use crate::schema::change_row_schema;
    use arrow_array::Array;

    fn col(name: &str, type_oid: u32, type_mod: i32) -> RelationColumn {
        RelationColumn {
            flags: 0,
            name: name.to_owned(),
            type_oid,
            type_mod,
        }
    }

    fn relation(cols: Vec<RelationColumn>) -> Relation {
        Relation {
            rel_oid: 1,
            namespace: "public".to_owned(),
            rel_name: "t".to_owned(),
            replica_identity: b'd',
            columns: cols,
        }
    }

    #[test]
    fn builds_a_batch_across_all_ops() {
        let rel = relation(vec![col("id", oid::INT4, -1), col("name", oid::TEXT, -1)]);
        let schema = change_row_schema(&rel);
        let rows = vec![
            ChangeRow {
                op: Op::Resync,
                lsn: 0,
                seq: 0,
                ts: 100,
                xid: None,
                cols: vec![TupleCol::Text("1".into()), TupleCol::Text("a".into())],
            },
            ChangeRow {
                op: Op::Insert,
                lsn: 10,
                seq: 1,
                ts: 200,
                xid: Some(5),
                cols: vec![TupleCol::Text("2".into()), TupleCol::Text("b".into())],
            },
            ChangeRow {
                op: Op::Update,
                lsn: 20,
                seq: 2,
                ts: 300,
                xid: Some(5),
                cols: vec![TupleCol::Text("2".into()), TupleCol::UnchangedToast],
            },
            ChangeRow {
                op: Op::Delete,
                lsn: 30,
                seq: 3,
                ts: 400,
                xid: Some(6),
                cols: vec![TupleCol::Text("2".into()), TupleCol::Null],
            },
            ChangeRow {
                op: Op::Truncate,
                lsn: 40,
                seq: 4,
                ts: 500,
                xid: Some(7),
                cols: vec![TupleCol::Null, TupleCol::Null],
            },
        ];
        let built = build_batch(&schema, &rows).expect("batch");
        assert_eq!(built.parse_errors, 0);
        assert_eq!(built.batch.num_rows(), 5);
        assert_eq!(built.batch.num_columns(), 7);

        let ops = built
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("downcast");
        assert_eq!(ops.value(0), "R");
        assert_eq!(ops.value(1), "I");
        assert_eq!(ops.value(2), "U");
        assert_eq!(ops.value(3), "D");
        assert_eq!(ops.value(4), "T");

        // _vg_xid null on the resync row, set otherwise.
        let xids = built
            .batch
            .column(4)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("downcast");
        assert!(xids.is_null(0));
        assert_eq!(xids.value(1), 5);

        // Unchanged-toast and null both land as null in the data column.
        let ids = built
            .batch
            .column(5)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("downcast");
        assert_eq!(ids.value(1), 2);
        let names = built
            .batch
            .column(6)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("downcast");
        assert_eq!(names.value(0), "a");
        assert!(names.is_null(2)); // unchanged toast
        assert!(names.is_null(3)); // null delete
    }

    #[test]
    fn parse_failure_nulls_the_cell_and_counts_it() {
        let rel = relation(vec![col("id", oid::INT4, -1)]);
        let schema = change_row_schema(&rel);
        let rows = vec![
            ChangeRow {
                op: Op::Insert,
                lsn: 1,
                seq: 0,
                ts: 0,
                xid: Some(1),
                cols: vec![TupleCol::Text("not-an-int".into())],
            },
            ChangeRow {
                op: Op::Insert,
                lsn: 2,
                seq: 1,
                ts: 0,
                xid: Some(1),
                cols: vec![TupleCol::Text("99".into())],
            },
        ];
        let built = build_batch(&schema, &rows).expect("batch");
        assert_eq!(built.parse_errors, 1);
        let ids = built
            .batch
            .column(5)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("downcast");
        assert!(ids.is_null(0)); // the bad value nulled
        assert_eq!(ids.value(1), 99); // the good value intact
    }

    #[test]
    fn parses_typed_scalars() {
        assert_eq!(parse_bool("t"), Some(true));
        assert_eq!(parse_bool("f"), Some(false));
        assert_eq!(parse_bool("maybe"), None);
        assert_eq!(
            parse_bytea("\\xdeadbeef"),
            Some(vec![0xde, 0xad, 0xbe, 0xef])
        );
        assert_eq!(parse_bytea("nothex"), None);
        assert_eq!(parse_date32("1970-01-02"), Some(1));
        assert_eq!(
            parse_timestamp_micros("2000-01-01 00:00:00"),
            Some(946_684_800_000_000)
        );
        assert_eq!(
            parse_timestamptz_micros("2000-01-01 00:00:00+00"),
            Some(946_684_800_000_000)
        );
        assert_eq!(parse_time64_micros("01:00:00"), Some(3_600_000_000));
        assert_eq!(parse_decimal_i128("12.34", 2), Some(1234));
        assert_eq!(parse_decimal_i128("-1.5", 2), Some(-150));
        assert_eq!(parse_decimal_i128("1.234", 2), None); // too many frac digits
        assert_eq!(parse_decimal_i128("abc", 2), None);
    }

    #[test]
    fn builds_decimal_and_timestamp_columns() {
        // numeric(10,2) and timestamptz.
        let rel = relation(vec![
            col("amount", oid::NUMERIC, ((10i32 << 16) | 2) + 4),
            col("at", oid::TIMESTAMPTZ, -1),
        ]);
        let schema = change_row_schema(&rel);
        let rows = vec![ChangeRow {
            op: Op::Insert,
            lsn: 1,
            seq: 0,
            ts: 0,
            xid: Some(1),
            cols: vec![
                TupleCol::Text("12.34".into()),
                TupleCol::Text("2000-01-01 00:00:00+00".into()),
            ],
        }];
        let built = build_batch(&schema, &rows).expect("batch");
        assert_eq!(built.parse_errors, 0);
        let amt = built
            .batch
            .column(5)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("downcast");
        assert_eq!(amt.value(0), 1234);
        let at = built
            .batch
            .column(6)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("downcast");
        assert_eq!(at.value(0), 946_684_800_000_000);
    }
}
