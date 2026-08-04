//! The Iceberg/Arrow change-row schema for a PG relation, and the schema-diff
//! that drives evolution decisions.
//!
//! Every CDC change is written as one change row: a fixed block of reserved
//! metadata columns (documented `_vg_` prefix) followed by one column per PG
//! relation column. The metadata columns record what kind of change it was and
//! where in the WAL it happened; the data columns carry the row's values. A
//! delete carries only the replica-identity columns and an unchanged-TOAST value
//! carries nothing, so every data column is nullable.
//!
//! The diff classifies a fresh [`Relation`] against a table's current columns so
//! the runner can decide: evolve on an added column, fail loudly on a column
//! whose mapped Arrow type changed.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};

use crate::pgoutput::Relation;
use crate::pgtype::pg_type_to_arrow;

/// The reserved metadata-column name prefix. No PG relation column may collide
/// with a name under this prefix (PG identifiers starting `_vg_` are the
/// platform's reserved namespace).
pub const RESERVED_PREFIX: &str = "_vg_";

/// The change-operation column: `"I"`, `"U"`, `"D"`, `"T"`, or `"R"`.
pub const COL_OP: &str = "_vg_op";
/// The WAL LSN of the change.
pub const COL_LSN: &str = "_vg_lsn";
/// A per-drain monotonic sequence, the tiebreak within one LSN/batch.
pub const COL_SEQ: &str = "_vg_seq";
/// The change's commit timestamp (unix micros, UTC).
pub const COL_TS: &str = "_vg_ts";
/// The change's transaction id (nullable — a resync row has none).
pub const COL_XID: &str = "_vg_xid";

/// The number of reserved metadata columns that precede the data columns.
pub const RESERVED_COLUMN_COUNT: usize = 5;

/// The reserved metadata columns, in the order they lead the change-row schema.
/// `_vg_op`/`_vg_lsn`/`_vg_seq`/`_vg_ts` are non-null; `_vg_xid` is nullable.
pub fn reserved_fields() -> Vec<Field> {
    vec![
        Field::new(COL_OP, DataType::Utf8, false),
        Field::new(COL_LSN, DataType::Int64, false),
        Field::new(COL_SEQ, DataType::Int64, false),
        Field::new(
            COL_TS,
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new(COL_XID, DataType::Int64, true),
    ]
}

/// The Arrow [`DataType`] of each of a relation's PG columns, in ordinal order,
/// paired with the column name. This is the projection the diff and the schema
/// builder both work from.
pub fn relation_column_types(relation: &Relation) -> Vec<(String, DataType)> {
    relation
        .columns
        .iter()
        .map(|c| (c.name.clone(), pg_type_to_arrow(c.type_oid, c.type_mod)))
        .collect()
}

/// Builds the change-row [`SchemaRef`] for a relation: the reserved metadata
/// columns first, then one nullable column per PG relation column, typed via
/// [`crate::pgtype`].
pub fn change_row_schema(relation: &Relation) -> SchemaRef {
    let mut fields = reserved_fields();
    for (name, data_type) in relation_column_types(relation) {
        fields.push(Field::new(name, data_type, true));
    }
    Arc::new(Schema::new(fields))
}

/// How one PG relation column compares to a table's current columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnDiff {
    /// The column is present with the same mapped Arrow type — no change.
    Unchanged,
    /// The column is not in the table yet — add it (as a nullable column).
    Added,
    /// The column is present but its mapped Arrow type changed — an
    /// incompatible change the runner must fail loudly on.
    TypeChanged {
        /// The table's current Arrow type for the column.
        old: DataType,
        /// The relation's new mapped Arrow type.
        new: DataType,
    },
}

/// Classifies each column of `relation` against `existing` — the table's current
/// data columns (name, Arrow type), excluding the reserved metadata columns.
/// Returns one `(name, ColumnDiff)` per relation column, in relation order.
///
/// A column absent from `existing` is [`ColumnDiff::Added`]; a column present
/// with a different mapped type is [`ColumnDiff::TypeChanged`]; otherwise
/// [`ColumnDiff::Unchanged`]. A column dropped from the relation but still in the
/// table is not reported — CDC never drops table columns.
pub fn diff_columns(
    existing: &[(String, DataType)],
    relation: &Relation,
) -> Vec<(String, ColumnDiff)> {
    let new_types = relation_column_types(relation);
    new_types
        .into_iter()
        .map(|(name, new_type)| {
            let diff = match existing.iter().find(|(n, _)| n == &name) {
                None => ColumnDiff::Added,
                Some((_, old_type)) if old_type == &new_type => ColumnDiff::Unchanged,
                Some((_, old_type)) => ColumnDiff::TypeChanged {
                    old: old_type.clone(),
                    new: new_type,
                },
            };
            (name, diff)
        })
        .collect()
}

/// The data columns of a change-row schema — every field after the reserved
/// metadata block — as `(name, Arrow type)` pairs. This is the `existing`
/// projection [`diff_columns`] compares a new relation against, recovered from a
/// live table's schema.
pub fn data_columns_of(schema: &Schema) -> Vec<(String, DataType)> {
    schema
        .fields()
        .iter()
        .skip(RESERVED_COLUMN_COUNT)
        .map(|f| (f.name().clone(), f.data_type().clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pgoutput::{Relation, RelationColumn};
    use crate::pgtype::oid;

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
    fn reserved_columns_lead_the_schema_in_order() {
        let rel = relation(vec![col("id", oid::INT4, -1)]);
        let schema = change_row_schema(&rel);
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec!["_vg_op", "_vg_lsn", "_vg_seq", "_vg_ts", "_vg_xid", "id"]
        );
        // Reserved nullability contract.
        assert!(!schema.field(0).is_nullable()); // _vg_op
        assert!(!schema.field(1).is_nullable()); // _vg_lsn
        assert!(!schema.field(2).is_nullable()); // _vg_seq
        assert!(!schema.field(3).is_nullable()); // _vg_ts
        assert!(schema.field(4).is_nullable()); // _vg_xid
        // Data column is nullable and typed via pgtype.
        assert!(schema.field(5).is_nullable());
        assert_eq!(schema.field(5).data_type(), &DataType::Int32);
    }

    #[test]
    fn added_column_is_detected() {
        let existing = vec![("id".to_owned(), DataType::Int32)];
        let rel = relation(vec![col("id", oid::INT4, -1), col("name", oid::TEXT, -1)]);
        let diff = diff_columns(&existing, &rel);
        assert_eq!(diff[0], ("id".to_owned(), ColumnDiff::Unchanged));
        assert_eq!(diff[1], ("name".to_owned(), ColumnDiff::Added));
    }

    #[test]
    fn int4_to_text_is_type_changed() {
        // Table had id as int4 -> Int32; relation now maps id to text -> Utf8.
        let existing = vec![("id".to_owned(), DataType::Int32)];
        let rel = relation(vec![col("id", oid::TEXT, -1)]);
        let diff = diff_columns(&existing, &rel);
        assert_eq!(
            diff[0],
            (
                "id".to_owned(),
                ColumnDiff::TypeChanged {
                    old: DataType::Int32,
                    new: DataType::Utf8,
                }
            )
        );
    }

    #[test]
    fn data_columns_recovers_the_projection() {
        let rel = relation(vec![col("id", oid::INT4, -1), col("name", oid::TEXT, -1)]);
        let schema = change_row_schema(&rel);
        let data = data_columns_of(&schema);
        assert_eq!(
            data,
            vec![
                ("id".to_owned(), DataType::Int32),
                ("name".to_owned(), DataType::Utf8),
            ]
        );
    }
}
