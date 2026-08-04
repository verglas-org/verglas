//! Mapping a PostgreSQL type oid (and typmod) to an [`arrow_schema::DataType`].
//!
//! The CDC change-log columns are typed from the source table's catalog: each
//! relation column carries a PG type oid and a typmod, and this module turns
//! that pair into the Arrow type the Iceberg column will hold. The mapping is
//! deliberately conservative — every type the runner does not have a first-class
//! Arrow mapping for falls back to `Utf8` and is carried as pgoutput's own text
//! form, so an exotic column is preserved as text rather than dropped.

use arrow_schema::DataType;

/// Well-known PostgreSQL built-in type oids (from `pg_type.h`).
pub mod oid {
    /// `bool`
    pub const BOOL: u32 = 16;
    /// `bytea`
    pub const BYTEA: u32 = 17;
    /// `char` (single byte, `"char"`)
    pub const CHAR: u32 = 18;
    /// `name`
    pub const NAME: u32 = 19;
    /// `int8` / `bigint`
    pub const INT8: u32 = 20;
    /// `int2` / `smallint`
    pub const INT2: u32 = 21;
    /// `int4` / `integer`
    pub const INT4: u32 = 23;
    /// `text`
    pub const TEXT: u32 = 25;
    /// `json`
    pub const JSON: u32 = 114;
    /// `float4` / `real`
    pub const FLOAT4: u32 = 700;
    /// `float8` / `double precision`
    pub const FLOAT8: u32 = 701;
    /// `bpchar` / `char(n)`
    pub const BPCHAR: u32 = 1042;
    /// `varchar`
    pub const VARCHAR: u32 = 1043;
    /// `date`
    pub const DATE: u32 = 1082;
    /// `time`
    pub const TIME: u32 = 1083;
    /// `timestamp`
    pub const TIMESTAMP: u32 = 1114;
    /// `timestamptz`
    pub const TIMESTAMPTZ: u32 = 1184;
    /// `numeric` / `decimal`
    pub const NUMERIC: u32 = 1700;
    /// `uuid`
    pub const UUID: u32 = 2950;
    /// `jsonb`
    pub const JSONB: u32 = 3802;
}

/// The Iceberg/Arrow decimal precision ceiling. `numeric` allows unbounded
/// precision; Arrow's `Decimal128` caps at 38 significant digits, so a declared
/// precision above that is clamped.
pub const MAX_DECIMAL_PRECISION: u8 = 38;

/// Decodes a `numeric` typmod into `(precision, scale)`, both capped/clamped to
/// what `Decimal128` accepts. Returns `None` when the typmod carries no
/// precision (an unqualified `numeric`, typmod `< 0`), in which case the value
/// is carried as text.
///
/// The atttypmod layout for `numeric(p, s)` is `((p << 16) | s) + 4`.
pub fn decode_numeric_typmod(typmod: i32) -> Option<(u8, i8)> {
    if typmod < 0 {
        return None;
    }
    let packed = typmod - 4;
    let precision = ((packed >> 16) & 0xFFFF) as u32;
    let scale = (packed & 0xFFFF) as u32;
    let precision = precision.min(MAX_DECIMAL_PRECISION as u32) as u8;
    Some((precision, scale as i8))
}

/// Maps a PostgreSQL type oid and typmod to the Arrow [`DataType`] the CDC
/// column will hold. Any oid without a first-class mapping becomes `Utf8` and is
/// carried as pgoutput's text form.
pub fn pg_type_to_arrow(type_oid: u32, typmod: i32) -> DataType {
    use oid::*;
    match type_oid {
        BOOL => DataType::Boolean,
        INT2 => DataType::Int16,
        INT4 => DataType::Int32,
        INT8 => DataType::Int64,
        FLOAT4 => DataType::Float32,
        FLOAT8 => DataType::Float64,
        NUMERIC => match decode_numeric_typmod(typmod) {
            Some((precision, scale)) => DataType::Decimal128(precision, scale),
            None => DataType::Utf8,
        },
        TEXT | VARCHAR | BPCHAR | CHAR | NAME => DataType::Utf8,
        BYTEA => DataType::Binary,
        DATE => DataType::Date32,
        TIMESTAMP => DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
        TIMESTAMPTZ => DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
        TIME => DataType::Time64(arrow_schema::TimeUnit::Microsecond),
        UUID => DataType::Utf8,
        JSON | JSONB => DataType::Utf8,
        _ => DataType::Utf8,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::TimeUnit;

    #[test]
    fn maps_the_scalar_types() {
        assert_eq!(pg_type_to_arrow(oid::BOOL, -1), DataType::Boolean);
        assert_eq!(pg_type_to_arrow(oid::INT2, -1), DataType::Int16);
        assert_eq!(pg_type_to_arrow(oid::INT4, -1), DataType::Int32);
        assert_eq!(pg_type_to_arrow(oid::INT8, -1), DataType::Int64);
        assert_eq!(pg_type_to_arrow(oid::FLOAT4, -1), DataType::Float32);
        assert_eq!(pg_type_to_arrow(oid::FLOAT8, -1), DataType::Float64);
        assert_eq!(pg_type_to_arrow(oid::BYTEA, -1), DataType::Binary);
        assert_eq!(pg_type_to_arrow(oid::DATE, -1), DataType::Date32);
        assert_eq!(
            pg_type_to_arrow(oid::TIME, -1),
            DataType::Time64(TimeUnit::Microsecond)
        );
    }

    #[test]
    fn maps_string_family_to_utf8() {
        for o in [
            oid::TEXT,
            oid::VARCHAR,
            oid::BPCHAR,
            oid::CHAR,
            oid::NAME,
            oid::UUID,
            oid::JSON,
            oid::JSONB,
        ] {
            assert_eq!(pg_type_to_arrow(o, -1), DataType::Utf8, "oid {o}");
        }
    }

    #[test]
    fn maps_timestamps_with_and_without_zone() {
        assert_eq!(
            pg_type_to_arrow(oid::TIMESTAMP, -1),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            pg_type_to_arrow(oid::TIMESTAMPTZ, -1),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
    }

    #[test]
    fn numeric_typmod_decodes_precision_and_scale() {
        // numeric(10,2): typmod = ((10<<16)|2)+4
        let typmod = ((10i32 << 16) | 2) + 4;
        assert_eq!(decode_numeric_typmod(typmod), Some((10, 2)));
        assert_eq!(
            pg_type_to_arrow(oid::NUMERIC, typmod),
            DataType::Decimal128(10, 2)
        );
    }

    #[test]
    fn numeric_without_typmod_is_text() {
        assert_eq!(decode_numeric_typmod(-1), None);
        assert_eq!(pg_type_to_arrow(oid::NUMERIC, -1), DataType::Utf8);
    }

    #[test]
    fn numeric_precision_is_capped_at_38() {
        // numeric(60,4): precision must clamp to 38.
        let typmod = ((60i32 << 16) | 4) + 4;
        assert_eq!(decode_numeric_typmod(typmod), Some((38, 4)));
    }

    #[test]
    fn unknown_oid_falls_back_to_utf8() {
        assert_eq!(pg_type_to_arrow(999_999, -1), DataType::Utf8);
    }
}
