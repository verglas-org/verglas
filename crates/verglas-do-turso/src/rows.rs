//! Honest JSON conversion for Turso query rows.
//!
//! SQLite values retain their type at the WIT JSON-row boundary: null stays
//! null, blobs become base64 strings through serde_json, and non-finite real
//! values are rejected instead of being silently changed.

use serde_json::{Map, Number, Value as JsonValue};
use turso::{Row, Value};

use crate::error::{Error, Result};

/// Converts one Turso scalar without changing its meaning.
pub fn value_to_json(value: Value) -> Result<JsonValue> {
    match value {
        Value::Null => Ok(JsonValue::Null),
        Value::Integer(value) => Ok(JsonValue::Number(value.into())),
        Value::Real(value) => Number::from_f64(value)
            .map(JsonValue::Number)
            .ok_or_else(|| Error::JsonValue(format!("non-finite real {value}"))),
        Value::Text(value) => Ok(JsonValue::String(value)),
        Value::Blob(value) => Ok(JsonValue::String(base64_encode(&value))),
    }
}

/// Encodes binary SQL values as the JSON boundary's canonical base64 string.
fn base64_encode(value: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::with_capacity(value.len().div_ceil(3) * 4);
    for chunk in value.chunks(3) {
        let first = chunk[0];
        output.push(TABLE[(first >> 2) as usize] as char);
        let second = chunk.get(1).copied();
        output.push(
            TABLE[(((first & 0x03) << 4) | second.map_or(0, |byte| byte >> 4)) as usize] as char,
        );
        match second {
            Some(second) => {
                let third = chunk.get(2).copied();
                output.push(
                    TABLE[(((second & 0x0f) << 2) | third.map_or(0, |byte| byte >> 6)) as usize]
                        as char,
                );
                output.push(third.map_or('=', |byte| TABLE[(byte & 0x3f) as usize] as char));
            }
            None => {
                output.push('=');
                output.push('=');
            }
        }
    }
    output
}

/// Converts every row in a Turso result into JSON objects with column names.
pub async fn rows_to_json(rows: &mut turso::Rows) -> Result<Vec<JsonValue>> {
    let names = rows.column_names();
    let mut output = Vec::new();
    while let Some(row) = rows.next().await? {
        output.push(row_to_json(&names, &row)?);
    }
    Ok(output)
}

/// Converts one typed Turso row into a JSON object.
fn row_to_json(names: &[String], row: &Row) -> Result<JsonValue> {
    let mut object = Map::new();
    for (index, name) in names.iter().enumerate() {
        let value = row.get_value(index)?;
        object.insert(name.clone(), value_to_json(value)?);
    }
    Ok(JsonValue::Object(object))
}
