//! Extracting `(id, vector)` rows from an Arrow `RecordBatch`.
//!
//! Shared by the maintenance MV (reads the source delta) and the brute-force
//! fallback (reads the whole column). An embedding stored as a
//! `FixedSizeList<Float32>` or `List<Float32>` decodes to a vector; a **null**
//! embedding is a *tombstone* — the append-only-friendly delete signal the
//! maintenance MV consumes (a row re-appended with a null vector deletes that
//! id). The id column must be a non-null `Int64` or `Int32`.

use arrow_array::cast::AsArray;
use arrow_array::types::Float32Type;
use arrow_array::{Array, Float32Array, Int32Array, Int64Array, RecordBatch};
use arrow_schema::DataType;

use crate::error::{Result, VectorError};

/// One extracted row: an id and either its vector or `None` (a delete tombstone
/// — the embedding was null).
#[derive(Debug, Clone)]
pub struct VectorRow {
    /// The row's identity value.
    pub id: i64,
    /// The embedding, or `None` when the embedding column was null (a delete).
    pub vector: Option<Vec<f32>>,
}

/// Extracts `(id, vector?)` rows from `batch`, reading `id_col` as the identity
/// and `vec_col` as the embedding. Returns an error if a column is missing or
/// has an unsupported type; a null id row is skipped (it cannot be keyed).
pub fn extract_rows(batch: &RecordBatch, id_col: &str, vec_col: &str) -> Result<Vec<VectorRow>> {
    let schema = batch.schema();
    let id_idx = schema
        .index_of(id_col)
        .map_err(|_| VectorError::NoIdColumn(format!("column '{id_col}' not found")))?;
    let vec_idx = schema
        .index_of(vec_col)
        .map_err(|_| VectorError::Field(format!("column '{vec_col}' not found")))?;

    let id_array = batch.column(id_idx);
    let ids: Vec<Option<i64>> = match id_array.data_type() {
        DataType::Int64 => {
            let a = id_array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| VectorError::NoIdColumn("id not Int64".to_owned()))?;
            (0..a.len())
                .map(|i| (!a.is_null(i)).then(|| a.value(i)))
                .collect()
        }
        DataType::Int32 => {
            let a = id_array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| VectorError::NoIdColumn("id not Int32".to_owned()))?;
            (0..a.len())
                .map(|i| (!a.is_null(i)).then(|| a.value(i) as i64))
                .collect()
        }
        other => {
            return Err(VectorError::NoIdColumn(format!(
                "id column '{id_col}' must be Int64/Int32, is {other:?}"
            )));
        }
    };

    let vec_array = batch.column(vec_idx);
    let vectors = extract_vectors(vec_array.as_ref(), vec_col)?;

    let mut out = Vec::with_capacity(ids.len());
    for (id, vector) in ids.into_iter().zip(vectors) {
        let Some(id) = id else { continue };
        out.push(VectorRow { id, vector });
    }
    Ok(out)
}

/// Decodes an embedding column into per-row `Option<Vec<f32>>` (null → `None`).
fn extract_vectors(array: &dyn Array, col: &str) -> Result<Vec<Option<Vec<f32>>>> {
    match array.data_type() {
        DataType::FixedSizeList(field, width) => {
            if !matches!(field.data_type(), DataType::Float32) {
                return Err(VectorError::Field(format!(
                    "embedding '{col}' must be list of Float32, child is {:?}",
                    field.data_type()
                )));
            }
            let list = array.as_fixed_size_list();
            let width = *width as usize;
            let mut out = Vec::with_capacity(list.len());
            for i in 0..list.len() {
                if list.is_null(i) {
                    out.push(None);
                    continue;
                }
                let values = list.value(i);
                let floats = values
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| VectorError::Field("FixedSizeList child not Float32".into()))?;
                let mut v = Vec::with_capacity(width);
                for j in 0..floats.len() {
                    v.push(floats.value(j));
                }
                out.push(Some(v));
            }
            Ok(out)
        }
        DataType::List(field) => {
            if !matches!(field.data_type(), DataType::Float32) {
                return Err(VectorError::Field(format!(
                    "embedding '{col}' must be list of Float32, child is {:?}",
                    field.data_type()
                )));
            }
            let list = array.as_list::<i32>();
            let mut out = Vec::with_capacity(list.len());
            for i in 0..list.len() {
                if list.is_null(i) {
                    out.push(None);
                    continue;
                }
                let values = list.value(i);
                let floats = values.as_primitive::<Float32Type>();
                out.push(Some(floats.values().to_vec()));
            }
            Ok(out)
        }
        other => Err(VectorError::Field(format!(
            "embedding column '{col}' must be FixedSizeList/List of Float32, is {other:?}"
        ))),
    }
}
