//! Serializable result shapes returned by every storage-engine verb.
//!
//! Pure wire contracts live in `verglas-api`; this module adds only the
//! Iceberg-specific schema conversion used to construct those reports.

use iceberg::spec::{NestedFieldRef, Type};
pub use verglas_api::report::*;

/// Converts an Iceberg schema field into the shared API report shape.
pub fn field_info(field: &NestedFieldRef) -> FieldInfo {
    FieldInfo {
        id: field.id,
        name: field.name.clone(),
        r#type: type_label(&field.field_type),
        required: field.required,
    }
}

/// Produces the stable API label for an Iceberg type.
pub fn type_label(ty: &Type) -> String {
    match serde_json::to_value(ty) {
        Ok(serde_json::Value::String(value)) => value,
        Ok(other) => other.to_string(),
        Err(_) => "unknown".to_owned(),
    }
}
