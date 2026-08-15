//! Portable json-render dashboard spec validation for the CLI.

use std::error::Error;
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The spec version this CLI writes and understands.
pub const SPEC_VERSION: u32 = 1;

const DATA_WIDGETS: &[&str] = &[
    "Table",
    "LogStream",
    "Chart",
    "GraphView",
    "VectorHits",
    "Metric",
];

/// A complete dashboard spec posted to Verglas Cloud.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardManifest {
    pub spec_version: u32,
    pub name: String,
    pub title: String,
    #[serde(default)]
    pub sources: serde_json::Map<String, Value>,
    pub root: String,
    pub elements: serde_json::Map<String, Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<serde_json::Map<String, Value>>,
}

impl DashboardManifest {
    /// Reads a spec file (`.toml` as TOML, otherwise JSON).
    pub fn from_file(path: &Path) -> Result<DashboardManifest, Box<dyn Error>> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("could not read spec file {}: {e}", path.display()))?;
        let manifest: DashboardManifest =
            if path.extension().and_then(|e| e.to_str()) == Some("toml") {
                toml::from_str(&text)
                    .map_err(|e| format!("{} is not valid TOML: {e}", path.display()))?
            } else {
                serde_json::from_str(&text)
                    .map_err(|e| format!("{} is not valid JSON: {e}", path.display()))?
            };
        manifest.validate()?;
        Ok(manifest)
    }

    /// Checks the spec is internally consistent and rejects embedded static datasets.
    pub fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.spec_version != SPEC_VERSION {
            return Err(format!(
                "unsupported dashboard spec_version {}: this CLI understands version {SPEC_VERSION}",
                self.spec_version
            )
            .into());
        }
        if !is_valid_name(&self.name) {
            return Err("the dashboard name must match ^[a-z][a-z0-9_-]{0,62}$".into());
        }
        if self.title.trim().is_empty() {
            return Err("the dashboard spec needs a title".into());
        }
        if self.root.trim().is_empty() {
            return Err("the dashboard spec needs a root element key".into());
        }
        if self.elements.is_empty() {
            return Err("the dashboard spec needs at least one element".into());
        }
        if !self.elements.contains_key(&self.root) {
            return Err("the dashboard root element is missing from elements".into());
        }

        if let Some(state) = &self.state {
            for (key, value) in state {
                if is_record_array(value) {
                    return Err(format!("state.{key} must not embed record arrays").into());
                }
            }
        }

        for (element_id, element) in &self.elements {
            let Some(element_obj) = element.as_object() else {
                return Err(format!("element {element_id} must be an object").into());
            };
            let props = element_obj.get("props").and_then(Value::as_object);
            if props.is_some_and(|props| contains_embedded_records(&Value::Object(props.clone()))) {
                return Err(format!("element {element_id} embeds static row data").into());
            }
            let element_type = element_obj
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("");
            if DATA_WIDGETS.contains(&element_type) {
                let source = props
                    .and_then(|props| props.get("source"))
                    .and_then(Value::as_str)
                    .unwrap_or("");
                if source.is_empty() || !self.sources.contains_key(source) {
                    return Err(format!("element {element_id} requires a declared source").into());
                }
            }
        }

        Ok(())
    }

    /// Serializes the manifest for `POST /v1/dashboards`.
    pub fn to_create_body(&self, tenant_id: Option<&str>) -> Value {
        let mut body = serde_json::to_value(self).expect("dashboard manifest serializes");
        if let Some(tenant_id) = tenant_id {
            body["tenant_id"] = Value::String(tenant_id.to_owned());
        }
        body
    }
}

fn is_valid_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    if bytes.is_empty() || bytes.len() > 63 {
        return false;
    }
    if !bytes[0].is_ascii_lowercase() {
        return false;
    }
    bytes[1..].iter().all(|byte| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'_' || *byte == b'-'
    })
}

fn is_record_array(value: &Value) -> bool {
    let Some(items) = value.as_array() else {
        return false;
    };
    !items.is_empty()
        && items
            .iter()
            .all(|entry| entry.is_object() && !entry.is_array())
}

fn contains_embedded_records(value: &Value) -> bool {
    match value {
        Value::Array(items) => items.iter().any(contains_embedded_records),
        Value::Object(map) => map.iter().any(|(key, entry)| {
            if key == "rows" && is_record_array(entry) {
                return true;
            }
            contains_embedded_records(entry)
        }),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_json_dashboard_spec() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("dash.json");
        std::fs::write(
            &path,
            r#"{
              "spec_version": 1,
              "name": "orders",
              "title": "Orders",
              "sources": {
                "rows": { "type": "table", "table": "sales.orders", "follow": true }
              },
              "root": "page",
              "elements": {
                "page": { "type": "Table", "props": { "source": "rows" }, "children": [] }
              }
            }"#,
        )
        .expect("write spec");
        let manifest = DashboardManifest::from_file(&path).expect("parse spec");
        assert_eq!(manifest.name, "orders");
    }

    #[test]
    fn rejects_static_rows_in_elements() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("bad.json");
        std::fs::write(
            &path,
            r#"{
              "spec_version": 1,
              "name": "bad",
              "title": "Bad",
              "sources": {
                "rows": { "type": "table", "table": "sales.orders", "follow": true }
              },
              "root": "page",
              "elements": {
                "page": {
                  "type": "Table",
                  "props": { "source": "rows", "rows": [{ "id": 1 }] },
                  "children": []
                }
              }
            }"#,
        )
        .expect("write spec");
        let err = DashboardManifest::from_file(&path).expect_err("static rows");
        assert!(err.to_string().contains("static row data"));
    }
}
