//! Direct Iceberg REST catalog access for table metadata and deletion.
//!
//! List, show, history, and delete are catalog operations, so they bypass
//! Verglas and communicate with the configured customer catalog. This module
//! resolves the catalog's REST prefix and applies its bearer token directly.

use std::error::Error;
use std::time::Duration;

use serde::Deserialize;
use serde_json::Value;
use verglas_core::config::{self, Catalog};
use verglas_sdk::report::{
    FieldInfo, HistoryReport, ListReport, ShowReport, SnapshotInfo, TableRef,
};

/// A minimal authenticated client for the tenant's Iceberg REST catalog.
pub struct CatalogClient {
    /// Base URI (before `/v1/...`), no trailing slash.
    base: String,
    /// Bearer token added to every request (bearer mode). SigV4-signed catalogs
    /// are not supported here — `table delete` targets the tenant catalogd, which
    /// authenticates with a bearer token.
    bearer: Option<String>,
    /// Optional warehouse passed to `/v1/config` so a multi-warehouse catalog
    /// returns the right route prefix.
    warehouse: Option<String>,
    /// Shared HTTP client.
    http: reqwest::Client,
}

/// `GET /v1/config` response: only the route `prefix` override matters here.
#[derive(Deserialize, Default)]
struct ConfigResponse {
    #[serde(default)]
    overrides: serde_json::Map<String, serde_json::Value>,
    #[serde(default)]
    defaults: serde_json::Map<String, serde_json::Value>,
}

impl CatalogClient {
    /// Builds a client from the `[catalog]` section of the user config file
    /// (`$VERGLAS_CONFIG` or `~/.verglas/config.toml`). A missing file, a missing
    /// `[catalog]` section, or an unresolvable bearer token each fails with a
    /// clear message rather than a silent no-auth request.
    pub fn from_agent_config(token: Option<&str>) -> Result<CatalogClient, Box<dyn Error>> {
        let path = config::user_config_path()
            .ok_or("no config file: set VERGLAS_CONFIG or HOME to locate ~/.verglas/config.toml")?;
        let text = std::fs::read_to_string(&path)
            .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
        // Parse only the `[catalog]` table; the rest of the server config (which
        // the CLI does not need) is ignored.
        #[derive(Deserialize)]
        struct CatalogOnly {
            catalog: Option<Catalog>,
        }
        let parsed: CatalogOnly =
            toml::from_str(&text).map_err(|e| format!("cannot parse {}: {e}", path.display()))?;
        let catalog = parsed
            .catalog
            .ok_or_else(|| format!("no [catalog] section in {}", path.display()))?;
        let mut client = Self::from_catalog(&catalog)?;
        if let Some(token) = token {
            client.bearer = Some(token.to_owned());
        }
        Ok(client)
    }

    /// Builds a client from a parsed `[catalog]` config.
    fn from_catalog(catalog: &Catalog) -> Result<CatalogClient, Box<dyn Error>> {
        let bearer = catalog.resolve_bearer_token()?;
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(5))
            .build()
            .map_err(|e| format!("building the HTTP client: {e}"))?;
        Ok(CatalogClient {
            base: catalog.uri.trim_end_matches('/').to_owned(),
            bearer,
            warehouse: catalog.warehouse.clone(),
            http,
        })
    }

    /// The catalog base URI, for the confirmation message.
    pub fn base(&self) -> &str {
        &self.base
    }

    /// Resolves the route root (`{base}/v1` or `{base}/v1/{prefix}`) by reading
    /// `/v1/config` once. The tenant catalogd advertises its prefix there, so the
    /// drop-table path is built the same way the server's catalog client builds
    /// it.
    async fn route_root(&self) -> Result<String, Box<dyn Error>> {
        let mut url = format!("{}/v1/config", self.base);
        if let Some(warehouse) = &self.warehouse {
            url.push_str("?warehouse=");
            url.push_str(&encode(warehouse));
        }
        let request = self.authed(self.http.get(&url));
        let response = request.send().await?;
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let body = response.text().await.unwrap_or_default();
            return Err(format!("catalog GET /v1/config failed (HTTP {status}): {body}").into());
        }
        let config: ConfigResponse = response.json().await?;
        let prefix = config
            .overrides
            .get("prefix")
            .or_else(|| config.defaults.get("prefix"))
            .and_then(|value| value.as_str())
            .unwrap_or_default();
        if prefix.is_empty() {
            Ok(format!("{}/v1", self.base))
        } else {
            Ok(format!("{}/v1/{prefix}", self.base))
        }
    }

    /// Issues `DELETE .../namespaces/{ns}/tables/{table}` against the catalog.
    /// `namespace` is the full dotted namespace (multi-level levels joined with
    /// the REST unit separator); a non-2xx status surfaces the catalog's message.
    pub async fn drop_table(
        &self,
        namespace: &[String],
        table: &str,
    ) -> Result<(), Box<dyn Error>> {
        let root = self.route_root().await?;
        let url = format!(
            "{root}/namespaces/{}/tables/{}",
            encode_namespace(namespace),
            encode(table)
        );
        let request = self.authed(self.http.delete(&url));
        let response = request.send().await?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            let detail = if body.is_empty() {
                String::new()
            } else {
                format!(": {body}")
            };
            return Err(format!(
                "catalog refused the drop (HTTP {}){detail}",
                status.as_u16()
            )
            .into());
        }
        Ok(())
    }

    /// Lists table identifiers directly from the Iceberg REST catalog.
    pub async fn list_tables(&self, namespace: Option<&str>) -> Result<ListReport, Box<dyn Error>> {
        let root = self.route_root().await?;
        let namespaces = match namespace {
            Some(namespace) => vec![namespace.split('.').map(str::to_owned).collect::<Vec<_>>()],
            None => {
                let value = self.get_json(&format!("{root}/namespaces")).await?;
                serde_json::from_value::<NamespacesResponse>(value)?.namespaces
            }
        };
        let mut tables = Vec::new();
        for namespace in namespaces {
            let value = self
                .get_json(&format!(
                    "{root}/namespaces/{}/tables",
                    encode_namespace(&namespace)
                ))
                .await?;
            for identifier in serde_json::from_value::<IdentifiersResponse>(value)?.identifiers {
                tables.push(TableRef {
                    namespace: identifier.namespace.join("."),
                    name: identifier.name,
                });
            }
        }
        tables.sort_by(|a, b| (&a.namespace, &a.name).cmp(&(&b.namespace, &b.name)));
        Ok(ListReport { tables })
    }

    /// Reads a table's current metadata directly from Iceberg REST.
    pub async fn show_table(&self, dotted: &str) -> Result<ShowReport, Box<dyn Error>> {
        let metadata = self.load_metadata(dotted).await?;
        let schema_id = metadata.get("current-schema-id").and_then(Value::as_i64);
        let schema = metadata
            .get("schemas")
            .and_then(Value::as_array)
            .and_then(|schemas| {
                schemas
                    .iter()
                    .find(|schema| schema.get("schema-id").and_then(Value::as_i64) == schema_id)
            })
            .and_then(|schema| schema.get("fields"))
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .map(|field| FieldInfo {
                id: field.get("id").and_then(Value::as_i64).unwrap_or_default() as i32,
                name: field
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                r#type: iceberg_type_label(field.get("type").unwrap_or(&Value::Null)),
                required: field
                    .get("required")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            })
            .collect();
        let spec_id = metadata.get("default-spec-id").and_then(Value::as_i64);
        let partition_by = metadata
            .get("partition-specs")
            .and_then(Value::as_array)
            .and_then(|specs| {
                specs
                    .iter()
                    .find(|spec| spec.get("spec-id").and_then(Value::as_i64) == spec_id)
            })
            .and_then(|spec| spec.get("fields"))
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|field| field.get("name").and_then(Value::as_str).map(str::to_owned))
            .collect();
        let current_snapshot_id = metadata.get("current-snapshot-id").and_then(Value::as_i64);
        let summary = current_snapshot_id.and_then(|id| {
            metadata
                .get("snapshots")?
                .as_array()?
                .iter()
                .find(|snapshot| snapshot.get("snapshot-id").and_then(Value::as_i64) == Some(id))?
                .get("summary")
        });
        Ok(ShowReport {
            table: dotted.to_owned(),
            schema,
            partition_by,
            row_count: summary.and_then(|s| summary_u64(s, "total-records")),
            file_count: summary.and_then(|s| summary_u64(s, "total-data-files")),
            size_bytes: summary.and_then(|s| summary_u64(s, "total-files-size")),
            current_snapshot_id,
        })
    }

    /// Reads the snapshot log directly from Iceberg REST.
    pub async fn table_history(&self, dotted: &str) -> Result<HistoryReport, Box<dyn Error>> {
        let metadata = self.load_metadata(dotted).await?;
        let mut snapshots = metadata
            .get("snapshots")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .map(|snapshot| {
                let timestamp_ms = snapshot
                    .get("timestamp-ms")
                    .and_then(Value::as_i64)
                    .unwrap_or_default();
                let summary = snapshot
                    .get("summary")
                    .and_then(Value::as_object)
                    .map(|summary| {
                        summary
                            .iter()
                            .filter_map(|(key, value)| {
                                value.as_str().map(|value| (key.clone(), value.to_owned()))
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                SnapshotInfo {
                    snapshot_id: snapshot
                        .get("snapshot-id")
                        .and_then(Value::as_i64)
                        .unwrap_or_default(),
                    parent_snapshot_id: snapshot.get("parent-snapshot-id").and_then(Value::as_i64),
                    timestamp_ms,
                    timestamp: chrono::DateTime::from_timestamp_millis(timestamp_ms)
                        .map(|time| time.to_rfc3339())
                        .unwrap_or_else(|| format!("epoch-ms:{timestamp_ms}")),
                    operation: snapshot
                        .get("summary")
                        .and_then(|summary| summary.get("operation"))
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                    summary,
                }
            })
            .collect::<Vec<_>>();
        snapshots.sort_by_key(|snapshot| snapshot.timestamp_ms);
        Ok(HistoryReport {
            table: dotted.to_owned(),
            snapshots,
        })
    }

    async fn load_metadata(&self, dotted: &str) -> Result<Value, Box<dyn Error>> {
        let (namespace, table) = split_ident(dotted)?;
        let root = self.route_root().await?;
        let value = self
            .get_json(&format!(
                "{root}/namespaces/{}/tables/{}",
                encode_namespace(&namespace),
                encode(&table)
            ))
            .await?;
        value
            .get("metadata")
            .cloned()
            .ok_or_else(|| "catalog load-table response omitted metadata".into())
    }

    async fn get_json(&self, url: &str) -> Result<Value, Box<dyn Error>> {
        let response = self.authed(self.http.get(url)).send().await?;
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let body = response.text().await.unwrap_or_default();
            return Err(format!("catalog GET failed (HTTP {status}): {body}").into());
        }
        Ok(response.json().await?)
    }

    /// Adds the bearer header when one is configured.
    fn authed(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.bearer {
            Some(token) => request.bearer_auth(token),
            None => request,
        }
    }
}

#[derive(Deserialize)]
struct NamespacesResponse {
    namespaces: Vec<Vec<String>>,
}

#[derive(Deserialize)]
struct IdentifiersResponse {
    identifiers: Vec<TableIdentifier>,
}

#[derive(Deserialize)]
struct TableIdentifier {
    namespace: Vec<String>,
    name: String,
}

fn iceberg_type_label(value: &Value) -> String {
    match value {
        Value::String(label) => label.clone(),
        other => serde_json::to_string(other).unwrap_or_else(|_| "unknown".to_owned()),
    }
}

fn summary_u64(summary: &Value, key: &str) -> Option<u64> {
    summary
        .get(key)
        .and_then(|value| value.as_u64().or_else(|| value.as_str()?.parse().ok()))
}

/// Splits a dotted `namespace.name` identifier into its namespace levels and
/// table name, matching the platform's identifier rule: the last component is
/// the table, everything before it is the (possibly multi-level) namespace. An
/// empty component or a bare name (no namespace) is rejected.
pub fn split_ident(dotted: &str) -> Result<(Vec<String>, String), Box<dyn Error>> {
    let parts: Vec<&str> = dotted.split('.').collect();
    if parts.len() < 2 || parts.iter().any(|p| p.is_empty()) {
        return Err(format!(
            "`{dotted}` is not a valid table identifier: expected `namespace.name`"
        )
        .into());
    }
    let (name, namespace) = parts
        .split_last()
        .ok_or_else(|| format!("`{dotted}` is not a valid table identifier"))?;
    Ok((
        namespace.iter().map(|s| (*s).to_owned()).collect(),
        (*name).to_owned(),
    ))
}

/// Percent-encodes one URL path segment (RFC 3986 unreserved characters pass
/// through). Multi-level namespaces travel as an encoded unit-separated string.
fn encode(component: &str) -> String {
    let mut out = String::with_capacity(component.len());
    for byte in component.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(byte as char);
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// Joins namespace levels with the REST unit separator (`0x1F`) and encodes the
/// result, as the Iceberg REST spec requires for a namespace path component.
fn encode_namespace(namespace: &[String]) -> String {
    encode(&namespace.join("\u{1f}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_ident_separates_namespace_and_table() {
        let (ns, table) = split_ident("agent_data.orders").expect("valid");
        assert_eq!(ns, vec!["agent_data".to_owned()]);
        assert_eq!(table, "orders");

        let (ns, table) = split_ident("a.b.c").expect("valid");
        assert_eq!(ns, vec!["a".to_owned(), "b".to_owned()]);
        assert_eq!(table, "c");
    }

    #[test]
    fn split_ident_rejects_bare_and_empty() {
        assert!(split_ident("orders").is_err());
        assert!(split_ident("a..b").is_err());
        assert!(split_ident("").is_err());
    }

    #[test]
    fn encode_namespace_joins_with_unit_separator() {
        assert_eq!(encode_namespace(&["a".to_owned(), "b".to_owned()]), "a%1Fb");
        assert_eq!(encode_namespace(&["agent_data".to_owned()]), "agent_data");
    }
}
