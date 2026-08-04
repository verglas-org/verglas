//! Direct Iceberg REST catalog access for `verglas table delete`.
//!
//! Every other table verb goes through the local daemon, but dropping a table is
//! a catalog control-plane operation: it removes the table's entry from the
//! tenant's Iceberg REST catalog (the per-tenant `catalogd`). This module reads
//! the `[catalog]` section of `~/.verglas/config.toml` — the same uri and bearer
//! the daemon's catalog watcher uses — resolves the route prefix from
//! `/v1/config`, and issues the REST `DELETE .../namespaces/{ns}/tables/{table}`.

use std::error::Error;
use std::time::Duration;

use serde::Deserialize;
use verglas_core::config::{self, Catalog};

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
    /// Builds a client from the `[catalog]` section of the agent config file
    /// (`$VERGLAS_CONFIG` or `~/.verglas/config.toml`). A missing file, a missing
    /// `[catalog]` section, or an unresolvable bearer token each fails with a
    /// clear message rather than a silent no-auth request.
    pub fn from_agent_config() -> Result<CatalogClient, Box<dyn Error>> {
        let path = config::agent_config_path()
            .ok_or("no config file: set VERGLAS_CONFIG or HOME to locate ~/.verglas/config.toml")?;
        let text = std::fs::read_to_string(&path)
            .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
        // Parse only the `[catalog]` table; the rest of the daemon config (which
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
        Self::from_catalog(&catalog)
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
    /// drop-table path is built the same way the daemon's catalog client builds
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

    /// Adds the bearer header when one is configured.
    fn authed(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.bearer {
            Some(token) => request.bearer_auth(token),
            None => request,
        }
    }
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
