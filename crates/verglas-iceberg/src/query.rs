//! Embedded SQL query over Iceberg tables.
//!
//! Queries run in-process against the catalog and the S3 endpoint the
//! connection names, so a warm read stays local. Without `--at`, the whole
//! catalog is registered and tables are referenced as `namespace.table`
//! (joins across tables work). With `--at <ref> <table>`, a single table is
//! pinned to a snapshot — by snapshot id, or by a timestamp resolved to the
//! snapshot live at that instant — for time-travel and provenance queries.
//!
//! The embedded engine is DataFusion with the Iceberg table provider. The
//! issue names DuckDB with the iceberg extension; that path is unavailable in
//! this build (see the crate docs / PR notes) so the offline-native DataFusion
//! provider stands in, wired to the same catalog and endpoint.

use std::any::Any;
use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::scalar::ScalarValue;
use futures::{StreamExt, TryStreamExt};
use iceberg::{Catalog, TableIdent};
use iceberg_datafusion::{IcebergCatalogProvider, IcebergStaticTableProvider};

use crate::error::{AgentError, Result};
use crate::ident::parse_table_ident;
use crate::report::QueryReport;
use verglas_api::query::QueryParameter;

/// A time-travel request: pin `table` to the snapshot named by `reference` (a
/// snapshot id or a timestamp) for the query.
#[derive(Debug, Clone)]
pub struct TimeTravel {
    /// A snapshot id, an epoch-millis timestamp, or an RFC 3339 timestamp.
    pub reference: String,
    /// The `namespace.table` to pin.
    pub table: String,
}

/// The DataFusion catalog name the whole-catalog path registers under and sets
/// as the default, so SQL can name tables as `namespace.table`.
const CATALOG_NAME: &str = "verglas";

/// Planned query metadata and its lazily executed record-batch stream.
///
/// This is the shared streaming primitive: `query`'s buffered `QueryReport`
/// is just `query_stream` collected, and every non-collecting caller (the
/// standalone query role's own `/v1/query` in `bins/query-node`, its
/// `estimate` module walking a plan without executing it, and the server's
/// embedded Arrow-IPC response path) is built on this same call, not a
/// second copy of it.
pub struct QueryExecution {
    /// Result column names in query order.
    pub columns: Vec<String>,
    /// Incremental DataFusion output; reading drives object-store I/O.
    pub batches: SendableRecordBatchStream,
}

/// Plans `sql` against `catalog` and returns its incremental execution stream
/// without collecting the result in memory: `execute_stream` builds the
/// physical plan and hands back a pull-based [`SendableRecordBatchStream`] —
/// nothing is read from the cache/backend until the caller polls it. Memory
/// tracks the query's operator working set (a join's hash table, a sort's
/// buffer) as it runs, not the result size, so a caller can stream each batch
/// out (over HTTP, to a file, wherever) as it is produced rather than holding
/// the whole result.
///
/// A planning error (bad SQL, an unknown table) surfaces here, before any
/// batch is pulled — the same point `query`'s `.collect()` would fail at. A
/// runtime error (a real read failure partway through) surfaces later, from
/// polling the returned stream; a caller that has already started forwarding
/// batches to its own caller cannot un-send those bytes, so this is a
/// documented, unavoidable property of streaming, not a bug.
pub async fn query_stream(
    catalog: Arc<dyn Catalog>,
    sql: &str,
    at: Option<TimeTravel>,
) -> Result<QueryExecution> {
    query_stream_with_params(catalog, sql, at, Vec::new()).await
}

/// Plans and streams a query after binding typed positional parameters.
pub async fn query_stream_with_params(
    catalog: Arc<dyn Catalog>,
    sql: &str,
    at: Option<TimeTravel>,
    params: Vec<QueryParameter>,
) -> Result<QueryExecution> {
    let ctx = match at {
        Some(travel) => time_travel_context(catalog, travel).await?,
        None => catalog_context(catalog).await?,
    };

    let sql = number_positional_placeholders(sql, params.len())?;
    let dataframe = ctx
        .sql(&sql)
        .await
        .map_err(|error| AgentError::Query(error.to_string()))?;
    let dataframe = dataframe
        .with_param_values(
            params
                .into_iter()
                .map(parameter_scalar)
                .collect::<Result<Vec<_>>>()?,
        )
        .map_err(|error| AgentError::Query(error.to_string()))?;
    let columns = dataframe
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    let batches = dataframe
        .execute_stream()
        .await
        .map_err(|error| AgentError::Query(error.to_string()))?;
    Ok(QueryExecution { columns, batches })
}

/// Runs `sql` against `catalog`. When `at` is set, a single time-traveled table
/// is registered under its bare name; otherwise the whole catalog is registered
/// and tables are referenced as `namespace.table`.
///
/// This collects the whole result into memory before returning — the CLI's
/// `verglas query` still needs one complete `QueryReport` value. A caller
/// that must not hold the whole result in memory uses [`query_stream`]
/// instead, which never collects.
pub async fn query(
    catalog: Arc<dyn Catalog>,
    sql: &str,
    at: Option<TimeTravel>,
) -> Result<QueryReport> {
    query_with_params(catalog, sql, at, Vec::new()).await
}

/// Runs a query with typed positional parameters and buffers its JSON report.
pub async fn query_with_params(
    catalog: Arc<dyn Catalog>,
    sql: &str,
    at: Option<TimeTravel>,
    params: Vec<QueryParameter>,
) -> Result<QueryReport> {
    let execution = query_stream_with_params(catalog, sql, at, params).await?;
    let columns = execution.columns;
    let batches = execution
        .batches
        .map(|batch| batch.map_err(|error| AgentError::Query(error.to_string())))
        .try_collect::<Vec<_>>()
        .await?;
    let rows = batches_to_rows(&batches)?;
    let row_count = rows.len();
    Ok(QueryReport {
        columns,
        rows,
        row_count,
    })
}

/// A session with the whole Iceberg catalog registered as the default, so a
/// two-part `namespace.table` reference resolves.
///
/// The Iceberg provider is wrapped so table (and schema) names resolve
/// case-insensitively. DataFusion folds an unquoted identifier to lowercase
/// before it reaches the provider, so without this an unquoted reference to a
/// table registered with uppercase letters would miss the exact-cased name and
/// fail to plan. A table identifier is an identifier, not a case-sensitive
/// string.
pub(crate) async fn catalog_context(catalog: Arc<dyn Catalog>) -> Result<SessionContext> {
    let provider = IcebergCatalogProvider::try_new(catalog)
        .await
        .map_err(AgentError::Iceberg)?;
    let provider = CaseInsensitiveCatalogProvider::new(Arc::new(provider));
    let config = SessionConfig::new()
        .with_default_catalog_and_schema(CATALOG_NAME, "default")
        .with_information_schema(true);
    let ctx = SessionContext::new_with_config(config);
    ctx.register_catalog(CATALOG_NAME, Arc::new(provider));
    Ok(ctx)
}

/// Converts one wire parameter into the scalar DataFusion binds to `$N`.
fn parameter_scalar(param: QueryParameter) -> Result<ScalarValue> {
    match param {
        QueryParameter::Null => Ok(ScalarValue::Null),
        QueryParameter::Boolean(value) => Ok(ScalarValue::Boolean(Some(value))),
        QueryParameter::Int64(value) => Ok(ScalarValue::Int64(Some(value))),
        QueryParameter::Uint64(value) => Ok(ScalarValue::UInt64(Some(value))),
        QueryParameter::Float64(value) => Ok(ScalarValue::Float64(Some(value))),
        QueryParameter::String(value) => Ok(ScalarValue::Utf8(Some(value))),
        QueryParameter::Timestamp(value) => {
            let parsed = chrono::DateTime::parse_from_rfc3339(&value).map_err(|error| {
                AgentError::Query(format!(
                    "invalid timestamp query parameter `{value}`: {error}"
                ))
            })?;
            Ok(ScalarValue::TimestampMicrosecond(
                Some(parsed.timestamp_micros()),
                Some("UTC".into()),
            ))
        }
        QueryParameter::Date(value) => {
            let parsed =
                chrono::NaiveDate::parse_from_str(&value, "%Y-%m-%d").map_err(|error| {
                    AgentError::Query(format!("invalid date query parameter `{value}`: {error}"))
                })?;
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                .ok_or_else(|| AgentError::Query("could not construct Unix epoch".to_owned()))?;
            let days = parsed.signed_duration_since(epoch).num_days();
            let days = i32::try_from(days).map_err(|_| {
                AgentError::Query(format!("date query parameter is out of range: {value}"))
            })?;
            Ok(ScalarValue::Date32(Some(days)))
        }
    }
}

/// Rewrites Rill's `?` placeholders to DataFusion's `$N` form without touching
/// quoted strings, quoted identifiers, or SQL comments.
fn number_positional_placeholders(sql: &str, expected: usize) -> Result<String> {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum State {
        Plain,
        SingleQuote,
        DoubleQuote,
        LineComment,
        BlockComment,
    }

    let bytes = sql.as_bytes();
    let mut output = String::with_capacity(sql.len() + expected * 2);
    let mut state = State::Plain;
    let mut index = 0;
    let mut count = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        let next = bytes.get(index + 1).copied();
        match state {
            State::Plain if byte == b'\'' => state = State::SingleQuote,
            State::Plain if byte == b'"' => state = State::DoubleQuote,
            State::Plain if byte == b'-' && next == Some(b'-') => state = State::LineComment,
            State::Plain if byte == b'/' && next == Some(b'*') => state = State::BlockComment,
            State::Plain if byte == b'?' => {
                count += 1;
                output.push('$');
                output.push_str(&count.to_string());
                index += 1;
                continue;
            }
            State::SingleQuote if byte == b'\'' && next == Some(b'\'') => {
                output.push_str("''");
                index += 2;
                continue;
            }
            State::SingleQuote if byte == b'\'' => state = State::Plain,
            State::DoubleQuote if byte == b'"' && next == Some(b'"') => {
                output.push_str("\"\"");
                index += 2;
                continue;
            }
            State::DoubleQuote if byte == b'"' => state = State::Plain,
            State::LineComment if byte == b'\n' => state = State::Plain,
            State::BlockComment if byte == b'*' && next == Some(b'/') => {
                output.push_str("*/");
                index += 2;
                state = State::Plain;
                continue;
            }
            _ => {}
        }
        if byte.is_ascii() {
            output.push(char::from(byte));
            index += 1;
        } else {
            let character = sql[index..].chars().next().ok_or_else(|| {
                AgentError::Query("query ended inside a UTF-8 character".to_owned())
            })?;
            output.push(character);
            index += character.len_utf8();
        }
    }
    if count != expected {
        return Err(AgentError::Query(format!(
            "query has {count} positional placeholders but received {expected} parameters"
        )));
    }
    Ok(output)
}

#[cfg(test)]
mod parameter_tests {
    use super::number_positional_placeholders;

    /// Placeholder numbering ignores question marks inside quoted SQL and
    /// comments instead of treating them as values to bind.
    #[test]
    fn numbers_only_plain_sql_placeholders() {
        let sql = "SELECT 'café?', \"?\" FROM t WHERE a = ? -- ?\nAND b = ? /* ? */";
        let numbered = number_positional_placeholders(sql, 2).expect("number placeholders");
        assert_eq!(
            numbered,
            "SELECT 'café?', \"?\" FROM t WHERE a = $1 -- ?\nAND b = $2 /* ? */"
        );
    }
}

/// A [`CatalogProvider`] that resolves schema and table names case-insensitively
/// on top of an inner provider.
///
/// The resolution rule is exact-first: an exact-case name always wins, and only
/// when it misses does a case-insensitive scan of the registered names run. The
/// platform never produces two tables in one namespace (or two namespaces)
/// differing only by case — the write path and the REST catalog name them
/// deterministically — so the fallback is unambiguous; the exact-first order
/// resolves even a hypothetical collision to the exact name.
#[derive(Debug)]
struct CaseInsensitiveCatalogProvider {
    /// The wrapped provider that owns the actual schemas and tables.
    inner: Arc<dyn CatalogProvider>,
}

impl CaseInsensitiveCatalogProvider {
    /// Wraps `inner` so its schema lookups fall back to case-insensitive matching.
    fn new(inner: Arc<dyn CatalogProvider>) -> Self {
        Self { inner }
    }
}

impl CatalogProvider for CaseInsensitiveCatalogProvider {
    /// Exposes this provider for downcasting, as the trait requires.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Delegates the exact-cased schema names of the inner provider.
    fn schema_names(&self) -> Vec<String> {
        self.inner.schema_names()
    }

    /// Resolves a schema exact-first, then case-insensitively, wrapping the
    /// result so its tables resolve case-insensitively too.
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let schema = self.inner.schema(name).or_else(|| {
            let matched = self
                .inner
                .schema_names()
                .into_iter()
                .find(|candidate| candidate.eq_ignore_ascii_case(name))?;
            self.inner.schema(&matched)
        })?;
        Some(Arc::new(CaseInsensitiveSchemaProvider::new(schema)))
    }
}

/// A [`SchemaProvider`] that resolves table names case-insensitively on top of
/// an inner schema. Exact-first, with a case-insensitive fallback — see
/// [`CaseInsensitiveCatalogProvider`] for the unambiguity assumption.
#[derive(Debug)]
struct CaseInsensitiveSchemaProvider {
    /// The wrapped schema that owns the actual tables.
    inner: Arc<dyn SchemaProvider>,
}

impl CaseInsensitiveSchemaProvider {
    /// Wraps `inner` so its table lookups fall back to case-insensitive matching.
    fn new(inner: Arc<dyn SchemaProvider>) -> Self {
        Self { inner }
    }

    /// The exact-cased registered name that matches `name` ignoring ASCII case,
    /// if any. Used only after an exact lookup has missed.
    fn match_name(&self, name: &str) -> Option<String> {
        self.inner
            .table_names()
            .into_iter()
            .find(|candidate| candidate.eq_ignore_ascii_case(name))
    }
}

#[async_trait::async_trait]
impl SchemaProvider for CaseInsensitiveSchemaProvider {
    /// Exposes this provider for downcasting, as the trait requires.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Delegates the exact-cased table names of the inner schema.
    fn table_names(&self) -> Vec<String> {
        self.inner.table_names()
    }

    /// Resolves a table exact-first, then by the single case-insensitive match.
    async fn table(
        &self,
        name: &str,
    ) -> std::result::Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        if let Some(table) = self.inner.table(name).await? {
            return Ok(Some(table));
        }
        match self.match_name(name) {
            Some(matched) => self.inner.table(&matched).await,
            None => Ok(None),
        }
    }

    /// True when a table resolves exact or case-insensitively.
    fn table_exist(&self, name: &str) -> bool {
        self.inner.table_exist(name) || self.match_name(name).is_some()
    }
}

/// A session with one time-traveled table registered under its bare name.
async fn time_travel_context(
    catalog: Arc<dyn Catalog>,
    travel: TimeTravel,
) -> Result<SessionContext> {
    let ident = parse_table_ident(&travel.table)?;
    let table = catalog.load_table(&ident).await?;
    let snapshot_id = resolve_reference(&table, &travel.reference)?;
    let provider = IcebergStaticTableProvider::try_new_from_table_snapshot(table, snapshot_id)
        .await
        .map_err(AgentError::Iceberg)?;
    let ctx = SessionContext::new();
    ctx.register_table(ident.name(), Arc::new(provider))
        .map_err(|e| AgentError::Query(e.to_string()))?;
    Ok(ctx)
}

/// Resolves a time-travel reference to a snapshot id: an exact snapshot id
/// first, then a timestamp (epoch millis or RFC 3339) mapped to the snapshot
/// live at that instant.
pub(crate) fn resolve_reference(table: &iceberg::table::Table, reference: &str) -> Result<i64> {
    let metadata = table.metadata();

    // An exact snapshot id wins.
    if let Ok(id) = reference.parse::<i64>()
        && metadata.snapshot_by_id(id).is_some()
    {
        return Ok(id);
    }

    // Otherwise treat it as a timestamp: epoch millis, then RFC 3339.
    let target_ms = reference
        .parse::<i64>()
        .ok()
        .or_else(|| {
            chrono::DateTime::parse_from_rfc3339(reference)
                .ok()
                .map(|dt| dt.timestamp_millis())
        })
        .ok_or_else(|| {
            AgentError::Query(format!(
                "`{reference}` is neither a snapshot id nor a timestamp (epoch-ms or RFC 3339)"
            ))
        })?;

    metadata
        .snapshots()
        .filter(|snapshot| snapshot.timestamp_ms() <= target_ms)
        .max_by_key(|snapshot| snapshot.timestamp_ms())
        .map(|snapshot| snapshot.snapshot_id())
        .ok_or_else(|| {
            AgentError::Query(format!(
                "no snapshot of `{}` is at or before {reference}",
                parse_ident_name(table)
            ))
        })
}

/// The dotted name of a loaded table, for error messages.
fn parse_ident_name(table: &iceberg::table::Table) -> String {
    let ident: &TableIdent = table.identifier();
    crate::ident::ident_to_dotted(ident)
}

/// Serializes result batches to one JSON object per row, keyed by column name.
fn batches_to_rows(
    batches: &[arrow_array::RecordBatch],
) -> Result<Vec<serde_json::Map<String, serde_json::Value>>> {
    if batches.iter().all(|b| b.num_rows() == 0) {
        return Ok(Vec::new());
    }
    let refs: Vec<&arrow_array::RecordBatch> = batches.iter().collect();
    let mut writer = arrow_json::ArrayWriter::new(Vec::new());
    writer
        .write_batches(&refs)
        .map_err(|e| AgentError::Query(e.to_string()))?;
    writer
        .finish()
        .map_err(|e| AgentError::Query(e.to_string()))?;
    let bytes = writer.into_inner();
    let values: Vec<serde_json::Value> =
        serde_json::from_slice(&bytes).map_err(|e| AgentError::Query(e.to_string()))?;
    let rows = values
        .into_iter()
        .filter_map(|v| match v {
            serde_json::Value::Object(map) => Some(map),
            _ => None,
        })
        .collect();
    Ok(rows)
}

/// Serializes one [`arrow_array::RecordBatch`] to the *inner* bytes of a JSON
/// row-object array — `{"a":1,"b":2},{"c":3,"d":4}`, with no enclosing `[`/`]`
/// — plus its row count. A streaming caller ([`crate::estimate`] does not use
/// this; the HTTP layer in `bins/query-node`/`bins/verglas-server` does) splices
/// this between its own `[`/`]` as batches arrive, one call per batch, so
/// nothing beyond one batch is ever held in memory building the response body.
/// An empty batch returns empty bytes and `0`, never a bare `[]`.
pub fn batch_to_json_rows_fragment(batch: &arrow_array::RecordBatch) -> Result<(Vec<u8>, usize)> {
    let row_count = batch.num_rows();
    if row_count == 0 {
        return Ok((Vec::new(), 0));
    }
    let mut writer = arrow_json::ArrayWriter::new(Vec::new());
    writer
        .write_batches(&[batch])
        .map_err(|e| AgentError::Query(e.to_string()))?;
    writer
        .finish()
        .map_err(|e| AgentError::Query(e.to_string()))?;
    let mut bytes = writer.into_inner();
    // `ArrayWriter` always wraps its output in `[`/`]`; strip them since the
    // caller manages the enclosing array across batches itself.
    if bytes.first() == Some(&b'[') && bytes.last() == Some(&b']') {
        bytes.remove(0);
        bytes.pop();
    }
    Ok((bytes, row_count))
}
