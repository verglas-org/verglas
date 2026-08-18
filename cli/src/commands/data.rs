//! `verglas ingest`/`verglas sql` — the /v0 data-plane verbs on Verglas Cloud:
//! one append-ingest shape (NDJSON to `POST /v0/events?name=<datasource>`) for
//! table writes, vector writes, and log shipping alike, plus ad hoc SQL
//! through `POST /v0/sql`.

use std::error::Error;
use std::io::Read;
use std::path::Path;

use serde_json::Value;
use verglas_sdk::DataClient;

use crate::cli::{IngestArgs, SqlArgs};

/// Appends NDJSON rows read from `--file` or standard input to one datasource.
pub async fn ingest(
    endpoint: &str,
    token: Option<&str>,
    args: IngestArgs,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let token = token.ok_or("not signed in; run `verglas login`")?;
    let rows = read_ndjson(args.file.as_deref())?;
    let client = DataClient::connect(endpoint, token)?;
    let result = client.append_events(&args.datasource, &rows).await?;
    if json {
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        println!(
            "appended {} row(s) to {} ({} quarantined)",
            result.successful_rows, args.datasource, result.quarantined_rows
        );
    }
    Ok(())
}

/// Runs one SQL query and prints its result rows.
pub async fn sql(
    endpoint: &str,
    token: Option<&str>,
    args: SqlArgs,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let token = token.ok_or("not signed in; run `verglas login`")?;
    let client = DataClient::connect(endpoint, token)?;
    let result: Value = client.sql(&args.query).await?;
    if json {
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        print_rows(&result);
    }
    Ok(())
}

/// Reads newline-delimited JSON rows from a file or standard input, one JSON
/// object per non-empty line, and rejects an empty result.
fn read_ndjson(file: Option<&Path>) -> Result<Vec<Value>, Box<dyn Error>> {
    let text = match file {
        Some(path) => std::fs::read_to_string(path)
            .map_err(|error| format!("could not read {}: {error}", path.display()))?,
        None => {
            let mut buffer = String::new();
            std::io::stdin().read_to_string(&mut buffer)?;
            buffer
        }
    };
    let mut rows = Vec::new();
    for (index, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(line)
            .map_err(|error| format!("line {}: invalid JSON: {error}", index + 1))?;
        rows.push(value);
    }
    if rows.is_empty() {
        return Err("no NDJSON rows to ingest".into());
    }
    Ok(rows)
}

/// Renders a `{meta, data}` query result as a tab-separated
/// table, falling back to raw JSON when the response does not match that shape.
fn print_rows(result: &Value) {
    let columns: Vec<String> = result
        .get("meta")
        .and_then(Value::as_array)
        .map(|meta| {
            meta.iter()
                .filter_map(|column| column.get("name").and_then(Value::as_str))
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();
    let rows = result.get("data").and_then(Value::as_array);
    let Some(rows) = rows.filter(|_| !columns.is_empty()) else {
        println!(
            "{}",
            serde_json::to_string_pretty(result).unwrap_or_else(|_| result.to_string())
        );
        return;
    };
    if rows.is_empty() {
        println!("(no rows)");
        return;
    }
    println!("{}", columns.join("\t"));
    for row in rows {
        let cells: Vec<String> = columns
            .iter()
            .map(|column| match row.get(column) {
                None | Some(Value::Null) => String::new(),
                Some(Value::String(value)) => value.clone(),
                Some(other) => other.to_string(),
            })
            .collect();
        println!("{}", cells.join("\t"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each non-empty line becomes one row; blank lines are skipped.
    #[test]
    fn read_ndjson_skips_blank_lines() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("rows.ndjson");
        std::fs::write(&path, "{\"a\":1}\n\n{\"a\":2}\n").expect("write rows");
        let rows = read_ndjson(Some(&path)).expect("parsed rows");
        assert_eq!(
            rows,
            vec![serde_json::json!({"a": 1}), serde_json::json!({"a": 2})]
        );
    }

    /// A file with no rows is rejected rather than silently ingesting nothing.
    #[test]
    fn read_ndjson_rejects_an_empty_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("empty.ndjson");
        std::fs::write(&path, "\n\n").expect("write empty");
        assert!(read_ndjson(Some(&path)).is_err());
    }

    /// A malformed line names its line number in the error.
    #[test]
    fn read_ndjson_reports_the_bad_line_number() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("bad.ndjson");
        std::fs::write(&path, "{\"a\":1}\nnot-json\n").expect("write rows");
        let error = read_ndjson(Some(&path)).expect_err("must fail");
        assert!(error.to_string().contains("line 2"), "{error}");
    }

    /// A meta/data query result renders as a tab-separated table.
    #[test]
    fn print_rows_does_not_panic_on_a_typical_query_result() {
        print_rows(&serde_json::json!({
            "meta": [{"name": "count", "type": "String"}],
            "data": [{"count": 3}],
            "rows": 1,
        }));
    }

    /// A response without the meta/data shape still renders without panicking.
    #[test]
    fn print_rows_falls_back_to_raw_json_for_an_unrecognized_shape() {
        print_rows(&serde_json::json!({"ok": true}));
    }
}
