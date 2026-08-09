//! `verglas query` — SQL over Iceberg tables through the server (issue #287).
//!
//! Posts the SQL to the selected database's
//! `POST /v1/databases/{database}/query` route (#84); the server dispatches it
//! to that database's isolated query role whose object reads return through
//! Verglas, and the CLI embeds nothing. `--at <ref> <table>` pins a table to a
//! snapshot for time travel. Output is `--json` (a stable `{columns,
//! rows, row_count}` shape) or a human-readable table.

use std::error::Error;

use verglas_sdk::report::QueryReport;

use crate::cli::QueryArgs;

/// Runs `verglas query`.
pub async fn run(args: QueryArgs, server_endpoint: &str, json: bool) -> Result<(), Box<dyn Error>> {
    let client = crate::backend::server(server_endpoint)?;

    // `--at REF TABLE` arrives as a two-element vector (clap enforces the arity).
    let at = match args.at {
        Some(pair) => {
            let mut it = pair.into_iter();
            let reference = it.next().ok_or("`--at` needs a snapshot id or timestamp")?;
            let table = it.next().ok_or("`--at` needs a table name")?;
            Some(serde_json::json!({"reference": reference, "table": table}))
        }
        None => None,
    };

    let mut body = serde_json::json!({"sql": args.sql});
    if let Some(at) = at {
        body["at"] = at;
    }
    let report: QueryReport = client
        .post_json(&format!("/v1/databases/{}/query", args.database), &body)
        .await?;
    if json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        render(&report);
    }
    Ok(())
}

/// Renders the query result as a plain aligned table.
fn render(report: &QueryReport) {
    if report.columns.is_empty() {
        println!("(no columns)");
        return;
    }
    println!("{}", report.columns.join("\t"));
    for row in &report.rows {
        let cells: Vec<String> = report
            .columns
            .iter()
            .map(|col| match row.get(col) {
                Some(serde_json::Value::String(s)) => s.clone(),
                Some(serde_json::Value::Null) | None => String::new(),
                Some(other) => other.to_string(),
            })
            .collect();
        println!("{}", cells.join("\t"));
    }
    println!("({} row(s))", report.row_count);
}
