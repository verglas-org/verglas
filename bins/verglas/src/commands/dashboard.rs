//! `verglas dashboard` commands for the optional on-prem Rill integration.

use std::error::Error;
use std::io::{self, Write};

use serde::{Deserialize, Serialize};

use crate::cli::DashboardCommand;

/// Dashboard information returned by `verglas-rest`.
#[derive(Debug, Deserialize, Serialize)]
struct DashboardInfo {
    name: String,
    table: String,
    url: String,
}

/// Dashboard list returned by `verglas-rest`.
#[derive(Debug, Deserialize, Serialize)]
struct DashboardList {
    dashboards: Vec<DashboardInfo>,
}

/// Dashboard deletion acknowledgement.
#[derive(Debug, Deserialize, Serialize)]
struct DashboardDeleted {
    deleted: String,
}

/// Runs one dashboard command against the selected server endpoint.
pub async fn run(
    command: DashboardCommand,
    endpoint: &str,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let client = crate::backend::server(endpoint)?;
    match command {
        DashboardCommand::Create(args) => {
            let mut body = serde_json::json!({"table": args.table});
            if let Some(name) = args.name {
                body["name"] = serde_json::Value::String(name);
            }
            let info: DashboardInfo = client.post_json("/v1/dashboards", &body).await?;
            emit_info(&info, json)?;
        }
        DashboardCommand::List => {
            let list: DashboardList = client.get("/v1/dashboards").await?;
            crate::output::emit(&list, json, |list| {
                let mut stdout = io::stdout();
                writeln!(stdout, "NAME\tTABLE\tURL")?;
                for dashboard in &list.dashboards {
                    writeln!(
                        stdout,
                        "{}\t{}\t{}",
                        dashboard.name, dashboard.table, dashboard.url
                    )?;
                }
                Ok(())
            })?;
        }
        DashboardCommand::Show(args) => {
            let info: DashboardInfo = client.get(&format!("/v1/dashboards/{}", args.name)).await?;
            emit_info(&info, json)?;
        }
        DashboardCommand::Delete(args) => {
            let deleted: DashboardDeleted = client
                .delete(&format!("/v1/dashboards/{}", args.name))
                .await?;
            crate::output::emit(&deleted, json, |deleted| {
                writeln!(io::stdout(), "deleted dashboard {}", deleted.deleted)?;
                Ok(())
            })?;
        }
    }
    Ok(())
}

/// Emits a dashboard as JSON or a concise human-readable record.
fn emit_info(info: &DashboardInfo, json: bool) -> Result<(), crate::output::OutputError> {
    crate::output::emit(info, json, |info| {
        let mut stdout = io::stdout();
        writeln!(stdout, "dashboard: {}", info.name)?;
        writeln!(stdout, "table:     {}", info.table)?;
        writeln!(stdout, "url:       {}", info.url)?;
        Ok(())
    })
}
