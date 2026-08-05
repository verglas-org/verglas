//! `verglas dashboard` commands for the optional on-prem Rill integration.

use std::error::Error;
use std::io::{self, Write};

use verglas_sdk::{Client, ConnectOptions, DashboardDeleted, DashboardInfo, DashboardList};

use crate::cli::DashboardCommand;

/// Runs one dashboard command against the selected server endpoint.
pub async fn run(
    command: DashboardCommand,
    endpoint: &str,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    let client = Client::connect(ConnectOptions::new(endpoint)).await?;
    match command {
        DashboardCommand::Create(args) => {
            let info = client
                .create_dashboard(&args.table, args.name.as_deref())
                .await?;
            emit_info(&info, json)?;
        }
        DashboardCommand::List => {
            let list: DashboardList = client.list_dashboards().await?;
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
            let info = client.show_dashboard(&args.name).await?;
            emit_info(&info, json)?;
        }
        DashboardCommand::Refresh(args) => {
            let info = client.refresh_dashboard(&args.name).await?;
            emit_info(&info, json)?;
        }
        DashboardCommand::Delete(args) => {
            let deleted: DashboardDeleted = client.delete_dashboard(&args.name).await?;
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
