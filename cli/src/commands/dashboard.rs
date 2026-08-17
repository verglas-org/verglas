//! `verglas dashboard` — Verglas Cloud json-render dashboards.
//!
//! Every verb targets Verglas Cloud at `/v1/dashboards`. Specs are uploaded with
//! `create --file`; static row arrays in element props or state are rejected by
//! the control plane and by local validation in `dashboard_spec`.

use std::error::Error;
use std::io::{self, Write};

use serde::{Deserialize, Serialize};

use crate::cli::DashboardCommand;

/// Dashboard information returned by Verglas Cloud.
#[derive(Debug, Deserialize, Serialize)]
struct DashboardInfo {
    name: String,
    #[serde(default)]
    title: Option<String>,
    url: String,
}

/// Dashboard list returned by Verglas Cloud.
#[derive(Debug, Deserialize, Serialize)]
struct DashboardList {
    dashboards: Vec<DashboardInfo>,
}

/// Dashboard deletion acknowledgement.
#[derive(Debug, Deserialize, Serialize)]
struct DashboardDeleted {
    deleted: String,
}

/// Runs one dashboard command against Verglas Cloud.
pub async fn run(
    command: DashboardCommand,
    endpoint: &str,
    token: Option<&str>,
    json: bool,
) -> Result<(), Box<dyn Error>> {
    crate::backend::require_cloud_dashboards(endpoint)?;
    let bearer = token.map(str::to_owned);
    match command {
        DashboardCommand::Create(args) => {
            let manifest = crate::dashboard_spec::DashboardManifest::from_file(&args.file)?;
            let body = manifest.to_create_body(args.tenant_id.as_deref());
            let info: DashboardInfo = crate::auth::cloud_call(bearer, |bearer| {
                let endpoint = endpoint.to_owned();
                let body = body.clone();
                async move {
                    let client = crate::backend::server(&endpoint, bearer.as_deref())?;
                    client.post_json("/v1/dashboards", &body).await
                }
            })
            .await?;
            emit_info(&info, json)?;
        }
        DashboardCommand::List(args) => {
            let path = match &args.tenant_id {
                Some(tenant_id) => format!("/v1/dashboards?tenant_id={tenant_id}"),
                None => "/v1/dashboards".to_owned(),
            };
            let list: DashboardList = crate::auth::cloud_call(bearer, |bearer| {
                let endpoint = endpoint.to_owned();
                let path = path.clone();
                async move {
                    let client = crate::backend::server(&endpoint, bearer.as_deref())?;
                    client.get(&path).await
                }
            })
            .await?;
            crate::output::emit(&list, json, |list| {
                let mut stdout = io::stdout();
                writeln!(stdout, "NAME\tURL")?;
                for dashboard in &list.dashboards {
                    writeln!(stdout, "{}\t{}", dashboard.name, dashboard.url)?;
                }
                Ok(())
            })?;
        }
        DashboardCommand::Get(args) => {
            let path = match &args.tenant_id {
                Some(tenant_id) => {
                    format!("/v1/dashboards/{}?tenant_id={tenant_id}", args.name)
                }
                None => format!("/v1/dashboards/{}", args.name),
            };
            let info: DashboardInfo = crate::auth::cloud_call(bearer, |bearer| {
                let endpoint = endpoint.to_owned();
                let path = path.clone();
                async move {
                    let client = crate::backend::server(&endpoint, bearer.as_deref())?;
                    client.get(&path).await
                }
            })
            .await?;
            emit_info(&info, json)?;
        }
        DashboardCommand::Delete(args) => {
            let path = match &args.tenant_id {
                Some(tenant_id) => {
                    format!("/v1/dashboards/{}?tenant_id={tenant_id}", args.name)
                }
                None => format!("/v1/dashboards/{}", args.name),
            };
            let deleted: DashboardDeleted = crate::auth::cloud_call(bearer, |bearer| {
                let endpoint = endpoint.to_owned();
                let path = path.clone();
                async move {
                    let client = crate::backend::server(&endpoint, bearer.as_deref())?;
                    client.delete(&path).await
                }
            })
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
        if let Some(title) = &info.title {
            writeln!(stdout, "title:     {title}")?;
        }
        writeln!(stdout, "url:       {}", info.url)?;
        Ok(())
    })
}
