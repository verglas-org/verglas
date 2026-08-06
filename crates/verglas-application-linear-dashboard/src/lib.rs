//! Linear dashboard Application Vessel.

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{Value, json};

struct AppState {
    integration_url: String,
    client: reqwest::Client,
}

/// Builds the Linear dashboard HTTP application.
pub fn router(integration_url: impl Into<String>) -> Router {
    let state = Arc::new(AppState {
        integration_url: integration_url.into().trim_end_matches('/').to_owned(),
        client: reqwest::Client::new(),
    });
    Router::new()
        .route("/", get(index))
        .route("/health", get(health))
        .route("/api/dashboard", get(dashboard))
        .with_state(state)
}

/// Reports application process health.
async fn health() -> StatusCode {
    StatusCode::NO_CONTENT
}

/// Serves the single-file generated dashboard UI.
async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

/// Loads the dashboard's bounded data from the Linear integration Vessel.
async fn dashboard(State(state): State<Arc<AppState>>) -> Result<Json<Value>, AppError> {
    let viewer = query(&state, json!({"operation":"viewer"})).await?;
    let teams = query(&state, json!({"operation":"teams","limit":25})).await?;
    let issues = query(&state, json!({"operation":"issues","limit":50})).await?;
    Ok(Json(
        json!({ "viewer": viewer, "teams": teams, "issues": issues }),
    ))
}

/// Calls one semantic operation on the configured integration Vessel.
async fn query(state: &AppState, request: Value) -> Result<Value, AppError> {
    let response = state
        .client
        .post(format!("{}/v1/query", state.integration_url))
        .json(&request)
        .send()
        .await
        .map_err(|error| AppError(error.to_string()))?;
    let status = response.status();
    let value = response
        .json::<Value>()
        .await
        .map_err(|error| AppError(error.to_string()))?;
    if status.is_success() {
        Ok(value)
    } else {
        Err(AppError(format!(
            "integration returned HTTP {status}: {value}"
        )))
    }
}

struct AppError(String);

impl IntoResponse for AppError {
    /// Converts integration failures to a bounded dashboard response.
    fn into_response(self) -> Response {
        (StatusCode::BAD_GATEWAY, Json(json!({ "error": self.0 }))).into_response()
    }
}

const INDEX_HTML: &str = r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Linear Workspace</title><style>
:root{color-scheme:dark;font-family:Inter,ui-sans-serif,system-ui;background:#090b10;color:#f3f5f7}*{box-sizing:border-box}
body{margin:0;background:radial-gradient(circle at 15% 0,#20243a 0,transparent 35%),#090b10}main{max-width:1200px;margin:auto;padding:40px 24px}
header{display:flex;justify-content:space-between;gap:24px;align-items:end;margin-bottom:30px}.eyebrow{color:#8c93ff;font:700 12px monospace;letter-spacing:.12em;text-transform:uppercase}
h1{font-size:38px;margin:7px 0 5px}.muted{color:#969daa}.status{padding:8px 12px;border:1px solid #30364a;border-radius:20px;color:#aeb4c1}
.metrics{display:grid;grid-template-columns:repeat(3,1fr);gap:14px;margin:22px 0}.card,.panel{background:#11141cdd;border:1px solid #282d3c;border-radius:14px;box-shadow:0 20px 60px #0005}
.card{padding:18px}.metric{font-size:30px;font-weight:700;margin-top:7px}.grid{display:grid;grid-template-columns:280px 1fr;gap:14px}.panel{padding:18px}h2{font-size:16px;margin:0 0 14px}
.team{padding:12px;border-radius:10px;background:#181c27;margin:8px 0}.team b{display:block}.team span{color:#969daa;font-size:13px}
.issue{display:grid;grid-template-columns:85px 1fr 90px;gap:12px;padding:13px 5px;border-top:1px solid #252a38;align-items:start}.issue a{color:#edf0ff;text-decoration:none}.issue a:hover{text-decoration:underline}
.id{font:12px monospace;color:#8c93ff}.priority{font-size:12px;color:#adb3c0;text-align:right}.empty{padding:30px;color:#969daa;text-align:center}@media(max-width:760px){.grid{grid-template-columns:1fr}.metrics{grid-template-columns:1fr}.issue{grid-template-columns:72px 1fr}.priority{display:none}}
</style></head><body><main><header><div><div class="eyebrow">Connected workspace</div><h1>Linear Workspace</h1><div id="subtitle" class="muted">Loading connected workspace…</div></div><div id="status" class="status">Connecting</div></header>
<section class="metrics"><div class="card"><div class="muted">Teams</div><div id="teamCount" class="metric">—</div></div><div class="card"><div class="muted">Recent issues</div><div id="issueCount" class="metric">—</div></div><div class="card"><div class="muted">High priority</div><div id="priorityCount" class="metric">—</div></div></section>
<section class="grid"><div class="panel"><h2>Teams</h2><div id="teams"></div></div><div class="panel"><h2>Recent issues</h2><div id="issues"></div></div></section></main>
<script>
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
async function load(){const status=document.querySelector('#status');try{const r=await fetch('api/dashboard');const d=await r.json();if(!r.ok)throw Error(d.error||r.statusText);const teams=d.teams.nodes||[],issues=d.issues.nodes||[];document.querySelector('#subtitle').textContent=`${d.viewer.organization.name} · ${d.viewer.user.name}`;status.textContent='Live';status.style.color='#74e0ad';document.querySelector('#teamCount').textContent=teams.length;document.querySelector('#issueCount').textContent=issues.length;document.querySelector('#priorityCount').textContent=issues.filter(i=>i.priority>=3).length;document.querySelector('#teams').innerHTML=teams.map(t=>`<div class="team"><b>${esc(t.key)} · ${esc(t.name)}</b><span>${esc(t.description||'No description')}</span></div>`).join('')||'<div class="empty">No teams</div>';document.querySelector('#issues').innerHTML=issues.map(i=>`<div class="issue"><span class="id">${esc(i.identifier)}</span><a href="${esc(i.url)}" target="_blank" rel="noreferrer">${esc(i.title)}</a><span class="priority">Priority ${esc(i.priority)}</span></div>`).join('')||'<div class="empty">No issues</div>'}catch(e){status.textContent='Error';document.querySelector('#issues').innerHTML=`<div class="empty">${esc(e.message)}</div>`}}load();
</script></body></html>"#;

#[cfg(test)]
mod tests {
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    use super::router;

    /// The application root is a usable dashboard rather than an empty shell.
    #[tokio::test]
    async fn root_renders_linear_dashboard() {
        let response = router("http://linear.invalid")
            .oneshot(Request::get("/").body(Body::empty()).expect("request"))
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 128 * 1024)
            .await
            .expect("body");
        let html = String::from_utf8(body.to_vec()).expect("utf8");
        assert!(html.contains("Linear Workspace"));
        assert!(html.contains("api/dashboard"));
    }
}
