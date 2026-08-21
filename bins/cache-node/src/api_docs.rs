//! One place to see every API this deployment exposes.
//!
//! The specifications are checked in and compiled into the binary, so this
//! surface cannot drift from the process serving it. Swagger UI is vendored
//! under `assets/swagger-ui/` for the same reason: a node with no outbound
//! network still serves its own API documentation.
//!
//! This lives on the admin listener, never the S3 port. The S3 port is the
//! customer data plane and carries sigv4; documentation belongs on neither.
//!
//! Listing an API here does not mean this process serves it. The SQL query API
//! is a separate binary, and the catalog and semantic APIs depend on
//! configuration. Every entry states where its API actually runs, so the page
//! describes the deployment rather than an aspiration.
//!
//! Catalog's management API is deliberately absent. The node mounts
//! `new_v1_hosted_router` — config, namespaces, tables, views — so warehouses,
//! projects, roles, users, and permissions resolve to nothing: this deployment
//! serves one warehouse from `[catalog_server]` and delegates authorization to
//! an external decision service. Generic tables are absent for the same
//! reason. `embedded_catalog.rs` probes a live node to keep this list and the
//! mounted surface from drifting apart.

use axum::http::{StatusCode, header};
use axum::response::{Html, IntoResponse, Response};
use axum::{Router, routing::get};

/// Where the API browser and its specifications are served.
pub const API_DOCS_PATH: &str = "/admin/api-docs";

/// The vendored Swagger UI bundle. See `assets/swagger-ui/PROVENANCE.md`.
const SWAGGER_UI_JS: &str = include_str!("../assets/swagger-ui/swagger-ui-bundle.js");
/// The vendored Swagger UI stylesheet, with every asset inlined as a data URI.
const SWAGGER_UI_CSS: &str = include_str!("../assets/swagger-ui/swagger-ui.css");

/// One documented API: what it is, where it runs, and its specification.
struct Api {
    /// The URL slug, and the value of the `#` fragment that selects it.
    slug: &'static str,
    /// The name shown in the sidebar.
    name: &'static str,
    /// Where this API is actually served. Shown beside the name.
    served: &'static str,
    /// The specification document.
    spec: Spec,
}

/// A specification's source format. Both are compiled in; YAML is converted to
/// JSON on the way out because Swagger UI consumes JSON.
enum Spec {
    /// A YAML document requiring conversion.
    Yaml(&'static str),
    /// A JSON document served as-is.
    Json(&'static str),
}

/// Every API this deployment knows about, in sidebar order.
///
/// One table drives the routes, the navigation, and the tests, so a
/// specification cannot be listed without being served.
const APIS: &[Api] = &[
    Api {
        slug: "catalog",
        name: "Catalog",
        served: "Iceberg REST · :8181/catalog/v1",
        spec: Spec::Yaml(include_str!(
            "../../../docs/reference/openapi/rest-catalog-open-api.yaml"
        )),
    },
    Api {
        slug: "query",
        name: "SQL",
        served: "Query the catalog · :8334/v1/query",
        spec: Spec::Yaml(include_str!(
            "../../../docs/reference/openapi/query-open-api.yaml"
        )),
    },
    Api {
        slug: "s3",
        name: "S3, graph & vector",
        served: "Objects, graphs, vectors · :8333",
        spec: Spec::Json(include_str!(
            "../../../crates/verglas-s3/models/s3-openapi.json"
        )),
    },
];

/// Serves the API browser, its vendored assets, and every specification.
///
/// Merged into the admin router unconditionally: an operator needs to see the
/// surfaces this node has switched off in order to know what to turn on.
pub fn router() -> Router {
    let mut router = Router::new()
        .route(API_DOCS_PATH, get(browser))
        .route(
            &format!("{API_DOCS_PATH}/assets/swagger-ui.js"),
            get(|| async {
                (
                    [
                        (header::CONTENT_TYPE, "application/javascript"),
                        (header::CACHE_CONTROL, "public, max-age=31536000, immutable"),
                    ],
                    SWAGGER_UI_JS,
                )
            }),
        )
        .route(
            &format!("{API_DOCS_PATH}/assets/swagger-ui.css"),
            get(|| async {
                (
                    [
                        (header::CONTENT_TYPE, "text/css"),
                        (header::CACHE_CONTROL, "public, max-age=31536000, immutable"),
                    ],
                    SWAGGER_UI_CSS,
                )
            }),
        );
    for api in APIS {
        router = router.route(
            &format!("{API_DOCS_PATH}/{}.json", api.slug),
            get(move || async move { api.spec.respond() }),
        );
    }
    router
}

impl Spec {
    /// Answers this specification as JSON.
    ///
    /// A malformed compiled-in document answers 500 with its parse error
    /// rather than panicking: one broken specification must not take the admin
    /// listener down with it.
    fn respond(&self) -> Response {
        let json = header::HeaderValue::from_static("application/json");
        match self {
            Spec::Json(raw) => ([(header::CONTENT_TYPE, json)], *raw).into_response(),
            Spec::Yaml(raw) => match serde_norway::from_str::<serde_json::Value>(raw) {
                Ok(document) => {
                    ([(header::CONTENT_TYPE, json)], axum::Json(document)).into_response()
                }
                Err(error) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    axum::Json(serde_json::json!({
                        "error": format!("compiled-in OpenAPI document is invalid: {error}")
                    })),
                )
                    .into_response(),
            },
        }
    }
}

/// Renders the API browser: a sidebar of every API beside one Swagger UI pane.
async fn browser() -> impl IntoResponse {
    let nav = APIS
        .iter()
        .map(|api| {
            format!(
                r##"<li><a href="#{slug}" data-slug="{slug}"><span class="name">{name}</span><span class="served">{served}</span></a></li>"##,
                slug = api.slug,
                name = api.name,
                served = api.served,
            )
        })
        .collect::<String>();
    Html(
        PAGE.replace("{{NAV}}", &nav)
            .replace("{{BASE}}", API_DOCS_PATH),
    )
}

/// The browser shell.
///
/// Swagger UI is instantiated once and its spec URL swapped on navigation, so
/// switching APIs does not tear down and rebuild the whole component. The
/// `#fragment` is the selection, which makes every view linkable and survives
/// a reload.
const PAGE: &str = r##"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Verglas APIs</title>
<link rel="stylesheet" href="{{BASE}}/assets/swagger-ui.css">
<style>
  :root { --line:#e3e6ea; --muted:#6b7280; --accent:#1f6feb; --bg:#fbfcfd; }
  * { box-sizing:border-box }
  body { margin:0; font:14px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",system-ui,sans-serif; color:#111 }
  #layout { display:flex; min-height:100vh }
  nav { width:290px; flex:0 0 290px; border-right:1px solid var(--line); background:var(--bg);
        position:sticky; top:0; height:100vh; overflow-y:auto }
  nav h1 { font-size:13px; letter-spacing:.08em; text-transform:uppercase; color:var(--muted);
           margin:0; padding:18px 18px 10px }
  nav ul { list-style:none; margin:0; padding:0 10px 18px }
  nav a { display:block; padding:9px 12px; border-radius:7px; text-decoration:none; color:inherit }
  nav a:hover { background:#eef1f4 }
  nav a.active { background:var(--accent); color:#fff }
  nav a.active .served { color:#dbe8ff }
  .name { display:block; font-weight:600; font-size:13.5px }
  .served { display:block; font-size:11.5px; color:var(--muted); margin-top:2px }
  .tag { font-size:10px; font-weight:600; text-transform:uppercase; letter-spacing:.04em;
         background:#e5e7eb; color:#4b5563; border-radius:4px; padding:1px 5px; margin-left:6px;
         vertical-align:1px }
  nav a.active .tag { background:rgba(255,255,255,.25); color:#fff }
  nav footer { padding:0 18px 20px; font-size:11.5px; color:var(--muted); border-top:1px solid var(--line);
               margin:0 10px; padding-top:14px }
  main { flex:1; min-width:0 }
  #swagger-ui .topbar { display:none }
  #swagger-ui .info { margin:24px 0 }
  #err { display:none; padding:28px; font-size:14px }
  #err code { background:#f3f4f6; padding:1px 5px; border-radius:4px }
</style>
</head>
<body>
<div id="layout">
  <nav>
    <h1>Verglas APIs</h1>
    <ul id="nav">{{NAV}}</ul>
    <footer>Reference only — requests are disabled. Specs are served by this node at
      <code>{{BASE}}/&lt;name&gt;.json</code>.</footer>
  </nav>
  <main>
    <div id="err"></div>
    <div id="swagger-ui"></div>
  </main>
</div>
<script src="{{BASE}}/assets/swagger-ui.js"></script>
<script>
(function () {
  var base = "{{BASE}}";
  var links = Array.prototype.slice.call(document.querySelectorAll('#nav a'));
  var ui = null;

  function fail(message) {
    var err = document.getElementById('err');
    err.style.display = 'block';
    err.innerHTML = message;
    document.getElementById('swagger-ui').innerHTML = '';
  }

  function select(slug) {
    var match = links.filter(function (a) { return a.dataset.slug === slug; })[0] || links[0];
    links.forEach(function (a) { a.classList.remove('active'); });
    match.classList.add('active');
    document.title = match.querySelector('.name').textContent.trim() + ' — Verglas APIs';
    var url = base + '/' + match.dataset.slug + '.json';
    document.getElementById('err').style.display = 'none';

    if (!window.SwaggerUIBundle) {
      fail('Swagger UI failed to load. The specification is still served at <code>' + url + '</code>.');
      return;
    }
    // Build once, then swap the spec URL. Rebuilding on every click discards
    // the component's own state and flashes the pane.
    if (ui) { ui.specActions.updateUrl(url); ui.specActions.download(url); return; }
    ui = SwaggerUIBundle({
      url: url,
      dom_id: '#swagger-ui',
      presets: [SwaggerUIBundle.presets.apis],
      layout: 'BaseLayout',
      supportedSubmitMethods: [],   // reference only: no try-it-out
      docExpansion: 'list',
      defaultModelsExpandDepth: 0,
      displayRequestDuration: false,
      tryItOutEnabled: false,
      deepLinking: true
    });
  }

  links.forEach(function (a) {
    a.addEventListener('click', function (event) {
      event.preventDefault();
      var slug = a.dataset.slug;
      if (location.hash.slice(1) !== slug) { history.pushState(null, '', '#' + slug); }
      select(slug);
    });
  });
  window.addEventListener('popstate', function () { select(location.hash.slice(1)); });
  select(location.hash.slice(1));
})();
</script>
</body>
</html>"##;

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    /// Fetches one path from the docs router.
    async fn get_path(path: &str) -> (StatusCode, Vec<u8>) {
        let response = router()
            .oneshot(
                Request::builder()
                    .uri(path)
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), 64 * 1024 * 1024)
            .await
            .expect("body");
        (status, body.to_vec())
    }

    /// Every listed API is served and parses as an OpenAPI document. A listed
    /// specification that 404s or fails to parse is the exact failure this
    /// page exists to prevent.
    #[tokio::test]
    async fn every_listed_api_is_served_and_parses() {
        for api in APIS {
            let path = format!("{API_DOCS_PATH}/{}.json", api.slug);
            let (status, body) = get_path(&path).await;
            assert_eq!(status, StatusCode::OK, "{path} is listed but not served");
            let document: serde_json::Value = serde_json::from_slice(&body)
                .unwrap_or_else(|error| panic!("{path} is not valid JSON: {error}"));
            assert!(
                document.get("openapi").is_some() || document.get("swagger").is_some(),
                "{path} is not an OpenAPI document"
            );
            assert!(
                document
                    .get("paths")
                    .and_then(serde_json::Value::as_object)
                    .is_some_and(|paths| !paths.is_empty()),
                "{path} documents no paths"
            );
        }
    }

    /// The sidebar names every API this node serves and the port each one
    /// answers on.
    #[tokio::test]
    async fn the_sidebar_lists_every_api_and_where_it_runs() {
        let (status, body) = get_path(API_DOCS_PATH).await;
        assert_eq!(status, StatusCode::OK);
        let page = String::from_utf8(body).expect("utf-8");
        for api in APIS {
            assert!(page.contains(api.name), "the sidebar omits {}", api.name);
            assert!(
                page.contains(api.served),
                "the sidebar does not say where {} runs",
                api.name
            );
        }
    }

    /// The UI loads from this node, not a CDN, so it works with no egress.
    #[tokio::test]
    async fn the_interface_is_served_locally_and_never_from_a_cdn() {
        let (status, body) = get_path(API_DOCS_PATH).await;
        assert_eq!(status, StatusCode::OK);
        let page = String::from_utf8(body).expect("utf-8");
        assert!(
            !page.contains("unpkg.com") && !page.contains("cdn."),
            "the API browser must not reference a CDN"
        );
        for asset in ["assets/swagger-ui.js", "assets/swagger-ui.css"] {
            let (status, body) = get_path(&format!("{API_DOCS_PATH}/{asset}")).await;
            assert_eq!(status, StatusCode::OK, "{asset} is not served");
            assert!(!body.is_empty(), "{asset} is empty");
        }
    }

    /// Requests are disabled: this is a reference, and the browser cannot sign
    /// sigv4 for the S3 surface anyway.
    #[tokio::test]
    async fn try_it_out_is_disabled() {
        let (_, body) = get_path(API_DOCS_PATH).await;
        let page = String::from_utf8(body).expect("utf-8");
        assert!(page.contains("supportedSubmitMethods: []"));
        assert!(page.contains("tryItOutEnabled: false"));
    }
}
