//! The one portable worker spec.
//!
//! A single versioned file (TOML or JSON) describes a worker completely: its
//! name, the command to run, the files it bundles, its environment (with
//! `@secret:` references), its trigger, its target tables, and its resource
//! hints. The SAME file drives `verglas workers create` against the local server
//! AND `verglas workers push` to the cloud — the CLI translates it into each
//! plane's request body, so the spec round-trips with no edits.
//!
//! A JS-module worker is not a special kind: it is just a spec whose `exec`
//! starts with `bun`. Nothing here assumes a runtime.
//!
//! Secrets never travel in the spec beyond a NAME. An env value of the form
//! `@secret:NAME` is a reference the server or the cloud resolves from its own
//! secret store at run time; the value is never in the file and never printed.

use std::collections::BTreeMap;
use std::error::Error;
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// The spec version this CLI writes and understands.
pub const SPEC_VERSION: u32 = 1;

/// The namespace a bare follow target table lands in (mirrors the server).
pub const FOLLOW_NAMESPACE: &str = "follow";

/// The prefix marking an env value as a reference to a named secret.
const SECRET_PREFIX: &str = "@secret:";

/// A worker trigger: exactly one bounded scheduler event or a local follow.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Trigger {
    /// Run on a cron schedule.
    Cron {
        /// A five-field cron expression.
        cron: String,
    },
    /// Run only when dispatched by hand.
    #[default]
    Manual,
    /// Run when an HTTP request reaches the worker's registered callback.
    Webhook {
        /// Optional dynamic path such as `/ingest/orders`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        path: Option<String>,
    },
    /// Run when the catalog reports a commit to one table.
    DataChange {
        /// Dotted Iceberg table name such as `app.events`.
        table: String,
    },
    /// Follow a local target continuously, appending each captured line to the
    /// target table. Local only — a follow worker cannot be pushed to the cloud.
    Follow {
        /// A file to tail. When absent, the worker's `exec` command is wrapped
        /// and its stdout and stderr are captured.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        file: Option<String>,
    },
}

/// Resource hints for cloud placement. Advisory locally.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Resources {
    /// Fractional vCPUs the worker is sized for.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vcpus: Option<f64>,
    /// Memory the worker is sized for, in MiB.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mem_mib: Option<u64>,
}

/// The portable worker spec, version 1.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerManifest {
    /// The spec format version. Only `1` is understood.
    #[serde(default = "default_spec_version")]
    pub spec_version: u32,
    /// The worker name.
    pub name: String,
    /// The command and its arguments. Element 0 is the program. May be empty only
    /// for a follow worker that tails a file.
    #[serde(default)]
    pub exec: Vec<String>,
    /// The working directory the command and bundled files resolve against.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    /// Files bundled with the worker, path to text content. On the cloud they
    /// ride the deployment; locally they are written under the worker's directory.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub files: BTreeMap<String, String>,
    /// Environment for the worker. A value of `@secret:NAME` is resolved per
    /// plane from that plane's secret store; the value is never in this file.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
    /// When the worker runs.
    #[serde(default)]
    pub trigger: Trigger,
    /// The Iceberg tables the worker writes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub target_tables: Vec<String>,
    /// Resource hints.
    #[serde(default, skip_serializing_if = "is_default_resources")]
    pub resources: Resources,
}

fn default_spec_version() -> u32 {
    SPEC_VERSION
}

fn is_default_resources(r: &Resources) -> bool {
    r.vcpus.is_none() && r.mem_mib.is_none()
}

impl WorkerManifest {
    /// Reads a spec file (`.toml` as TOML, otherwise JSON) and validates it.
    pub fn from_file(path: &Path) -> Result<WorkerManifest, Box<dyn Error>> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("could not read spec file {}: {e}", path.display()))?;
        let manifest: WorkerManifest = if path.extension().and_then(|e| e.to_str()) == Some("toml")
        {
            toml::from_str(&text)
                .map_err(|e| format!("{} is not valid TOML: {e}", path.display()))?
        } else {
            serde_json::from_str(&text)
                .map_err(|e| format!("{} is not valid JSON: {e}", path.display()))?
        };
        manifest.validate()?;
        Ok(manifest)
    }

    /// Checks the spec is internally consistent and this CLI understands it.
    pub fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.spec_version != SPEC_VERSION {
            return Err(format!(
                "unsupported worker spec_version {}: this CLI understands version {SPEC_VERSION}",
                self.spec_version
            )
            .into());
        }
        if self.name.trim().is_empty() {
            return Err("the worker spec needs a name".into());
        }
        let follow_file = matches!(&self.trigger, Trigger::Follow { file: Some(_) });
        if self.exec.is_empty() && !follow_file {
            return Err(
                "the worker spec needs an `exec` command (only a follow worker that tails a \
                 file may omit it)"
                    .into(),
            );
        }
        if let Trigger::Webhook { path: Some(path) } = &self.trigger
            && (!path.starts_with('/') || path.contains('?'))
        {
            return Err("a webhook path must start with `/` and contain no query string".into());
        }
        if let Trigger::DataChange { table } = &self.trigger {
            let mut parts = table.split('.');
            if parts.next().is_none_or(str::is_empty)
                || parts.next().is_none_or(str::is_empty)
                || parts.next().is_some()
            {
                return Err("a data_change table must have the form `namespace.table`".into());
            }
        }
        Ok(())
    }

    /// Whether this worker's trigger is follow — local only.
    pub fn is_follow(&self) -> bool {
        matches!(self.trigger, Trigger::Follow { .. })
    }

    /// The names of the secrets this worker references through `@secret:` env
    /// values, in sorted order and de-duplicated.
    pub fn secret_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .env
            .values()
            .filter_map(|v| v.strip_prefix(SECRET_PREFIX))
            .map(str::to_owned)
            .collect();
        names.sort();
        names.dedup();
        names
    }

    /// The single output table a local worker writes, if any (the first target).
    pub fn primary_target(&self) -> Option<&str> {
        self.target_tables.first().map(String::as_str)
    }

    /// The `code` JSON a local worker row carries: the exec array and cwd, the
    /// same shape the harness runs and the cloud launch contract uses.
    fn code_json(&self) -> Value {
        let mut code = json!({ "exec": self.exec });
        if let Some(cwd) = &self.cwd {
            code["cwd"] = Value::String(cwd.clone());
        }
        Value::String(code.to_string())
    }

    /// The `triggers` JSON array a local worker row carries.
    fn local_triggers(&self) -> Value {
        let specs = match &self.trigger {
            Trigger::Cron { cron } => json!([{ "type": "cron", "schedule": cron }]),
            Trigger::Manual => json!([]),
            Trigger::Webhook { path } => match path {
                Some(path) => json!([{ "type": "webhook", "path": path }]),
                None => json!([{ "type": "webhook" }]),
            },
            Trigger::DataChange { table } => {
                json!([{ "type": "data_change", "table": table }])
            }
            Trigger::Follow { file: Some(file) } => json!([{ "type": "follow", "file": file }]),
            Trigger::Follow { file: None } => json!([{ "type": "follow" }]),
        };
        Value::String(specs.to_string())
    }

    /// The worker `config` JSON string a local worker row carries: env and any
    /// bundled files, secrets by name only.
    fn config_json(&self) -> Value {
        let mut config = serde_json::Map::new();
        if !self.env.is_empty() {
            config.insert("env".to_owned(), json!(self.env));
        }
        if !self.files.is_empty() {
            config.insert("files".to_owned(), json!(self.files));
        }
        Value::String(Value::Object(config).to_string())
    }

    /// Translates this spec into the local server's `POST /v1/workers` body.
    pub fn to_local_worker(&self) -> Value {
        let mut body = json!({
            "name": self.name,
            "code": self.code_json(),
            "triggers": self.local_triggers(),
            "config": self.config_json(),
            "created_by": "cli",
        });
        if let Some(target) = self.primary_target() {
            body["output"] = Value::String(target.to_owned());
        }
        body
    }

    /// Translates this spec into the cloud `POST /v1/deployments` body at
    /// `placement`. The launch shape lives in `config` (spec_version, exec, files,
    /// env, resources) exactly as the fleet launch expects; `code` carries a
    /// self-describing job spec so the required non-empty field is meaningful.
    pub fn to_cloud_deployment(&self, placement: &str) -> Value {
        let (trigger, schedule, trigger_specs) = match &self.trigger {
            Trigger::Cron { cron } => (
                "cron",
                Some(cron.clone()),
                json!([{ "type": "cron", "config": { "schedule": cron } }]),
            ),
            Trigger::Webhook { path } => {
                let config = path
                    .as_ref()
                    .map_or_else(|| json!({}), |path| json!({ "path": path }));
                (
                    "webhook",
                    None,
                    json!([{ "type": "webhook", "config": config }]),
                )
            }
            Trigger::DataChange { table } => (
                "manual",
                None,
                json!([{ "type": "data_change", "config": { "table": table } }]),
            ),
            // A follow worker is local-only; callers reject it before here.
            Trigger::Manual | Trigger::Follow { .. } => ("manual", None, json!([])),
        };
        let mut config = serde_json::Map::new();
        config.insert("spec_version".to_owned(), json!(self.spec_version));
        config.insert("exec".to_owned(), json!(self.exec));
        config.insert("triggers".to_owned(), trigger_specs);
        if let Some(cwd) = &self.cwd {
            config.insert("cwd".to_owned(), json!(cwd));
        }
        if !self.files.is_empty() {
            config.insert("files".to_owned(), json!(self.files));
        }
        if !self.env.is_empty() {
            config.insert("env".to_owned(), json!(self.env));
        }
        if let Some(v) = self.resources.vcpus {
            config.insert("vcpus".to_owned(), json!(v));
        }
        if let Some(m) = self.resources.mem_mib {
            config.insert("mem_mib".to_owned(), json!(m));
        }
        let code = json!({ "name": self.name, "exec": self.exec }).to_string();
        let mut body = json!({
            "kind": "worker",
            "name": self.name,
            "code": code,
            "trigger": trigger,
            "placement": placement,
            "target_tables": self.target_tables,
            "config": Value::Object(config),
            "created_by": "cli",
        });
        if let Some(schedule) = schedule {
            body["schedule"] = Value::String(schedule);
        }
        body
    }

    /// Rebuilds a spec from a worker row read back from the local server (the
    /// `pull` and round-trip path). `code`, `triggers`, and `config` are JSON
    /// strings on the row.
    pub fn from_local_worker(row: &Value) -> Result<WorkerManifest, Box<dyn Error>> {
        let name = row
            .get("name")
            .and_then(Value::as_str)
            .ok_or("worker row has no name")?
            .to_owned();
        let code: Value = parse_embedded(row.get("code"));
        let exec: Vec<String> = code
            .get("exec")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default();
        let cwd = code.get("cwd").and_then(Value::as_str).map(str::to_owned);
        let config: Value = parse_embedded(row.get("config"));
        let env: BTreeMap<String, String> = config
            .get("env")
            .and_then(Value::as_object)
            .map(|o| {
                o.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_owned())))
                    .collect()
            })
            .unwrap_or_default();
        let files: BTreeMap<String, String> = config
            .get("files")
            .and_then(Value::as_object)
            .map(|o| {
                o.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_owned())))
                    .collect()
            })
            .unwrap_or_default();
        let triggers: Value = parse_embedded(row.get("triggers"));
        let trigger = trigger_from_local(&triggers);
        let target_tables = row
            .get("output")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(|s| vec![s.to_owned()])
            .unwrap_or_default();
        Ok(WorkerManifest {
            spec_version: SPEC_VERSION,
            name,
            exec,
            cwd,
            files,
            env,
            trigger,
            target_tables,
            resources: Resources::default(),
        })
    }

    /// Rebuilds a spec from a cloud deployment detail (the `pull` path). The
    /// deployment's `config` object carries the launch shape (exec, cwd, files,
    /// env, resources); `trigger`/`schedule`/`target_tables` are top-level.
    pub fn from_cloud_deployment(dep: &Value) -> Result<WorkerManifest, Box<dyn Error>> {
        let name = dep
            .get("name")
            .and_then(Value::as_str)
            .ok_or("deployment has no name")?
            .to_owned();
        let config = dep.get("config").cloned().unwrap_or(Value::Null);
        let exec: Vec<String> = config
            .get("exec")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default();
        let cwd = config.get("cwd").and_then(Value::as_str).map(str::to_owned);
        let files = string_map(config.get("files"));
        let env = string_map(config.get("env"));
        let resources = Resources {
            vcpus: config.get("vcpus").and_then(Value::as_f64),
            mem_mib: config.get("mem_mib").and_then(Value::as_u64),
        };
        let trigger = trigger_from_cloud(config.get("triggers"));
        let target_tables = dep
            .get("target_tables")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default();
        Ok(WorkerManifest {
            spec_version: SPEC_VERSION,
            name,
            exec,
            cwd,
            files,
            env,
            trigger,
            target_tables,
            resources,
        })
    }

    /// Renders the spec as pretty TOML for `pull` to write to a file.
    pub fn to_toml(&self) -> Result<String, Box<dyn Error>> {
        Ok(toml::to_string_pretty(self)?)
    }
}

/// Reads a JSON object of string values into a sorted string map, or empty.
fn string_map(field: Option<&Value>) -> BTreeMap<String, String> {
    field
        .and_then(Value::as_object)
        .map(|o| {
            o.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_owned())))
                .collect()
        })
        .unwrap_or_default()
}

/// Parses a possibly-embedded JSON string column into a `Value` (a row's `code`
/// / `config` / `triggers` are stored as JSON strings). A plain object passes
/// through; anything unparseable becomes null.
fn parse_embedded(field: Option<&Value>) -> Value {
    match field {
        Some(Value::String(s)) => serde_json::from_str(s).unwrap_or(Value::Null),
        Some(other) => other.clone(),
        None => Value::Null,
    }
}

/// Maps a local `triggers` JSON array back to a spec trigger (the first
/// recognized one wins; none means manual).
fn trigger_from_local(triggers: &Value) -> Trigger {
    let Some(list) = triggers.as_array() else {
        return Trigger::Manual;
    };
    for t in list {
        match t.get("type").and_then(Value::as_str) {
            Some("cron") => {
                if let Some(cron) = t.get("schedule").and_then(Value::as_str) {
                    return Trigger::Cron {
                        cron: cron.to_owned(),
                    };
                }
            }
            Some("follow") => {
                return Trigger::Follow {
                    file: t.get("file").and_then(Value::as_str).map(str::to_owned),
                };
            }
            Some("webhook") => {
                return Trigger::Webhook {
                    path: t.get("path").and_then(Value::as_str).map(str::to_owned),
                };
            }
            Some("data_change") => {
                if let Some(table) = t.get("table").and_then(Value::as_str) {
                    return Trigger::DataChange {
                        table: table.to_owned(),
                    };
                }
            }
            _ => {}
        }
    }
    Trigger::Manual
}

/// Maps the cloud control plane's `config.triggers` array back to one portable
/// trigger. The manifest intentionally carries one trigger today.
fn trigger_from_cloud(triggers: Option<&Value>) -> Trigger {
    let Some(list) = triggers.and_then(Value::as_array) else {
        return Trigger::Manual;
    };
    for trigger in list {
        let config = trigger.get("config").and_then(Value::as_object);
        match trigger.get("type").and_then(Value::as_str) {
            Some("cron") => {
                if let Some(schedule) = config
                    .and_then(|value| value.get("schedule"))
                    .and_then(Value::as_str)
                {
                    return Trigger::Cron {
                        cron: schedule.to_owned(),
                    };
                }
            }
            Some("webhook") => {
                return Trigger::Webhook {
                    path: config
                        .and_then(|value| value.get("path"))
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                };
            }
            Some("data_change") => {
                if let Some(table) = config
                    .and_then(|value| value.get("table"))
                    .and_then(Value::as_str)
                {
                    return Trigger::DataChange {
                        table: table.to_owned(),
                    };
                }
            }
            _ => {}
        }
    }
    Trigger::Manual
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cron_manifest() -> WorkerManifest {
        WorkerManifest {
            spec_version: 1,
            name: "collector".to_owned(),
            exec: vec!["python3".to_owned(), "collect.py".to_owned()],
            cwd: Some("/app".to_owned()),
            files: BTreeMap::new(),
            env: BTreeMap::from([
                ("LOG_LEVEL".to_owned(), "info".to_owned()),
                ("API_KEY".to_owned(), "@secret:MY_KEY".to_owned()),
            ]),
            trigger: Trigger::Cron {
                cron: "*/5 * * * *".to_owned(),
            },
            target_tables: vec!["metrics.samples".to_owned()],
            resources: Resources {
                vcpus: Some(0.25),
                mem_mib: Some(256),
            },
        }
    }

    /// Webhook and data-change manifests project to the local trigger registry
    /// without requiring callers to hand-write embedded JSON strings.
    #[test]
    fn translates_event_triggers_to_local_workers() {
        let mut webhook = cron_manifest();
        webhook.trigger = Trigger::Webhook {
            path: Some("/ingest/orders".to_owned()),
        };
        let webhook_body = webhook.to_local_worker();
        let webhook_triggers: Value =
            serde_json::from_str(webhook_body["triggers"].as_str().expect("triggers string"))
                .expect("webhook triggers");
        assert_eq!(
            webhook_triggers,
            json!([{
                "type": "webhook",
                "path": "/ingest/orders"
            }])
        );

        let mut data_change = cron_manifest();
        data_change.trigger = Trigger::DataChange {
            table: "app.events".to_owned(),
        };
        let data_body = data_change.to_local_worker();
        let data_triggers: Value =
            serde_json::from_str(data_body["triggers"].as_str().expect("triggers string"))
                .expect("data triggers");
        assert_eq!(
            data_triggers,
            json!([{
                "type": "data_change",
                "table": "app.events"
            }])
        );
    }

    /// The portable trigger shape populates the cloud trigger registry while
    /// the deployment envelope carries the control plane's execution class.
    #[test]
    fn translates_event_triggers_to_cloud_deployments() {
        let mut webhook = cron_manifest();
        webhook.trigger = Trigger::Webhook { path: None };
        let webhook_body = webhook.to_cloud_deployment("cloud");
        assert_eq!(webhook_body["trigger"], "webhook");
        assert_eq!(
            webhook_body["config"]["triggers"],
            json!([{"type":"webhook","config":{}}])
        );

        let mut data_change = cron_manifest();
        data_change.trigger = Trigger::DataChange {
            table: "app.events".to_owned(),
        };
        let data_body = data_change.to_cloud_deployment("cloud");
        assert_eq!(data_body["trigger"], "manual");
        assert_eq!(
            data_body["config"]["triggers"],
            json!([{"type":"data_change","config":{"table":"app.events"}}])
        );
    }

    /// JSON and TOML manifests accept the public webhook and data-change forms.
    #[test]
    fn parses_event_trigger_manifests() {
        let webhook: WorkerManifest = toml::from_str(
            r#"
name = "hook"
exec = ["sh", "worker.sh"]
[trigger]
type = "webhook"
path = "/callbacks/orders"
"#,
        )
        .expect("webhook manifest");
        assert!(matches!(
            webhook.trigger,
            Trigger::Webhook { path: Some(path) } if path == "/callbacks/orders"
        ));

        let data_change: WorkerManifest = serde_json::from_value(json!({
            "name": "changed",
            "exec": ["sh", "worker.sh"],
            "trigger": {"type": "data_change", "table": "app.events"}
        }))
        .expect("data-change manifest");
        assert!(matches!(
            data_change.trigger,
            Trigger::DataChange { table } if table == "app.events"
        ));
    }

    /// Event triggers survive both local registry and cloud detail round trips.
    #[test]
    fn event_triggers_round_trip() {
        for trigger in [
            Trigger::Webhook {
                path: Some("/callbacks/orders".to_owned()),
            },
            Trigger::DataChange {
                table: "app.events".to_owned(),
            },
        ] {
            let mut manifest = cron_manifest();
            manifest.trigger = trigger;
            let local = WorkerManifest::from_local_worker(&manifest.to_local_worker())
                .expect("local round trip");
            assert_eq!(local.local_triggers(), manifest.local_triggers());

            let cloud =
                WorkerManifest::from_cloud_deployment(&manifest.to_cloud_deployment("cloud"))
                    .expect("cloud round trip");
            assert_eq!(cloud.local_triggers(), manifest.local_triggers());
        }
    }

    /// Secret references are extracted by name, sorted and de-duplicated; the
    /// values never appear.
    #[test]
    fn secret_names_are_extracted_by_name() {
        assert_eq!(cron_manifest().secret_names(), vec!["MY_KEY".to_owned()]);
    }

    /// The local worker body carries the exec array in `code`, the cron trigger,
    /// the output table, and env/files in `config`.
    #[test]
    fn translates_to_a_local_worker() {
        let body = cron_manifest().to_local_worker();
        assert_eq!(body["name"], "collector");
        assert_eq!(body["output"], "metrics.samples");
        let code: Value =
            serde_json::from_str(body["code"].as_str().expect("code string")).expect("code parses");
        assert_eq!(code["exec"][0], "python3");
        assert_eq!(code["cwd"], "/app");
        let triggers: Value =
            serde_json::from_str(body["triggers"].as_str().expect("triggers string"))
                .expect("triggers parse");
        assert_eq!(triggers[0]["type"], "cron");
        assert_eq!(triggers[0]["schedule"], "*/5 * * * *");
        let config: Value = serde_json::from_str(body["config"].as_str().expect("config string"))
            .expect("config parses");
        assert_eq!(config["env"]["API_KEY"], "@secret:MY_KEY");
    }

    /// The cloud deployment body is a worker kind with the exec array in config,
    /// the cron schedule projected, and the resource hints carried.
    #[test]
    fn translates_to_a_cloud_deployment() {
        let body = cron_manifest().to_cloud_deployment("cloud");
        assert_eq!(body["kind"], "worker");
        assert_eq!(body["trigger"], "cron");
        assert_eq!(body["schedule"], "*/5 * * * *");
        assert_eq!(body["placement"], "cloud");
        assert_eq!(body["config"]["spec_version"], 1);
        assert_eq!(body["config"]["exec"][1], "collect.py");
        assert_eq!(body["config"]["vcpus"], 0.25);
        assert!(
            body["code"]
                .as_str()
                .expect("code string")
                .contains("collector")
        );
    }

    /// A spec round-trips through the local worker body and back with no edits.
    #[test]
    fn round_trips_through_a_local_worker_row() {
        let manifest = cron_manifest();
        let row = manifest.to_local_worker();
        let back = WorkerManifest::from_local_worker(&row).expect("rebuild");
        assert_eq!(back.name, manifest.name);
        assert_eq!(back.exec, manifest.exec);
        assert_eq!(back.cwd, manifest.cwd);
        assert_eq!(back.env, manifest.env);
        assert!(matches!(back.trigger, Trigger::Cron { .. }));
        assert_eq!(back.target_tables, manifest.target_tables);
    }

    /// A follow worker is recognized as local-only; a file-follow may omit exec.
    #[test]
    fn follow_worker_validates_without_exec_when_tailing_a_file() {
        let manifest = WorkerManifest {
            spec_version: 1,
            name: "tail".to_owned(),
            exec: vec![],
            cwd: None,
            files: BTreeMap::new(),
            env: BTreeMap::new(),
            trigger: Trigger::Follow {
                file: Some("/var/log/app.log".to_owned()),
            },
            target_tables: vec!["follow.app".to_owned()],
            resources: Resources::default(),
        };
        assert!(manifest.validate().is_ok());
        assert!(manifest.is_follow());
    }

    /// An unknown spec_version is rejected with a clear message.
    #[test]
    fn rejects_an_unknown_spec_version() {
        let mut manifest = cron_manifest();
        manifest.spec_version = 2;
        assert!(manifest.validate().is_err());
    }
}
