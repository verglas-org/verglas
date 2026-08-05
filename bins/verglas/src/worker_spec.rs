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
use verglas_sdk::worker::Catchup;

/// The spec version this CLI writes and understands.
pub const SPEC_VERSION: u32 = 1;

/// The namespace a bare follow target table lands in (mirrors the server).
pub const FOLLOW_NAMESPACE: &str = "follow";

/// The prefix marking an env value as a reference to a named secret.
const SECRET_PREFIX: &str = "@secret:";

/// The prefix that bundles a text file relative to the manifest.
const FILE_PREFIX: &str = "@file:";

/// One bounded scheduler trigger or a local continuous follow declaration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Trigger {
    /// Run on a cron schedule.
    Cron {
        /// A five-field cron expression.
        cron: String,
        /// Backfill anchor for scheduled intervals.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        start_date: Option<String>,
        /// How the scheduler drains intervals between the anchor and now.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        catchup: Option<Catchup>,
    },
    /// Run when an HTTP request reaches the worker's registered callback.
    Webhook {
        /// Optional dynamic path such as `/ingest/orders`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        path: Option<String>,
    },
    /// Run when a CloudEvent matches exact subscription attributes.
    Event {
        /// Exact CloudEvent type to accept.
        event_type: String,
        /// Optional exact CloudEvent source filter.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        source: Option<String>,
        /// Optional exact CloudEvent subject filter.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        subject: Option<String>,
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

impl Trigger {
    /// Returns the deployment trigger discriminant used by the cloud envelope.
    fn kind(&self) -> &'static str {
        match self {
            Trigger::Cron { .. } => "cron",
            Trigger::Webhook { .. } => "webhook",
            Trigger::Event { .. } => "event",
            Trigger::Follow { .. } => "follow",
        }
    }
}

/// Projects one portable trigger into the local worker registry contract.
fn local_trigger(trigger: &Trigger) -> Value {
    match trigger {
        Trigger::Cron {
            cron,
            start_date,
            catchup,
        } => json!({
            "type": "cron",
            "schedule": cron,
            "startDate": start_date,
            "catchup": catchup,
        }),
        Trigger::Webhook { path } => json!({ "type": "webhook", "path": path }),
        Trigger::Event {
            event_type,
            source,
            subject,
        } => json!({
            "type": "event",
            "eventType": event_type,
            "source": source,
            "subject": subject,
        }),
        Trigger::Follow { file } => json!({ "type": "follow", "file": file }),
    }
}

/// Projects one portable trigger into the cloud deployment config contract.
fn cloud_trigger(trigger: &Trigger) -> Value {
    match trigger {
        Trigger::Cron {
            cron,
            start_date,
            catchup,
        } => json!({ "type": "cron", "config": {
            "schedule": cron,
            "startDate": start_date,
            "catchup": catchup,
        }}),
        Trigger::Webhook { path } => json!({
            "type": "webhook",
            "config": path
                .as_ref()
                .map_or_else(|| json!({}), |path| json!({ "path": path })),
        }),
        Trigger::Event {
            event_type,
            source,
            subject,
        } => json!({ "type": "event", "config": {
            "eventType": event_type,
            "source": source,
            "subject": subject,
        }}),
        Trigger::Follow { file } => json!({ "type": "follow", "config": { "file": file } }),
    }
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
#[serde(deny_unknown_fields)]
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
    /// Events that run the worker. Manual dispatch is available to every worker
    /// and therefore is not a trigger declaration.
    #[serde(default)]
    pub triggers: Vec<Trigger>,
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
        let mut manifest: WorkerManifest =
            if path.extension().and_then(|e| e.to_str()) == Some("toml") {
                toml::from_str(&text)
                    .map_err(|e| format!("{} is not valid TOML: {e}", path.display()))?
            } else {
                serde_json::from_str(&text)
                    .map_err(|e| format!("{} is not valid JSON: {e}", path.display()))?
            };
        let base = path.parent().unwrap_or_else(|| Path::new("."));
        for content in manifest.files.values_mut() {
            let Some(reference) = content.strip_prefix(FILE_PREFIX) else {
                continue;
            };
            let reference = Path::new(reference);
            if reference.as_os_str().is_empty()
                || reference.is_absolute()
                || reference
                    .components()
                    .any(|part| !matches!(part, std::path::Component::Normal(_)))
            {
                return Err(format!(
                    "{} has invalid file reference `{}`",
                    path.display(),
                    reference.display()
                )
                .into());
            }
            *content = std::fs::read_to_string(base.join(reference)).map_err(|error| {
                format!(
                    "could not bundle {} referenced by {}: {error}",
                    reference.display(),
                    path.display()
                )
            })?;
        }
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
        let follows: Vec<&Trigger> = self
            .triggers
            .iter()
            .filter(|trigger| matches!(trigger, Trigger::Follow { .. }))
            .collect();
        if !follows.is_empty() && self.triggers.len() != 1 {
            return Err("a follow worker cannot declare bounded triggers".into());
        }
        let follow_file = matches!(follows.first(), Some(Trigger::Follow { file: Some(_) }));
        if self.exec.is_empty() && !follow_file {
            return Err(
                "the worker spec needs an `exec` command (only a follow worker that tails a \
                 file may omit it)"
                    .into(),
            );
        }
        for trigger in &self.triggers {
            if let Trigger::Webhook { path: Some(path) } = trigger
                && (!path.starts_with('/') || path.contains('?'))
            {
                return Err(
                    "a webhook path must start with `/` and contain no query string".into(),
                );
            }
            if let Trigger::Event { event_type, .. } = trigger
                && event_type.trim().is_empty()
            {
                return Err("an event trigger needs an event_type".into());
            }
        }
        Ok(())
    }

    /// Whether this worker's trigger is follow — local only.
    pub fn is_follow(&self) -> bool {
        self.triggers
            .iter()
            .any(|trigger| matches!(trigger, Trigger::Follow { .. }))
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
        let specs: Vec<Value> = self.triggers.iter().map(local_trigger).collect();
        Value::String(Value::Array(specs).to_string())
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
        let trigger_specs: Vec<Value> = self.triggers.iter().map(cloud_trigger).collect();
        let primary = self.triggers.first();
        let trigger = primary.map_or("manual", Trigger::kind);
        let schedule = match primary {
            Some(Trigger::Cron { cron, .. }) => Some(cron.clone()),
            _ => None,
        };
        let mut config = serde_json::Map::new();
        config.insert("spec_version".to_owned(), json!(self.spec_version));
        config.insert("exec".to_owned(), json!(self.exec));
        config.insert("triggers".to_owned(), Value::Array(trigger_specs));
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
        let triggers = triggers_from_local(&triggers);
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
            triggers,
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
        let triggers = triggers_from_cloud(config.get("triggers"));
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
            triggers,
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

/// Maps a local `triggers` JSON array back to every portable trigger.
fn triggers_from_local(triggers: &Value) -> Vec<Trigger> {
    let Some(list) = triggers.as_array() else {
        return Vec::new();
    };
    let mut parsed = Vec::new();
    for t in list {
        match t.get("type").and_then(Value::as_str) {
            Some("cron") => {
                if let Some(cron) = t.get("schedule").and_then(Value::as_str) {
                    parsed.push(Trigger::Cron {
                        cron: cron.to_owned(),
                        start_date: t
                            .get("startDate")
                            .and_then(Value::as_str)
                            .map(str::to_owned),
                        catchup: t
                            .get("catchup")
                            .cloned()
                            .and_then(|value| serde_json::from_value(value).ok()),
                    });
                }
            }
            Some("follow") => {
                parsed.push(Trigger::Follow {
                    file: t.get("file").and_then(Value::as_str).map(str::to_owned),
                });
            }
            Some("webhook") => {
                parsed.push(Trigger::Webhook {
                    path: t.get("path").and_then(Value::as_str).map(str::to_owned),
                });
            }
            Some("event") => {
                if let Some(event_type) = t.get("eventType").and_then(Value::as_str) {
                    parsed.push(Trigger::Event {
                        event_type: event_type.to_owned(),
                        source: t.get("source").and_then(Value::as_str).map(str::to_owned),
                        subject: t.get("subject").and_then(Value::as_str).map(str::to_owned),
                    });
                }
            }
            _ => {}
        }
    }
    parsed
}

/// Maps the cloud control plane's trigger config back to portable triggers.
fn triggers_from_cloud(triggers: Option<&Value>) -> Vec<Trigger> {
    let Some(list) = triggers.and_then(Value::as_array) else {
        return Vec::new();
    };
    let mut parsed = Vec::new();
    for trigger in list {
        let config = trigger.get("config").and_then(Value::as_object);
        match trigger.get("type").and_then(Value::as_str) {
            Some("cron") => {
                if let Some(schedule) = config
                    .and_then(|value| value.get("schedule"))
                    .and_then(Value::as_str)
                {
                    parsed.push(Trigger::Cron {
                        cron: schedule.to_owned(),
                        start_date: config
                            .and_then(|value| value.get("startDate"))
                            .and_then(Value::as_str)
                            .map(str::to_owned),
                        catchup: config
                            .and_then(|value| value.get("catchup"))
                            .cloned()
                            .and_then(|value| serde_json::from_value(value).ok()),
                    });
                }
            }
            Some("webhook") => {
                parsed.push(Trigger::Webhook {
                    path: config
                        .and_then(|value| value.get("path"))
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                });
            }
            Some("event") => {
                if let Some(event_type) = config
                    .and_then(|value| value.get("eventType"))
                    .and_then(Value::as_str)
                {
                    parsed.push(Trigger::Event {
                        event_type: event_type.to_owned(),
                        source: config
                            .and_then(|value| value.get("source"))
                            .and_then(Value::as_str)
                            .map(str::to_owned),
                        subject: config
                            .and_then(|value| value.get("subject"))
                            .and_then(Value::as_str)
                            .map(str::to_owned),
                    });
                }
            }
            _ => {}
        }
    }
    parsed
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
            triggers: vec![Trigger::Cron {
                cron: "*/5 * * * *".to_owned(),
                start_date: None,
                catchup: None,
            }],
            target_tables: vec!["metrics.samples".to_owned()],
            resources: Resources {
                vcpus: Some(0.25),
                mem_mib: Some(256),
            },
        }
    }

    /// One portable declaration can expose manual dispatch, an HTTP callback,
    /// scheduled backfill, and a broker-fed CloudEvent subscription together.
    #[test]
    fn parses_one_worker_with_every_bounded_trigger() {
        let manifest: WorkerManifest = toml::from_str(
            r#"
name = "market-data-ingest"
exec = ["python3", "ingest.py"]
cwd = "."

[[triggers]]
type = "webhook"
path = "/market-data/daily-bars"

[[triggers]]
type = "cron"
cron = "0 22 * * 1-5"
start_date = "2025-08-04T00:00:00Z"
catchup = "sequential"

[[triggers]]
type = "event"
event_type = "com.yahoo.finance.quote"
source = "urn:rabbitmq:market-data"
subject = "SPY"
"#,
        )
        .expect("multi-trigger manifest");

        assert_eq!(manifest.triggers.len(), 3);
        assert!(manifest.validate().is_ok());
        let local: Value = serde_json::from_str(
            manifest.to_local_worker()["triggers"]
                .as_str()
                .expect("trigger JSON"),
        )
        .expect("local triggers");
        assert_eq!(local.as_array().expect("trigger array").len(), 3);
        assert_eq!(local[1]["startDate"], "2025-08-04T00:00:00Z");
        assert_eq!(local[1]["catchup"], "sequential");
    }

    /// A manifest can bundle readable source files by path instead of copying
    /// their complete contents into TOML.
    #[test]
    fn resolves_relative_file_references() {
        let dir = tempfile::tempdir().expect("temp worker");
        std::fs::write(dir.path().join("worker.py"), "print('SPY')\n").expect("worker source");
        std::fs::write(
            dir.path().join("worker.toml"),
            r#"
name = "spy"
exec = ["python3", "worker.py"]
[files]
"worker.py" = "@file:worker.py"
"#,
        )
        .expect("manifest");

        let manifest =
            WorkerManifest::from_file(&dir.path().join("worker.toml")).expect("resolved manifest");
        assert_eq!(
            manifest.files.get("worker.py").map(String::as_str),
            Some("print('SPY')\n")
        );
    }

    /// Webhook and CloudEvent manifests project to the local trigger registry
    /// without requiring callers to hand-write embedded JSON strings.
    #[test]
    fn translates_event_triggers_to_local_workers() {
        let mut webhook = cron_manifest();
        webhook.triggers = vec![Trigger::Webhook {
            path: Some("/ingest/orders".to_owned()),
        }];
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

        let mut event = cron_manifest();
        event.triggers = vec![Trigger::Event {
            event_type: "org.apache.iceberg.snapshot.committed".to_owned(),
            source: None,
            subject: Some("app.events".to_owned()),
        }];
        let data_body = event.to_local_worker();
        let data_triggers: Value =
            serde_json::from_str(data_body["triggers"].as_str().expect("triggers string"))
                .expect("data triggers");
        assert_eq!(
            data_triggers,
            json!([{
                "type": "event",
                "eventType": "org.apache.iceberg.snapshot.committed",
                "source": null,
                "subject": "app.events"
            }])
        );
    }

    /// The portable trigger shape populates the cloud trigger registry while
    /// the deployment envelope carries the control plane's execution class.
    #[test]
    fn translates_event_triggers_to_cloud_deployments() {
        let mut webhook = cron_manifest();
        webhook.triggers = vec![Trigger::Webhook { path: None }];
        let webhook_body = webhook.to_cloud_deployment("cloud");
        assert_eq!(webhook_body["trigger"], "webhook");
        assert_eq!(
            webhook_body["config"]["triggers"],
            json!([{"type":"webhook","config":{}}])
        );

        let mut event = cron_manifest();
        event.triggers = vec![Trigger::Event {
            event_type: "org.apache.iceberg.snapshot.committed".to_owned(),
            source: None,
            subject: Some("app.events".to_owned()),
        }];
        let data_body = event.to_cloud_deployment("cloud");
        assert_eq!(data_body["trigger"], "event");
        assert_eq!(
            data_body["config"]["triggers"],
            json!([{"type":"event","config":{
                "eventType":"org.apache.iceberg.snapshot.committed",
                "source":null,
                "subject":"app.events"
            }}])
        );
    }

    /// JSON and TOML manifests accept the public webhook and CloudEvent forms.
    #[test]
    fn parses_event_trigger_manifests() {
        let webhook: WorkerManifest = toml::from_str(
            r#"
name = "hook"
exec = ["sh", "worker.sh"]
[[triggers]]
type = "webhook"
path = "/callbacks/orders"
"#,
        )
        .expect("webhook manifest");
        assert!(matches!(
            webhook.triggers.as_slice(),
            [Trigger::Webhook { path: Some(path) }] if path == "/callbacks/orders"
        ));

        let event: WorkerManifest = serde_json::from_value(json!({
            "name": "changed",
            "exec": ["sh", "worker.sh"],
            "triggers": [{
                "type": "event",
                "event_type": "org.apache.iceberg.snapshot.committed",
                "subject": "app.events"
            }]
        }))
        .expect("event manifest");
        assert!(matches!(
            event.triggers.as_slice(),
            [Trigger::Event { event_type, subject: Some(subject), .. }]
                if event_type == "org.apache.iceberg.snapshot.committed" && subject == "app.events"
        ));
    }

    /// Event triggers survive both local registry and cloud detail round trips.
    #[test]
    fn event_triggers_round_trip() {
        for trigger in [
            Trigger::Webhook {
                path: Some("/callbacks/orders".to_owned()),
            },
            Trigger::Event {
                event_type: "org.apache.iceberg.snapshot.committed".to_owned(),
                source: None,
                subject: Some("app.events".to_owned()),
            },
        ] {
            let mut manifest = cron_manifest();
            manifest.triggers = vec![trigger];
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
        assert!(matches!(back.triggers.as_slice(), [Trigger::Cron { .. }]));
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
            triggers: vec![Trigger::Follow {
                file: Some("/var/log/app.log".to_owned()),
            }],
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
