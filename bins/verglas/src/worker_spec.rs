//! The one portable worker spec.
//!
//! A single versioned file (TOML or JSON) describes a worker completely: its
//! name, its locked build runtime, container command, files, environment (with
//! `@secret:` references), its trigger, its target tables, and its resource
//! hints. The CLI translates it into the local server's `POST /v1/workers`
//! body so the same file registers and round-trips without edits.
//!
//! Python and Bun projects use platform-owned OCI build policies. A container
//! project supplies its own auditable Dockerfile.
//!
//! Secrets never travel in the spec beyond a NAME. An env value of the form
//! `@secret:NAME` is a reference the server resolves from its own secret store
//! at run time; the value is never in the file and never printed.

use std::collections::BTreeMap;
use std::error::Error;
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use verglas_sdk::worker::Catchup;

/// Build policy used to create an immutable worker image.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerRuntime {
    /// Python project locked by `uv.lock`.
    Python,
    /// JavaScript or TypeScript project locked by `bun.lock`.
    Bun,
    /// Project with an explicit Dockerfile.
    Container,
}

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
    /// target table. Local only.
    Follow {
        /// A file to tail. When absent, the worker's `exec` command is wrapped
        /// and its stdout and stderr are captured.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        file: Option<String>,
    },
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

/// Advisory resource hints for the worker.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Resources {
    /// Fractional vCPUs the worker is sized for.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vcpus: Option<f64>,
    /// Memory the worker is sized for, in MiB.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mem_mib: Option<u64>,
    /// Maximum number of processes in one invocation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pids: Option<i64>,
    /// Maximum wall-clock duration in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u64>,
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
    /// Platform-owned build policy for this worker project.
    pub runtime: WorkerRuntime,
    /// Container command and arguments. Element 0 is the executable.
    pub entrypoint: Vec<String>,
    /// Files bundled with the worker, path to text content. Written under the
    /// worker's directory on the local server.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub files: BTreeMap<String, String>,
    /// Environment for the worker. A value of `@secret:NAME` is resolved from
    /// the server's secret store; the value is never in this file.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
    /// Events that run the worker. Manual dispatch is available to every worker
    /// and therefore is not a trigger declaration.
    #[serde(default)]
    pub triggers: Vec<Trigger>,
    /// The Iceberg tables the worker writes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub target_tables: Vec<String>,
    /// Optional absolute path backed by operator-owned persistent scratch storage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scratch_target: Option<String>,
    /// Resource hints.
    #[serde(default, skip_serializing_if = "is_default_resources")]
    pub resources: Resources,
}

fn default_spec_version() -> u32 {
    SPEC_VERSION
}

fn is_default_resources(r: &Resources) -> bool {
    r.vcpus.is_none() && r.mem_mib.is_none() && r.pids.is_none() && r.timeout_secs.is_none()
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
        if self.entrypoint.is_empty() && !follow_file {
            return Err("the worker spec needs a non-empty `entrypoint`".into());
        }
        for required in match self.runtime {
            WorkerRuntime::Python => &["pyproject.toml", "uv.lock"][..],
            WorkerRuntime::Bun => &["package.json", "bun.lock"][..],
            WorkerRuntime::Container => &["Dockerfile"][..],
        } {
            if !self.files.contains_key(*required) {
                return Err(
                    format!("the {:?} worker project needs `{required}`", self.runtime).into(),
                );
            }
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
        if self.scratch_target.as_ref().is_some_and(|path| {
            !path.starts_with('/')
                || path.len() == 1
                || path.split('/').any(|part| matches!(part, "." | ".."))
        }) {
            return Err("scratch_target must be a clean absolute container path".into());
        }
        Ok(())
    }

    /// Whether this worker's trigger is follow.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn is_follow(&self) -> bool {
        self.triggers
            .iter()
            .any(|trigger| matches!(trigger, Trigger::Follow { .. }))
    }

    /// The names of the secrets this worker references through `@secret:` env
    /// values, in sorted order and de-duplicated.
    #[cfg_attr(not(test), allow(dead_code))]
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

    /// The immutable worker project encoded in the registry's `code` column.
    fn code_json(&self) -> Value {
        Value::String(
            json!({
                "name": self.name,
                "runtime": self.runtime,
                "entrypoint": self.entrypoint,
                "files": self.files,
            })
            .to_string(),
        )
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
        config.insert("resources".to_owned(), json!(self.resources));
        if let Some(target) = &self.scratch_target {
            config.insert("scratch_target".to_owned(), json!(target));
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

    /// Rebuilds a spec from a worker row read back from the local server.
    /// `code`, `triggers`, and `config` are JSON
    /// strings on the row.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn from_local_worker(row: &Value) -> Result<WorkerManifest, Box<dyn Error>> {
        let name = row
            .get("name")
            .and_then(Value::as_str)
            .ok_or("worker row has no name")?
            .to_owned();
        let code: Value = parse_embedded(row.get("code"));
        let runtime = serde_json::from_value(
            code.get("runtime")
                .cloned()
                .ok_or("worker row has no runtime")?,
        )?;
        let entrypoint: Vec<String> = code
            .get("entrypoint")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default();
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
        let files: BTreeMap<String, String> = code
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
        let resources = config
            .get("resources")
            .cloned()
            .map(serde_json::from_value)
            .transpose()?
            .unwrap_or_default();
        let scratch_target = config
            .get("scratch_target")
            .and_then(Value::as_str)
            .map(str::to_owned);
        Ok(WorkerManifest {
            spec_version: SPEC_VERSION,
            name,
            runtime,
            entrypoint,
            files,
            env,
            triggers,
            target_tables,
            scratch_target,
            resources,
        })
    }
}

/// Parses a possibly-embedded JSON string column into a `Value` (a row's `code`
/// / `config` / `triggers` are stored as JSON strings). A plain object passes
/// through; anything unparseable becomes null.
#[cfg_attr(not(test), allow(dead_code))]
fn parse_embedded(field: Option<&Value>) -> Value {
    match field {
        Some(Value::String(s)) => serde_json::from_str(s).unwrap_or(Value::Null),
        Some(other) => other.clone(),
        None => Value::Null,
    }
}

/// Maps a local `triggers` JSON array back to every portable trigger.
#[cfg_attr(not(test), allow(dead_code))]
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A Python worker declares its runtime, locked dependencies, and container entrypoint.
    #[test]
    fn parses_dependency_aware_python_worker() {
        let manifest: WorkerManifest = toml::from_str(
            r#"
name = "massive-options"
runtime = "python"
entrypoint = ["python", "worker.py"]
scratch_target = "/scratch"

[files]
"pyproject.toml" = "[project]\nname='worker'\nversion='0.1.0'\n"
"uv.lock" = "version = 1\n"
"worker.py" = "print('ok')\n"
"#,
        )
        .expect("worker manifest");

        assert_eq!(manifest.runtime, WorkerRuntime::Python);
        assert_eq!(manifest.entrypoint, ["python", "worker.py"]);
        assert_eq!(manifest.scratch_target.as_deref(), Some("/scratch"));
        assert!(manifest.validate().is_ok());
    }

    fn cron_manifest() -> WorkerManifest {
        WorkerManifest {
            spec_version: 1,
            name: "collector".to_owned(),
            runtime: WorkerRuntime::Python,
            entrypoint: vec!["python".to_owned(), "collect.py".to_owned()],
            files: BTreeMap::from([
                (
                    "pyproject.toml".to_owned(),
                    "[project]\nname='collector'\nversion='0.1.0'\n".to_owned(),
                ),
                ("uv.lock".to_owned(), "version = 1\n".to_owned()),
                ("collect.py".to_owned(), "print('ok')\n".to_owned()),
            ]),
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
            scratch_target: None,
            resources: Resources {
                vcpus: Some(0.25),
                mem_mib: Some(256),
                pids: Some(64),
                timeout_secs: Some(300),
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
runtime = "python"
entrypoint = ["python", "ingest.py"]

[files]
"pyproject.toml" = "[project]\nname='ingest'\nversion='0.1.0'\n"
"uv.lock" = "version = 1\n"
"ingest.py" = "print('ok')\n"

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
runtime = "python"
entrypoint = ["python", "worker.py"]
[files]
"pyproject.toml" = "[project]\nname='spy'\nversion='0.1.0'\n"
"uv.lock" = "version = 1\n"
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

    /// JSON and TOML manifests accept the public webhook and CloudEvent forms.
    #[test]
    fn parses_event_trigger_manifests() {
        let webhook: WorkerManifest = toml::from_str(
            r#"
name = "hook"
runtime = "container"
entrypoint = ["sh", "worker.sh"]
[files]
"Dockerfile" = "FROM alpine:3.22\nWORKDIR /app\nCOPY . .\n"
"worker.sh" = "exit 0\n"
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
            "runtime": "container",
            "entrypoint": ["sh", "worker.sh"],
            "files": {
                "Dockerfile": "FROM alpine:3.22\nWORKDIR /app\nCOPY . .\n",
                "worker.sh": "exit 0\n"
            },
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

    /// Event triggers survive a local registry round trip.
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
        }
    }

    /// Secret references are extracted by name, sorted and de-duplicated; the
    /// values never appear.
    #[test]
    fn secret_names_are_extracted_by_name() {
        assert_eq!(cron_manifest().secret_names(), vec!["MY_KEY".to_owned()]);
    }

    /// The local worker body carries the build project, trigger, output, and runtime config.
    #[test]
    fn translates_to_a_local_worker() {
        let body = cron_manifest().to_local_worker();
        assert_eq!(body["name"], "collector");
        assert_eq!(body["output"], "metrics.samples");
        let code: Value =
            serde_json::from_str(body["code"].as_str().expect("code string")).expect("code parses");
        assert_eq!(code["runtime"], "python");
        assert_eq!(code["entrypoint"][0], "python");
        assert!(code["files"]["uv.lock"].is_string());
        let triggers: Value =
            serde_json::from_str(body["triggers"].as_str().expect("triggers string"))
                .expect("triggers parse");
        assert_eq!(triggers[0]["type"], "cron");
        assert_eq!(triggers[0]["schedule"], "*/5 * * * *");
        let config: Value = serde_json::from_str(body["config"].as_str().expect("config string"))
            .expect("config parses");
        assert_eq!(config["env"]["API_KEY"], "@secret:MY_KEY");
    }

    /// A spec round-trips through the local worker body and back with no edits.
    #[test]
    fn round_trips_through_a_local_worker_row() {
        let manifest = cron_manifest();
        let row = manifest.to_local_worker();
        let back = WorkerManifest::from_local_worker(&row).expect("rebuild");
        assert_eq!(back.name, manifest.name);
        assert_eq!(back.runtime, manifest.runtime);
        assert_eq!(back.entrypoint, manifest.entrypoint);
        assert_eq!(back.files, manifest.files);
        assert_eq!(back.env, manifest.env);
        assert!(matches!(back.triggers.as_slice(), [Trigger::Cron { .. }]));
        assert_eq!(back.target_tables, manifest.target_tables);
    }

    /// A follow worker is recognized; a file-follow may omit an entrypoint.
    #[test]
    fn follow_worker_validates_without_exec_when_tailing_a_file() {
        let manifest = WorkerManifest {
            spec_version: 1,
            name: "tail".to_owned(),
            runtime: WorkerRuntime::Container,
            entrypoint: vec![],
            files: BTreeMap::from([("Dockerfile".to_owned(), "FROM alpine:3.22\n".to_owned())]),
            env: BTreeMap::new(),
            triggers: vec![Trigger::Follow {
                file: Some("/var/log/app.log".to_owned()),
            }],
            target_tables: vec!["follow.app".to_owned()],
            scratch_target: None,
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
