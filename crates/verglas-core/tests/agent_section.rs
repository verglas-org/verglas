//! Acceptance for the `[agent]` config section (#295): agent-side identity and
//! local state live in the daemon's one config file, `~/.verglas/config.toml`,
//! read directly by the CLI, the MCP server, the MV executor, and the capture
//! hook. env.sh is gone; there is one config.

use std::path::PathBuf;

use verglas_core::config::{Config, agent_env_pairs};

/// A minimal daemon config with a one-line `[agent]` section parses, and the
/// agent fields land. Unset agent fields default (spool and model cache derive
/// from the config directory at resolution time, not parse time).
#[test]
fn minimal_agent_section_parses_with_defaults() {
    let toml = r#"
[backend]
provider = "s3"
endpoint = "https://s3.us-west-2.amazonaws.com"
region = "us-west-2"
bucket = "b"

[agent]
agent_id = "poc-agent"
"#;
    let config = Config::from_toml_str(toml).expect("parses");
    assert_eq!(config.agent.agent_id, "poc-agent");
    assert_eq!(config.agent.memory_namespace, None);
    assert_eq!(config.agent.spool_dir, None, "derived later, not at parse");
    assert_eq!(config.agent.model_cache, None);
    assert_eq!(config.agent.aws_profile, None);
}

/// A config with no `[agent]` section at all still parses — the section is
/// optional with full defaults, so the daemon's existing config is untouched.
#[test]
fn absent_agent_section_defaults() {
    let toml = r#"
[backend]
provider = "s3"
endpoint = "https://s3.example"
region = "r"
bucket = "b"
"#;
    let config = Config::from_toml_str(toml).expect("parses");
    assert_eq!(config.agent.agent_id, "default");
}

/// A typo inside `[agent]` is rejected at parse time like every other section
/// (deny_unknown_fields).
#[test]
fn unknown_agent_field_is_rejected() {
    let toml = r#"
[agent]
agent_idd = "oops"
"#;
    assert!(Config::from_toml_str(toml).is_err());
}

/// The env derivation the agent binaries apply at startup: the daemon endpoint
/// comes from [listen] (localhost + admin_port), the warehouse from [catalog],
/// the region from [backend], identity from [agent], and the local dirs
/// default under the config file's directory. Nothing is invented.
#[test]
fn agent_env_pairs_derive_from_the_one_config() {
    let toml = r#"
[listen]
s3_port = 9000
admin_port = 9001

[backend]
provider = "s3"
endpoint = "https://s3.us-west-2.amazonaws.com"
region = "us-west-2"
bucket = "b"

[catalog]
uri = "https://s3tables.us-west-2.amazonaws.com/iceberg"
warehouse = "arn:aws:s3tables:us-west-2:1:bucket/x"

[agent]
agent_id = "poc-agent"
memory_namespace = "poc"
aws_profile = "verglas-lab"
"#;
    let config = Config::from_toml_str(toml).expect("parses");
    let config_dir = PathBuf::from("/home/u/.verglas");
    let pairs = agent_env_pairs(&config, &config_dir);
    let get = |k: &str| {
        pairs
            .iter()
            .find(|(key, _)| key == k)
            .map(|(_, v)| v.clone())
            .unwrap_or_else(|| panic!("{k} missing from {pairs:?}"))
    };
    assert_eq!(get("VERGLAS_ENDPOINT"), "http://127.0.0.1:9001");
    assert_eq!(
        get("VERGLAS_WAREHOUSE"),
        "arn:aws:s3tables:us-west-2:1:bucket/x"
    );
    assert_eq!(get("VERGLAS_REGION"), "us-west-2");
    assert_eq!(get("VERGLAS_AGENT_ID"), "poc-agent");
    assert_eq!(get("VERGLAS_MEMORY_NAMESPACE"), "poc");
    assert_eq!(get("VERGLAS_SPOOL_DIR"), "/home/u/.verglas/agent/spool");
    assert_eq!(get("VERGLAS_MODEL_CACHE"), "/home/u/.verglas/models");
    assert_eq!(get("AWS_PROFILE"), "verglas-lab");
    assert_eq!(get("AWS_REGION"), "us-west-2");
}

/// Explicit agent dirs win over the derived defaults, and an absent catalog or
/// namespace simply omits its pair (the CLI's own fallbacks handle the rest).
#[test]
fn explicit_dirs_win_and_absent_values_are_omitted() {
    let toml = r#"
[backend]
provider = "s3"
endpoint = "https://s3.example"
region = "r"
bucket = "b"

[agent]
agent_id = "a"
spool_dir = "/data/spool"
model_cache = "/data/models"
"#;
    let config = Config::from_toml_str(toml).expect("parses");
    let pairs = agent_env_pairs(&config, &PathBuf::from("/home/u/.verglas"));
    let get = |k: &str| {
        pairs
            .iter()
            .find(|(key, _)| key == k)
            .map(|(_, v)| v.clone())
    };
    assert_eq!(get("VERGLAS_SPOOL_DIR").as_deref(), Some("/data/spool"));
    assert_eq!(get("VERGLAS_MODEL_CACHE").as_deref(), Some("/data/models"));
    assert_eq!(get("VERGLAS_WAREHOUSE"), None, "no [catalog], no pair");
    assert_eq!(get("VERGLAS_MEMORY_NAMESPACE"), None);
    assert_eq!(get("AWS_PROFILE"), None);
}
