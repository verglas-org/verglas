//! Smoke tests for the `verglas` CLI help surface.
//!
//! `--help` must list exactly the shipped command set and nothing else, the
//! global option surface is `--json` alone, and per-command help stays free
//! of configuration plumbing.

use std::process::Command;

/// Every subcommand `verglas --help` is allowed to list, and nothing else. The
/// source/MV/sink platform primitives were removed with the worker refocus; the
/// local `workers` command is the surviving deployment surface. `graph` and
/// `vector` (#144) wrap the S3 semantic listener's property-graph and vector
/// index REST-JSON surfaces. `ingest` and `sql` are the /v0 data-plane verbs
/// (append-ingest and ad hoc SQL) that replaced the retired tenant-local
/// access-service era's `lakehouse` and `token` commands.
const SURVIVING_COMMANDS: [&str; 12] = [
    "login",
    "logout",
    "status",
    "table",
    "dashboard",
    "workers",
    "secret",
    "lakehouse",
    "ingest",
    "sql",
    "graph",
    "vector",
];

/// The CLI exposes exactly one global flag: endpoints and credentials
/// resolve from the profile, settings, and environment, never from options.
#[test]
fn top_level_options_are_exactly_json_help_version() {
    let out = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .arg("--help")
        .output()
        .expect("binary runs");
    let stdout = String::from_utf8(out.stdout).expect("utf8");
    let options: Vec<&str> = stdout
        .lines()
        .filter_map(|line| {
            let t = line.trim_start();
            t.starts_with("--")
                .then(|| t.split_whitespace().next().unwrap_or_default())
                .or_else(|| {
                    t.starts_with("-")
                        .then(|| t.split(',').next().unwrap_or_default().trim())
                })
        })
        .collect();
    assert_eq!(
        options,
        ["--json", "-h", "-V"],
        "global options are exactly --json, -h, -V: {stdout}"
    );
}

/// `login --help` shows only login's own options.
#[test]
fn login_help_has_no_global_noise() {
    let out = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .args(["login", "--help"])
        .output()
        .expect("binary runs");
    let stdout = String::from_utf8(out.stdout).expect("utf8");
    assert!(stdout.contains("--api-key"), "{stdout}");
    assert!(stdout.contains("--no-browser"), "{stdout}");
    for retired in [
        "--url",
        "--dashboard-url",
        "--access-endpoint",
        "--s3-endpoint",
    ] {
        assert!(
            !stdout.contains(retired),
            "{retired} must not appear in login help: {stdout}"
        );
    }
}

#[test]
fn long_version_flag_prints_cli_version() {
    let out = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .arg("--version")
        .output()
        .expect("binary runs");
    assert!(out.status.success());
    let stdout = String::from_utf8(out.stdout).expect("utf8");
    assert!(
        stdout.starts_with("verglas "),
        "--version must print the CLI version: {stdout}"
    );
}

#[test]
fn short_version_flag_prints_cli_version() {
    // `-V` is clap's built-in short version flag (#288): it prints the CLI's own
    // version without contacting any server.
    let out = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .arg("-V")
        .output()
        .expect("binary runs");
    assert!(out.status.success());
    let stdout = String::from_utf8(out.stdout).expect("utf8");
    assert!(
        stdout.starts_with("verglas "),
        "-V must print the CLI version: {stdout}"
    );
}

#[test]
fn dashboard_help_is_cloud_json_render() {
    let out = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .args(["dashboard", "--help"])
        .output()
        .expect("binary runs");
    assert!(out.status.success());
    let stdout = String::from_utf8(out.stdout).expect("utf8");
    assert!(
        stdout.contains("json-render") || stdout.contains("Verglas Cloud"),
        "dashboard help must describe Cloud json-render: {stdout}"
    );
    assert!(
        !stdout.contains("Rill") && !stdout.contains("Compose analytics"),
        "dashboard help must not mention Rill: {stdout}"
    );
}

/// The subcommand names `verglas --help` advertises, taken from the `Commands:`
/// block only — so a command word appearing inside a description or an option
/// (`-V, --version`) is not mistaken for a listed subcommand.
fn help_command_names(help: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut in_commands = false;
    for line in help.lines() {
        if line.starts_with("Commands:") {
            in_commands = true;
            continue;
        }
        if in_commands {
            // The block ends at the first blank line (before `Options:`).
            if line.trim().is_empty() {
                break;
            }
            // Each entry is `  <name>  <description>`; the name is the first
            // whitespace-delimited token.
            if let Some(name) = line.split_whitespace().next() {
                names.push(name.to_owned());
            }
        }
    }
    names
}

#[test]
fn help_lists_exactly_the_surviving_commands() {
    let out = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .arg("--help")
        .output()
        .expect("binary runs");
    assert!(out.status.success());

    let stdout = String::from_utf8(out.stdout).expect("utf8");
    let listed = help_command_names(&stdout);

    for command in SURVIVING_COMMANDS {
        assert!(
            listed.iter().any(|c| c == command),
            "expected --help to list `{command}`, got {listed:?}"
        );
    }
    // The only entries beyond the surviving set is clap's own `help`.
    for name in &listed {
        assert!(
            SURVIVING_COMMANDS.contains(&name.as_str()) || name == "help",
            "unexpected command `{name}` in --help: {listed:?}"
        );
    }
}

#[test]
fn ingest_and_sql_help_render() {
    for args in [["ingest", "--help"], ["sql", "--help"]] {
        let out = Command::new(env!("CARGO_BIN_EXE_verglas"))
            .args(args)
            .output()
            .expect("binary runs");
        assert!(
            out.status.success(),
            "`verglas {} --help` must render",
            args[0]
        );
    }
}

#[test]
fn table_help_lists_exactly_the_mvp_verbs() {
    // `verglas table --help` lists exactly the shipped verbs.
    let out = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .args(["table", "--help"])
        .output()
        .expect("binary runs");
    assert!(out.status.success());
    let stdout = String::from_utf8(out.stdout).expect("utf8");
    let verbs = help_command_names(&stdout);
    let mut expected = vec![
        "create", "append", "list", "show", "history", "delete", "help",
    ];
    expected.sort_unstable();
    let mut listed = verbs.clone();
    listed.sort_unstable();
    assert_eq!(
        listed, expected,
        "`verglas table --help` lists exactly the shipped verbs"
    );
}

#[test]
fn graph_help_lists_every_verb() {
    let out = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .args(["graph", "--help"])
        .output()
        .expect("binary runs");
    assert!(out.status.success());
    let stdout = String::from_utf8(out.stdout).expect("utf8");
    for command in [
        "create",
        "add-node",
        "add-edge",
        "neighbors",
        "index",
        "show",
        "delete",
        "list",
        "k-hop",
        "paths",
    ] {
        assert!(
            stdout
                .lines()
                .any(|line| line.trim_start().starts_with(command)),
            "graph help must list {command}: {stdout}"
        );
    }
}

#[test]
fn vector_help_lists_every_verb() {
    let out = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .args(["vector", "--help"])
        .output()
        .expect("binary runs");
    assert!(out.status.success());
    let stdout = String::from_utf8(out.stdout).expect("utf8");
    for command in [
        "create-bucket",
        "create-index",
        "put",
        "query",
        "list",
        "get",
        "delete",
        "delete-index",
        "delete-bucket",
        "list-buckets",
        "list-indexes",
    ] {
        assert!(
            stdout
                .lines()
                .any(|line| line.trim_start().starts_with(command)),
            "vector help must list {command}: {stdout}"
        );
    }
}

#[test]
fn table_delete_help_renders_and_offers_yes_flag() {
    // The new `verglas table delete` verb must exist and require an explicit
    // `--yes` to skip the interactive confirmation.
    let out = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .args(["table", "delete", "--help"])
        .output()
        .expect("binary runs");
    assert!(out.status.success(), "`table delete --help` must render");
    let stdout = String::from_utf8(out.stdout).expect("utf8");
    assert!(
        stdout.contains("--yes"),
        "`table delete --help` must offer --yes: {stdout}"
    );
}
