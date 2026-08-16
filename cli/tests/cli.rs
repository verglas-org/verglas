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
/// index REST-JSON surfaces.
const SURVIVING_COMMANDS: [&str; 11] = [
    "login",
    "logout",
    "status",
    "table",
    "dashboard",
    "workers",
    "lakehouse",
    "secret",
    "token",
    "graph",
    "vector",
];

/// The CLI exposes exactly one global flag. Endpoints and credentials resolve
/// from the connection profile, config.toml overrides, and environment
/// variables — never from `--` options that pollute every subcommand's help.
#[test]
fn top_level_options_are_only_json() {
    let out = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .arg("--help")
        .output()
        .expect("binary runs");
    let stdout = String::from_utf8(out.stdout).expect("utf8");
    for retired in [
        "--access-endpoint",
        "--s3-endpoint",
        "--credentials-file",
        "--token",
    ] {
        assert!(
            !stdout.contains(retired),
            "{retired} must not be a CLI flag: {stdout}"
        );
    }
    assert!(stdout.contains("--json"), "--json survives: {stdout}");
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

#[test]
fn server_endpoint_flag_is_gone() {
    let help = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .arg("--help")
        .output()
        .expect("binary runs");
    assert!(help.status.success());
    let stdout = String::from_utf8(help.stdout).expect("utf8");
    assert!(
        !stdout.contains("--server-endpoint"),
        "cloud is the default; the flag must not appear: {stdout}"
    );

    let out = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .args(["--server-endpoint", "http://127.0.0.1:8334", "status"])
        .output()
        .expect("binary runs");
    assert!(
        !out.status.success(),
        "--server-endpoint must be an unknown flag"
    );
    let stderr = String::from_utf8(out.stderr).expect("utf8");
    assert!(
        stderr.contains("unexpected argument") || stderr.contains("unrecognized"),
        "--server-endpoint must be rejected: {stderr}"
    );
}

#[test]
fn internal_seed_target_is_hidden_from_help() {
    // The installer detaches background seeding to a hidden internal subcommand
    // (`__seed`). It exists solely as the detach target and must never surface in
    // `--help` as a user verb.
    let out = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .arg("--help")
        .output()
        .expect("binary runs");
    assert!(out.status.success());
    let stdout = String::from_utf8(out.stdout).expect("utf8");
    assert!(
        !stdout.contains("__seed"),
        "the internal seed target must be hidden from --help: {stdout}"
    );
    assert!(
        !help_command_names(&stdout)
            .iter()
            .any(|c| c == "__seed" || c == "seed"),
        "no seed verb is listed in --help"
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
fn lakehouse_help_lists_the_create_verb() {
    let out = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .args(["lakehouse", "--help"])
        .output()
        .expect("binary runs");
    assert!(out.status.success());
    let stdout = String::from_utf8(out.stdout).expect("utf8");
    assert!(
        stdout
            .lines()
            .any(|line| line.trim_start().starts_with("create")),
        "lakehouse help must list create: {stdout}"
    );
}

#[test]
fn tables_plural_is_an_unknown_command() {
    // `verglas tables` duplicated `verglas table` and was removed. Invoking it
    // must be a clap unknown-command error and exit non-zero.
    let out = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .arg("tables")
        .output()
        .expect("binary runs");
    assert!(
        !out.status.success(),
        "`verglas tables` must fail as an unknown command"
    );
    let stderr = String::from_utf8(out.stderr).expect("utf8");
    assert!(
        stderr.contains("unrecognized subcommand") || stderr.contains("unexpected argument"),
        "`verglas tables` must be a clap unknown-command error: {stderr}"
    );
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
fn table_index_is_an_unknown_command() {
    // The retired `table index` path must error.
    let out = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .args(["table", "index"])
        .output()
        .expect("binary runs");
    assert!(
        !out.status.success(),
        "`verglas table index` must fail as an unknown subcommand"
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
