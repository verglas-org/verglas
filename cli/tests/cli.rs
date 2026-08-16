//! Smoke tests for the `verglas` CLI help surface.
//!
//! These pin the command tree to the set that actually works (#288): a command
//! is listed only if invoking it performs its advertised job end to end and
//! operates on the local node only — the CLI never does cluster activities.
//! The removed stubs (`analyze`, `deploy`, `keys`, `warm`,
//! `doctor`), the former `version` subcommand, the remote-targeting `node`
//! verb, the removed `uninstall` verb, and the `tables` (plural) group that
//! duplicated `table` must not reappear in `--help`, so nothing creeps back
//! silently. The `memory` and `skills` verbs are gone: durable agent memory is
//! not part of this product. OS service lifecycle (`init`/`start`/`stop`/
//! `restart`/`logs`/`dev`) is gone — the server runs in Docker. Removed
//! hosted control-plane verbs (`login`, `containers`, `volumes`, `secrets`) are
//! gone (#66). Issue #84 adds singular local `db` and `secret` resource APIs.

use std::process::Command;

/// Every subcommand `verglas --help` is allowed to list, and nothing else. The
/// source/MV/sink platform primitives were removed with the worker refocus; the
/// local `workers` command is the surviving deployment surface. `graph` and
/// `vector` (#144) wrap the S3 semantic listener's property-graph and vector
/// index REST-JSON surfaces.
const SURVIVING_COMMANDS: [&str; 11] = [
    "login",
    "connection",
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

/// Commands removed from the CLI: `--help` must not name them. `graph` (top-
/// level `verglas graph`) came back with #144 as the property-graph command
/// family, so it moved out of this list into `SURVIVING_COMMANDS`; the
/// standalone `query`/`index` verbs it replaced are still gone.
const REMOVED_COMMANDS: [&str; 32] = [
    // Drain is an admin-API operation for self-hosters, not a CLI verb (it
    // implied a user could drain a cloud cache).
    "drain",
    "query",
    "index",
    // MVP surface prune (#146): only Lakekeeper-managed lakehouses and the
    // workers runtime survive. `db` became `lakehouse`; vessel and queue
    // provisioning are gone.
    "db",
    "vessel",
    "queue",
    "kv",
    "version",
    "analyze",
    "deploy",
    "keys",
    "node",
    "uninstall",
    "warm",
    "doctor",
    "memory",
    "skills",
    "deployments",
    "instrument",
    "source",
    "mv",
    "sink",
    "init",
    "start",
    "stop",
    "restart",
    "logs",
    "dev",
    // `tables` (plural) duplicated `table`. Only `table` (singular) remains.
    "tables",
    // Removed control-plane surface (#66). `login` returned with the
    // monorepo client merge.
    "containers",
    "volumes",
    "secrets",
];

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
fn version_subcommand_is_an_unknown_command() {
    // `version` is no longer a subcommand (#288): it is a flag. Invoking
    // `verglas version` must be a clap unrecognized-subcommand error, exit
    // non-zero, and never reach out to a server.
    let out = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .arg("version")
        .output()
        .expect("binary runs");
    assert!(
        !out.status.success(),
        "`verglas version` must fail as an unknown command"
    );
    let stderr = String::from_utf8(out.stderr).expect("utf8");
    assert!(
        stderr.contains("unrecognized subcommand") || stderr.contains("unexpected argument"),
        "`verglas version` must be a clap unknown-command error: {stderr}"
    );
}

#[test]
fn memory_and_skills_subcommands_are_unknown() {
    // Durable agent memory (`memory`, `skills`) is not part of this product.
    for args in [
        vec!["memory"],
        vec!["memory", "seed"],
        vec!["memory", "migrate-spool"],
        vec!["skills"],
        vec!["skills", "install"],
    ] {
        let out = Command::new(env!("CARGO_BIN_EXE_verglas"))
            .args(&args)
            .output()
            .expect("binary runs");
        assert!(
            !out.status.success(),
            "`verglas {}` must fail as an unknown command",
            args.join(" ")
        );
        let stderr = String::from_utf8(out.stderr).expect("utf8");
        assert!(
            stderr.contains("unrecognized subcommand") || stderr.contains("unexpected argument"),
            "`verglas {}` must be a clap unknown-command error: {stderr}",
            args.join(" ")
        );
    }
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
    for command in REMOVED_COMMANDS {
        assert!(
            !listed.iter().any(|c| c == command),
            "--help must not list removed command `{command}`, got {listed:?}"
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
fn removed_control_plane_commands_are_unknown() {
    // #66 removed the hosted resource groups (`login` returned with the
    // monorepo client merge); `drain` moved to the admin API only.
    for command in ["drain", "containers", "volumes", "secrets"] {
        let out = Command::new(env!("CARGO_BIN_EXE_verglas"))
            .arg(command)
            .output()
            .expect("binary runs");
        assert!(
            !out.status.success(),
            "`verglas {command}` must fail as an unknown command"
        );
        let stderr = String::from_utf8(out.stderr).expect("utf8");
        assert!(
            stderr.contains("unrecognized subcommand") || stderr.contains("unexpected argument"),
            "`verglas {command}` must be a clap unknown-command error: {stderr}"
        );
    }
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
    // `verglas table --help` must not list retired verbs (`index`; `metrics`
    // and `compact` left with the MVP prune, #146) and must list every
    // surviving table verb.
    let out = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .args(["table", "--help"])
        .output()
        .expect("binary runs");
    assert!(out.status.success());
    let stdout = String::from_utf8(out.stdout).expect("utf8");
    let verbs = help_command_names(&stdout);
    for retired in ["index", "metrics", "compact"] {
        assert!(
            !verbs.iter().any(|v| v == retired),
            "`verglas table --help` must not list `{retired}`: {verbs:?}"
        );
    }
    for verb in ["create", "append", "list", "show", "history", "delete"] {
        assert!(
            verbs.iter().any(|v| v == verb),
            "`verglas table --help` must list `{verb}`: {verbs:?}"
        );
    }
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
