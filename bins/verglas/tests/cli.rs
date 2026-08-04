//! Smoke tests for the `verglas` CLI help surface.
//!
//! These pin the command tree to the set that actually works (#288): a command
//! is listed only if invoking it performs its advertised job end to end and
//! operates on the local node only — the CLI never does cluster activities.
//! The removed stubs (`analyze`, `deploy`, `keys`, `warm`,
//! `doctor`), the former `version` subcommand, the remote-targeting `node`
//! verb, the removed `uninstall` verb, and the `tables` (plural) group that
//! duplicated `table` must not reappear in `--help`, so nothing creeps back
//! silently. The `memory` verb was removed too: seeding is
//! automatic at install and the one-shot spool migration is gone, so there is
//! no user-facing memory command (the installer's detached seed target `__seed`
//! is hidden and is not a user verb).

use std::process::Command;

/// Every subcommand `verglas --help` is allowed to list, and nothing else. The
/// source/MV/sink platform primitives were removed with the worker refocus; the
/// cloud `workers` command is the surviving deployment surface.
const SURVIVING_COMMANDS: [&str; 19] = [
    "dev",
    "drain",
    "init",
    "start",
    "stop",
    "restart",
    "status",
    "logs",
    "table",
    "graph",
    "query",
    "login",
    "index",
    "workers",
    "containers",
    "db",
    "volumes",
    "secrets",
    // `skills` returned with the MCP-endpoint rebuild: `verglas skills install`
    // wires an agent session to the tenant's memory MCP.
    "skills",
];

/// Commands removed from the CLI: `--help` must not name them. `version` became a
/// flag; `node` resolved and targeted OTHER nodes, which is not a CLI concern
/// (`drain` acts on the local daemon only); `uninstall` was replaced by
/// documented manual steps; the rest were unimplemented stubs. `memory` was
/// removed once seeding became automatic and the spool migration was deleted.
/// `deployments` was removed once the platform primitives (`source`/`mv`/`sink`)
/// became the commands: each primitive's `list` now shows local and cloud
/// together, so a separate generic verb is redundant.
const REMOVED_COMMANDS: [&str; 15] = [
    "version",
    "analyze",
    "deploy",
    "keys",
    "node",
    "uninstall",
    "warm",
    "doctor",
    "memory",
    "deployments",
    "instrument",
    "source",
    "mv",
    "sink",
    // `tables` (plural) duplicated `table`; its unique verbs moved under
    // `table` (metrics) and `index`. Only `table` (singular) remains.
    "tables",
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
    // version without contacting any daemon.
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
fn version_subcommand_is_an_unknown_command() {
    // `version` is no longer a subcommand (#288): it is a flag. Invoking
    // `verglas version` must be a clap unrecognized-subcommand error, exit
    // non-zero, and never reach out to a daemon.
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
fn memory_subcommand_is_an_unknown_command() {
    // The `verglas memory` verb was removed: seeding is automatic at install and
    // the one-shot spool migration is gone. Invoking `verglas memory` (or its old
    // `seed`/`migrate-spool` children) must be a clap unknown-command error and
    // must never reach a daemon.
    for args in [
        vec!["memory"],
        vec!["memory", "seed"],
        vec!["memory", "migrate-spool"],
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
fn table_help_no_longer_lists_index() {
    // Indexes are owned by the top-level `verglas index` group; `table index`
    // was removed. `verglas table --help` must not list an `index` verb, but must
    // still list the surviving `table` verbs including the new `delete` and
    // `metrics`.
    let out = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .args(["table", "--help"])
        .output()
        .expect("binary runs");
    assert!(out.status.success());
    let stdout = String::from_utf8(out.stdout).expect("utf8");
    let verbs = help_command_names(&stdout);
    assert!(
        !verbs.iter().any(|v| v == "index"),
        "`verglas table --help` must not list `index`: {verbs:?}"
    );
    for verb in [
        "create", "append", "list", "show", "history", "delete", "metrics",
    ] {
        assert!(
            verbs.iter().any(|v| v == verb),
            "`verglas table --help` must list `{verb}`: {verbs:?}"
        );
    }
}

#[test]
fn table_index_is_an_unknown_command() {
    // `table index` moved out to `verglas index`; the old path must error.
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

#[test]
fn index_help_lists_add_search_and_list() {
    // `verglas index` is now the one index command group: it gained `add` and
    // `search` (moved from `table index`) alongside `list`.
    let out = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .args(["index", "--help"])
        .output()
        .expect("binary runs");
    assert!(out.status.success());
    let stdout = String::from_utf8(out.stdout).expect("utf8");
    let verbs = help_command_names(&stdout);
    for verb in ["list", "add", "search"] {
        assert!(
            verbs.iter().any(|v| v == verb),
            "`verglas index --help` must list `{verb}`: {verbs:?}"
        );
    }
}

#[test]
fn dev_offers_a_required_bucket_flag() {
    // `verglas dev` serves exactly one bucket, named by a required `--bucket`
    // flag. `--help` must list it alongside the other flags.
    let out = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .args(["dev", "--help"])
        .output()
        .expect("binary runs");

    assert!(out.status.success());
    let stdout = String::from_utf8(out.stdout).expect("utf8");
    assert!(
        stdout.contains("--bucket"),
        "`verglas dev` must offer a --bucket flag: {stdout}"
    );
    // The other flags are still there.
    assert!(stdout.contains("--cache-dir") && stdout.contains("--port"));
}

#[test]
fn dev_help_offers_dram_flag_defaulting_to_one_gib() {
    // `verglas dev --dram` exposes the DRAM ceiling (issue #141); its default
    // stays 1GB so the dram-resident profile is the out-of-the-box behavior.
    let out = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .args(["dev", "--help"])
        .output()
        .expect("binary runs");

    assert!(out.status.success());
    let stdout = String::from_utf8(out.stdout).expect("utf8");
    assert!(
        stdout.contains("--dram"),
        "expected a --dram flag: {stdout}"
    );
    assert!(
        stdout.contains("1GB"),
        "expected --dram to default to 1GB: {stdout}"
    );
}

#[test]
fn dev_help_offers_nodes_flag_defaulting_to_one() {
    // `verglas dev --nodes N` boots a local pseudo-cluster (issue #160); its
    // default is 1 so the single-node cluster-of-one stays the out-of-the-box
    // behavior.
    let out = Command::new(env!("CARGO_BIN_EXE_verglas"))
        .args(["dev", "--help"])
        .output()
        .expect("binary runs");

    assert!(out.status.success());
    let stdout = String::from_utf8(out.stdout).expect("utf8");
    assert!(
        stdout.contains("--nodes"),
        "expected a --nodes flag: {stdout}"
    );
    assert!(
        stdout.contains("default: 1") || stdout.contains("[default: 1]"),
        "expected --nodes to default to 1: {stdout}"
    );
}
