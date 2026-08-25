//! Verifies that root workspace dependencies match the retained crate manifests.
//!
//! This is intentionally a static check: Cargo accepts extra workspace declarations,
//! but stale entries keep deleted dependency graphs reachable during resolution.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn every_workspace_dependency_is_referenced() {
    let root = repository_root();
    let root_manifest = read_manifest(&root.join("Cargo.toml"));
    let declared = workspace_dependency_names(&root_manifest);
    let mut referenced = BTreeSet::new();

    for member in workspace_members(&root_manifest) {
        let manifest = read_manifest(&root.join(member).join("Cargo.toml"));
        collect_workspace_references(&manifest, &mut referenced);
    }

    let unused: Vec<_> = declared.difference(&referenced).cloned().collect();
    assert!(
        unused.is_empty(),
        "workspace dependencies are not referenced by retained manifests: {}",
        unused.join(", ")
    );
}

/// Resolves the checkout root from this integration test's crate directory.
fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Reads and parses a TOML manifest, attaching its path to any failure.
fn read_manifest(path: &Path) -> toml::Value {
    let contents = fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
    toml::from_str(&contents)
        .unwrap_or_else(|error| panic!("failed to parse {}: {error}", path.display()))
}

/// Returns the names declared in the root workspace dependency table.
fn workspace_dependency_names(root_manifest: &toml::Value) -> BTreeSet<String> {
    root_manifest
        .get("workspace")
        .and_then(toml::Value::as_table)
        .and_then(|workspace| workspace.get("dependencies"))
        .and_then(toml::Value::as_table)
        .unwrap_or_else(|| panic!("root manifest has no [workspace.dependencies] table"))
        .keys()
        .cloned()
        .collect()
}

/// Returns the retained workspace member paths from the root manifest.
fn workspace_members(root_manifest: &toml::Value) -> Vec<String> {
    root_manifest
        .get("workspace")
        .and_then(toml::Value::as_table)
        .and_then(|workspace| workspace.get("members"))
        .and_then(toml::Value::as_array)
        .unwrap_or_else(|| panic!("root manifest has no workspace members"))
        .iter()
        .map(|member| {
            member
                .as_str()
                .unwrap_or_else(|| panic!("workspace member is not a string: {member:?}"))
                .to_owned()
        })
        .collect()
}

/// Collects `.workspace = true` references from all dependency sections.
fn collect_workspace_references(value: &toml::Value, references: &mut BTreeSet<String>) {
    let Some(table) = value.as_table() else {
        return;
    };

    for (key, child) in table {
        if is_dependency_section(key) {
            if let Some(dependencies) = child.as_table() {
                for (name, specification) in dependencies {
                    if specification
                        .as_table()
                        .and_then(|table| table.get("workspace"))
                        .and_then(toml::Value::as_bool)
                        == Some(true)
                    {
                        references.insert(name.clone());
                    }
                }
            }
        }
        collect_workspace_references(child, references);
    }
}

/// Identifies Cargo dependency tables, including target-specific variants.
fn is_dependency_section(key: &str) -> bool {
    matches!(
        key,
        "dependencies" | "dev-dependencies" | "build-dependencies"
    )
}
