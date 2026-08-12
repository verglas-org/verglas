//! Installs release-embedded agent skills into supported coding-agent layouts.

use std::ffi::OsString;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use thiserror::Error;

use crate::cli::{BundledSkill, SkillCommand, SkillInstallArgs, SkillTarget};

/// One file embedded into the native CLI at compile time.
struct EmbeddedFile {
    /// Path relative to the installed skill directory.
    path: &'static str,
    /// Exact checked-in file contents.
    contents: &'static [u8],
}

/// One host-managed artifact installed without replacing its parent directory.
enum ManagedArtifact {
    /// A directory whose complete contents are owned by RIME.
    Directory {
        /// Final directory path.
        destination: PathBuf,
        /// Files written below the directory.
        files: &'static [EmbeddedFile],
    },
    /// A single host registration file owned by RIME.
    File {
        /// Final file path.
        destination: PathBuf,
        /// Exact file contents.
        contents: &'static [u8],
    },
}

/// Complete installation plan for one supported host.
struct HostInstallation {
    /// User-facing host label.
    target: &'static str,
    /// Artifacts required for native discovery.
    artifacts: Vec<ManagedArtifact>,
}

/// The complete RIME skill artifact shipped with this CLI release.
const RIME_FILES: &[EmbeddedFile] = &[
    EmbeddedFile {
        path: "SKILL.md",
        contents: include_bytes!("../../../../apps/os/packages/rime/skills/rime/SKILL.md"),
    },
    EmbeddedFile {
        path: "agents/openai.yaml",
        contents: include_bytes!(
            "../../../../apps/os/packages/rime/skills/rime/agents/openai.yaml"
        ),
    },
    EmbeddedFile {
        path: "references/algorithm.md",
        contents: include_bytes!(
            "../../../../apps/os/packages/rime/skills/rime/references/algorithm.md"
        ),
    },
    EmbeddedFile {
        path: "references/engineering-objective.md",
        contents: include_bytes!(
            "../../../../apps/os/packages/rime/skills/rime/references/engineering-objective.md"
        ),
    },
    EmbeddedFile {
        path: "references/graph-state.md",
        contents: include_bytes!(
            "../../../../apps/os/packages/rime/skills/rime/references/graph-state.md"
        ),
    },
    EmbeddedFile {
        path: "references/workspace-lifecycle.md",
        contents: include_bytes!(
            "../../../../apps/os/packages/rime/skills/rime/references/workspace-lifecycle.md"
        ),
    },
];

/// The complete Claude skills-directory plugin, including its native worker.
const CLAUDE_PLUGIN_FILES: &[EmbeddedFile] = &[
    EmbeddedFile {
        path: ".claude-plugin/plugin.json",
        contents: include_bytes!("../../../../apps/os/packages/rime/.claude-plugin/plugin.json"),
    },
    EmbeddedFile {
        path: "agents/rime-worker.md",
        contents: include_bytes!("../../../../apps/os/packages/rime/agents/rime-worker.md"),
    },
    EmbeddedFile {
        path: "skills/rime/SKILL.md",
        contents: include_bytes!("../../../../apps/os/packages/rime/skills/rime/SKILL.md"),
    },
    EmbeddedFile {
        path: "skills/rime/agents/openai.yaml",
        contents: include_bytes!(
            "../../../../apps/os/packages/rime/skills/rime/agents/openai.yaml"
        ),
    },
    EmbeddedFile {
        path: "skills/rime/references/algorithm.md",
        contents: include_bytes!(
            "../../../../apps/os/packages/rime/skills/rime/references/algorithm.md"
        ),
    },
    EmbeddedFile {
        path: "skills/rime/references/engineering-objective.md",
        contents: include_bytes!(
            "../../../../apps/os/packages/rime/skills/rime/references/engineering-objective.md"
        ),
    },
    EmbeddedFile {
        path: "skills/rime/references/graph-state.md",
        contents: include_bytes!(
            "../../../../apps/os/packages/rime/skills/rime/references/graph-state.md"
        ),
    },
    EmbeddedFile {
        path: "skills/rime/references/workspace-lifecycle.md",
        contents: include_bytes!(
            "../../../../apps/os/packages/rime/skills/rime/references/workspace-lifecycle.md"
        ),
    },
];

/// Failures that prevent a skill from being installed completely.
#[derive(Debug, Error)]
pub enum SkillInstallError {
    /// A required user configuration root is unavailable.
    #[error("cannot resolve {target} skill directory; set {variable}")]
    MissingHome {
        /// Agent whose installation root is unresolved.
        target: &'static str,
        /// Environment override accepted by the installer.
        variable: &'static str,
    },
    /// A filesystem operation failed for the managed skill directory.
    #[error("could not install RIME at {path}: {source}")]
    Io {
        /// Managed path involved in the failed transaction.
        path: PathBuf,
        /// Underlying filesystem failure.
        source: io::Error,
    },
}

/// Dispatches an agent skill distribution command without consulting credentials.
pub fn run(command: &SkillCommand) -> Result<(), SkillInstallError> {
    match command {
        SkillCommand::Install(args) => install(args),
    }
}

/// Installs the selected embedded skill into every requested agent layout.
fn install(args: &SkillInstallArgs) -> Result<(), SkillInstallError> {
    match args.skill {
        BundledSkill::Rime => {}
    }
    for installation in installations(args.target)? {
        for artifact in &installation.artifacts {
            install_artifact_atomically(artifact)?;
        }
        println!("installed RIME integration for {}", installation.target);
    }
    Ok(())
}

/// Resolves every host artifact before writing any installation.
fn installations(target: SkillTarget) -> Result<Vec<HostInstallation>, SkillInstallError> {
    let mut installations = Vec::new();
    if matches!(target, SkillTarget::Pi | SkillTarget::All) {
        let home = user_home("Pi", "HOME")?;
        let root = home.join(".pi/agent");
        installations.push(HostInstallation {
            target: "Pi",
            artifacts: vec![
                ManagedArtifact::Directory {
                    destination: root.join("skills/rime"),
                    files: RIME_FILES,
                },
                ManagedArtifact::File {
                    destination: root.join("extensions/rime.ts"),
                    contents: include_bytes!(
                        "../../../../apps/os/packages/rime/extensions/rime.ts"
                    ),
                },
                ManagedArtifact::File {
                    destination: root.join("agents/rime-worker.md"),
                    contents: include_bytes!(
                        "../../../../apps/os/packages/rime/host/pi/rime-worker.md"
                    ),
                },
            ],
        });
    }
    if matches!(target, SkillTarget::Codex | SkillTarget::All) {
        let root = configured_root("CODEX_HOME", "Codex", ".codex")?;
        installations.push(HostInstallation {
            target: "Codex",
            artifacts: vec![
                ManagedArtifact::Directory {
                    destination: root.join("skills/rime"),
                    files: RIME_FILES,
                },
                ManagedArtifact::File {
                    destination: root.join("agents/rime_worker.toml"),
                    contents: include_bytes!(
                        "../../../../apps/os/packages/rime/host/codex/rime_worker.toml"
                    ),
                },
            ],
        });
    }
    if matches!(target, SkillTarget::Claude | SkillTarget::All) {
        let root = configured_root("CLAUDE_CONFIG_DIR", "Claude", ".claude")?;
        installations.push(HostInstallation {
            target: "Claude",
            artifacts: vec![ManagedArtifact::Directory {
                destination: root.join("skills/rime"),
                files: CLAUDE_PLUGIN_FILES,
            }],
        });
    }
    Ok(installations)
}

/// Installs one managed file or directory through its own rename transaction.
fn install_artifact_atomically(artifact: &ManagedArtifact) -> Result<(), SkillInstallError> {
    match artifact {
        ManagedArtifact::Directory { destination, files } => install_atomically(destination, files),
        ManagedArtifact::File {
            destination,
            contents,
        } => install_file_atomically(destination, contents),
    }
}

/// Uses an explicit agent root or derives its conventional directory from the user home.
fn configured_root(
    environment: &'static str,
    target: &'static str,
    conventional: &'static str,
) -> Result<PathBuf, SkillInstallError> {
    match std::env::var_os(environment) {
        Some(root) => Ok(PathBuf::from(root)),
        None => Ok(user_home(target, environment)?.join(conventional)),
    }
}

/// Resolves the current user's home directory on Unix and Windows.
fn user_home(target: &'static str, variable: &'static str) -> Result<PathBuf, SkillInstallError> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .ok_or(SkillInstallError::MissingHome { target, variable })
}

/// Replaces only the managed skill directory through a staged rename transaction.
fn install_atomically(destination: &Path, files: &[EmbeddedFile]) -> Result<(), SkillInstallError> {
    let parent = destination.parent().ok_or_else(|| SkillInstallError::Io {
        path: destination.to_owned(),
        source: io::Error::new(
            io::ErrorKind::InvalidInput,
            "skill destination has no parent",
        ),
    })?;
    fs::create_dir_all(parent).map_err(|source| SkillInstallError::Io {
        path: destination.to_owned(),
        source,
    })?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let stage = sibling_path(
        destination,
        &format!("install-{}-{nonce}", std::process::id()),
    );
    let backup = sibling_path(
        destination,
        &format!("backup-{}-{nonce}", std::process::id()),
    );

    if let Err(source) = write_staged_skill(&stage, files) {
        let _ = fs::remove_dir_all(&stage);
        return Err(SkillInstallError::Io {
            path: destination.to_owned(),
            source,
        });
    }

    let had_destination = destination.exists();
    if had_destination && let Err(source) = fs::rename(destination, &backup) {
        let _ = fs::remove_dir_all(&stage);
        return Err(SkillInstallError::Io {
            path: destination.to_owned(),
            source,
        });
    }
    if let Err(source) = fs::rename(&stage, destination) {
        if had_destination {
            let _ = fs::rename(&backup, destination);
        }
        let _ = fs::remove_dir_all(&stage);
        return Err(SkillInstallError::Io {
            path: destination.to_owned(),
            source,
        });
    }
    if had_destination {
        fs::remove_dir_all(&backup).map_err(|source| SkillInstallError::Io {
            path: destination.to_owned(),
            source,
        })?;
    }
    Ok(())
}

/// Replaces one managed registration file while preserving sibling host configuration.
fn install_file_atomically(destination: &Path, contents: &[u8]) -> Result<(), SkillInstallError> {
    let parent = destination.parent().ok_or_else(|| SkillInstallError::Io {
        path: destination.to_owned(),
        source: io::Error::new(
            io::ErrorKind::InvalidInput,
            "file destination has no parent",
        ),
    })?;
    fs::create_dir_all(parent).map_err(|source| SkillInstallError::Io {
        path: destination.to_owned(),
        source,
    })?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let stage = sibling_path(
        destination,
        &format!("install-{}-{nonce}", std::process::id()),
    );
    let backup = sibling_path(
        destination,
        &format!("backup-{}-{nonce}", std::process::id()),
    );
    fs::write(&stage, contents).map_err(|source| SkillInstallError::Io {
        path: destination.to_owned(),
        source,
    })?;
    let had_destination = destination.exists();
    if had_destination && let Err(source) = fs::rename(destination, &backup) {
        let _ = fs::remove_file(&stage);
        return Err(SkillInstallError::Io {
            path: destination.to_owned(),
            source,
        });
    }
    if let Err(source) = fs::rename(&stage, destination) {
        if had_destination {
            let _ = fs::rename(&backup, destination);
        }
        let _ = fs::remove_file(&stage);
        return Err(SkillInstallError::Io {
            path: destination.to_owned(),
            source,
        });
    }
    if had_destination {
        fs::remove_file(&backup).map_err(|source| SkillInstallError::Io {
            path: destination.to_owned(),
            source,
        })?;
    }
    Ok(())
}

/// Builds a collision-resistant sibling path without accepting an external target.
fn sibling_path(destination: &Path, suffix: &str) -> PathBuf {
    let mut name = OsString::from(".");
    name.push(destination.file_name().unwrap_or_default());
    name.push(".");
    name.push(suffix);
    destination.with_file_name(name)
}

/// Writes every embedded artifact into an unpublished staging directory.
fn write_staged_skill(stage: &Path, files: &[EmbeddedFile]) -> io::Result<()> {
    fs::create_dir(stage)?;
    for file in files {
        let destination = stage.join(file.path);
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(destination, file.contents)?;
    }
    Ok(())
}
