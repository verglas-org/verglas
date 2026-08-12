//! Keeps self-hosted documentation snippets identical to the files readers run.

use std::fs;
use std::path::{Path, PathBuf};

/// Returns the workspace root for this integration test.
fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Extracts a named fenced code block from one documentation page.
fn named_fence(page: &str, name: &str) -> String {
    let opening = page
        .lines()
        .find(|line| line.ends_with(&format!(" {name}")))
        .unwrap_or_else(|| panic!("missing named fence for {name}"));
    let indent = opening.len() - opening.trim_start().len();
    let start = page
        .find(opening)
        .unwrap_or_else(|| panic!("missing opening fence for {name}"))
        + opening.len()
        + 1;
    let closing = format!("\n{}```", " ".repeat(indent));
    let remainder = &page[start..];
    let end = remainder
        .find(&closing)
        .unwrap_or_else(|| panic!("unterminated named fence for {name}"));
    let prefix = " ".repeat(indent);
    let body = remainder[..end]
        .lines()
        .map(|line| line.strip_prefix(&prefix).unwrap_or(line))
        .collect::<Vec<_>>()
        .join("\n");
    format!("{body}\n")
}

/// Confirms one documented file is a literal copy of its runnable source file.
fn assert_documented_file(page: &str, name: &str, source: &str) {
    let root = workspace_root();
    let page_text = fs::read_to_string(root.join(page))
        .unwrap_or_else(|error| panic!("failed to read {page}: {error}"));
    let source_text = fs::read_to_string(root.join(source))
        .unwrap_or_else(|error| panic!("failed to read {source}: {error}"));
    assert_eq!(
        named_fence(&page_text, name),
        source_text,
        "{page}'s {name} block drifted from {source}"
    );
}

#[test]
fn self_hosted_compose_is_complete_and_runnable() {
    let root = workspace_root();
    let compose = fs::read_to_string(root.join("docker-compose.yml"))
        .unwrap_or_else(|error| panic!("failed to read docker-compose.yml: {error}"));
    for required in [
        "verglas-server:",
        "verglas-container-runtime:",
        "verglas-lakekeeper:",
        "verglas-cache-node-0:",
        "VERGLAS_BACKEND_BUCKET:",
        "VERGLAS_BACKEND_ENDPOINT:",
        "VERGLAS_ACCESS_URI:",
        "VERGLAS_MANAGED_CATALOG_URI:",
        "VERGLAS_MANAGED_POSTGRES_SAFEKEEPERS:",
        "VERGLAS_S3_ACCESS_KEY_ID:",
        "VERGLAS_S3_SECRET_ACCESS_KEY:",
        "VERGLAS_SCHEDULER_URL:",
        "VERGLAS_CACHE_HOST_DIR:-verglas-cache",
        "nofile:",
        "soft: 8192",
        "hard: 8192",
        "/var/run/docker.sock:/var/run/docker.sock",
        "verglas-runtime-state:/var/lib/verglas-container-runtime",
        "name: verglas-runtime",
        "external: true",
        "verglas-cache:",
        "verglas-runtime-state:",
    ] {
        assert!(
            compose.contains(required),
            "documented Compose file is missing {required}"
        );
    }
    assert!(
        !compose.contains("config.toml"),
        "the server must be configured entirely by Compose"
    );
    assert!(
        !compose.contains("rill:"),
        "bootstrap Compose must not directly own rill:"
    );
    assert!(
        !compose.contains("pg-ring:"),
        "cache nodes must not consume a redundant fourth Coolify network"
    );
    for peer in [
        "cache-0=verglas-cache-node-0:8336",
        "cache-1=verglas-cache-node-1:8336",
        "cache-2=verglas-cache-node-2:8336",
    ] {
        assert!(compose.contains(peer), "cache ring is missing peer {peer}");
    }
}

#[test]
fn worker_files_are_shown_in_full_before_use() {
    let examples = "examples/workers/market-data-ingest";
    for (page, name, file) in [
        ("docs/workers/overview.mdx", "ingest.py", "ingest.py"),
        ("docs/workers/overview.mdx", "worker.toml", "worker.toml"),
        ("docs/workers/cloudevents.mdx", "quote.json", "quote.json"),
        (
            "docs/workers/cloudevents.mdx",
            "requirements.txt",
            "requirements.txt",
        ),
        (
            "docs/workers/cloudevents.mdx",
            "rabbitmq_bridge.py",
            "rabbitmq_bridge.py",
        ),
        (
            "docs/workers/cloudevents.mdx",
            "Dockerfile.rabbitmq-bridge",
            "Dockerfile.rabbitmq-bridge",
        ),
    ] {
        assert_documented_file(page, name, &format!("{examples}/{file}"));
    }
}
