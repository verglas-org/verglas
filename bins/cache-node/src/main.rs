//! `verglas-cache-node` — the standalone Verglas cache serving server.
//!
//! One job: run the cache. It loads the TOML subset
//! (`[listen] [log] [cache] [backend] [auth] [catalog]`), verifies SigV4 against
//! the `[auth]` credentials file, serves the S3 endpoint from the local foyer
//! cache tiers, and reads through / writes through to the origin bucket. It
//! owns a cached catalog gateway and watcher for local query workers, but no
//! query engine, jobs framework, or platform executor.
//!
//! The config schema and its validation are `verglas-core`'s, reused verbatim,
//! so this binary accepts exactly the config `verglas-cache-node` does
//! (`--config <path>`, same flag).

use verglas_core::config::Config;

mod admin;
mod blockdev;
mod consensus;
mod logging;
mod nbd;
mod page_cache;
mod ring;
mod safekeeper;
mod serve;
mod tables_api;
mod tls;

/// The server version, from the package manifest. Reported by `/admin/version`
/// and stamped on operator log lines.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Parses `--config <path>` from the command line, loading and validating the
/// file. The cache node always requires a config (unlike verglas-cache-node's config-less
/// admin-only smoke mode): with no origin bucket and no auth there is nothing to
/// serve. Exits non-zero with the loader's actionable message on any failure.
fn load_config_from_args() -> Config {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg != "--config" {
            continue;
        }
        let Some(path) = args.next() else {
            eprintln!("verglas-cache-node: --config requires a path to a TOML config file");
            std::process::exit(1);
        };
        match Config::load(std::path::Path::new(&path)) {
            Ok(config) => return config,
            Err(error) => {
                eprintln!("verglas-cache-node: {error}");
                std::process::exit(1);
            }
        }
    }
    eprintln!("verglas-cache-node: --config <path> is required");
    std::process::exit(1);
}

/// Generates a dev access keypair for a cache node started without `[auth]`. The
/// caller prints it once so an engine can still be pointed at this node. Copied
/// from `bins/cache-node/src/main.rs::generate_auth`.
fn generate_auth() -> (String, String) {
    use std::hash::{BuildHasher, RandomState};
    // RandomState is seeded from OS entropy; good enough for dev keys.
    let r = || RandomState::new().hash_one(0u64);
    (
        format!("VG{:016X}", r()),
        format!("{:016x}{:016x}", r(), r()),
    )
}

/// Resolves the static keypair the S3 endpoint accepts. When `[auth]` names a
/// credentials file the keypair is read from it (AWS-INI, mode 0600) — the
/// secret never lives in `config.toml`. With no `[auth]` an ephemeral pair is
/// generated and printed once. Uses the shared AWS-INI parser so endpoint and
/// backend credential files follow the same strict format.
fn resolve_auth(config: &Config) -> Result<(String, String), String> {
    match &config.auth {
        Some(auth) => {
            let profile = auth
                .credentials_profile
                .as_deref()
                .filter(|s| !s.is_empty())
                .unwrap_or("default");
            let text = std::fs::read_to_string(&auth.credentials_file).map_err(|e| {
                format!(
                    "cannot read auth.credentials_file `{}`: {e}",
                    auth.credentials_file
                )
            })?;
            verglas_backend::read_aws_keypair(&text, profile).ok_or_else(|| {
                format!(
                    "auth.credentials_file `{}` has no complete `[{profile}]` profile (need aws_access_key_id and aws_secret_access_key)",
                    auth.credentials_file
                )
            })
        }
        None => {
            let (access_key_id, secret_access_key) = generate_auth();
            println!(
                "verglas-cache-node generated access keys: access_key_id={access_key_id} secret_access_key={secret_access_key}"
            );
            Ok((access_key_id, secret_access_key))
        }
    }
}

#[tokio::main]
async fn main() {
    if std::env::args().any(|arg| arg == "--version" || arg == "-V") {
        println!("verglas-cache-node {VERSION}");
        return;
    }

    // Pin the process rustls CryptoProvider so an https origin (R2/S3) resolves
    // TLS through a chosen provider rather than panicking when the dep graph
    // carries more than one. Harmless when ring is the only provider present.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let config = load_config_from_args();

    // Install the subscriber from `[log]` before any serving work, so backend and
    // fill logs are visible from the first line. `RUST_LOG` still overrides the
    // level; `VERGLAS_LOG_FORMAT=pretty` forces human output.
    logging::install(config.log.format, &config.log.level);
    println!(
        "verglas-cache-node {VERSION} config ok: {}",
        config.summary()
    );

    let credentials = match resolve_auth(&config) {
        Ok(credentials) => credentials,
        Err(e) => {
            eprintln!("verglas-cache-node: {e}");
            std::process::exit(1);
        }
    };

    if let Err(error) = serve::run(&config, credentials).await {
        eprintln!("verglas-cache-node failed: {error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use verglas_core::config::LogFormat;

    /// Test helper on `Config` — validation reads the real filesystem for the
    /// cache dir, so tests must point it at a directory that exists.
    trait ConfigTestExt {
        /// Parses and validates `toml`, asserting the cache dir is `cache_dir`.
        fn load_from_str_for_test(toml: &str, cache_dir: &std::path::Path) -> Config;
    }

    impl ConfigTestExt for Config {
        fn load_from_str_for_test(toml: &str, cache_dir: &std::path::Path) -> Config {
            let config = Config::from_toml_str(toml).expect("valid TOML for the schema");
            config
                .validate()
                .expect("config passes semantic validation");
            assert_eq!(config.cache.dir, cache_dir);
            config
        }
    }

    /// Renders a representative cache-node config — every section, in the same shape,
    /// with a backend credentials file and an `[auth]` credentials file — and
    /// asserts `verglas-core`'s loader accepts it and resolves each field. This
    /// is the container boot-script contract.
    #[test]
    fn loads_the_exact_config_the_cache_image_renders() {
        let dir = tempfile::tempdir().expect("temp cache dir");
        let cache_dir = dir.path().join("cache");
        std::fs::create_dir_all(&cache_dir).expect("cache dir");
        let backend_creds = dir.path().join("backend-credentials");
        std::fs::write(
            &backend_creds,
            "[default]\naws_access_key_id = AKIABACKEND\naws_secret_access_key = backendsecret\n",
        )
        .expect("backend creds");
        let auth_creds = dir.path().join("auth-credentials");
        std::fs::write(
            &auth_creds,
            "[default]\naws_access_key_id = VGENGINEKEY\naws_secret_access_key = enginesecret\n",
        )
        .expect("auth creds");

        // The literal template from cache-boot.sh, with the rendered values
        // filled in: [listen] [log] [cache] [backend] (with credentials_file)
        // and [auth] (with credentials_file). Small capacity so the
        // capacity-vs-filesystem check passes on any CI disk.
        let toml = format!(
            "[listen]\n\
             s3_port = 8333\n\
             admin_port = 8334\n\n\
             [log]\n\
             format = \"json\"\n\
             level = \"info\"\n\n\
             [cache]\n\
             dir = \"{cache}\"\n\
             capacity_bytes = \"64MB\"\n\
             dram_bytes = \"80MB\"\n\n\
             [backend]\n\
             bucket = \"verglas-cache-test\"\n\
             endpoint = \"https://accountid.r2.cloudflarestorage.com\"\n\
             region = \"us-east-1\"\n\
             credentials_file = \"{backend_creds}\"\n\n\
             [auth]\n\
             credentials_file = \"{auth_creds}\"\n",
            cache = cache_dir.display(),
            backend_creds = backend_creds.display(),
            auth_creds = auth_creds.display(),
        );

        let config = Config::load_from_str_for_test(&toml, &cache_dir);
        assert_eq!(config.listen.s3_port, 8333);
        assert_eq!(config.listen.admin_port, 8334);
        assert_eq!(config.log.format, LogFormat::Json);
        assert_eq!(config.cache.capacity_bytes.0, 64 * 1024 * 1024);
        assert_eq!(config.cache.dram_bytes.0, 80 * 1024 * 1024);
        assert_eq!(config.backend.bucket.as_deref(), Some("verglas-cache-test"));
        assert_eq!(
            config.backend.endpoint.as_deref(),
            Some("https://accountid.r2.cloudflarestorage.com")
        );
        assert_eq!(
            config.backend.credentials_file.as_deref(),
            Some(backend_creds.to_str().expect("utf-8 path"))
        );
        let auth = config.auth.as_ref().expect("[auth] present");
        assert_eq!(
            auth.credentials_file,
            auth_creds.to_str().expect("utf-8 path")
        );

        // The `[auth]` credentials file resolves the SigV4 keypair the S3
        // endpoint accepts — the same resolution `main` runs at startup.
        let (key, secret) = resolve_auth(&config).expect("auth resolves");
        assert_eq!(key, "VGENGINEKEY");
        assert_eq!(secret, "enginesecret");
    }

    /// With no `[auth]`, an ephemeral keypair is generated (matching verglas-cache-node's
    /// no-config-auth behaviour) rather than failing startup.
    #[test]
    fn generates_a_keypair_when_auth_is_absent() {
        let (key, secret) = generate_auth();
        assert!(key.starts_with("VG"));
        assert!(!secret.is_empty());
    }
}
