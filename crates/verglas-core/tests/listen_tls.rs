//! Tests for the S3 listener's TLS termination and virtual-hosted addressing
//! configuration (issue #11). TLS cert/key paths and the addressing base domain
//! land here, with startup validation that names the exact field at fault.

use std::path::PathBuf;

use verglas_core::config::Config;

/// A unique writable scratch directory for `cache.dir` and PEM fixtures.
fn scratch_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("verglas-listen-{}-{tag}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create scratch dir");
    dir
}

/// Writes a placeholder PEM file (validation only checks the file exists and is
/// readable; the bytes are parsed later, when the listener actually binds).
fn write_pem(path: &std::path::Path) {
    std::fs::write(path, b"-----PLACEHOLDER-----\n").expect("write pem fixture");
}

#[test]
fn tls_paths_parse_and_validate_when_the_files_exist() {
    let dir = scratch_dir("tls-ok");
    let cert = dir.join("fullchain.pem");
    let key = dir.join("privkey.pem");
    write_pem(&cert);
    write_pem(&key);
    let toml = format!(
        "[backend]\nbucket = \"test-bucket\"\n[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\n\n[listen.tls]\ncert_path = \"{}\"\nkey_path = \"{}\"\n",
        dir.display(),
        cert.display(),
        key.display(),
    );
    let config = Config::from_toml_str(&toml).expect("tls config parses");
    let tls = config.listen.tls.as_ref().expect("tls table present");
    assert_eq!(tls.cert_path, cert);
    assert_eq!(tls.key_path, key);
    config
        .validate()
        .expect("tls config validates when cert and key exist");
}

#[test]
fn tls_missing_cert_is_rejected_naming_the_field() {
    let dir = scratch_dir("tls-missing-cert");
    let key = dir.join("privkey.pem");
    write_pem(&key);
    let toml = format!(
        "[backend]\nbucket = \"test-bucket\"\n[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\n\n[listen.tls]\ncert_path = \"{}\"\nkey_path = \"{}\"\n",
        dir.display(),
        dir.join("does-not-exist.pem").display(),
        key.display(),
    );
    let config = Config::from_toml_str(&toml).expect("parses");
    let err = config
        .validate()
        .expect_err("missing cert must fail validation");
    let message = err.to_string();
    assert!(
        message.contains("listen.tls.cert_path"),
        "error must name the field, got: {message}"
    );
}

#[test]
fn plain_http_is_the_dev_default() {
    // No [listen.tls] and no domain: the daemon serves plain HTTP path-style,
    // the local-dev default.
    let dir = scratch_dir("plain");
    let toml = format!(
        "[backend]\nbucket = \"test-bucket\"\n[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\n",
        dir.display()
    );
    let config = Config::from_toml_str(&toml).expect("parses");
    assert!(config.listen.tls.is_none(), "TLS is off by default");
    assert!(config.listen.domain.is_none(), "path-style is the default");
    config.validate().expect("the dev default validates");
}

#[test]
fn base_domain_parses_and_validates() {
    let dir = scratch_dir("domain-ok");
    let toml = format!(
        "[backend]\nbucket = \"test-bucket\"\n[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\n\n[listen]\ndomain = \"s3.verglas.example\"\n",
        dir.display(),
    );
    let config = Config::from_toml_str(&toml).expect("parses");
    assert_eq!(config.listen.domain.as_deref(), Some("s3.verglas.example"));
    config.validate().expect("a valid base domain validates");
}

#[test]
fn malformed_base_domain_is_rejected_naming_the_field() {
    let dir = scratch_dir("domain-bad");
    let toml = format!(
        "[backend]\nbucket = \"test-bucket\"\n[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\n\n[listen]\ndomain = \"http://s3.example/\"\n",
        dir.display(),
    );
    let config = Config::from_toml_str(&toml).expect("parses");
    let err = config
        .validate()
        .expect_err("a scheme-and-slash domain must fail validation");
    assert!(
        err.to_string().contains("listen.domain"),
        "error must name the field, got: {err}"
    );
}
