//! Backend endpoint-settings resolution (#221): config endpoint/region/
//! addressing values win over the AWS env chain, with env fallback when config
//! leaves a field unset. Credentials resolve lazily on the origin request path.

use std::collections::HashMap;

use verglas_backend::BackendSettings;
use verglas_core::config::Backend;

/// An env reader backed by a fixed map, so a test controls exactly which
/// `AWS_*` vars are "set".
fn env_reader(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
    let map: HashMap<String, String> = pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect();
    move |name: &str| map.get(name).cloned()
}

#[test]
fn config_endpoint_and_region_win_over_env() {
    // A configured endpoint/region beats the AWS env vars, so a configured
    // daemon reaches OCI without exporting anything.
    let backend = Backend {
        endpoint: Some("https://oci.example.com".to_owned()),
        region: Some("us-ashburn-1".to_owned()),
        ..Backend::default()
    };
    let read = env_reader(&[
        ("AWS_ENDPOINT", "https://should-be-ignored.example.com"),
        ("AWS_REGION", "us-east-1"),
    ]);
    let settings = BackendSettings::resolve(&backend, &read).expect("resolves");
    assert_eq!(settings.endpoint, "https://oci.example.com");
    assert_eq!(settings.region, "us-ashburn-1");
}

#[test]
fn env_fills_endpoint_and_region_when_config_unset() {
    // With no config override, the AWS env supplies endpoint/region — production
    // IAM/instance-role and existing env setups are unchanged.
    let backend = Backend::default();
    let read = env_reader(&[
        ("AWS_ENDPOINT", "https://env.example.com"),
        ("AWS_REGION", "eu-west-1"),
    ]);
    let settings = BackendSettings::resolve(&backend, &read).expect("resolves");
    assert_eq!(settings.endpoint, "https://env.example.com");
    assert_eq!(settings.region, "eu-west-1");
}

#[test]
fn region_defaults_to_us_east_1_and_endpoint_from_region() {
    // Nothing set anywhere: region defaults to us-east-1 and the endpoint is the
    // regional AWS endpoint, exactly as before.
    let backend = Backend::default();
    let read = env_reader(&[]);
    let settings = BackendSettings::resolve(&backend, &read).expect("resolves");
    assert_eq!(settings.region, "us-east-1");
    assert_eq!(settings.endpoint, "https://s3.us-east-1.amazonaws.com");
    assert!(!settings.virtual_hosted_style);
}

#[test]
fn settings_do_not_require_eager_credentials() {
    // Startup must not freeze or reject a role-derived identity: the refreshing
    // provider resolves it only when an origin request needs signing.
    let backend = Backend::default();
    let read = env_reader(&[]);
    let settings = BackendSettings::resolve(&backend, &read).expect("settings resolve");
    assert_eq!(settings.region, "us-east-1");
}
