//! Integration tests for the M1 config: typos
//! and bad values produce errors naming the field, and defaults apply.

use std::path::PathBuf;

use verglas_core::config::{ByteSize, Config};

/// Creates a unique writable scratch directory to stand in for `cache.dir`.
/// Lives under the OS temp dir; leaked on purpose (tiny, OS-cleaned).
fn scratch_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("verglas-config-{}-{tag}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create scratch dir");
    dir
}

/// A minimal valid document: a real cache dir and the required `backend.bucket`
/// (the server serves exactly one bucket and refuses to start without it).
/// Pins a tiny `capacity_bytes`: `validate()` gates the budget against the
/// real free space backing `cache.dir`, and the multi-GB default would fail on
/// a nearly-full CI runner. Tests that need a different budget replace the
/// `capacity_bytes = "64MB"` line.
fn valid_toml(tag: &str) -> String {
    // `[backend]` first so appended `[cache]` fields in tests still land under
    // `[cache]` (the trailing table); the required bucket lives up top.
    format!(
        "[backend]\nbucket = \"test-bucket\"\n[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\n",
        scratch_dir(tag).display()
    )
}

#[test]
fn minimal_config_gets_defaults_and_validates() {
    let config = Config::from_toml_str(&valid_toml("minimal")).expect("parses");
    config.validate().expect("valid config validates");
    assert_eq!(config.listen.s3_port, 8333);
    assert_eq!(config.cache.dram_bytes, ByteSize(1024 * 1024 * 1024));
    assert_eq!(config.cache.data_block_bytes, ByteSize(2 * 1024 * 1024));
    assert!(config.auth.is_none());
    // The summary names the served bucket.
    assert!(config.summary().contains("backend=test-bucket"));
}

#[test]
fn data_block_bytes_accepts_aligned_sizes_and_rejects_invalid_geometry() {
    let tuned = format!(
        "{}data_block_bytes = \"4MB\"\n",
        valid_toml("block-geometry-tuned")
    );
    let config = Config::from_toml_str(&tuned).expect("parses");
    assert_eq!(config.cache.data_block_bytes, ByteSize(4 * 1024 * 1024));
    config.validate().expect("4 MiB geometry validates");

    for bad in ["0", "512KB", "3MB", "16MB"] {
        let toml = format!(
            "{}data_block_bytes = \"{bad}\"\n",
            valid_toml("block-geometry-invalid")
        );
        let config = Config::from_toml_str(&toml).expect("parses");
        let err = config.validate().expect_err("invalid geometry rejected");
        assert!(
            err.to_string().contains("cache.data_block_bytes"),
            "error names the field: {err}"
        );
    }
}

#[test]
fn auth_names_a_credentials_file_and_carries_no_inline_secret() {
    // `[auth]` is file-based (#221): it names an AWS-format credentials file the
    // server reads the endpoint keypair from, with an optional profile. The
    // keypair never lives inline in the config.
    let toml = format!(
        "{}[auth]\ncredentials_file = \"/home/op/.verglas/credentials/endpoint\"\ncredentials_profile = \"default\"\n",
        valid_toml("auth-file"),
    );
    let config = Config::from_toml_str(&toml).expect("parses");
    config.validate().expect("validates");
    // With auth configured the summary reports it configured, not generated.
    assert!(config.summary().contains("auth=configured"));
    let auth = config.auth.as_ref().expect("auth present");
    assert_eq!(
        auth.credentials_file,
        "/home/op/.verglas/credentials/endpoint"
    );
    assert_eq!(auth.credentials_profile.as_deref(), Some("default"));
}

#[test]
fn auth_credentials_profile_defaults_to_none() {
    // The profile is optional; absent means the server reads the default profile.
    let toml = format!(
        "{}[auth]\ncredentials_file = \"/home/op/.verglas/credentials/endpoint\"\n",
        valid_toml("auth-noprofile"),
    );
    let config = Config::from_toml_str(&toml).expect("parses");
    let auth = config.auth.expect("auth present");
    assert!(auth.credentials_profile.is_none());
}

#[test]
fn meta_fraction_defaults_to_five_percent_and_validates() {
    // Absent field: the metadata store gets the documented 5% of the cache.
    let config = Config::from_toml_str(&valid_toml("meta-frac")).expect("parses");
    assert_eq!(config.cache.meta_fraction, 0.05);
    config.validate().expect("default meta_fraction validates");
}

#[test]
fn mutable_mapping_ttl_defaults_to_five_seconds() {
    // Absent field: mutable mappings (#14) are trusted for 5s before a
    // conditional revalidation. Immutable Iceberg mappings ignore it.
    let config = Config::from_toml_str(&valid_toml("ttl-default")).expect("parses");
    assert_eq!(config.cache.mutable_mapping_ttl_secs, 5);
    config.validate().expect("default TTL validates");
}

#[test]
fn mutable_mapping_ttl_can_be_tuned_including_zero() {
    // 0 is a legitimate setting: revalidate on every read of a mutable key.
    let toml = format!("{}mutable_mapping_ttl_secs = 0\n", valid_toml("ttl-zero"));
    let config = Config::from_toml_str(&toml).expect("parses");
    assert_eq!(config.cache.mutable_mapping_ttl_secs, 0);
    config.validate().expect("zero TTL validates");
}

#[test]
fn meta_fraction_can_be_tuned() {
    let toml = format!("{}meta_fraction = 0.1\n", valid_toml("meta-frac-tuned"));
    let config = Config::from_toml_str(&toml).expect("parses");
    assert_eq!(config.cache.meta_fraction, 0.1);
    config.validate().expect("tuned meta_fraction validates");
}

#[test]
fn meta_fraction_rejects_out_of_range() {
    // The fraction is a share of the cache: (0,1) exclusive on both ends — a
    // zero store pins nothing, a whole-cache store starves the data tier.
    for bad in ["0.0", "1.0", "1.5", "-0.1"] {
        let toml = format!("{}meta_fraction = {bad}\n", valid_toml("meta-frac-bad"));
        let config = Config::from_toml_str(&toml).expect("parses");
        let err = config
            .validate()
            .expect_err("out-of-range meta_fraction rejected");
        assert!(
            err.to_string().contains("cache.meta_fraction"),
            "error names the field: {err}"
        );
    }
}

#[test]
fn admission_defaults_on_and_admits_on_second_sight() {
    // Absent [cache.admission] table: scan-resistant admission is on by default
    // with the "admit on the second sighting" doorkeeper threshold.
    let config = Config::from_toml_str(&valid_toml("admission-default")).expect("parses");
    assert!(config.cache.admission.enabled);
    assert_eq!(config.cache.admission.frequency_threshold, 2);
    // Resident-biased probabilistic admission (#164) is off by default: a
    // probability of 1.0 admits every candidate that clears the frequency gate,
    // exactly the pre-#164 doorkeeper behavior.
    assert_eq!(config.cache.admission.churn_admit_probability, 1.0);
    config.validate().expect("default admission validates");
}

#[test]
fn admission_accepts_churn_admit_probability() {
    // #164: under sustained cache pressure a cyclic scan is thinned to a
    // fraction of candidates so a stable resident subset survives; the fraction
    // is configurable in (0, 1].
    let toml = format!(
        "{}[cache.admission]\nchurn_admit_probability = 0.1\n",
        valid_toml("admission-churn"),
    );
    let config = Config::from_toml_str(&toml).expect("parses");
    assert_eq!(config.cache.admission.churn_admit_probability, 0.1);
    config.validate().expect("churn probability validates");
}

#[test]
fn admission_rejects_out_of_range_churn_probability() {
    for bad in ["0.0", "-0.5", "1.5"] {
        let toml = format!(
            "{}[cache.admission]\nchurn_admit_probability = {bad}\n",
            valid_toml("admission-churn-bad"),
        );
        let config = Config::from_toml_str(&toml).expect("parses");
        let err = config
            .validate()
            .expect_err("out-of-range churn probability must fail");
        assert!(
            err.to_string()
                .contains("cache.admission.churn_admit_probability"),
            "should name the field, got: {err}"
        );
    }
}

#[test]
fn admission_can_be_disabled_and_tuned() {
    let toml = format!(
        "{}[cache.admission]\nenabled = false\nfrequency_threshold = 5\n",
        valid_toml("admission-tuned"),
    );
    let config = Config::from_toml_str(&toml).expect("parses");
    assert!(!config.cache.admission.enabled);
    assert_eq!(config.cache.admission.frequency_threshold, 5);
    config.validate().expect("tuned admission validates");
}

#[test]
fn admission_rejects_zero_threshold() {
    let toml = format!(
        "{}[cache.admission]\nfrequency_threshold = 0\n",
        valid_toml("admission-zero"),
    );
    let config = Config::from_toml_str(&toml).expect("parses");
    let err = config.validate().expect_err("zero threshold must fail");
    assert!(
        err.to_string()
            .contains("cache.admission.frequency_threshold"),
        "should name the field, got: {err}"
    );
}

#[test]
fn warming_defaults_are_on_and_conservative() {
    // Absent [cache.warming] table: warming on, 64 in flight, a 64 KiB footer
    // window, and a 256 MiB/s byte ceiling.
    let config = Config::from_toml_str(&valid_toml("warming-default")).expect("parses");
    assert!(config.cache.warming.enabled);
    assert_eq!(config.cache.warming.concurrency, 64);
    assert_eq!(config.cache.warming.footer_read_bytes.0, 64 * 1024);
    assert_eq!(
        config.cache.warming.byte_budget_bytes_per_sec.0,
        256 * 1024 * 1024
    );
    config.validate().expect("default warming validates");
}

#[test]
fn warming_can_be_disabled_and_tuned() {
    let toml = format!(
        "{}[cache.warming]\nenabled = false\nconcurrency = 8\nfooter_read_bytes = \"32KB\"\nbyte_budget_bytes_per_sec = \"0\"\n",
        valid_toml("warming-tuned"),
    );
    let config = Config::from_toml_str(&toml).expect("parses");
    assert!(!config.cache.warming.enabled);
    assert_eq!(config.cache.warming.concurrency, 8);
    assert_eq!(config.cache.warming.footer_read_bytes.0, 32 * 1024);
    assert_eq!(config.cache.warming.byte_budget_bytes_per_sec.0, 0);
    config.validate().expect("tuned warming validates");
}

#[test]
fn warming_rejects_zero_concurrency() {
    let toml = format!(
        "{}[cache.warming]\nconcurrency = 0\n",
        valid_toml("warming-zero"),
    );
    let config = Config::from_toml_str(&toml).expect("parses");
    let err = config.validate().expect_err("zero concurrency must fail");
    assert!(
        err.to_string().contains("cache.warming.concurrency"),
        "should name the field, got: {err}"
    );
}

#[test]
fn prefetch_defaults_are_on_and_conservative() {
    // Absent [cache.prefetch] table: prefetch on with the documented defaults.
    let config = Config::from_toml_str(&valid_toml("prefetch-default")).expect("parses");
    assert!(config.cache.prefetch.enabled);
    assert_eq!(config.cache.prefetch.concurrency, 32);
    assert_eq!(config.cache.prefetch.footer_read_bytes.0, 64 * 1024);
    assert_eq!(config.cache.prefetch.organic_yield_k, 32);
    assert_eq!(config.cache.prefetch.heat_epoch_secs, 300);
    assert_eq!(config.cache.prefetch.heat_table_cap, 64 * 1024);
    config.validate().expect("default prefetch validates");
}

#[test]
fn prefetch_can_be_disabled_and_tuned() {
    let toml = format!(
        "{}[cache.prefetch]\nenabled = false\nconcurrency = 4\nfooter_read_bytes = \"16KB\"\norganic_yield_k = 8\nmax_queue = 256\nheat_epoch_secs = 60\nheat_table_cap = 1024\nheat_channel_capacity = 512\n",
        valid_toml("prefetch-tuned"),
    );
    let config = Config::from_toml_str(&toml).expect("parses");
    assert!(!config.cache.prefetch.enabled);
    assert_eq!(config.cache.prefetch.concurrency, 4);
    assert_eq!(config.cache.prefetch.organic_yield_k, 8);
    assert_eq!(config.cache.prefetch.heat_epoch_secs, 60);
    config.validate().expect("tuned prefetch validates");
}

#[test]
fn prefetch_rejects_zero_concurrency() {
    let toml = format!(
        "{}[cache.prefetch]\nconcurrency = 0\n",
        valid_toml("prefetch-zero"),
    );
    let config = Config::from_toml_str(&toml).expect("parses");
    let err = config.validate().expect_err("zero concurrency must fail");
    assert!(
        err.to_string().contains("cache.prefetch.concurrency"),
        "should name the field, got: {err}"
    );
}

#[test]
fn typoed_field_error_names_it() {
    let err = Config::from_toml_str("[backend]\nmax_concurrrent_requests = 8\n")
        .expect_err("unknown field must fail");
    assert!(
        err.to_string().contains("max_concurrrent_requests"),
        "should name the typo, got: {err}"
    );
}

#[test]
fn backend_bucket_is_required_and_gates_validation() {
    // The server serves a configured set of buckets (#235). The common single
    // case names one `bucket`; a config with a bucket set validates.
    let with_bucket = Config::from_toml_str(&format!(
        "[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\n[backend]\nbucket = \"my-lake\"\n",
        scratch_dir("with-bucket").display()
    ))
    .expect("parses");
    with_bucket
        .validate()
        .expect("a config with a bucket validates");
    assert_eq!(with_bucket.backend.bucket.as_deref(), Some("my-lake"));
}

#[test]
fn catalog_archive_has_its_own_explicit_target_and_reserved_prefix() {
    let config = Config::from_toml_str(&format!(
        "[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\n[backend]\nbucket = \"catalog-archive\"\n[catalog_archive]\nbucket = \"catalog-archive\"\n",
        scratch_dir("catalog-archive-prefix").display()
    ))
    .expect("catalog archive config parses");
    config.validate().expect("catalog archive config validates");
    assert_eq!(
        config.catalog_archive.expect("catalog archive").prefix,
        "_verglas/catalog"
    );
}

#[test]
fn catalog_archive_requires_an_explicitly_served_bucket() {
    let config = Config::from_toml_str(&format!(
        "[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\n[backend]\nbucket = \"table-data\"\n[catalog_archive]\nbucket = \"unserved-archive\"\n",
        scratch_dir("catalog-archive-unserved").display()
    ))
    .expect("catalog archive config parses");
    let error = config
        .validate()
        .expect_err("an archive bucket outside the authorized set must fail");
    assert!(error.to_string().contains("catalog_archive.bucket"));
}

#[test]
fn global_wal_archive_config_is_rejected() {
    let error = Config::from_toml_str(&format!(
        "[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\n[backend]\nbucket = \"tenant-bucket\"\n[wal_archive]\nbucket = \"shared-wal\"\n",
        scratch_dir("global-wal-archive").display()
    ))
    .expect_err("a process-global WAL archive would defeat per-database isolation");
    assert!(error.to_string().contains("wal_archive"));
}

#[test]
fn backend_bucket_globs_alone_validate() {
    // A config that names only `bucket_globs` (no single `bucket`) validates:
    // the server serves any bucket matching a glob (#235, the S3 Tables case).
    let config = Config::from_toml_str(&format!(
        "[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\n[backend]\nbucket_globs = [\"*--table-s3\"]\n",
        scratch_dir("globs-only").display()
    ))
    .expect("parses");
    config
        .validate()
        .expect("a config with only bucket_globs validates");
    assert!(config.backend.bucket.is_none());
    assert_eq!(config.backend.bucket_globs, vec!["*--table-s3".to_owned()]);
    // The set gate matches a glob and rejects a non-match.
    assert!(config.backend.serves_bucket("abc--table-s3"));
    assert!(!config.backend.serves_bucket("other-bucket"));
}

#[test]
fn backend_bucket_and_globs_together_validate() {
    // Both a single bucket and globs may be set; the served set is their union.
    let config = Config::from_toml_str(&format!(
        "[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\n[backend]\nbucket = \"my-lake\"\nbucket_globs = [\"*--table-s3\"]\n",
        scratch_dir("bucket-and-globs").display()
    ))
    .expect("parses");
    config.validate().expect("bucket + globs validates");
    assert!(config.backend.serves_bucket("my-lake"));
    assert!(config.backend.serves_bucket("abc--table-s3"));
    assert!(!config.backend.serves_bucket("nope"));
}

#[test]
fn missing_backend_bucket_fails_validation() {
    // A blank `[backend]` (or no table at all) PARSES — both bucket fields are
    // optional in the schema so a scaffold can leave them commented — but does
    // NOT validate: the server needs at least one bucket or glob to serve. The
    // error names the fields.
    let toml = format!(
        "[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\n",
        scratch_dir("nobucket").display()
    );
    let config = Config::from_toml_str(&toml).expect("parses without a bucket");
    assert_eq!(config.backend.max_concurrent_requests, 64);
    let err = config
        .validate()
        .expect_err("must fail validation without a bucket or glob");
    assert!(
        err.to_string().contains("backend.bucket"),
        "error must name the field, got: {err}"
    );
}

#[test]
fn empty_backend_bucket_glob_fails_validation() {
    // An empty glob string would match nothing meaningfully and is a config
    // mistake; validation rejects it naming the field.
    let config = Config::from_toml_str(&format!(
        "[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\n[backend]\nbucket_globs = [\"\"]\n",
        scratch_dir("emptyglob").display()
    ))
    .expect("parses");
    let err = config
        .validate()
        .expect_err("an empty glob must fail validation");
    assert!(
        err.to_string().contains("backend.bucket_globs"),
        "error must name the field, got: {err}"
    );
}

#[test]
fn backend_provider_defaults_to_s3() {
    // With no `provider` set the backend is S3 (AWS/OCI/MinIO), the common case.
    use verglas_core::config::BackendProvider;
    let config = Config::from_toml_str(&valid_toml("provdefault")).expect("parses");
    assert_eq!(config.backend.provider, BackendProvider::S3);
    config.validate().expect("default provider validates");
}

#[test]
fn backend_provider_azure_parses_and_validates() {
    // An azure backend parses and validates: the container is the `bucket`, and
    // the endpoint http gate still applies. No credentials are needed at validate.
    use verglas_core::config::BackendProvider;
    let toml = format!(
        "[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\n[backend]\nprovider = \"azure\"\nbucket = \"my-container\"\n",
        scratch_dir("azure").display()
    );
    let config = Config::from_toml_str(&toml).expect("azure config parses");
    assert_eq!(config.backend.provider, BackendProvider::Azure);
    config.validate().expect("azure config validates");
}

#[test]
fn backend_provider_gcp_parses_and_validates() {
    // A gcp backend parses and validates the same way; the bucket is the GCS
    // bucket name.
    use verglas_core::config::BackendProvider;
    let toml = format!(
        "[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\n[backend]\nprovider = \"gcp\"\nbucket = \"my-gcs-bucket\"\n",
        scratch_dir("gcp").display()
    );
    let config = Config::from_toml_str(&toml).expect("gcp config parses");
    assert_eq!(config.backend.provider, BackendProvider::Gcp);
    config.validate().expect("gcp config validates");
}

#[test]
fn backend_provider_rejects_unknown_value() {
    // An unknown provider is a parse error, not a silent default.
    let toml = format!(
        "[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\n[backend]\nprovider = \"dropbox\"\nbucket = \"b\"\n",
        scratch_dir("badprov").display()
    );
    assert!(
        Config::from_toml_str(&toml).is_err(),
        "an unknown provider must not parse"
    );
}

#[test]
fn max_concurrent_requests_defaults_and_rejects_zero() {
    // Absent field: the documented default applies.
    let config = Config::from_toml_str(&valid_toml("concdefault")).expect("parses");
    assert_eq!(config.backend.max_concurrent_requests, 64);

    // Explicit value survives round-trip.
    let cache = format!(
        "[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\n",
        scratch_dir("concset").display()
    );
    let toml_text =
        format!("{cache}\n[backend]\nbucket = \"test-bucket\"\nmax_concurrent_requests = 8\n");
    let config = Config::from_toml_str(&toml_text).expect("parses");
    assert_eq!(config.backend.max_concurrent_requests, 8);
    config.validate().expect("8 is valid");

    // Zero is meaningless (would deadlock every fill) and is rejected by name.
    let cache = format!(
        "[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\n",
        scratch_dir("conczero").display()
    );
    let toml_text = format!("{cache}\n[backend]\nmax_concurrent_requests = 0\n");
    let err = Config::from_toml_str(&toml_text)
        .expect("parses")
        .validate()
        .expect_err("zero must fail");
    assert!(
        err.to_string().contains("backend.max_concurrent_requests"),
        "should name the field, got: {err}"
    );
}

#[test]
fn backend_retry_and_breaker_default_when_absent() {
    // #20: retry/backoff and circuit-breaker policy land under `[backend]`;
    // omitting them takes the documented defaults.
    let config = Config::from_toml_str(&valid_toml("resil-defaults")).expect("parses");
    config.validate().expect("defaults validate");
    let retry = &config.backend.retry;
    assert_eq!(retry.max_retries, 3);
    assert_eq!(retry.initial_backoff_ms, 100);
    assert_eq!(retry.max_backoff_ms, 3_000);
    assert_eq!(retry.budget_ms, 10_000);
    let breaker = &config.backend.breaker;
    assert_eq!(breaker.failure_rate, 0.5);
    assert_eq!(breaker.min_samples, 20);
    assert_eq!(breaker.cooldown_ms, 5_000);
    assert_eq!(breaker.half_open_max_probes, 3);
}

#[test]
fn backend_retry_and_breaker_round_trip_explicit_values() {
    let cache = format!(
        "[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\n",
        scratch_dir("resil-set").display()
    );
    let toml_text = format!(
        "{cache}\n[backend]\nbucket = \"test-bucket\"\n\
         [backend.retry]\nmax_retries = 5\ninitial_backoff_ms = 50\n\
         max_backoff_ms = 1000\nbudget_ms = 4000\n\
         [backend.breaker]\nfailure_rate = 0.25\nmin_samples = 10\n\
         cooldown_ms = 2000\nhalf_open_max_probes = 2\n"
    );
    let config = Config::from_toml_str(&toml_text).expect("parses");
    config.validate().expect("valid values validate");
    assert_eq!(config.backend.retry.max_retries, 5);
    assert_eq!(config.backend.retry.initial_backoff_ms, 50);
    assert_eq!(config.backend.retry.max_backoff_ms, 1000);
    assert_eq!(config.backend.retry.budget_ms, 4000);
    assert_eq!(config.backend.breaker.failure_rate, 0.25);
    assert_eq!(config.backend.breaker.min_samples, 10);
    assert_eq!(config.backend.breaker.cooldown_ms, 2000);
    assert_eq!(config.backend.breaker.half_open_max_probes, 2);
}

#[test]
fn backend_retry_backoff_inversion_error_names_field() {
    let cache = format!(
        "[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\n",
        scratch_dir("resil-inv").display()
    );
    let toml_text =
        format!("{cache}\n[backend.retry]\ninitial_backoff_ms = 5000\nmax_backoff_ms = 1000\n");
    let err = Config::from_toml_str(&toml_text)
        .expect("parses")
        .validate()
        .expect_err("max < initial must fail");
    assert!(
        err.to_string().contains("backend.retry.max_backoff_ms"),
        "should name the field, got: {err}"
    );
}

#[test]
fn backend_retry_zero_initial_backoff_error_names_field() {
    let cache = format!(
        "[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\n",
        scratch_dir("resil-zero").display()
    );
    let toml_text = format!("{cache}\n[backend.retry]\ninitial_backoff_ms = 0\n");
    let err = Config::from_toml_str(&toml_text)
        .expect("parses")
        .validate()
        .expect_err("zero backoff must fail");
    assert!(
        err.to_string().contains("backend.retry.initial_backoff_ms"),
        "should name the field, got: {err}"
    );
}

#[test]
fn backend_breaker_failure_rate_out_of_range_error_names_field() {
    let cache = format!(
        "[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\n",
        scratch_dir("resil-rate").display()
    );
    let toml_text = format!("{cache}\n[backend.breaker]\nfailure_rate = 1.5\n");
    let err = Config::from_toml_str(&toml_text)
        .expect("parses")
        .validate()
        .expect_err("failure_rate > 1 must fail");
    assert!(
        err.to_string().contains("backend.breaker.failure_rate"),
        "should name the field, got: {err}"
    );
}

#[test]
fn backend_breaker_zero_probes_error_names_field() {
    let cache = format!(
        "[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\n",
        scratch_dir("resil-probes").display()
    );
    let toml_text = format!("{cache}\n[backend.breaker]\nhalf_open_max_probes = 0\n");
    let err = Config::from_toml_str(&toml_text)
        .expect("parses")
        .validate()
        .expect_err("zero probes must fail");
    assert!(
        err.to_string()
            .contains("backend.breaker.half_open_max_probes"),
        "should name the field, got: {err}"
    );
}

#[test]
fn missing_cache_dir_error_names_field() {
    let toml_text = "[backend]\nbucket = \"test-bucket\"\n[cache]\ndir = \"/nonexistent/verglas\"\ncapacity_bytes = \"64MB\"\n";
    let err = Config::from_toml_str(toml_text)
        .expect("parses")
        .validate()
        .expect_err("missing dir must fail");
    assert!(
        err.to_string().contains("cache.dir"),
        "should name the field, got: {err}"
    );
}

#[test]
fn conflicting_ports_error_names_field() {
    let toml_text = format!(
        "{}\n[listen]\ns3_port = 9\nadmin_port = 9\n",
        valid_toml("ports")
    );
    let err = Config::from_toml_str(&toml_text)
        .expect("parses")
        .validate()
        .expect_err("equal ports must fail");
    assert!(
        err.to_string().contains("listen.admin_port"),
        "should name the field, got: {err}"
    );
}

#[test]
fn catalog_section_parses_with_defaults_and_validates() {
    let toml_text = format!(
        "{}\n[catalog]\nuri = \"http://localhost:8181\"\n",
        valid_toml("catalog-defaults")
    );
    let config = Config::from_toml_str(&toml_text).expect("parses");
    config.validate().expect("valid catalog config validates");
    let catalog = config.catalog.expect("catalog section present");
    assert_eq!(
        catalog.consistency,
        verglas_core::config::CatalogConsistency::Eventual
    );
    assert_eq!(catalog.uri, "http://localhost:8181");
    assert_eq!(catalog.poll_interval_secs, 30);
    assert!(catalog.include.is_empty());
    assert!(catalog.exclude.is_empty());
    assert!(catalog.bearer_token.is_none());
    assert!(catalog.warehouse.is_none());
}

#[test]
fn catalog_accepts_only_strong_or_eventual_consistency() {
    let strong = Config::from_toml_str(&format!(
        "{}\n[catalog]\nuri = \"http://localhost:8181\"\nconsistency = \"strong\"\n",
        valid_toml("catalog-strong")
    ))
    .expect("strong parses");
    assert_eq!(
        strong.catalog.expect("catalog").consistency,
        verglas_core::config::CatalogConsistency::Strong
    );

    let error = Config::from_toml_str(&format!(
        "{}\n[catalog]\nuri = \"http://localhost:8181\"\nconsistency = \"session\"\n",
        valid_toml("catalog-invalid-consistency")
    ))
    .expect_err("third consistency mode must be rejected");
    assert!(error.to_string().contains("consistency"));
}

#[test]
fn absent_catalog_section_is_none() {
    let config = Config::from_toml_str(&valid_toml("catalog-absent")).expect("parses");
    assert!(config.catalog.is_none());
    assert!(config.summary().contains("catalog=off"));
}

#[test]
fn catalog_filters_and_auth_parse() {
    let toml_text = format!(
        concat!(
            "{}\n[catalog]\nuri = \"https://polaris.example/api/catalog\"\n",
            "poll_interval_secs = 5\n",
            "include = [\"db.*\"]\nexclude = [\"db.tmp_*\"]\n",
            "bearer_token = \"sekrit\"\nwarehouse = \"lake\"\n"
        ),
        valid_toml("catalog-full")
    );
    let config = Config::from_toml_str(&toml_text).expect("parses");
    config.validate().expect("validates");
    assert!(
        config
            .summary()
            .contains("catalog=https://polaris.example/api/catalog")
    );
    let catalog = config.catalog.expect("catalog section present");
    assert_eq!(catalog.poll_interval_secs, 5);
    assert_eq!(catalog.include, vec!["db.*".to_owned()]);
    assert_eq!(catalog.exclude, vec!["db.tmp_*".to_owned()]);
    assert_eq!(catalog.bearer_token.as_deref(), Some("sekrit"));
    assert_eq!(catalog.warehouse.as_deref(), Some("lake"));
}

#[test]
fn catalog_non_http_uri_error_names_field() {
    let toml_text = format!(
        "{}\n[catalog]\nuri = \"localhost:8181\"\n",
        valid_toml("catalog-uri")
    );
    let err = Config::from_toml_str(&toml_text)
        .expect("parses")
        .validate()
        .expect_err("non-http uri must fail");
    assert!(
        err.to_string().contains("catalog.uri"),
        "should name the field, got: {err}"
    );
}

#[test]
fn catalog_zero_interval_error_names_field() {
    let toml_text = format!(
        "{}\n[catalog]\nuri = \"http://localhost:8181\"\npoll_interval_secs = 0\n",
        valid_toml("catalog-interval")
    );
    let err = Config::from_toml_str(&toml_text)
        .expect("parses")
        .validate()
        .expect_err("zero interval must fail");
    assert!(
        err.to_string().contains("catalog.poll_interval_secs"),
        "should name the field, got: {err}"
    );
}

#[test]
fn bad_size_suffix_is_a_parse_error() {
    let toml_text =
        valid_toml("size").replace("capacity_bytes = \"64MB\"", "capacity_bytes = \"20QB\"");
    let err = Config::from_toml_str(&toml_text).expect_err("bad suffix must fail");
    assert!(
        err.to_string().contains("B/KB/MB/GB/TB"),
        "should list suffixes, got: {err}"
    );
}

// --- [cluster] gossip membership (#27) ---------------------------------------

/// Absent `[cluster]` is single-node: no gossip, no membership — the turn-off
/// path where a lone server behaves exactly as before this feature landed.
#[test]
fn cluster_absent_means_single_node() {
    let config = Config::from_toml_str(&valid_toml("no-cluster")).expect("parses");
    config.validate().expect("valid");
    assert!(config.cluster.is_none());
    assert!(config.summary().contains("cluster=off"));
}

/// A `[cluster]` table parses its fields and applies documented defaults:
/// `pod_id` defaults, `seeds` may be empty (a bootstrap seed), and the
/// rendezvous weight is optional (defaults to the cache capacity later).
#[test]
fn cluster_parses_with_defaults() {
    let toml_text = format!(
        "{}[cluster]\ngossip_addr = \"10.0.0.1:7946\"\n",
        valid_toml("cluster-defaults")
    );
    let config = Config::from_toml_str(&toml_text).expect("parses");
    config.validate().expect("valid");
    let cluster = config.cluster.as_ref().expect("cluster present");
    assert_eq!(cluster.gossip_addr, "10.0.0.1:7946");
    assert_eq!(cluster.pod_id, "default");
    assert!(cluster.node_id.is_none());
    assert!(cluster.seeds.is_empty());
    assert!(cluster.weight.is_none());
    assert!(cluster.advertise_addr.is_none());
    // The lifecycle windows default (#30/#31): a 5-minute join warm-up and a
    // 10-minute drain donor window.
    assert_eq!(cluster.warm_from_peers_secs, 300);
    assert_eq!(cluster.drain_timeout_secs, 600);
    assert!(config.summary().contains("cluster=default"));
}

/// The lifecycle windows round-trip (#30/#31): an explicit warm-from-peers
/// window and drain timeout override their defaults.
#[test]
fn cluster_lifecycle_windows_round_trip() {
    let toml_text = format!(
        "{}[cluster]\ngossip_addr = \"10.0.0.1:7946\"\nwarm_from_peers_secs = 120\ndrain_timeout_secs = 90\n",
        valid_toml("cluster-lifecycle")
    );
    let config = Config::from_toml_str(&toml_text).expect("parses");
    config.validate().expect("valid");
    let cluster = config.cluster.as_ref().expect("cluster present");
    assert_eq!(cluster.warm_from_peers_secs, 120);
    assert_eq!(cluster.drain_timeout_secs, 90);
}

/// All cluster fields round-trip: pod/node identity, seeds, the advertised
/// peer address (#29 extension point), an explicit rendezvous weight, and the tuned
/// suspicion window.
#[test]
fn cluster_all_fields_round_trip() {
    let toml_text = format!(
        "{}[cluster]\npod_id = \"az-1a\"\nnode_id = \"node-a\"\ngossip_addr = \"10.0.0.1:7946\"\nadvertise_addr = \"10.0.0.1:8333\"\nseeds = [\"10.0.0.2:7946\", \"seed.pod.internal:7946\"]\nweight = 800\nsuspicion_secs = 3\n",
        valid_toml("cluster-full")
    );
    let config = Config::from_toml_str(&toml_text).expect("parses");
    config.validate().expect("valid");
    let cluster = config.cluster.as_ref().expect("cluster present");
    assert_eq!(cluster.pod_id, "az-1a");
    assert_eq!(cluster.node_id.as_deref(), Some("node-a"));
    assert_eq!(cluster.advertise_addr.as_deref(), Some("10.0.0.1:8333"));
    assert_eq!(cluster.seeds.len(), 2);
    assert_eq!(cluster.weight, Some(800));
    assert_eq!(cluster.suspicion_secs, 3);
    // The peer-fetch (#29) knobs default when unset.
    assert_eq!(cluster.secret, None);
    assert_eq!(cluster.peer_connect_timeout_ms, 5);
    assert_eq!(cluster.peer_request_timeout_ms, 50);
}

/// The vector-index cluster id round-trips through `[cluster].id`, and
/// `resolve_cluster_id` returns it (no env override in this test's environment).
#[test]
fn cluster_id_round_trips_and_resolves() {
    let toml_text = format!(
        "{}[cluster]\ngossip_addr = \"10.0.0.1:7946\"\nid = \"pod-west-1\"\n",
        valid_toml("cluster-id")
    );
    let config = Config::from_toml_str(&toml_text).expect("parses");
    config.validate().expect("valid");
    let cluster = config.cluster.as_ref().expect("cluster present");
    assert_eq!(cluster.id.as_deref(), Some("pod-west-1"));
    // With no `VERGLAS_CLUSTER_ID` in the test environment the configured id
    // wins over the hostname.
    if std::env::var_os("VERGLAS_CLUSTER_ID").is_none() {
        assert_eq!(config.resolve_cluster_id(), "pod-west-1");
    }
}

/// The peer-fetch (#29) knobs round-trip: the shared secret and the tight
/// connect/request timeout budget.
#[test]
fn cluster_peer_fetch_fields_round_trip() {
    let toml_text = format!(
        "{}[cluster]\ngossip_addr = \"10.0.0.1:7946\"\nsecret = \"pod-secret\"\n\
         peer_connect_timeout_ms = 8\npeer_request_timeout_ms = 80\n",
        valid_toml("cluster-peer")
    );
    let config = Config::from_toml_str(&toml_text).expect("parses");
    config.validate().expect("valid");
    let cluster = config.cluster.as_ref().expect("cluster present");
    assert_eq!(cluster.secret.as_deref(), Some("pod-secret"));
    assert_eq!(cluster.peer_connect_timeout_ms, 8);
    assert_eq!(cluster.peer_request_timeout_ms, 80);
}

/// A zero peer connect timeout is rejected: a zero budget would abandon every
/// peer fetch instantly, defeating the rung.
#[test]
fn cluster_zero_peer_connect_timeout_is_rejected() {
    let toml_text = format!(
        "{}[cluster]\ngossip_addr = \"10.0.0.1:7946\"\npeer_connect_timeout_ms = 0\n",
        valid_toml("cluster-zero-connect")
    );
    let err = Config::from_toml_str(&toml_text)
        .expect("parses")
        .validate()
        .expect_err("zero connect timeout must fail");
    assert!(
        err.to_string().contains("cluster.peer_connect_timeout_ms"),
        "should name the field, got: {err}"
    );
}

/// A zero peer request timeout is rejected for the same reason.
#[test]
fn cluster_zero_peer_request_timeout_is_rejected() {
    let toml_text = format!(
        "{}[cluster]\ngossip_addr = \"10.0.0.1:7946\"\npeer_request_timeout_ms = 0\n",
        valid_toml("cluster-zero-request")
    );
    let err = Config::from_toml_str(&toml_text)
        .expect("parses")
        .validate()
        .expect_err("zero request timeout must fail");
    assert!(
        err.to_string().contains("cluster.peer_request_timeout_ms"),
        "should name the field, got: {err}"
    );
}

/// A malformed `gossip_addr` is rejected at validation, naming the field.
#[test]
fn cluster_bad_gossip_addr_is_rejected() {
    let toml_text = format!(
        "{}[cluster]\ngossip_addr = \"not-an-addr\"\n",
        valid_toml("cluster-bad-gossip")
    );
    let err = Config::from_toml_str(&toml_text)
        .expect("parses")
        .validate()
        .expect_err("bad gossip addr must fail");
    assert!(
        err.to_string().contains("cluster.gossip_addr"),
        "should name the field, got: {err}"
    );
}

/// A malformed advertised peer address is rejected, naming the field.
#[test]
fn cluster_bad_advertise_addr_is_rejected() {
    let toml_text = format!(
        "{}[cluster]\ngossip_addr = \"10.0.0.1:7946\"\nadvertise_addr = \"nope\"\n",
        valid_toml("cluster-bad-adv")
    );
    let err = Config::from_toml_str(&toml_text)
        .expect("parses")
        .validate()
        .expect_err("bad advertise addr must fail");
    assert!(
        err.to_string().contains("cluster.advertise_addr"),
        "should name the field, got: {err}"
    );
}

/// A zero rendezvous weight is rejected: a node claiming zero capacity would
/// own no keyspace, which is never the intent of advertising a weight.
#[test]
fn cluster_zero_weight_is_rejected() {
    let toml_text = format!(
        "{}[cluster]\ngossip_addr = \"10.0.0.1:7946\"\nweight = 0\n",
        valid_toml("cluster-zero-weight")
    );
    let err = Config::from_toml_str(&toml_text)
        .expect("parses")
        .validate()
        .expect_err("zero weight must fail");
    assert!(
        err.to_string().contains("cluster.weight"),
        "should name the field, got: {err}"
    );
}

/// A zero suspicion window is rejected: the failure detector needs a positive
/// interval to accrue against.
#[test]
fn cluster_zero_suspicion_is_rejected() {
    let toml_text = format!(
        "{}[cluster]\ngossip_addr = \"10.0.0.1:7946\"\nsuspicion_secs = 0\n",
        valid_toml("cluster-zero-susp")
    );
    let err = Config::from_toml_str(&toml_text)
        .expect("parses")
        .validate()
        .expect_err("zero suspicion must fail");
    assert!(
        err.to_string().contains("cluster.suspicion_secs"),
        "should name the field, got: {err}"
    );
}

/// An unknown key inside `[cluster]` is a parse error naming the typo — the
/// same strict schema the rest of the config uses.
#[test]
fn cluster_unknown_field_is_rejected() {
    let toml_text = format!(
        "{}[cluster]\ngossip_addr = \"10.0.0.1:7946\"\nweightt = 5\n",
        valid_toml("cluster-typo")
    );
    let err = Config::from_toml_str(&toml_text).expect_err("unknown field must fail");
    assert!(err.to_string().contains("weightt"), "got: {err}");
}

// ---- write-back geometry validation (#180) --------------------------------

/// Write-back is off by default and its geometry is not validated when off.
#[test]
fn writeback_off_by_default() {
    let config = Config::from_toml_str(&valid_toml("wb-off")).expect("parses");
    assert!(!config.cache.writeback.enabled);
    config.validate().expect("disabled writeback validates");
}

/// A valid enabled geometry (k=4, m=2, w=5) validates.
#[test]
fn writeback_valid_geometry_validates() {
    let toml = format!(
        "{}[cache.writeback]\nenabled = true\nk = 4\nm = 2\nw = 5\n",
        valid_toml("wb-valid")
    );
    let config = Config::from_toml_str(&toml).expect("parses");
    config.validate().expect("valid geometry validates");
}

/// A geometry that can never reach quorum (w > k+m) is rejected, naming the
/// field.
#[test]
fn writeback_rejects_unreachable_quorum() {
    let toml = format!(
        "{}[cache.writeback]\nenabled = true\nk = 2\nm = 1\nw = 4\n",
        valid_toml("wb-unreachable")
    );
    let config = Config::from_toml_str(&toml).expect("parses");
    let err = config
        .validate()
        .expect_err("w > k+m can never reach quorum");
    assert!(
        err.to_string().contains("cache.writeback"),
        "error names the field: {err}"
    );
}

/// A geometry whose acked set cannot reconstruct (w < k) is rejected.
#[test]
fn writeback_rejects_w_below_k() {
    let toml = format!(
        "{}[cache.writeback]\nenabled = true\nk = 4\nm = 2\nw = 3\n",
        valid_toml("wb-wlow")
    );
    let config = Config::from_toml_str(&toml).expect("parses");
    let err = config.validate().expect_err("w < k cannot reconstruct");
    assert!(err.to_string().contains("cache.writeback"));
}

/// Zero data or parity fragments are rejected.
#[test]
fn writeback_rejects_zero_k_or_m() {
    for (k, m, w) in [(0usize, 2usize, 1usize), (2, 0, 2)] {
        let toml = format!(
            "{}[cache.writeback]\nenabled = true\nk = {k}\nm = {m}\nw = {w}\n",
            valid_toml(&format!("wb-zero-{k}-{m}"))
        );
        let config = Config::from_toml_str(&toml).expect("parses");
        config.validate().expect_err("zero k or m rejected");
    }
}

/// There is one NVMe budget and no fragment sizing knob (#223): enabling the
/// write-back tier adds no size field — fragments share `cache.capacity_bytes`
/// with the block cache first come, first served, enforced by the server's
/// accounting. Any attempted sizing field is rejected as unknown.
#[test]
fn writeback_has_no_fragment_sizing_knob() {
    let toml = format!(
        "{}[cache.writeback]\nenabled = true\nk = 4\nm = 2\nw = 5\n",
        valid_toml("wb-one-budget")
    );
    let config = Config::from_toml_str(&toml).expect("parses");
    config
        .validate()
        .expect("enabling write-back needs no sizing field");

    for knob in ["fragment_fraction = 0.5", "staging_bytes = \"2GB\""] {
        let toml = format!(
            "{}[cache.writeback]\nenabled = true\nk = 4\nm = 2\nw = 5\n{knob}\n",
            valid_toml("wb-no-knob")
        );
        let err = Config::from_toml_str(&toml).expect_err("sizing knobs do not exist");
        let field = knob.split(' ').next().unwrap_or(knob);
        assert!(
            err.to_string().contains(field),
            "error names the unknown field `{field}`: {err}"
        );
    }
}

/// Startup refuses a `cache.capacity_bytes` larger than the free space on the
/// filesystem backing `cache.dir`, naming the field, the configured size, and
/// the available space (#96). A server that would run the disk out never boots.
#[test]
fn rejects_capacity_bytes_larger_than_free_disk() {
    // 1 EiB — larger than any real test filesystem's free space.
    let huge: u64 = 1 << 60;
    let toml = valid_toml("cap-too-big").replace(
        "capacity_bytes = \"64MB\"",
        &format!("capacity_bytes = \"{huge}\""),
    );
    let config = Config::from_toml_str(&toml).expect("parses");
    let err = config
        .validate()
        .expect_err("an oversized budget is refused");
    let msg = err.to_string();
    assert!(
        msg.contains("cache.capacity_bytes"),
        "names the field: {msg}"
    );
    assert!(
        msg.contains(&huge.to_string()),
        "names the configured size: {msg}"
    );
    assert!(msg.contains("free"), "names the available space: {msg}");
}

/// A warm cache must be able to restart (#298): bytes already held by files
/// under `cache.dir` count toward the capacity check, so a budget larger than
/// free space alone but within free + already-owned validates. Regression: a
/// server with a 4 TB budget and 3.6 TB cached was refused on reboot because
/// the gate compared the budget against free space only.
#[test]
fn warm_cache_contents_count_toward_the_capacity_check() {
    let dir = scratch_dir("cap-warm");
    // A real (non-sparse) payload so the filesystem's free space is already
    // down by these bytes, exactly like a warm cache after a restart.
    let payload = vec![0u8; 64 * 1024 * 1024];
    std::fs::write(dir.join("warm-device-file"), &payload).expect("write warm payload");
    let free = verglas_core::disk::free_bytes(&dir).expect("free-space probe works in tests");
    // Over free space alone, but within free + what the cache already owns.
    // The 32 MiB margin (half the payload) tolerates concurrent tests moving
    // free space between this probe and the one inside validate().
    let budget = free + 32 * 1024 * 1024;
    let toml = format!(
        "[backend]\nbucket = \"test-bucket\"\n[cache]\ndir = \"{}\"\ncapacity_bytes = \"{budget}\"\n",
        dir.display()
    );
    let config = Config::from_toml_str(&toml).expect("parses");
    config
        .validate()
        .expect("a budget covered by free space plus the cache's own bytes validates");
    let _ = std::fs::remove_file(dir.join("warm-device-file"));
}

/// The fragment scrub interval defaults to 6 hours when unset (#220).
#[test]
fn writeback_scrub_interval_defaults() {
    let config = Config::from_toml_str(&valid_toml("wb-scrub-default")).expect("parses");
    assert_eq!(config.cache.writeback.scrub_interval_secs, 6 * 60 * 60);
}

/// A custom scrub interval parses and validates.
#[test]
fn writeback_scrub_interval_custom_validates() {
    let toml = format!(
        "{}[cache.writeback]\nenabled = true\nk = 4\nm = 2\nw = 5\nscrub_interval_secs = 900\n",
        valid_toml("wb-scrub-custom")
    );
    let config = Config::from_toml_str(&toml).expect("parses");
    assert_eq!(config.cache.writeback.scrub_interval_secs, 900);
    config.validate().expect("custom scrub interval validates");
}

/// A zero scrub interval is rejected when the tier is enabled — a scrubber that
/// never runs would defeat the durability guarantee (#220).
#[test]
fn writeback_rejects_zero_scrub_interval() {
    let toml = format!(
        "{}[cache.writeback]\nenabled = true\nk = 4\nm = 2\nw = 5\nscrub_interval_secs = 0\n",
        valid_toml("wb-scrub-zero")
    );
    let config = Config::from_toml_str(&toml).expect("parses");
    let err = config.validate().expect_err("zero scrub interval rejected");
    assert!(
        err.to_string().contains("scrub_interval_secs"),
        "error names the field: {err}"
    );
}

/// The object offload size limit defaults to 16 MiB (#164 §4) — the frozen
/// benchmark protocol in `tests/cluster-local/OBJECTIVE.md` bakes this exact
/// default into its PUT-count bound (`total_bytes / size_limit + 1`).
#[test]
fn writeback_offload_size_limit_defaults() {
    let config = Config::from_toml_str(&valid_toml("wb-offload-default")).expect("parses");
    assert_eq!(
        config.cache.writeback.offload_size_limit_bytes,
        ByteSize(16 * 1024 * 1024)
    );
}

/// A custom offload size limit parses and validates.
#[test]
fn writeback_offload_size_limit_custom_validates() {
    let toml = format!(
        "{}[cache.writeback]\nenabled = true\nk = 4\nm = 2\nw = 5\noffload_size_limit_bytes = \"1MB\"\n",
        valid_toml("wb-offload-custom")
    );
    let config = Config::from_toml_str(&toml).expect("parses");
    assert_eq!(
        config.cache.writeback.offload_size_limit_bytes,
        ByteSize(1024 * 1024)
    );
    config
        .validate()
        .expect("custom offload size limit validates");
}

/// A zero offload size limit is rejected when the tier is enabled — every
/// object would then bypass accumulation, defeating the point of the stream.
#[test]
fn writeback_rejects_zero_offload_size_limit() {
    let toml = format!(
        "{}[cache.writeback]\nenabled = true\nk = 4\nm = 2\nw = 5\noffload_size_limit_bytes = \"0\"\n",
        valid_toml("wb-offload-zero")
    );
    let config = Config::from_toml_str(&toml).expect("parses");
    let err = config
        .validate()
        .expect_err("zero offload size limit rejected");
    assert!(
        err.to_string().contains("offload_size_limit_bytes"),
        "error names the field: {err}"
    );
}

/// The offload drain loop interval defaults to 5 seconds (#164 §4) — short
/// enough that a partially filled stream still drains inside the frozen
/// benchmark's 30 s post-write window.
#[test]
fn writeback_offload_drain_interval_defaults() {
    let config = Config::from_toml_str(&valid_toml("wb-offload-drain-default")).expect("parses");
    assert_eq!(config.cache.writeback.offload_drain_interval_secs, 5);
}

/// A custom offload drain interval parses and validates.
#[test]
fn writeback_offload_drain_interval_custom_validates() {
    let toml = format!(
        "{}[cache.writeback]\nenabled = true\nk = 4\nm = 2\nw = 5\noffload_drain_interval_secs = 30\n",
        valid_toml("wb-offload-drain-custom")
    );
    let config = Config::from_toml_str(&toml).expect("parses");
    assert_eq!(config.cache.writeback.offload_drain_interval_secs, 30);
    config
        .validate()
        .expect("custom offload drain interval validates");
}

/// A zero offload drain interval is rejected when the tier is enabled — a
/// loop that never fires would strand small objects below the size limit.
#[test]
fn writeback_rejects_zero_offload_drain_interval() {
    let toml = format!(
        "{}[cache.writeback]\nenabled = true\nk = 4\nm = 2\nw = 5\noffload_drain_interval_secs = 0\n",
        valid_toml("wb-offload-drain-zero")
    );
    let config = Config::from_toml_str(&toml).expect("parses");
    let err = config
        .validate()
        .expect_err("zero offload drain interval rejected");
    assert!(
        err.to_string().contains("offload_drain_interval_secs"),
        "error names the field: {err}"
    );
}

/// A per-prefix override with an unreachable geometry is rejected too.
#[test]
fn writeback_rejects_bad_prefix_geometry() {
    let toml = format!(
        "{}[cache.writeback]\nenabled = true\nk = 4\nm = 2\nw = 5\n\n\
         [[cache.writeback.prefixes]]\nprefix = \"data/\"\nw = 9\n",
        valid_toml("wb-prefix-bad")
    );
    let config = Config::from_toml_str(&toml).expect("parses");
    let err = config.validate().expect_err("prefix w=9 > k+m=6 rejected");
    assert!(err.to_string().contains("cache.writeback.prefixes"));
}

// --- #221: backend endpoint/region/addressing/credentials in config -------- //

#[test]
fn backend_endpoint_region_and_addressing_parse_and_default_to_none() {
    // The `[backend]` table gains endpoint/region/allow_http/virtual_hosted/
    // credentials fields (#221). Absent, they are None/false so the client
    // falls back to the AWS env — production IAM/instance-role is unchanged.
    let config = Config::from_toml_str(&valid_toml("backend-defaults")).expect("parses");
    assert_eq!(config.backend.endpoint, None);
    assert_eq!(config.backend.region, None);
    assert!(!config.backend.allow_http);
    assert!(!config.backend.virtual_hosted_style);
    assert_eq!(config.backend.credentials_file, None);
    assert_eq!(config.backend.credentials_profile, None);
}

#[test]
fn backend_endpoint_region_and_credentials_round_trip_from_toml() {
    // A configured OCI/MinIO backend sets the endpoint, region, path-style, and
    // names a credentials file + profile under ~/.verglas/credentials.
    let toml = format!(
        "[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\n[backend]\n\
         bucket = \"test-bucket\"\n\
         endpoint = \"https://ns.compat.objectstorage.us-ashburn-1.oci.customer-oci.com\"\n\
         region = \"us-ashburn-1\"\n\
         allow_http = false\n\
         virtual_hosted_style = false\n\
         credentials_file = \"/home/op/.verglas/credentials/oci\"\n\
         credentials_profile = \"default\"\n",
        scratch_dir("backend-oci").display()
    );
    let config = Config::from_toml_str(&toml).expect("parses");
    assert_eq!(
        config.backend.endpoint.as_deref(),
        Some("https://ns.compat.objectstorage.us-ashburn-1.oci.customer-oci.com")
    );
    assert_eq!(config.backend.region.as_deref(), Some("us-ashburn-1"));
    assert_eq!(
        config.backend.credentials_file.as_deref(),
        Some("/home/op/.verglas/credentials/oci")
    );
    assert_eq!(
        config.backend.credentials_profile.as_deref(),
        Some("default")
    );
    // The whole config still validates.
    config.validate().expect("validates");
}

#[test]
fn backend_http_endpoint_requires_allow_http() {
    // An http endpoint without allow_http is a config error named at the field,
    // so a plaintext MinIO/OCI endpoint is opt-in, not silent.
    let toml = format!(
        "[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\n[backend]\nbucket = \"test-bucket\"\nendpoint = \"http://127.0.0.1:9000\"\n",
        scratch_dir("backend-http-guard").display()
    );
    let config = Config::from_toml_str(&toml).expect("parses");
    let err = config
        .validate()
        .expect_err("http endpoint without allow_http is rejected");
    assert!(
        err.to_string().contains("backend.endpoint"),
        "error names the field: {err}"
    );
}

#[test]
fn catalog_sigv4_settings_parse_and_validate() {
    // SigV4 catalog auth (#236): region + signing name turn on AWS signing for
    // an S3 Tables / Glue catalog. A warehouse ARN is the common companion.
    let toml = format!(
        "[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\n[backend]\nbucket = \"b\"\n\n\
         [catalog]\nuri = \"https://s3tables.us-west-2.amazonaws.com/iceberg\"\n\
         warehouse = \"arn:aws:s3tables:us-west-2:1:bucket/rlean-data\"\n\
         sigv4_region = \"us-west-2\"\nsigv4_signing_name = \"s3tables\"\n",
        scratch_dir("catalog-sigv4").display()
    );
    let config = Config::from_toml_str(&toml).expect("parses");
    config.validate().expect("sigv4 catalog validates");
    let catalog = config.catalog.as_ref().expect("catalog present");
    assert_eq!(catalog.sigv4_region.as_deref(), Some("us-west-2"));
    assert_eq!(catalog.sigv4_signing_name.as_deref(), Some("s3tables"));
    assert!(catalog.sigv4_enabled(), "both fields set turns SigV4 on");
    // No bearer token in SigV4 mode.
    assert_eq!(catalog.resolve_bearer_token().expect("resolves"), None);
}

#[test]
fn catalog_sigv4_requires_both_region_and_signing_name() {
    // Setting only one of the SigV4 pair is a config mistake: signing needs both
    // a region and a signing name. Validation names the field.
    let toml = format!(
        "[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\n[backend]\nbucket = \"b\"\n\n\
         [catalog]\nuri = \"https://c.example.com\"\nsigv4_region = \"us-west-2\"\n",
        scratch_dir("catalog-sigv4-half").display()
    );
    let config = Config::from_toml_str(&toml).expect("parses");
    let err = config.validate().expect_err("half a SigV4 pair must fail");
    assert!(
        err.to_string().contains("sigv4_signing_name"),
        "error names the missing field, got: {err}"
    );
}

#[test]
fn catalog_sigv4_and_bearer_token_are_mutually_exclusive() {
    // SigV4 and a bearer token are two different auth modes; setting both is
    // rejected loudly, naming the fields.
    let toml = format!(
        "[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\n[backend]\nbucket = \"b\"\n\n\
         [catalog]\nuri = \"https://c.example.com\"\n\
         sigv4_region = \"us-west-2\"\nsigv4_signing_name = \"s3tables\"\n\
         bearer_token = \"tok\"\n",
        scratch_dir("catalog-sigv4-bearer").display()
    );
    let config = Config::from_toml_str(&toml).expect("parses");
    let err = config.validate().expect_err("sigv4 + bearer must fail");
    assert!(
        err.to_string().contains("sigv4"),
        "error names the conflict, got: {err}"
    );
}
