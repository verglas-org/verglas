//! Backend object-store client construction, isolation, and binding registry.
//!
//! On a cache miss Verglas fills from the customer's real bucket ("the
//! backend"). The server serves a configured SET of buckets (#235): the single
//! `backend.bucket` plus `backend.bucket_globs` (glob patterns like
//! `*--table-s3` for AWS S3 Tables). A request whose bucket is in the set is
//! served; any other bucket is rejected with `NoSuchBucket` — the server never
//! builds a client for a bucket outside the set. Each served bucket's client is
//! built lazily on first request and memoized, so a dynamic family (S3 Tables
//! buckets appearing as tables are created) costs nothing until touched. Every
//! bucket carries its OWN concurrency limiter, circuit breaker, and retry
//! policy, so a miss storm on one bucket cannot exhaust another's permits or
//! trip another's breaker. One credential set covers each immutable storage
//! binding. [`BackendRegistry`] routes multiple providers and credential sets
//! concurrently while retaining independent resilience state. Each client hands back a
//! plain `object_store` handle so the read/write interfaces in `verglas-s3`
//! consume it without knowing which object store they are talking to.
//!
//! The [`BackendStores`] trait is the front-end seam so `verglas-s3` can route
//! by the request's bucket without depending on the concrete [`BackendStore`].

mod breaker;
mod credentials;
mod limit;
mod origin;
mod raw;
mod resilience;
mod retry;
mod settings;

pub use breaker::{
    Admission, BreakerSnapshot, BreakerState, CircuitBreaker, Clock, SystemClock, Ticket,
};
pub use limit::{LimitedStore, MultipartObjectStore};
pub use raw::{
    RawAttributes, RawChecksums, RawCompletedPart, RawError, RawForwardResponse, RawGet,
    RawListEntry, RawListPage, RawMultipartCreation, RawObjectMeta, RawObjectPart, RawObjectParts,
    RawPartUpload, RawPutOutcome, RawRange, RawReadExtras, RawS3, RawUpload, RawUploadsPage,
    RawWriteChecksum, RawWriteHeaders, encode_key,
};
pub use resilience::{CircuitOpen, ResilientRawS3, ResilientStore};
pub use retry::ThrottleHint;
pub use settings::{
    AzureAuth, AzureCredentials, BackendSettings, GcpCredentials, read_aws_keypair,
};

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, RwLock};

use object_store::ObjectStoreExt;
use object_store::RetryConfig;
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;
use verglas_core::config::{Backend, BackendProvider, BreakerPolicy, RetryPolicy};

use credentials::{azure_file_credentials, s3_credentials};
use origin::OriginProvider;
use resilience::RetryMetrics;

/// How the S3 client will source credentials, resolved from the environment for
/// the startup log only — the real resolution is `object_store`'s default AWS
/// chain, evaluated lazily per request. Verglas never holds long-lived customer
/// secrets, so there is no "static from config" mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialMode {
    /// `[backend] credentials_file` names an AWS-format credentials file (#221):
    /// the server reads the keys from there, no env needed.
    CredentialsFile,
    /// `AWS_ACCESS_KEY_ID` present: static keys from the environment (dev/CI,
    /// including MinIO).
    StaticEnv,
    /// `AWS_WEB_IDENTITY_TOKEN_FILE` present: web-identity federation (e.g.
    /// EKS IRSA).
    WebIdentity,
    /// No credential env vars: the IMDS instance role (production on EC2). This
    /// path cannot be exercised off an instance and is verified manually.
    InstanceRole,
}

impl CredentialMode {
    /// Best-effort classification of the credential source for the startup log:
    /// a config-named credentials file wins, then static env keys, then
    /// web-identity, then the instance role.
    fn resolve(backend: &Backend) -> Self {
        if backend
            .credentials_file
            .as_ref()
            .is_some_and(|s| !s.is_empty())
        {
            CredentialMode::CredentialsFile
        } else if std::env::var_os("AWS_ACCESS_KEY_ID").is_some() {
            CredentialMode::StaticEnv
        } else if std::env::var_os("AWS_WEB_IDENTITY_TOKEN_FILE").is_some() {
            CredentialMode::WebIdentity
        } else {
            CredentialMode::InstanceRole
        }
    }

    /// The human-readable credential-mode name for the startup log line.
    fn label(self) -> &'static str {
        match self {
            CredentialMode::CredentialsFile => "credentials-file",
            CredentialMode::StaticEnv => "static-env",
            CredentialMode::WebIdentity => "web-identity",
            CredentialMode::InstanceRole => "instance-role",
        }
    }
}

/// Backend client construction failures. Each renders one operator-facing line.
#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    /// A request named no registered immutable storage binding.
    #[error("unknown storage binding `{storage_binding_id}`")]
    UnknownStorageBinding {
        /// Binding identifier supplied by the request.
        storage_binding_id: String,
    },
    /// Registry construction attempted to reuse one immutable binding ID.
    #[error("duplicate storage binding `{storage_binding_id}`")]
    DuplicateStorageBinding {
        /// Binding identifier that appeared more than once.
        storage_binding_id: String,
    },
    /// A storage binding identifier was empty or malformed.
    #[error("invalid storage binding: {message}")]
    InvalidStorageBinding {
        /// Operator-facing validation detail.
        message: String,
    },
    /// A panic poisoned the binding registry; routing cannot remain trustworthy.
    #[error("storage binding registry is unavailable")]
    StorageBindingRegistryUnavailable,
    /// The underlying `object_store` client could not be constructed for the
    /// configured bucket (a malformed endpoint override, say). Surfaced at the
    /// front-end as an S3-shaped internal error.
    #[error("cannot build backend client for bucket `{bucket}`: {source}")]
    Build {
        /// Bucket whose client failed to build.
        bucket: String,
        /// The `object_store` builder error.
        source: object_store::Error,
    },
    /// A request named a bucket outside the server's configured bucket set
    /// (`backend.bucket` / `backend.bucket_globs`). It gets `NoSuchBucket`,
    /// surfaced at the front-end as S3's 404.
    #[error("this node does not serve bucket `{requested}` (served set: {configured})")]
    NoSuchBucket {
        /// The bucket the request named.
        requested: String,
        /// A description of the served bucket set.
        configured: String,
    },
}

/// The object key the startup probe HEADs. It is a sentinel that is not expected
/// to exist; a `NotFound` answer still proves the origin was reached and the
/// credentials were accepted, which is all the probe needs to confirm.
const STARTUP_PROBE_KEY: &str = ".verglas-startup-probe";

/// A startup-probe failure (#233): the configured backend bucket could not be
/// built, reached, or authenticated before serving. The server fails to start on
/// this rather than degrading to an empty in-memory store. Each variant names
/// the bucket and the credential chain that was tried so the operator can act.
#[derive(Debug, thiserror::Error)]
pub enum StartupProbeError {
    /// The bucket's client could not be built (a malformed endpoint, an
    /// unresolvable credentials file). Surfaced before serving.
    #[error(
        "cannot build backend client for configured bucket `{bucket}` (credential chain tried: {credential_chain}): {source}"
    )]
    Build {
        /// The configured bucket that failed to build.
        bucket: String,
        /// The credential chain the server resolved for the startup log.
        credential_chain: &'static str,
        /// The underlying build failure.
        #[source]
        source: BackendError,
    },
    /// The bucket's client built, but the probe HEAD could not reach or
    /// authenticate to the origin (a 403 from unusable credentials, a refused
    /// connection). A `NotFound` answer is success, not this error.
    #[error(
        "cannot reach configured backend bucket `{bucket}` (credential chain tried: {credential_chain}): {source}"
    )]
    Unreachable {
        /// The configured bucket the probe could not reach.
        bucket: String,
        /// The credential chain the server resolved for the startup log.
        credential_chain: &'static str,
        /// The underlying request failure from the probe HEAD.
        #[source]
        source: object_store::Error,
    },
}

/// Resolves the concurrency-limited backend store for a bucket named in a
/// request. The server serves a configured bucket set (#235); a request naming a
/// bucket outside it is rejected with [`BackendError::NoSuchBucket`]. The trait
/// is the front-end seam so `verglas-s3` can route by the request's bucket
/// without depending on the concrete [`BackendStore`].
pub trait BackendStores: Send + Sync {
    /// Returns the store for `bucket` when `bucket` is in the served set,
    /// building it lazily on first request and memoizing it; else
    /// [`BackendError::NoSuchBucket`]. A construction failure is the other
    /// possible error; a key that does not exist at the origin is not detected
    /// here (it surfaces on the request itself).
    fn store_for(
        &self,
        storage_binding_id: &str,
        bucket: &str,
    ) -> Result<Arc<dyn MultipartObjectStore>, BackendError>;

    /// Returns `bucket`'s circuit breaker if the store maintains one, for
    /// subsystems that consult backend health without issuing a request (e.g.
    /// #51 prefetch yielding to an open breaker). A bucket outside the served
    /// set has no breaker. Building the breaker lazily matches `store_for`. The
    /// default is `None`.
    fn breaker_for(
        &self,
        storage_binding_id: &str,
        _bucket: &str,
    ) -> Result<Option<Arc<CircuitBreaker>>, BackendError> {
        Err(BackendError::UnknownStorageBinding {
            storage_binding_id: storage_binding_id.to_owned(),
        })
    }

    /// Returns `bucket`'s byte-exact raw request path (issue #189) when the
    /// origin is a real S3 endpoint; `Ok(None)` when the origin is an injected
    /// typed store with no raw surface (test stores). A bucket outside the served
    /// set is [`BackendError::NoSuchBucket`]. The raw client carries keys and
    /// headers `object_store`'s typed abstractions drop (trailing-slash /
    /// empty-segment keys, the `Expires` header); it is served behind the SAME
    /// per-bucket concurrency budget, circuit breaker, and retry policy as the
    /// typed client. Construction failures (bad endpoint, missing credentials)
    /// are errors, not `None` — a raw-requiring request must fail loudly rather
    /// than fall back to a lossy path.
    fn raw_for(
        &self,
        storage_binding_id: &str,
        _bucket: &str,
    ) -> Result<Option<Arc<ResilientRawS3>>, BackendError> {
        Err(BackendError::UnknownStorageBinding {
            storage_binding_id: storage_binding_id.to_owned(),
        })
    }
}

/// The set of buckets a node serves (#235): an optional single `bucket` plus a
/// list of glob patterns. A request's bucket is served if it equals the single
/// bucket or matches a glob. This is the one gate every serving path consults.
#[derive(Debug, Clone)]
pub struct BucketSet {
    /// The single named bucket, if any.
    bucket: Option<String>,
    /// Glob patterns (`*` matches any run of characters), e.g. `*--table-s3`.
    globs: Vec<String>,
}

impl BucketSet {
    /// A set that serves exactly one named bucket.
    pub fn one(bucket: impl Into<String>) -> BucketSet {
        BucketSet {
            bucket: Some(bucket.into()),
            globs: Vec::new(),
        }
    }

    /// The set from a `[backend]` config: its `bucket` and `bucket_globs`.
    fn from_config(backend: &Backend) -> BucketSet {
        BucketSet {
            bucket: backend.bucket.clone().filter(|b| !b.is_empty()),
            globs: backend.bucket_globs.clone(),
        }
    }

    /// Whether `bucket` is in the set: it equals the single bucket or matches a
    /// glob. Uses the shared matcher so bucket and table globs agree on `*`.
    pub fn contains(&self, bucket: &str) -> bool {
        if self.bucket.as_deref() == Some(bucket) {
            return true;
        }
        self.globs
            .iter()
            .any(|pattern| verglas_core::glob::matches(pattern, bucket))
    }

    /// A one-line description for the startup log / error text: the single
    /// bucket and/or the globs, or `<none>` when empty.
    pub fn describe(&self) -> String {
        let mut parts: Vec<String> = Vec::new();
        if let Some(bucket) = &self.bucket {
            parts.push(bucket.clone());
        }
        for glob in &self.globs {
            parts.push(format!("glob:{glob}"));
        }
        if parts.is_empty() {
            "<none>".to_owned()
        } else {
            parts.join(",")
        }
    }
}

/// One served bucket's built clients: the resilient typed store, the resilient
/// raw HTTP client (S3 only; `None` for injected typed stores), and the breaker,
/// semaphore, and retry metrics they all share. Built lazily on first request
/// and memoized in [`BackendStore::clients`], so every bucket carries its OWN
/// budget and breaker — a miss storm on one bucket cannot exhaust another's.
struct BucketClients {
    /// The resilient (retry + breaker over limiter) typed store for the bucket.
    store: Arc<dyn MultipartObjectStore>,
    /// The resilient raw HTTP client (same budget/breaker as the typed stack).
    /// `None` when the origin is an injected typed store with no raw surface.
    raw: Option<Arc<ResilientRawS3>>,
    /// The bucket's circuit breaker, shared with `store` and `raw`.
    breaker: Arc<CircuitBreaker>,
    /// Retry counters for the bucket's store (aggregated for #46).
    metrics: Arc<RetryMetrics>,
}

/// A closure that builds one bucket's clients on demand: it resolves the
/// provider client(s) for a concrete bucket name and wraps them in the
/// limiter/breaker/retry stack. Boxed so `from_config` (real providers) and the
/// test factories share one code path through [`BackendStore::get_or_build`].
type ClientBuilder = Box<dyn Fn(&str) -> Result<BucketClients, BackendError> + Send + Sync>;

/// The backend store. Serves a [`BucketSet`], building each served bucket's
/// [`BucketClients`] lazily on first request and memoizing them. A request for a
/// bucket outside the set is rejected with [`BackendError::NoSuchBucket`]; the
/// server never builds a client for a bucket it does not serve.
pub struct BackendStore {
    /// Immutable identifier used in every cache and routing key.
    storage_binding_id: String,
    /// The buckets this node serves.
    set: BucketSet,
    /// Builds one bucket's clients on first request.
    builder: ClientBuilder,
    /// Memoized per-bucket clients, keyed by concrete bucket name. A `Mutex` is
    /// fine: building is rare (once per bucket ever) and cheap relative to a
    /// fill, and no lock is held across an await — the closure only constructs
    /// clients, it does not do I/O.
    clients: Mutex<HashMap<String, Arc<BucketClients>>>,
    /// Ambient credential mode, resolved once for the startup log line.
    credential_mode: CredentialMode,
}

impl std::fmt::Debug for BackendStore {
    /// Names the store and its served bucket set.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackendStore")
            .field("set", &self.set.describe())
            .field("credential_mode", &self.credential_mode)
            .finish_non_exhaustive()
    }
}

/// Wraps an already-resolved typed `store` (and optional `raw_s3`) in a fresh
/// per-bucket breaker, semaphore, limiter, and retry stack. Infallible — the
/// clients are built. The typed and raw surfaces share the bucket's ONE
/// semaphore and breaker so both draw one budget and trip one breaker (#189).
fn build_clients(
    bucket: &str,
    max_concurrent_requests: usize,
    retry: &RetryPolicy,
    breaker_policy: &BreakerPolicy,
    typed: Arc<dyn MultipartObjectStore>,
    raw_s3: Option<Arc<RawS3>>,
) -> BucketClients {
    let metrics = Arc::new(RetryMetrics::default());
    let breaker = Arc::new(CircuitBreaker::production(breaker_policy.clone()));
    let semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrent_requests));
    let limited: Arc<dyn MultipartObjectStore> =
        Arc::new(LimitedStore::with_semaphore(typed, semaphore.clone()));
    let store: Arc<dyn MultipartObjectStore> = Arc::new(ResilientStore::new(
        limited,
        bucket.to_owned(),
        retry.clone(),
        breaker.clone(),
        metrics.clone(),
    ));
    let raw = raw_s3.map(|raw_s3| {
        Arc::new(ResilientRawS3::new(
            raw_s3,
            bucket.to_owned(),
            retry.clone(),
            breaker.clone(),
            semaphore.clone(),
            metrics.clone(),
        ))
    });
    BucketClients {
        store,
        raw,
        breaker,
        metrics,
    }
}

impl BackendStore {
    /// Production store: serves the config's bucket set (`backend.bucket` /
    /// `backend.bucket_globs`), building each bucket's client on first request
    /// per `backend.provider` (s3/azure/gcp), using the `[backend]`
    /// endpoint/region/addressing/credentials overrides and the provider's env
    /// chain as the fallback (#221). The bucket set is required and gated by
    /// config validation before this runs.
    pub fn from_config(
        storage_binding_id: impl Into<String>,
        backend: &Backend,
    ) -> Arc<BackendStore> {
        let set = BucketSet::from_config(backend);
        let credential_mode = CredentialMode::resolve(backend);
        // The builder captures the config and builds one bucket's clients on
        // demand. A typed build failure propagates as an error (#233): a
        // configured backend must never degrade to an empty in-memory store that
        // answers NoSuchKey for every read while looking healthy. Only an
        // S3-family origin is raw-addressable (#189); azure/gcp have no raw
        // surface, so `raw` stays `None` and requests use the typed client.
        let origin = OriginProvider::from_backend(backend.clone());
        let builder: ClientBuilder = Box::new(move |bucket: &str| {
            let typed = origin.build_typed(bucket)?;
            let raw_s3 = origin.build_raw(bucket).ok();
            let backend = origin.backend();
            Ok(build_clients(
                bucket,
                backend.max_concurrent_requests,
                &backend.retry,
                &backend.breaker,
                typed,
                raw_s3,
            ))
        });
        Arc::new(BackendStore {
            storage_binding_id: storage_binding_id.into(),
            set,
            builder,
            clients: Mutex::new(HashMap::new()),
            credential_mode,
        })
    }

    /// A store over the glob set (`bucket` and/or `globs`) whose typed store
    /// comes from `factory`, called once per served bucket on first request and
    /// wrapped in that bucket's own limiter/breaker/retry stack. Tests use it to
    /// prove lazy per-bucket construction and budget isolation. A build failure
    /// propagates from `store_for`.
    pub fn with_glob_factory<F>(
        storage_binding_id: impl Into<String>,
        bucket: Option<String>,
        globs: Vec<String>,
        max_concurrent_requests: usize,
        retry: RetryPolicy,
        breaker_policy: BreakerPolicy,
        factory: F,
    ) -> Arc<BackendStore>
    where
        F: Fn(&str) -> Result<Arc<dyn MultipartObjectStore>, BackendError> + Send + Sync + 'static,
    {
        let set = BucketSet { bucket, globs };
        let builder: ClientBuilder = Box::new(move |bucket: &str| {
            let typed = factory(bucket)?;
            Ok(build_clients(
                bucket,
                max_concurrent_requests,
                &retry,
                &breaker_policy,
                typed,
                None,
            ))
        });
        Arc::new(BackendStore {
            storage_binding_id: storage_binding_id.into(),
            set,
            builder,
            clients: Mutex::new(HashMap::new()),
            credential_mode: CredentialMode::resolve(&Backend::default()),
        })
    }

    /// A single-bucket store whose typed store comes from `factory`, called once
    /// on first request for `bucket`. Convenience over [`with_glob_factory`] for
    /// tests that serve exactly one bucket. A build failure propagates.
    pub fn with_factory<F>(
        storage_binding_id: impl Into<String>,
        bucket: String,
        max_concurrent_requests: usize,
        retry: RetryPolicy,
        breaker_policy: BreakerPolicy,
        factory: F,
    ) -> Result<Arc<BackendStore>, BackendError>
    where
        F: Fn(&str) -> Result<Arc<dyn MultipartObjectStore>, BackendError> + Send + Sync + 'static,
    {
        let store = Self::with_glob_factory(
            storage_binding_id,
            Some(bucket.clone()),
            Vec::new(),
            max_concurrent_requests,
            retry,
            breaker_policy,
            factory,
        );
        // Surface a build failure eagerly so callers that expect a construction
        // error (a bad factory) still see it at construction time, as before.
        store.get_or_build(&bucket)?;
        Ok(store)
    }

    /// A single-bucket store serving `store` for `bucket`. For single-origin
    /// setups and tests where one object store stands in for the origin;
    /// production uses [`BackendStore::from_config`] instead. A request for any
    /// other bucket is rejected with [`BackendError::NoSuchBucket`].
    pub fn single(
        storage_binding_id: impl Into<String>,
        bucket: impl Into<String>,
        store: Arc<dyn MultipartObjectStore>,
    ) -> Arc<BackendStore> {
        let backend = Backend::default();
        Self::with_glob_factory(
            storage_binding_id,
            Some(bucket.into()),
            Vec::new(),
            backend.max_concurrent_requests,
            backend.retry,
            backend.breaker,
            move |_bucket| Ok(store.clone()),
        )
    }

    /// A description of the served bucket set (the startup log / error text).
    pub fn bucket_set(&self) -> String {
        self.set.describe()
    }

    /// Returns the immutable identity this backend is registered under.
    pub fn storage_binding_id(&self) -> &str {
        &self.storage_binding_id
    }

    /// Total retry attempts issued across all built buckets (a metrics counter
    /// #46 scrapes). Sums each built bucket's retry counter.
    pub fn retries_total(&self) -> u64 {
        self.locked()
            .values()
            .map(|c| c.metrics.retries.load(Ordering::Relaxed))
            .sum()
    }

    /// A one-line description of the backend for the server's startup log: the
    /// served bucket set, annotated with the resolved credential source.
    pub fn describe(&self) -> String {
        format!("{}/{}", self.set.describe(), self.credential_mode.label())
    }

    /// Probes the configured single bucket before serving so a backend that
    /// cannot be reached or authenticated fails startup instead of degrading to
    /// an empty store (#233). It builds the bucket's client through the same
    /// refreshing provider chain the read path uses, then issues one HEAD on a
    /// sentinel key. A `NotFound` answer counts as success — it proves the origin
    /// was reached and the credentials were accepted (an empty bucket, or the
    /// absent sentinel, is healthy). Any other request error, or a build failure,
    /// is a [`StartupProbeError`] naming the credential chain that was tried.
    ///
    /// A glob-only or bucketless config (dev/test, or an injected store with no
    /// single named bucket) has no concrete bucket to HEAD, so the probe is a
    /// no-op success. The server runs this once at startup and exits on the
    /// error.
    pub async fn probe(&self) -> Result<(), StartupProbeError> {
        let Some(bucket) = self.set.bucket.clone() else {
            return Ok(());
        };
        let credential_chain = self.credential_mode.label();
        let store = self
            .get_or_build(&bucket)
            .map(|clients| clients.store.clone())
            .map_err(|source| StartupProbeError::Build {
                bucket: bucket.clone(),
                credential_chain,
                source,
            })?;
        match store
            .head(&object_store::path::Path::from(STARTUP_PROBE_KEY))
            .await
        {
            Ok(_) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(source) => Err(StartupProbeError::Unreachable {
                bucket,
                credential_chain,
                source,
            }),
        }
    }

    /// A single breaker snapshot aggregated across every built bucket, for the
    /// node-level metrics (#46): trips and rejections summed, and the state
    /// reported as the most-open across buckets (any open ⇒ open, else any
    /// half-open ⇒ half-open, else closed). Buckets not yet touched have built no
    /// breaker and contribute nothing. Reads only — never on a hot path.
    pub fn aggregate_breaker_snapshot(&self) -> BreakerSnapshot {
        let mut trips = 0;
        let mut rejections = 0;
        let mut state = BreakerState::Closed;
        for clients in self.locked().values() {
            let snap = clients.breaker.snapshot();
            trips += snap.trips;
            rejections += snap.rejections;
            state = match (state, snap.state) {
                (BreakerState::Open, _) | (_, BreakerState::Open) => BreakerState::Open,
                (BreakerState::HalfOpen, _) | (_, BreakerState::HalfOpen) => BreakerState::HalfOpen,
                _ => BreakerState::Closed,
            };
        }
        BreakerSnapshot {
            state,
            trips,
            rejections,
        }
    }

    /// Locks the memoization map, tolerating poisoning — a panic while building
    /// one bucket's clients must not wedge every other bucket.
    fn locked(&self) -> std::sync::MutexGuard<'_, HashMap<String, Arc<BucketClients>>> {
        self.clients
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Resolves `bucket`'s clients: rejects a bucket outside the served set with
    /// [`BackendError::NoSuchBucket`], else returns the memoized clients,
    /// building them on first request. A build failure propagates and is not
    /// memoized, so a transient build error can be retried.
    fn get_or_build(&self, bucket: &str) -> Result<Arc<BucketClients>, BackendError> {
        if !self.set.contains(bucket) {
            return Err(BackendError::NoSuchBucket {
                requested: bucket.to_owned(),
                configured: self.set.describe(),
            });
        }
        {
            let clients = self.locked();
            if let Some(existing) = clients.get(bucket) {
                return Ok(existing.clone());
            }
        }
        // Build outside the lock's fast path guard above, then insert under the
        // lock. A concurrent racer that built the same bucket first wins; we keep
        // its entry so the memoized client is a single shared handle.
        let built = Arc::new((self.builder)(bucket)?);
        let mut clients = self.locked();
        Ok(clients.entry(bucket.to_owned()).or_insert(built).clone())
    }
}

impl BackendStores for BackendStore {
    /// Returns `bucket`'s typed store, building and memoizing it on first
    /// request; [`BackendError::NoSuchBucket`] for a bucket outside the set.
    fn store_for(
        &self,
        storage_binding_id: &str,
        bucket: &str,
    ) -> Result<Arc<dyn MultipartObjectStore>, BackendError> {
        self.require_binding(storage_binding_id)?;
        Ok(self.get_or_build(bucket)?.store.clone())
    }

    /// Returns `bucket`'s breaker (building its clients if needed), or `None`
    /// for a bucket outside the set or when a build fails.
    fn breaker_for(
        &self,
        storage_binding_id: &str,
        bucket: &str,
    ) -> Result<Option<Arc<CircuitBreaker>>, BackendError> {
        self.require_binding(storage_binding_id)?;
        Ok(self
            .get_or_build(bucket)
            .ok()
            .map(|clients| clients.breaker.clone()))
    }

    /// Returns `bucket`'s resilient raw client — present when the origin is a
    /// real S3 endpoint ([`BackendStore::from_config`]); `None` for an injected
    /// typed store. A bucket outside the set is [`BackendError::NoSuchBucket`].
    fn raw_for(
        &self,
        storage_binding_id: &str,
        bucket: &str,
    ) -> Result<Option<Arc<ResilientRawS3>>, BackendError> {
        self.require_binding(storage_binding_id)?;
        Ok(self.get_or_build(bucket)?.raw.clone())
    }
}

impl BackendStore {
    /// Rejects requests addressed to a different immutable binding.
    fn require_binding(&self, storage_binding_id: &str) -> Result<(), BackendError> {
        if self.storage_binding_id.trim().is_empty() || storage_binding_id.trim().is_empty() {
            return Err(BackendError::InvalidStorageBinding {
                message: "storage binding ID must not be empty".to_owned(),
            });
        }
        if self.storage_binding_id == storage_binding_id {
            Ok(())
        } else {
            Err(BackendError::UnknownStorageBinding {
                storage_binding_id: storage_binding_id.to_owned(),
            })
        }
    }
}

/// Concurrent registry of independently configured storage backends.
pub struct BackendRegistry {
    /// Immutable binding ID to the backend with its own clients and resilience state.
    bindings: RwLock<HashMap<String, Arc<BackendStore>>>,
}

impl BackendRegistry {
    /// Builds a registry and rejects empty or duplicate binding identities.
    pub fn new(bindings: Vec<Arc<BackendStore>>) -> Result<Arc<Self>, BackendError> {
        let mut indexed = HashMap::with_capacity(bindings.len());
        for binding in bindings {
            let id = binding.storage_binding_id().to_owned();
            if id.trim().is_empty() {
                return Err(BackendError::InvalidStorageBinding {
                    message: "storage binding ID must not be empty".to_owned(),
                });
            }
            if indexed.insert(id.clone(), binding).is_some() {
                return Err(BackendError::DuplicateStorageBinding {
                    storage_binding_id: id,
                });
            }
        }
        Ok(Arc::new(Self {
            bindings: RwLock::new(indexed),
        }))
    }

    /// Registers a new binding for immediate routing without restarting the cache.
    pub fn insert(&self, binding: Arc<BackendStore>) -> Result<(), BackendError> {
        let id = binding.storage_binding_id().to_owned();
        if id.trim().is_empty() {
            return Err(BackendError::InvalidStorageBinding {
                message: "storage binding ID must not be empty".to_owned(),
            });
        }
        let mut bindings = self
            .bindings
            .write()
            .map_err(|_| BackendError::StorageBindingRegistryUnavailable)?;
        if bindings.contains_key(&id) {
            return Err(BackendError::DuplicateStorageBinding {
                storage_binding_id: id,
            });
        }
        bindings.insert(id, binding);
        Ok(())
    }

    /// Removes a binding so later requests fail closed instead of using stale credentials.
    pub fn remove(&self, storage_binding_id: &str) -> Result<Arc<BackendStore>, BackendError> {
        if storage_binding_id.trim().is_empty() {
            return Err(BackendError::InvalidStorageBinding {
                message: "storage binding ID must not be empty".to_owned(),
            });
        }
        self.bindings
            .write()
            .map_err(|_| BackendError::StorageBindingRegistryUnavailable)?
            .remove(storage_binding_id)
            .ok_or_else(|| BackendError::UnknownStorageBinding {
                storage_binding_id: storage_binding_id.to_owned(),
            })
    }

    /// Resolves a registered binding or fails without consulting another origin.
    fn binding(&self, storage_binding_id: &str) -> Result<Arc<BackendStore>, BackendError> {
        self.bindings
            .read()
            .map_err(|_| BackendError::StorageBindingRegistryUnavailable)?
            .get(storage_binding_id)
            .cloned()
            .ok_or_else(|| BackendError::UnknownStorageBinding {
                storage_binding_id: storage_binding_id.to_owned(),
            })
    }
}

impl BackendStores for BackendRegistry {
    /// Routes to the exact binding and then resolves that binding's bucket client.
    fn store_for(
        &self,
        storage_binding_id: &str,
        bucket: &str,
    ) -> Result<Arc<dyn MultipartObjectStore>, BackendError> {
        self.binding(storage_binding_id)?
            .store_for(storage_binding_id, bucket)
    }

    /// Returns only the exact binding and bucket's independent breaker.
    fn breaker_for(
        &self,
        storage_binding_id: &str,
        bucket: &str,
    ) -> Result<Option<Arc<CircuitBreaker>>, BackendError> {
        self.binding(storage_binding_id)?
            .breaker_for(storage_binding_id, bucket)
    }

    /// Returns only the exact binding and bucket's raw S3 client.
    fn raw_for(
        &self,
        storage_binding_id: &str,
        bucket: &str,
    ) -> Result<Option<Arc<ResilientRawS3>>, BackendError> {
        self.binding(storage_binding_id)?
            .raw_for(storage_binding_id, bucket)
    }
}

/// Builds the byte-exact raw HTTP client for `bucket` from the resolved
/// `[backend]` settings (#189/#221). The raw client is SigV4/S3-specific, so it
/// exists only for the S3 provider; azure and gcp return an error here and the
/// caller keeps `raw = None`, routing every request through the typed client.
/// A malformed endpoint fails here; credentials resolve later, for each signed
/// origin request, so temporary sessions can refresh in the running server.
fn build_raw(bucket: &str, backend: &Backend) -> Result<Arc<RawS3>, BackendError> {
    if backend.provider != BackendProvider::S3 {
        return Err(BackendError::Build {
            bucket: bucket.to_owned(),
            source: object_store::Error::Generic {
                store: "RawS3",
                source: "raw client is S3-only; this provider has no raw surface".into(),
            },
        });
    }
    let settings =
        BackendSettings::resolve(backend, &|name| std::env::var(name).ok()).map_err(|e| {
            BackendError::Build {
                bucket: bucket.to_owned(),
                source: object_store::Error::Generic {
                    store: "RawS3",
                    source: e.to_string().into(),
                },
            }
        })?;
    RawS3::from_settings(bucket, &settings, s3_credentials(backend)).map(Arc::new)
}

/// The disabled HTTP-level retry every typed client is built with. Verglas
/// retries one layer up, in [`ResilientStore`], so backoff releases the
/// concurrency permit and each outcome reaches the circuit breaker — neither of
/// which the library's in-call retry can do (see [`crate::retry`]).
fn no_lib_retry() -> RetryConfig {
    RetryConfig {
        max_retries: 0,
        ..RetryConfig::default()
    }
}

/// Builds the S3 client for `bucket` from resolved [`BackendSettings`] (#221):
/// the `[backend]` config endpoint/region/addressing values win, while a
/// refreshing provider resolves AWS credentials as each request is signed. The
/// builder starts from the env (so any `AWS_*` the operator relies on still
/// applies) and then the resolved endpoint values override it, so a configured
/// OCI/MinIO endpoint no longer needs env exports.
fn build_s3(
    bucket: &str,
    backend: &Backend,
) -> Result<Arc<object_store::aws::AmazonS3>, BackendError> {
    let settings =
        BackendSettings::resolve(backend, &|name| std::env::var(name).ok()).map_err(|e| {
            BackendError::Build {
                bucket: bucket.to_owned(),
                source: object_store::Error::Generic {
                    store: "AmazonS3",
                    source: e.to_string().into(),
                },
            }
        })?;
    let builder = AmazonS3Builder::from_env()
        .with_bucket_name(bucket)
        .with_region(&settings.region)
        .with_endpoint(&settings.endpoint)
        .with_allow_http(settings.allow_http)
        .with_virtual_hosted_style_request(settings.virtual_hosted_style)
        .with_credentials(s3_credentials(backend))
        .with_retry(no_lib_retry());
    builder
        .build()
        .map(Arc::new)
        .map_err(|source| BackendError::Build {
            bucket: bucket.to_owned(),
            source,
        })
}

/// Builds the Azure Blob client for the `bucket` container from the resolved
/// Azure origin adapter. A configured file supplies the stable account endpoint
/// and a dynamic shared-key/SAS provider, while client-secret OAuth delegates
/// short-lived access-token refresh to the object-store client. Without a file,
/// the Azure environment/default identity chain supplies and refreshes tokens.
/// A configured `[backend]` endpoint overrides the default Blob host.
fn build_azure(
    bucket: &str,
    backend: &Backend,
) -> Result<Arc<object_store::azure::MicrosoftAzure>, BackendError> {
    let credentials_file = backend
        .credentials_file
        .as_ref()
        .filter(|file| !file.is_empty());
    let mut builder = match credentials_file {
        Some(file) => {
            let creds = AzureCredentials::resolve(backend, &|name| std::env::var(name).ok())
                .map_err(|error| azure_gcp_error(bucket, "MicrosoftAzure", error))?;
            let builder = MicrosoftAzureBuilder::new()
                .with_container_name(bucket)
                .with_account(&creds.account)
                .with_allow_http(backend.allow_http)
                .with_retry(no_lib_retry());
            match &creds.auth {
                AzureAuth::AccountKey(_) | AzureAuth::SasToken(_) => builder.with_credentials(
                    azure_file_credentials(std::path::PathBuf::from(file), creds.account),
                ),
                AzureAuth::ClientSecret {
                    client_id,
                    client_secret,
                    tenant_id,
                } => builder.with_client_secret_authorization(client_id, client_secret, tenant_id),
            }
        }
        None => MicrosoftAzureBuilder::from_env()
            .with_container_name(bucket)
            .with_allow_http(backend.allow_http)
            .with_retry(no_lib_retry()),
    };
    if let Some(endpoint) = backend.endpoint.as_ref().filter(|s| !s.is_empty()) {
        builder = builder.with_endpoint(endpoint.clone());
    }
    builder
        .build()
        .map(Arc::new)
        .map_err(|source| BackendError::Build {
            bucket: bucket.to_owned(),
            source,
        })
}

/// Builds the Google Cloud Storage client for `bucket` through the GCP origin
/// adapter. A configured credentials file selects an explicit service-account
/// path; otherwise application-default credentials or workload metadata supply
/// refreshable OAuth tokens. GCS has no endpoint/region overrides here.
fn build_gcp(
    bucket: &str,
    backend: &Backend,
) -> Result<Arc<object_store::gcp::GoogleCloudStorage>, BackendError> {
    let mut builder = GoogleCloudStorageBuilder::from_env()
        .with_bucket_name(bucket)
        .with_retry(no_lib_retry());
    if backend
        .credentials_file
        .as_ref()
        .is_some_and(|file| !file.is_empty())
    {
        let creds = GcpCredentials::resolve(backend, &|name| std::env::var(name).ok())
            .map_err(|error| azure_gcp_error(bucket, "GoogleCloudStorage", error))?;
        builder = builder.with_service_account_path(&creds.service_account_path);
    }
    builder
        .build()
        .map(Arc::new)
        .map_err(|source| BackendError::Build {
            bucket: bucket.to_owned(),
            source,
        })
}

/// Re-tags a credential-resolution [`BackendError`] with the bucket and the
/// object-store name for the azure/gcp build path, so the operator sees which
/// client failed to construct.
fn azure_gcp_error(bucket: &str, store: &'static str, err: BackendError) -> BackendError {
    BackendError::Build {
        bucket: bucket.to_owned(),
        source: object_store::Error::Generic {
            store,
            source: err.to_string().into(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::{Arc, Mutex};

    use axum::Router;
    use axum::extract::State;
    use axum::http::{HeaderMap, StatusCode, header::AUTHORIZATION};
    use axum::routing::any;
    use object_store::ObjectStoreExt;
    use object_store::azure::AzureCredential;
    use object_store::path::Path;

    /// Recorded SigV4 access keys from the origin fixture.
    #[derive(Clone, Default)]
    struct OriginRequests {
        /// Access-key IDs extracted from each origin request's Authorization header.
        access_keys: Arc<Mutex<Vec<String>>>,
    }

    /// Records the access key that signed an origin request and returns a valid HEAD response.
    async fn record_origin_request(
        State(requests): State<OriginRequests>,
        headers: HeaderMap,
    ) -> (StatusCode, [(&'static str, &'static str); 2]) {
        let authorization = headers
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .expect("origin request carries an Authorization header");
        let credential = authorization
            .split("Credential=")
            .nth(1)
            .and_then(|value| value.split('/').next())
            .expect("SigV4 Authorization carries a credential scope");
        requests
            .access_keys
            .lock()
            .expect("origin request recorder is not poisoned")
            .push(credential.to_owned());
        (
            StatusCode::OK,
            [("content-length", "3"), ("etag", "\"origin-etag\"")],
        )
    }

    /// Starts a local origin fixture that records every origin SigV4 access key.
    async fn start_origin_fixture() -> (String, OriginRequests, tokio::task::JoinHandle<()>) {
        let requests = OriginRequests::default();
        let app = Router::new()
            .fallback(any(record_origin_request))
            .with_state(requests.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind origin fixture");
        let endpoint = format!("http://{}", listener.local_addr().expect("fixture address"));
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("origin fixture serves requests");
        });
        (endpoint, requests, server)
    }

    /// Writes `contents` to a fresh temp credentials file and returns its path.
    fn write_creds(tag: &str, contents: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "verglas-provider-{tag}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&dir).expect("mkdir");
        let path = dir.join("creds");
        std::fs::write(&path, contents).expect("write creds");
        path
    }

    /// #245: the same typed and raw origin clients must re-read a rotated
    /// credentials file instead of retaining the first temporary session for
    /// their full process lifetime.
    #[tokio::test]
    async fn s3_clients_use_replacement_credentials_file_without_restart() {
        let (endpoint, requests, server) = start_origin_fixture().await;
        let credentials = write_creds(
            "rotating",
            "[default]\naws_access_key_id = FIRST\naws_secret_access_key = first-secret\n",
        );
        let backend = Backend {
            provider: BackendProvider::S3,
            bucket: Some("bucket".into()),
            endpoint: Some(endpoint),
            region: Some("us-east-1".into()),
            allow_http: true,
            credentials_file: Some(credentials.display().to_string()),
            ..Backend::default()
        };
        let typed = OriginProvider::from_backend(backend.clone())
            .build_typed("bucket")
            .expect("typed client builds");
        let raw = build_raw("bucket", &backend).expect("raw client builds");

        raw.head("object")
            .await
            .expect("raw request with first key");
        typed
            .head(&Path::from("object"))
            .await
            .expect("typed request with first key");

        std::fs::write(
            &credentials,
            "[default]\naws_access_key_id = SECOND\naws_secret_access_key = second-secret\n",
        )
        .expect("replace credentials file");

        raw.head("object")
            .await
            .expect("raw request with replacement key");
        typed
            .head(&Path::from("object"))
            .await
            .expect("typed request with replacement key");

        let access_keys = requests
            .access_keys
            .lock()
            .expect("origin request recorder is not poisoned")
            .clone();
        assert_eq!(access_keys, ["FIRST", "FIRST", "SECOND", "SECOND"]);
        server.abort();
    }

    /// #245: losing the operator-managed credentials file must fail the next
    /// origin request promptly instead of using the last expired key forever.
    #[tokio::test]
    async fn missing_replacement_credentials_surface_without_an_origin_request() {
        let (endpoint, requests, server) = start_origin_fixture().await;
        let credentials = write_creds(
            "missing-after-rotation",
            "[default]\naws_access_key_id = FIRST\naws_secret_access_key = first-secret\n",
        );
        let backend = Backend {
            provider: BackendProvider::S3,
            bucket: Some("bucket".into()),
            endpoint: Some(endpoint),
            region: Some("us-east-1".into()),
            allow_http: true,
            credentials_file: Some(credentials.display().to_string()),
            ..Backend::default()
        };
        let typed = OriginProvider::from_backend(backend.clone())
            .build_typed("bucket")
            .expect("typed client builds");
        let raw = build_raw("bucket", &backend).expect("raw client builds");
        std::fs::remove_file(&credentials).expect("remove credentials file");

        let raw_error =
            tokio::time::timeout(std::time::Duration::from_millis(250), raw.head("object"))
                .await
                .expect("raw credential refresh does not hang")
                .expect_err("raw credential refresh fails");
        assert!(matches!(raw_error, RawError::Credentials(_)));

        let typed_error = tokio::time::timeout(
            std::time::Duration::from_millis(250),
            typed.head(&Path::from("object")),
        )
        .await
        .expect("typed credential refresh does not hang")
        .expect_err("typed credential refresh fails");
        assert!(typed_error.to_string().contains("backend.credentials_file"));

        assert!(
            requests
                .access_keys
                .lock()
                .expect("origin request recorder is not poisoned")
                .is_empty(),
            "a failed refresh must not send a stale signed origin request"
        );
        server.abort();
    }

    /// #245: an Azure SAS supplied through the configured credentials file must
    /// be re-read by an already-built client, just like an AWS session file.
    #[tokio::test]
    async fn azure_client_uses_replacement_sas_file_without_restart() {
        let credentials = write_creds(
            "azure-rotating",
            "azure_storage_account_name = account\nazure_storage_sas_key = sv=2023-11-03&sig=FIRST\n",
        );
        let backend = Backend {
            provider: BackendProvider::Azure,
            bucket: Some("container".into()),
            credentials_file: Some(credentials.display().to_string()),
            ..Backend::default()
        };
        let client = build_azure("container", &backend).expect("azure client builds");

        let first = client
            .credentials()
            .get_credential()
            .await
            .expect("first SAS credential");
        assert!(matches!(
            first.as_ref(),
            AzureCredential::SASToken(pairs)
                if pairs.iter().any(|(key, value)| key == "sig" && value == "FIRST")
        ));

        std::fs::write(
            &credentials,
            "azure_storage_account_name = account\nazure_storage_sas_key = sv=2023-11-03&sig=SECOND\n",
        )
        .expect("replace Azure credentials file");

        let second = client
            .credentials()
            .get_credential()
            .await
            .expect("replacement SAS credential");
        assert!(matches!(
            second.as_ref(),
            AzureCredential::SASToken(pairs)
                if pairs.iter().any(|(key, value)| key == "sig" && value == "SECOND")
        ));
    }

    /// #245: GCP must build from its ambient application/default identity rather
    /// than requiring a static credentials file before a request needs signing.
    #[test]
    fn gcp_client_defers_to_ambient_identity_without_a_static_file() {
        let backend = Backend {
            provider: BackendProvider::Gcp,
            bucket: Some("bucket".into()),
            ..Backend::default()
        };
        build_gcp("bucket", &backend).expect("GCP client defers credential lookup");
    }

    /// Client construction picks the right builder per provider and builds the
    /// client config without a live network. Each provider's credentials come
    /// from a file (no env, no round-trip against a real object store), so the assertion
    /// is that selection + construction succeed, not that the bytes move.
    #[test]
    fn origin_provider_selects_the_builder_per_provider() {
        // S3: config endpoint/region + an AWS-format credentials file.
        let s3_creds = write_creds(
            "s3",
            "[default]\naws_access_key_id = AK\naws_secret_access_key = SK\n",
        );
        let s3 = Backend {
            provider: BackendProvider::S3,
            bucket: Some("s3-bucket".into()),
            endpoint: Some("https://s3.example.com".into()),
            region: Some("us-east-1".into()),
            credentials_file: Some(s3_creds.display().to_string()),
            ..Backend::default()
        };
        OriginProvider::from_backend(s3)
            .build_typed("s3-bucket")
            .expect("s3 client builds");

        // Azure: account name + account key from the key=value file. The account
        // key must be valid base64 — object-store decodes it at build time.
        let az_creds = write_creds(
            "az",
            "azure_storage_account_name = acct\nazure_storage_account_key = dGVzdGtleQ==\n",
        );
        let azure = Backend {
            provider: BackendProvider::Azure,
            bucket: Some("container".into()),
            credentials_file: Some(az_creds.display().to_string()),
            ..Backend::default()
        };
        OriginProvider::from_backend(azure)
            .build_typed("container")
            .expect("azure client builds");

        // GCP: a service-account path from the key=value file. object-store reads
        // and parses the JSON at build time, so the path must point at a real
        // service-account file. `disable_oauth` lets the client build without a
        // real signing key — enough to prove the GCP builder was selected without
        // a live network.
        let sa_json = write_creds(
            "gcp-sa",
            r#"{"private_key":"private_key","private_key_id":"private_key_id","client_email":"svc@example.iam.gserviceaccount.com","disable_oauth":true}"#,
        );
        let gcp_creds = write_creds(
            "gcp",
            &format!("google_service_account_path = {}\n", sa_json.display()),
        );
        let gcp = Backend {
            provider: BackendProvider::Gcp,
            bucket: Some("gcs-bucket".into()),
            credentials_file: Some(gcp_creds.display().to_string()),
            ..Backend::default()
        };
        OriginProvider::from_backend(gcp)
            .build_typed("gcs-bucket")
            .expect("gcp client builds");
    }

    /// The raw byte-exact client is S3-only. It builds for S3 and is refused for
    /// azure/gcp, so [`BackendStore::from_config`] leaves `raw = None` there and
    /// requests route through the typed client.
    #[test]
    fn raw_client_is_s3_only() {
        let s3_creds = write_creds(
            "raws3",
            "[default]\naws_access_key_id = AK\naws_secret_access_key = SK\n",
        );
        let s3 = Backend {
            provider: BackendProvider::S3,
            bucket: Some("b".into()),
            endpoint: Some("https://s3.example.com".into()),
            credentials_file: Some(s3_creds.display().to_string()),
            ..Backend::default()
        };
        assert!(build_raw("b", &s3).is_ok(), "s3 origin is raw-addressable");

        for provider in [BackendProvider::Azure, BackendProvider::Gcp] {
            let backend = Backend {
                provider,
                bucket: Some("b".into()),
                ..Backend::default()
            };
            assert!(
                build_raw("b", &backend).is_err(),
                "{provider:?} has no raw surface"
            );
        }
    }
}
