//! Strict signed S3-shaped endpoint used by managed-CAS process tests.

use std::collections::HashMap;
use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{OriginalUri, State};
use axum::http::header::{
    AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, ETAG, IF_MATCH, IF_NONE_MATCH, LAST_MODIFIED,
};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use chrono::Utc;
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion};
use sha2::{Digest, Sha256};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, oneshot};
use tokio::task::JoinHandle;
use uuid::Uuid;

const BUCKET: &str = "managed-cas";
const REGION: &str = "us-east-1";
const ACCESS_KEY_ID: &str = "cas-test-access";
const SECRET_ACCESS_KEY: &str = "cas-test-secret";

/// Provider-neutral connection details for the strict integration endpoint.
#[derive(Clone, Debug)]
pub struct StoreDescriptor {
    /// HTTP endpoint where the object store listens.
    pub endpoint: String,
    /// Bucket name sent in path-style S3 requests.
    pub bucket: String,
    /// Signing region used by the S3 client.
    pub region: String,
    /// Explicit access key used for request signing.
    pub access_key_id: String,
    /// Explicit secret key used for request signing.
    pub secret_access_key: String,
}

impl StoreDescriptor {
    /// Builds the signed path-style client used by both the launcher and inspector.
    pub fn build_client(&self) -> object_store::Result<Arc<dyn ObjectStore>> {
        let store = AmazonS3Builder::new()
            .with_bucket_name(&self.bucket)
            .with_region(&self.region)
            .with_endpoint(&self.endpoint)
            .with_allow_http(true)
            .with_virtual_hosted_style_request(false)
            .with_access_key_id(&self.access_key_id)
            .with_secret_access_key(&self.secret_access_key)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .build()?;
        Ok(Arc::new(store))
    }
}

/// Parent-owned strict endpoint and its object-store inspector client.
pub struct StoreEndpoint {
    descriptor: StoreDescriptor,
    inspector: Arc<dyn ObjectStore>,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl StoreEndpoint {
    /// Starts an empty endpoint on an operating-system-selected loopback port.
    pub async fn start() -> object_store::Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.map_err(|error| {
            object_store::Error::Generic {
                store: "strict-test-s3",
                source: error.to_string().into(),
            }
        })?;
        let address = listener
            .local_addr()
            .map_err(|error| object_store::Error::Generic {
                store: "strict-test-s3",
                source: error.to_string().into(),
            })?;
        let descriptor = StoreDescriptor {
            endpoint: format!("http://{address}"),
            bucket: BUCKET.to_owned(),
            region: REGION.to_owned(),
            access_key_id: ACCESS_KEY_ID.to_owned(),
            secret_access_key: SECRET_ACCESS_KEY.to_owned(),
        };
        let inspector = descriptor.build_client()?;
        let state = Arc::new(StoreState {
            bucket: descriptor.bucket.clone(),
            objects: Mutex::new(HashMap::new()),
        });
        let application = Router::new()
            .fallback(any(handle_request))
            .with_state(state);
        let (shutdown, signal) = oneshot::channel();
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, application)
                .with_graceful_shutdown(async {
                    let _ = signal.await;
                })
                .await;
        });
        Ok(Self {
            descriptor,
            inspector,
            shutdown: Some(shutdown),
            task: Some(task),
        })
    }

    /// Returns the explicit store fields needed to launch a child process.
    pub fn descriptor(&self) -> &StoreDescriptor {
        &self.descriptor
    }

    /// Returns the parent-side client used only for object-store inspection.
    pub fn inspector(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.inspector)
    }

    /// Proves the endpoint's conditional semantics before any worker launches.
    pub async fn capability_probe(&self) -> object_store::Result<()> {
        let path = Path::from(format!("capability/{}", Uuid::new_v4()));
        let first = self
            .inspector
            .put_opts(
                &path,
                Bytes::from_static(b"first").into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await?;
        let first_etag = first
            .e_tag
            .clone()
            .ok_or_else(|| object_store::Error::Generic {
                store: "strict-test-s3",
                source: "capability create did not return an ETag".into(),
            })?;
        let metadata = self.inspector.head(&path).await?;
        if metadata.e_tag.as_deref() != Some(first_etag.as_str()) || metadata.size != 5 {
            return Err(object_store::Error::Generic {
                store: "strict-test-s3",
                source: "capability HEAD did not expose the created object".into(),
            });
        }
        let bytes = self.inspector.get(&path).await?.bytes().await?;
        if bytes.as_ref() != b"first" {
            return Err(object_store::Error::Generic {
                store: "strict-test-s3",
                source: "capability GET did not expose the created object".into(),
            });
        }
        let duplicate = self
            .inspector
            .put_opts(
                &path,
                Bytes::from_static(b"duplicate").into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await;
        if !matches!(duplicate, Err(object_store::Error::AlreadyExists { .. })) {
            return Err(object_store::Error::Generic {
                store: "strict-test-s3",
                source: format!("capability create race returned {duplicate:?}").into(),
            });
        }
        let second = self
            .inspector
            .put_opts(
                &path,
                Bytes::from_static(b"second").into(),
                PutOptions {
                    mode: PutMode::Update(UpdateVersion {
                        e_tag: Some(first_etag.clone()),
                        version: first.version.clone(),
                    }),
                    ..Default::default()
                },
            )
            .await?;
        let second_etag = second
            .e_tag
            .clone()
            .ok_or_else(|| object_store::Error::Generic {
                store: "strict-test-s3",
                source: "capability update did not return an ETag".into(),
            })?;
        if second_etag == first_etag {
            return Err(object_store::Error::Generic {
                store: "strict-test-s3",
                source: "capability update did not change the ETag".into(),
            });
        }
        let stale = self
            .inspector
            .put_opts(
                &path,
                Bytes::from_static(b"stale").into(),
                PutOptions {
                    mode: PutMode::Update(UpdateVersion {
                        e_tag: Some(first_etag),
                        version: first.version,
                    }),
                    ..Default::default()
                },
            )
            .await;
        if !matches!(stale, Err(object_store::Error::Precondition { .. })) {
            return Err(object_store::Error::Generic {
                store: "strict-test-s3",
                source: format!("capability stale update returned {stale:?}").into(),
            });
        }
        let final_bytes = self.inspector.get(&path).await?.bytes().await?;
        if final_bytes.as_ref() != b"second" {
            return Err(object_store::Error::Generic {
                store: "strict-test-s3",
                source: "capability read-after-write returned stale bytes".into(),
            });
        }
        self.inspector.delete(&path).await
    }

    /// Stops the endpoint and waits for its listener task to finish.
    pub async fn stop(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for StoreEndpoint {
    /// Aborts the listener if a test exits without an explicit stop.
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

struct StoreState {
    bucket: String,
    objects: Mutex<HashMap<String, StoredObject>>,
}

struct StoredObject {
    bytes: Bytes,
    e_tag: String,
    last_modified: String,
}

/// Serves the small strict subset required by object_store's signed S3 client.
async fn handle_request(
    State(state): State<Arc<StoreState>>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if headers.get(AUTHORIZATION).is_none() {
        return s3_error(
            StatusCode::FORBIDDEN,
            "AccessDenied",
            "signed authorization is required",
        );
    }
    if method == Method::GET && uri.query().is_some_and(is_list_query) {
        return list_objects(&state, &uri).await;
    }
    if method == Method::POST && uri.query().is_some_and(is_bulk_delete_query) {
        return bulk_delete_objects(&state, &body).await;
    }
    let Some(key) = object_key(&uri, &state.bucket) else {
        return s3_error(StatusCode::NOT_FOUND, "NoSuchKey", "object path is invalid");
    };
    match method {
        Method::PUT => put_object(&state, &key, &headers, body).await,
        Method::GET => read_object(&state, &key, false).await,
        Method::HEAD => read_object(&state, &key, true).await,
        Method::DELETE => delete_object(&state, &key).await,
        _ => s3_error(
            StatusCode::METHOD_NOT_ALLOWED,
            "MethodNotAllowed",
            "method is not supported",
        ),
    }
}

/// Extracts a path-style object key without decoding the signed URI.
fn object_key(uri: &Uri, bucket: &str) -> Option<String> {
    uri.path()
        .strip_prefix('/')?
        .strip_prefix(bucket)?
        .strip_prefix('/')
        .filter(|key| !key.is_empty())
        .map(ToOwned::to_owned)
}

/// Identifies the ListObjectsV2 query emitted by the object-store client.
fn is_list_query(query: &str) -> bool {
    query.split('&').any(|field| field == "list-type=2")
}

/// Identifies the S3 multi-object-delete query emitted by the capability probe.
fn is_bulk_delete_query(query: &str) -> bool {
    query
        .split('&')
        .any(|field| field == "delete" || field == "delete=")
}

/// Returns one decoded query parameter from a path-style S3 request.
fn query_parameter(query: Option<&str>, name: &str) -> Option<String> {
    query?.split('&').find_map(|field| {
        let (key, value) = field.split_once('=')?;
        (key == name).then(|| value.replace("%2F", "/").replace("%2f", "/"))
    })
}

/// Lists matching immutable objects in the strict test bucket.
async fn list_objects(state: &StoreState, uri: &Uri) -> Response {
    let prefix = query_parameter(uri.query(), "prefix").unwrap_or_default();
    let objects = state.objects.lock().await;
    let mut matching = objects
        .iter()
        .filter(|(key, _)| key.starts_with(&prefix))
        .collect::<Vec<_>>();
    matching.sort_by(|left, right| left.0.cmp(right.0));
    let mut xml = format!(
        "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>{}</Name><Prefix>{}</Prefix><KeyCount>{}</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>",
        xml_escape(&state.bucket),
        xml_escape(&prefix),
        matching.len()
    );
    for (key, object) in matching {
        xml.push_str(&format!(
            "<Contents><Key>{}</Key><LastModified>{}</LastModified><ETag>{}</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents>",
            xml_escape(key),
            list_timestamp(&object.last_modified),
            xml_escape(&object.e_tag),
            object.bytes.len()
        ));
    }
    xml.push_str("</ListBucketResult>");
    let mut response = xml.into_response();
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/xml"));
    response
}

/// Deletes all keys named by one S3 multi-object-delete XML request.
async fn bulk_delete_objects(state: &StoreState, body: &Bytes) -> Response {
    let body = String::from_utf8_lossy(body);
    let mut keys = Vec::new();
    let mut remainder = body.as_ref();
    while let Some(start) = remainder.find("<Key>") {
        let value = &remainder[start + 5..];
        let Some(end) = value.find("</Key>") else {
            break;
        };
        keys.push(value[..end].to_owned());
        remainder = &value[end + 6..];
    }
    let mut objects = state.objects.lock().await;
    let mut response_body =
        "<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">".to_owned();
    for key in keys {
        objects.remove(&key);
        response_body.push_str(&format!(
            "<Deleted><Key>{}</Key></Deleted>",
            xml_escape(&key)
        ));
    }
    response_body.push_str("</DeleteResult>");
    let mut response = response_body.into_response();
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/xml"));
    response
}

/// Converts the HTTP timestamp into the RFC3339 form used by S3 listings.
fn list_timestamp(value: &str) -> String {
    match chrono::DateTime::parse_from_rfc2822(value) {
        Ok(timestamp) => timestamp.with_timezone(&Utc).to_rfc3339(),
        Err(_) => value.to_owned(),
    }
}

/// Escapes the small set of XML characters allowed in S3 object metadata.
fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

/// Applies an atomic create or ETag-matched update while holding one state lock.
async fn put_object(state: &StoreState, key: &str, headers: &HeaderMap, bytes: Bytes) -> Response {
    let mut objects = state.objects.lock().await;
    let existing = objects.get(key);
    if headers
        .get(IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.trim() == "*")
    {
        if existing.is_some() {
            return s3_error(
                StatusCode::PRECONDITION_FAILED,
                "PreconditionFailed",
                "object already exists",
            );
        }
    } else if let Some(expected) = headers.get(IF_MATCH).and_then(|value| value.to_str().ok()) {
        let matches = existing.is_some_and(|object| object.e_tag == expected.trim());
        if !matches {
            return s3_error(
                StatusCode::PRECONDITION_FAILED,
                "PreconditionFailed",
                "object ETag does not match",
            );
        }
    } else {
        return s3_error(
            StatusCode::PRECONDITION_FAILED,
            "PreconditionRequired",
            "conditional write is required",
        );
    }
    let e_tag = format!("\"{}\"", hex::encode(Sha256::digest(&bytes)));
    let last_modified = Utc::now().to_rfc2822();
    objects.insert(
        key.to_owned(),
        StoredObject {
            bytes,
            e_tag: e_tag.clone(),
            last_modified,
        },
    );
    let mut response = StatusCode::OK.into_response();
    let response_headers = response.headers_mut();
    response_headers.insert(ETAG, HeaderValue::from_str(&e_tag).expect("valid ETag"));
    response_headers.insert(CONTENT_LENGTH, HeaderValue::from_static("0"));
    response
}

/// Returns one object with metadata required by object_store's S3 parser.
async fn read_object(state: &StoreState, key: &str, head_only: bool) -> Response {
    let objects = state.objects.lock().await;
    let Some(object) = objects.get(key) else {
        return s3_error(StatusCode::NOT_FOUND, "NoSuchKey", "object does not exist");
    };
    let mut response = if head_only {
        Bytes::new().into_response()
    } else {
        object.bytes.clone().into_response()
    };
    let response_headers = response.headers_mut();
    response_headers.insert(
        ETAG,
        HeaderValue::from_str(&object.e_tag).expect("valid ETag"),
    );
    response_headers.insert(
        CONTENT_LENGTH,
        HeaderValue::from_str(&object.bytes.len().to_string()).expect("valid length"),
    );
    response_headers.insert(
        LAST_MODIFIED,
        HeaderValue::from_str(&object.last_modified).expect("valid timestamp"),
    );
    response
}

/// Deletes one object for capability-probe cleanup.
async fn delete_object(state: &StoreState, key: &str) -> Response {
    state.objects.lock().await.remove(key);
    StatusCode::NO_CONTENT.into_response()
}

/// Produces the S3-shaped XML errors used to exercise object_store's mapping.
fn s3_error(status: StatusCode, code: &str, message: &str) -> Response {
    let request_id = Uuid::new_v4();
    let body = format!(
        "<Error><Code>{code}</Code><Message>{message}</Message><RequestId>{request_id}</RequestId></Error>"
    );
    let mut response = (status, body).into_response();
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/xml"));
    response
}
