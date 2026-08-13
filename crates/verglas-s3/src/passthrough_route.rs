//! Passthrough-by-default for unmodeled bucket-config operations (issue #152).
//!
//! Some S3 requests are pure control-plane calls about a bucket — HeadBucket,
//! GetBucketLocation, and the rest of the bucket-config surface — that Verglas
//! does not model and would otherwise answer `501 NotImplemented`, breaking
//! client tools whose init handshake issues one of them. This module forwards a
//! reviewed **allowlist** of such operations verbatim to the origin, re-signed
//! with the node credentials, and streams the origin's response back untouched.
//!
//! # Where it hooks in
//!
//! It is an [`S3Route`], which s3s consults *before* its typed operation
//! dispatch: [`is_match`](S3Route::is_match) recognizes an allowlisted op and
//! [`call`](S3Route::call) forwards it, so the request never reaches the typed
//! handler that would 501. The client's SigV4 signature is validated by the
//! auth layer first, exactly as for a modeled request; the upstream request is
//! then signed afresh with the node credentials — the same re-signing the write
//! path already does — via the byte-exact raw client
//! ([`verglas_backend::ResilientRawS3`], reused, not duplicated). An operation
//! NOT on the allowlist is not matched here and keeps s3s's default 501.
//!
//! # Allowlist, not blanket
//!
//! The allowlist starts deliberately small — HeadBucket and GetBucketLocation,
//! the two the whitepaper calls out as SDK-handshake calls — rather than
//! blind-forwarding anything unrecognized. It expands one group at a time as
//! the conformance suite proves each; the classifier below is the single place
//! a new op is added. Path-style addressing only (`/{bucket}[/{key}]`), which is
//! what the raw client and the whole server use.
//!
//! # Cache correctness
//!
//! Forwarding a bucket-config operation is safe for the cache by construction:
//! these calls never read or write object bytes, and every cached block is
//! keyed by its object's ETag (#14), so no bucket-config mutation — including
//! enabling versioning on the origin — can invalidate or stale a cached block.
//! A forwarded response is therefore never cached and touches neither the block
//! nor the meta cache: `call` talks only to the raw origin client.

use std::sync::Arc;

use axum::http::{Extensions, HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri};
use s3s::route::S3Route;
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result};
use verglas_backend::{BackendStores, RawError, RawForwardResponse};

/// An unmodeled bucket-config operation Verglas forwards to the origin. Each
/// variant fixes the HTTP method and the SigV4-canonical query string the
/// forwarded request carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ForwardOp {
    /// `HEAD /{bucket}` — does the bucket exist / is it reachable.
    HeadBucket,
    /// `GET /{bucket}?location` — the bucket's region.
    GetBucketLocation,
}

impl ForwardOp {
    /// The HTTP method the forwarded request uses.
    fn method(self) -> &'static str {
        match self {
            ForwardOp::HeadBucket => "HEAD",
            ForwardOp::GetBucketLocation => "GET",
        }
    }

    /// The SigV4-canonical query string the forwarded request carries (a
    /// valueless sub-resource is canonicalized as `key=`).
    fn query(self) -> &'static str {
        match self {
            ForwardOp::HeadBucket => "",
            ForwardOp::GetBucketLocation => "location=",
        }
    }
}

/// Classifies a request into an allowlisted [`ForwardOp`], or `None` when
/// Verglas models the request (so s3s should handle it) or it is not on the
/// allowlist (so s3s's default 501 stands). A request is bucket-level only when
/// its path is exactly `/{bucket}` or `/{bucket}/` — an object key (a further
/// path segment) is never a bucket-config op, so GetObject/HeadObject are never
/// hijacked.
fn classify(method: &Method, uri: &Uri) -> Option<ForwardOp> {
    let rest = uri.path().strip_prefix('/')?;
    let bucket_level = match rest.split_once('/') {
        // "/bucket" — one segment, the bucket.
        None => !rest.is_empty(),
        // "/bucket/..." — bucket-level only when nothing follows the slash.
        Some((bucket, key)) => !bucket.is_empty() && key.is_empty(),
    };
    if !bucket_level {
        return None;
    }
    let has_location = has_query_key(uri.query(), "location");
    match *method {
        // A bucket-level HEAD is HeadBucket (HeadObject always carries a key).
        Method::HEAD if !has_location => Some(ForwardOp::HeadBucket),
        // A bucket-level GET is a listing unless it names the ?location
        // sub-resource, which only GetBucketLocation does.
        Method::GET if has_location => Some(ForwardOp::GetBucketLocation),
        _ => None,
    }
}

/// Whether the query string contains a parameter named `key` (value ignored),
/// e.g. `location` in `?location` or `?location=`.
fn has_query_key(query: Option<&str>, key: &str) -> bool {
    query.is_some_and(|q| {
        q.split('&')
            .any(|pair| pair.split('=').next().unwrap_or(pair) == key)
    })
}

/// The bucket name from a path-style URI (`/{bucket}[/...]`).
fn bucket_of(uri: &Uri) -> Option<&str> {
    let rest = uri.path().strip_prefix('/')?;
    let bucket = rest.split('/').next()?;
    (!bucket.is_empty()).then_some(bucket)
}

/// The s3s custom route that forwards allowlisted bucket-config operations to
/// the origin (issue #152). Holds the backend store so it can resolve the
/// configured bucket's raw client that carries the node credentials and the
/// bucket's resilience budget.
pub struct BucketConfigPassthrough {
    /// Immutable storage binding assigned to this S3 endpoint.
    storage_binding_id: String,
    /// The backend store that hands out the configured bucket's raw origin
    /// client.
    stores: Arc<dyn BackendStores>,
}

impl BucketConfigPassthrough {
    /// Builds the route over the backend store the server already shares
    /// with the read/write passthroughs.
    pub fn new(storage_binding_id: impl Into<String>, stores: Arc<dyn BackendStores>) -> Self {
        BucketConfigPassthrough {
            storage_binding_id: storage_binding_id.into(),
            stores,
        }
    }
}

#[async_trait::async_trait]
impl S3Route for BucketConfigPassthrough {
    /// Matches exactly the allowlisted bucket-config operations, checked before
    /// s3s's typed dispatch. Pure and cheap — it runs on every request.
    fn is_match(
        &self,
        method: &Method,
        uri: &Uri,
        _headers: &HeaderMap,
        _extensions: &mut Extensions,
    ) -> bool {
        classify(method, uri).is_some()
    }

    /// Forwards the matched operation to the origin and returns its response
    /// verbatim. The signature was validated by the auth layer before routing
    /// (the default [`check_access`](S3Route::check_access) requires
    /// credentials); the upstream request is re-signed with the node
    /// credentials inside the raw client.
    async fn call(&self, req: S3Request<Body>) -> S3Result<S3Response<Body>> {
        let op = classify(&req.method, &req.uri).ok_or_else(not_supported)?;
        let bucket = bucket_of(&req.uri).ok_or_else(not_supported)?;
        // No raw surface (a store over an injected typed store, e.g. unit
        // tests) cannot forward — fall back to the default 501. A bucket other
        // than the one served surfaces as a backend error below.
        let client = self
            .stores
            .raw_for(&self.storage_binding_id, bucket)
            .map_err(|error| {
                S3Error::with_message(
                    S3ErrorCode::InternalError,
                    format!("backend unavailable for `{bucket}`: {error}"),
                )
            })?
            .ok_or_else(not_supported)?;
        let forwarded = client
            .forward_bucket(op.method(), op.query())
            .await
            .map_err(forward_error)?;
        build_response(forwarded)
    }
}

/// The 501 for an unforwardable request — either not on the allowlist or a
/// backend with no raw surface. Mirrors s3s's own default so clients see one
/// vocabulary.
fn not_supported() -> S3Error {
    S3Error::with_message(
        S3ErrorCode::NotImplemented,
        "This operation is not supported by Verglas",
    )
}

/// Maps a raw-client transport failure (not an origin status — those forward
/// verbatim) onto a 500.
fn forward_error(error: RawError) -> S3Error {
    S3Error::with_message(
        S3ErrorCode::InternalError,
        format!("forwarding to origin failed: {error}"),
    )
}

/// Rebuilds the origin's forwarded response as an s3s response: its status
/// verbatim (s3s honours a custom route's status), its headers, and the
/// buffered body. Header names/values the http types reject are dropped rather
/// than failing the whole forward.
fn build_response(forwarded: RawForwardResponse) -> S3Result<S3Response<Body>> {
    let status = StatusCode::from_u16(forwarded.status).map_err(|error| {
        S3Error::with_message(
            S3ErrorCode::InternalError,
            format!("origin returned an invalid status: {error}"),
        )
    })?;
    let mut headers = HeaderMap::new();
    for (name, value) in &forwarded.headers {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(value),
        ) {
            headers.insert(name, value);
        }
    }
    let mut response = S3Response::new(Body::from(forwarded.body));
    response.status = Some(status);
    response.headers = headers;
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a URI from a path-and-query string for classification tests.
    fn uri(s: &str) -> Uri {
        s.parse().expect("valid uri")
    }

    /// HeadBucket: a bucket-level HEAD, with or without a trailing slash.
    #[test]
    fn classifies_head_bucket() {
        assert_eq!(
            classify(&Method::HEAD, &uri("/mybucket")),
            Some(ForwardOp::HeadBucket)
        );
        assert_eq!(
            classify(&Method::HEAD, &uri("/mybucket/")),
            Some(ForwardOp::HeadBucket)
        );
    }

    /// GetBucketLocation: a bucket-level GET naming the ?location sub-resource,
    /// whether or not it carries an `=`.
    #[test]
    fn classifies_get_bucket_location() {
        assert_eq!(
            classify(&Method::GET, &uri("/mybucket?location")),
            Some(ForwardOp::GetBucketLocation)
        );
        assert_eq!(
            classify(&Method::GET, &uri("/mybucket?location=")),
            Some(ForwardOp::GetBucketLocation)
        );
    }

    /// Modeled object reads (a key is present) are never hijacked.
    #[test]
    fn ignores_object_reads() {
        assert_eq!(classify(&Method::GET, &uri("/mybucket/data/foo")), None);
        assert_eq!(classify(&Method::HEAD, &uri("/mybucket/data/foo")), None);
        assert_eq!(
            classify(&Method::GET, &uri("/mybucket/key?location")),
            None,
            "a location query on an object is not GetBucketLocation"
        );
    }

    /// Modeled bucket-level listings are never hijacked: a bucket-level GET
    /// without ?location is ListObjects(V2).
    #[test]
    fn ignores_listings() {
        assert_eq!(classify(&Method::GET, &uri("/mybucket")), None);
        assert_eq!(classify(&Method::GET, &uri("/mybucket?list-type=2")), None);
        assert_eq!(
            classify(&Method::GET, &uri("/mybucket?prefix=a&delimiter=/")),
            None
        );
    }

    /// Other bucket-config sub-resources and mutating verbs are not on the
    /// allowlist and keep s3s's default 501 (classify returns None).
    #[test]
    fn ignores_unallowlisted_ops() {
        assert_eq!(classify(&Method::GET, &uri("/mybucket?acl")), None);
        assert_eq!(classify(&Method::GET, &uri("/mybucket?versioning")), None);
        assert_eq!(classify(&Method::PUT, &uri("/mybucket?cors")), None);
        assert_eq!(classify(&Method::DELETE, &uri("/mybucket")), None);
        assert_eq!(classify(&Method::PUT, &uri("/mybucket")), None);
    }

    /// The service root (no bucket) is never a bucket-config op.
    #[test]
    fn ignores_root() {
        assert_eq!(classify(&Method::GET, &uri("/")), None);
        assert_eq!(classify(&Method::HEAD, &uri("/")), None);
        assert_eq!(classify(&Method::GET, &uri("/?location")), None);
    }
}
