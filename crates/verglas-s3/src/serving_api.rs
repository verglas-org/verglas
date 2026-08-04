//! The SigV4-gated `/v1` serving surface on the S3 data port.
//!
//! The daemon models a small non-S3 API — `POST /v1/query` and the
//! `/v1/tables/...` group — that until now lived only on the loopback admin
//! listener. The Cloudflare edge re-signs a cache-pathed `/v1` request with the
//! cache keypair and forwards it to this data port, so the same routes must also
//! answer here, gated by SigV4. This module supplies the s3s custom route that
//! does that.
//!
//! # Where it hooks in
//!
//! Like [`BucketConfigPassthrough`](crate::passthrough_route::BucketConfigPassthrough),
//! it is an [`S3Route`], consulted by s3s *before* its typed operation dispatch.
//! [`V1ServingRoute::is_match`] recognizes the `/v1` paths and
//! [`V1ServingRoute::call`] buffers the body and hands it to the injected
//! [`ServingApi`].
//!
//! # Why the SigV4 gate is free
//!
//! s3s validates the request's SigV4 signature (via the configured
//! [`StaticAuth`](crate::auth::StaticAuth)) and populates the request
//! credentials *before* it consults a custom route. This route does **not**
//! override [`S3Route::check_access`], so the trait's default applies: a request
//! whose credentials are `None` (unsigned, or a bad signature that never
//! authenticated) is rejected with `AccessDenied` before [`call`](S3Route::call)
//! runs. That default is exactly the gate the edge relies on.
//!
//! # Keeping this crate free of daemon types
//!
//! The route talks to the daemon through the [`ServingApi`] trait over an owned
//! [`ApiRequest`]/[`ApiResponse`] pair (bytes, not streams), so this protocol
//! crate never depends on verglasd's router or state types. The daemon supplies
//! an implementation that drives its existing axum `/v1` router.

use std::sync::Arc;

use axum::http::{Extensions, HeaderMap, Method, StatusCode, Uri};
use bytes::Bytes;
use http_body_util::BodyExt;
use s3s::route::S3Route;
use s3s::validation::{AwsNameValidation, NameValidation};
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result};

/// The reserved first path segment the `/v1` serving surface lives under. It is
/// two characters, which AWS bucket-name rules already forbid, so no real bucket
/// is ever named this — reserving it collides with nothing.
pub const SERVING_PREFIX: &str = "v1";

/// An owned HTTP request handed to a [`ServingApi`]. The body is fully buffered
/// so the implementation never touches the s3s stream types.
pub struct ApiRequest {
    /// The request method.
    pub method: Method,
    /// The full request URI (path and query).
    pub uri: Uri,
    /// The request headers.
    pub headers: HeaderMap,
    /// The buffered request body.
    pub body: Bytes,
}

/// An owned HTTP response returned by a [`ServingApi`], buffered end to end.
pub struct ApiResponse {
    /// The response status.
    pub status: StatusCode,
    /// The response headers.
    pub headers: HeaderMap,
    /// The buffered response body.
    pub body: Bytes,
}

/// The daemon's `/v1` serving API, behind an owned request/response pair so the
/// protocol crate depends on none of the daemon's router or state types.
#[async_trait::async_trait]
pub trait ServingApi: Send + Sync + 'static {
    /// Handles one buffered `/v1` request and returns the buffered response.
    async fn handle(&self, req: ApiRequest) -> ApiResponse;
}

/// The s3s custom route that forwards `/v1/query` and `/v1/tables/...` to the
/// injected [`ServingApi`]. Checked before s3s's typed dispatch; SigV4-gated by
/// the trait's default [`check_access`](S3Route::check_access) (not overridden).
pub struct V1ServingRoute {
    /// The daemon-side handler the matched request is dispatched to.
    api: Arc<dyn ServingApi>,
}

impl V1ServingRoute {
    /// Builds the route over the daemon's serving API.
    pub fn new(api: Arc<dyn ServingApi>) -> Self {
        V1ServingRoute { api }
    }

    /// Whether `uri` names a served `/v1` path: exactly `/v1/query`, or anything
    /// under the `/v1/tables` prefix.
    fn path_matches(uri: &Uri) -> bool {
        let path = uri.path();
        path == "/v1/query" || path.starts_with("/v1/tables")
    }
}

#[async_trait::async_trait]
impl S3Route for V1ServingRoute {
    /// Matches the served `/v1` paths. Pure and cheap — it runs on every request.
    fn is_match(
        &self,
        _method: &Method,
        uri: &Uri,
        _headers: &HeaderMap,
        _extensions: &mut Extensions,
    ) -> bool {
        Self::path_matches(uri)
    }

    /// Buffers the request body and dispatches to the [`ServingApi`]. The
    /// signature was validated by the auth layer before routing (the default
    /// [`check_access`](S3Route::check_access) requires credentials), which is
    /// the SigV4 gate the edge relies on.
    async fn call(&self, req: S3Request<Body>) -> S3Result<S3Response<Body>> {
        let body = req
            .input
            .collect()
            .await
            .map_err(|error| {
                S3Error::with_message(
                    S3ErrorCode::InternalError,
                    format!("reading the /v1 request body failed: {error}"),
                )
            })?
            .to_bytes();
        let response = self
            .api
            .handle(ApiRequest {
                method: req.method,
                uri: req.uri,
                headers: req.headers,
                body,
            })
            .await;
        let mut out = S3Response::new(Body::from(response.body));
        out.status = Some(response.status);
        out.headers = response.headers;
        Ok(out)
    }
}

/// Bucket-name validation that additionally accepts the reserved
/// [`SERVING_PREFIX`], keeping full AWS rules for every other name.
///
/// s3s validates the first path segment as a bucket name *before* it consults a
/// custom route, and `v1` (two characters) fails the AWS minimum-length rule, so
/// `/v1/...` would be rejected `InvalidBucketName` before [`V1ServingRoute`]
/// could see it. Reserving `v1` lets s3s parse the path far enough for the route
/// to intercept it; because `v1` can never be a real bucket, nothing else is
/// affected.
pub struct ServingNameValidation;

impl NameValidation for ServingNameValidation {
    fn validate_bucket_name(&self, name: &str) -> bool {
        name == SERVING_PREFIX || AwsNameValidation::new().validate_bucket_name(name)
    }
}

/// An [`S3Route`] over an ordered list of routes: matches when any does, and
/// dispatches a matched request to the first that matches. s3s takes a single
/// route ([`set_route`](s3s::service::S3ServiceBuilder::set_route) is singular),
/// so composing more than one — the bucket-config passthrough and the `/v1`
/// serving route — goes through this.
///
/// The composite does not override [`check_access`](S3Route::check_access):
/// every route it holds wants the same SigV4 gate (credentials required), so the
/// trait default covers them all before any inner [`call`](S3Route::call) runs.
pub struct CompositeRoute {
    /// The routes, tried in order.
    routes: Vec<Box<dyn S3Route>>,
}

impl CompositeRoute {
    /// Builds the composite over the ordered routes.
    pub fn new(routes: Vec<Box<dyn S3Route>>) -> Self {
        CompositeRoute { routes }
    }
}

#[async_trait::async_trait]
impl S3Route for CompositeRoute {
    /// Matches when any composed route matches.
    fn is_match(
        &self,
        method: &Method,
        uri: &Uri,
        headers: &HeaderMap,
        extensions: &mut Extensions,
    ) -> bool {
        self.routes
            .iter()
            .any(|route| route.is_match(method, uri, headers, extensions))
    }

    /// Dispatches to the first composed route that matches. Re-matches with a
    /// throwaway [`Extensions`] because `call` does not receive the extensions
    /// `is_match` was given; no composed route depends on extension state to
    /// route, so the throwaway is equivalent.
    async fn call(&self, req: S3Request<Body>) -> S3Result<S3Response<Body>> {
        for route in &self.routes {
            let mut extensions = Extensions::new();
            if route.is_match(&req.method, &req.uri, &req.headers, &mut extensions) {
                return route.call(req).await;
            }
        }
        // Unreachable in practice: s3s only calls `call` after `is_match`
        // returned true, so at least one composed route matches.
        Err(S3Error::with_message(
            S3ErrorCode::NotImplemented,
            "This operation is not supported by Verglas",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a URI from a path-and-query string for matching tests.
    fn uri(s: &str) -> Uri {
        s.parse().expect("valid uri")
    }

    /// The served `/v1` paths match: `/v1/query` exactly and anything under the
    /// `/v1/tables` prefix.
    #[test]
    fn matches_served_v1_paths() {
        assert!(V1ServingRoute::path_matches(&uri("/v1/query")));
        assert!(V1ServingRoute::path_matches(&uri("/v1/tables")));
        assert!(V1ServingRoute::path_matches(&uri(
            "/v1/tables/analytics.events/commit"
        )));
        assert!(V1ServingRoute::path_matches(&uri("/v1/query?ignored=1")));
    }

    /// Object reads and unrelated paths are never claimed by the serving route.
    #[test]
    fn ignores_other_paths() {
        assert!(!V1ServingRoute::path_matches(&uri("/mybucket/data/foo")));
        assert!(!V1ServingRoute::path_matches(&uri("/v1")));
        assert!(!V1ServingRoute::path_matches(&uri("/v1/queryextra")));
        assert!(!V1ServingRoute::path_matches(&uri("/")));
        // A real bucket whose name merely starts with `v1` is not a served path.
        assert!(!V1ServingRoute::path_matches(&uri("/v1tables/object")));
    }

    /// The reserved `v1` prefix is accepted, every other name keeps AWS rules.
    #[test]
    fn validation_reserves_only_v1() {
        let validation = ServingNameValidation;
        assert!(validation.validate_bucket_name(SERVING_PREFIX));
        assert!(validation.validate_bucket_name("real-bucket"));
        // A different too-short name is still rejected.
        assert!(!validation.validate_bucket_name("ab"));
        assert!(!validation.validate_bucket_name(""));
    }
}
