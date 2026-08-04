//! Request-scoped trace identity shared across the serving path (#61).
//!
//! Every S3 request is stamped with a `request_id` at the front-end. The id is
//! returned to the client as `x-amz-request-id` and threaded through the whole
//! serving path — cache lookup, ring routing, peer fetch, origin fill — so the
//! logs for one request share a single correlation key. Across nodes the peer
//! RPC (#29) carries the originating id in a header, so a cold fill served by a
//! peer logs under the id the client saw.
//!
//! The id lives in a tokio task-local rather than in every serving-path
//! signature: reading it is a cheap thread-local load, never a lock or an
//! allocation, so the hot path pays nothing to keep the id available for a log
//! line or a peer header. Off the request path (background warming, prefetch)
//! there is no request, so [`current`] returns `None`.

use std::fmt;
use std::hash::{BuildHasher, RandomState};
use std::sync::Arc;

/// HTTP header the peer RPC (#29) carries the originating request id in, so a
/// cross-node fill logs under the id the client saw. Named in the `x-verglas-*`
/// namespace beside the other peer headers.
pub const REQUEST_ID_HEADER: &str = "x-verglas-request-id";

/// A request's correlation id: 16 uppercase hex digits, the shape S3 uses for
/// `x-amz-request-id`. Cheap to clone (one `Arc` bump) so it can be captured
/// into a streaming-body guard or a peer request without reallocating.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestId(Arc<str>);

impl RequestId {
    /// Generates a fresh random request id. `RandomState` is seeded from OS
    /// entropy; a correlation id needs unpredictability, not cryptographic
    /// strength.
    pub fn generate() -> Self {
        let n = RandomState::new().hash_one(0u64);
        RequestId(Arc::from(format!("{n:016X}").as_str()))
    }

    /// Adopts an id received over the wire (a peer RPC header), keeping it
    /// verbatim so a cross-node request logs under one id. Empty or oversized
    /// input is rejected so a peer cannot inject an unbounded log field.
    pub fn from_wire(value: &str) -> Option<Self> {
        let trimmed = value.trim();
        if trimmed.is_empty() || trimmed.len() > 64 {
            return None;
        }
        Some(RequestId(Arc::from(trimmed)))
    }

    /// The id as a string, for headers and log fields.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

tokio::task_local! {
    /// The current request's id, set for the whole request future by the
    /// front-end (and by the peer server for a cross-node fill).
    static REQUEST_ID: RequestId;
}

/// Runs `future` with `id` as the current request id. Everything the future
/// awaits — cache lookup, peer fetch, origin fill — reads the same id through
/// [`current`].
pub async fn scope<F>(id: RequestId, future: F) -> F::Output
where
    F: std::future::Future,
{
    REQUEST_ID.scope(id, future).await
}

/// The current request's id, or `None` off the request path. A cheap
/// thread-local read; never locks or allocates.
pub fn current() -> Option<RequestId> {
    REQUEST_ID.try_with(|id| id.clone()).ok()
}

/// A short, stable, non-reversible digest of an object key, for logs. The raw
/// key is never logged (key redaction, #61); this 16-hex digest lets two log
/// lines be tied to the same object without disclosing its name. XXH3 keeps it
/// cheap enough to compute on a fill-error path.
pub fn key_hash(bucket: &str, key: &str) -> String {
    let mut h = xxhash_rust::xxh3::Xxh3::new();
    h.update(bucket.as_bytes());
    h.update(b"\0");
    h.update(key.as_bytes());
    format!("{:016x}", h.digest())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A generated id is 16 uppercase hex digits, the `x-amz-request-id` shape.
    #[test]
    fn generated_id_is_sixteen_hex_digits() {
        let id = RequestId::generate();
        assert_eq!(id.as_str().len(), 16);
        assert!(
            id.as_str()
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'A'..=b'F').contains(&b))
        );
    }

    /// A wire id is adopted verbatim; blank and oversized ids are rejected so a
    /// peer cannot inject an unbounded or empty log field.
    #[test]
    fn from_wire_validates_length() {
        assert_eq!(
            RequestId::from_wire("ABC123").expect("valid id").as_str(),
            "ABC123"
        );
        assert!(RequestId::from_wire("   ").is_none());
        assert!(RequestId::from_wire(&"x".repeat(65)).is_none());
    }

    /// `current` reads the scoped id inside the future and is `None` outside it.
    #[tokio::test]
    async fn current_reads_the_scoped_id() {
        assert!(current().is_none());
        let id = RequestId::generate();
        let seen = scope(id.clone(), async { current() }).await;
        assert_eq!(seen, Some(id));
        assert!(current().is_none());
    }
}
