//! Request identity and redacted object-key correlation for the S3 serving path.
//!
//! Every S3 request receives the standard response request id. Object keys are
//! represented in logs only by a stable non-reversible digest.

use std::fmt;
use std::hash::{BuildHasher, RandomState};
use std::sync::Arc;

/// A request's correlation id: 16 uppercase hex digits, the shape S3 uses for
/// `x-amz-request-id`. Cheap to clone (one `Arc` bump) so it can be captured
/// into a streaming-body guard without reallocating.
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
    /// The current request id shared by local serving-path diagnostics.
    static REQUEST_ID: RequestId;
}

/// Runs one local request future under its correlation id.
pub async fn scope<F>(id: RequestId, future: F) -> F::Output
where
    F: std::future::Future,
{
    REQUEST_ID.scope(id, future).await
}

/// Returns the current local request id when called inside a request scope.
pub fn current() -> Option<RequestId> {
    REQUEST_ID.try_with(Clone::clone).ok()
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

    /// Local request scope exposes one correlation id and clears it afterward.
    #[tokio::test]
    async fn current_reads_the_scoped_id() {
        assert!(current().is_none());
        let id = RequestId::generate();
        let seen = scope(id.clone(), async { current() }).await;
        assert_eq!(seen, Some(id));
        assert!(current().is_none());
    }
}
