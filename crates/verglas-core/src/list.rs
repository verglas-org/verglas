//! The `ObjectList` interface between the S3 front-end and the backend bucket.
//!
//! Unlike [`ObjectRead`](crate::read::ObjectRead), listings are **never
//! cached**: an engine that writes a file and immediately lists must see it
//! (issue #8's correctness rule), so this interface always passes through to the
//! real bucket. That is also why LIST does not ride on the read interface — when
//! the cache lands (#12) it sits behind `ObjectRead` only, while the lister
//! stays direct-to-origin forever.
//!
//! The interface is protocol-neutral. Both S3 LIST dialects reduce to the same
//! request: v1's `marker` and v2's `continuation-token`/`start-after` are all
//! a single exclusive lower bound ([`ListRequest::start_after`]), and the
//! front-end owns the (de)obfuscation of the v2 token. A page is bounded (at
//! most `max_keys` entries), so it is returned materialized rather than
//! streamed.

use std::time::SystemTime;

/// A LIST request in interface terms. The three client pagination knobs (v1
/// `marker`, v2 `continuation-token`, v2 `start-after`) have already been
/// collapsed by the front-end into [`ListRequest::start_after`]; the interface only
/// ever sees a raw exclusive lower bound.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListRequest {
    /// Bucket to list, exactly as the request named it. The server serves one
    /// bucket; a request for any other bucket is `NoSuchBucket`.
    pub bucket: String,
    /// Raw S3 key prefix to filter by. Empty lists the whole bucket. This is a
    /// pure string prefix, matching S3 (not a path-segment prefix).
    pub prefix: String,
    /// Grouping delimiter. When set, keys sharing a substring between `prefix`
    /// and the first delimiter occurrence roll up into a single common prefix.
    /// Any delimiter string is honored, not just `/`.
    pub delimiter: Option<String>,
    /// Exclusive lower bound: listing resumes strictly after this key. `None`
    /// starts at the first key under `prefix`.
    pub start_after: Option<String>,
    /// Hard ceiling on entries returned (objects + common prefixes together),
    /// already clamped by the front-end to S3's `0..=1000`.
    pub max_keys: usize,
}

/// One object entry in a listing page — the fields S3's `<Contents>` carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListObject {
    /// Object key, exactly as stored (special characters unmodified).
    pub key: String,
    /// Size in bytes.
    pub size: u64,
    /// Entity tag as the origin reports it (front-end normalizes quoting).
    pub e_tag: Option<String>,
    /// Last modification time.
    pub last_modified: Option<SystemTime>,
}

/// One page of a listing: the objects and rolled-up common prefixes that fit
/// under `max_keys`, whether more remain, and where the next page resumes.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ListPage {
    /// Objects in this page, ascending by key.
    pub objects: Vec<ListObject>,
    /// Rolled-up common prefixes in this page, ascending. Empty unless the
    /// request set a delimiter.
    pub common_prefixes: Vec<String>,
    /// Whether the listing was truncated (more entries exist beyond this page).
    pub is_truncated: bool,
    /// The exclusive lower bound the next page must resume after — a real key,
    /// which the front-end obfuscates into a v2 continuation token or hands
    /// back as a v1 marker. `Some` iff [`ListPage::is_truncated`].
    pub next_start_after: Option<String>,
}

/// List-path errors. As with the read/write interfaces the variants map onto the S3
/// error codes the front-end must emit; there is no `NoSuchKey` (a listing of a
/// prefix that matches nothing is an empty success, not an error).
#[derive(Debug, thiserror::Error)]
pub enum ListError {
    /// The bucket does not exist: either the request named a bucket other than
    /// the one this server serves, or the origin reports `NoSuchBucket` (404).
    #[error("bucket does not exist")]
    NoSuchBucket,
    /// The origin refused the request's credentials (`AccessDenied`, 403).
    #[error("access denied by origin")]
    AccessDenied,
    /// Any other backend failure (`InternalError`, 500). Carries the
    /// underlying message for logs.
    #[error("backend list failed: {0}")]
    Backend(String),
}

/// Source of object listings for the S3 front-end.
///
/// The single method returns one bounded page. Implementations must list in
/// ascending key order and must never cache: a page reflects the bucket's
/// state at the moment it is served (issue #8's freshness rule).
///
/// Unlike the read/write interfaces this is an `#[async_trait]` trait: the
/// front-end holds it as `Arc<dyn ObjectList>` (there is only ever one
/// implementation — the passthrough — so no generic is warranted), which
/// requires dyn compatibility.
#[async_trait::async_trait]
pub trait ObjectList: Send + Sync + 'static {
    /// Lists one page for `request`, rolling up common prefixes when a
    /// delimiter is set and honoring `max_keys` across objects and prefixes
    /// together.
    async fn list(&self, request: ListRequest) -> Result<ListPage, ListError>;
}
