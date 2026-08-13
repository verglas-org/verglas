//! Conditional-request precondition evaluation for GET and HEAD (issue #10).
//!
//! HTTP conditional requests let a client skip re-downloading an unchanged
//! object. S3 honours four condition headers on a read, following RFC 7232:
//! `If-Match` and `If-Unmodified-Since` (the "412 group": a false condition
//! means `PreconditionFailed`), then `If-None-Match` and `If-Modified-Since`
//! (the "304 group": a matched condition means `Not Modified`). This module is
//! the pure decision function — it takes the parsed headers and the object's
//! metadata and returns one of three outcomes. The protocol layer
//! ([`crate::frontend`]) turns that outcome into a 200/304/412 response.
//!
//! # Cache interplay (issues #12 / #14)
//!
//! The evaluation runs on the [`ObjectMeta`] the front-end resolves through the
//! [`ObjectRead::head`](verglas_core::read::ObjectRead::head) interface, which
//! is the same TTL-aware key→ETag mapping resolution the cache engine uses:
//!
//! * an immutable-classified or within-TTL mutable mapping is served from cache
//!   with zero backend traffic, so a cached object answered `304`/`412` never
//!   touches the origin — the point of answering conditionals from cache;
//! * an expired mutable mapping is revalidated (a conditional `If-None-Match`
//!   HEAD to the origin, issue #14) *before* evaluation, so the precondition is
//!   always judged against the object's current version, never a stale ETag;
//! * a `304`/`412` short-circuits before any body GET, so no bytes are served.
//!
//! The comparison is on whole seconds: S3's `Last-Modified` has one-second
//! resolution and both sides here originate from second-precision HTTP dates.

use s3s::dto::{ETag, ETagCondition, Timestamp};
use verglas_core::read::ObjectMeta;

/// The condition headers a GET/HEAD may carry, borrowed from the s3s input.
pub struct Preconditions<'a> {
    /// `If-Match`: serve only if the object's ETag matches; else 412.
    pub if_match: Option<&'a ETagCondition>,
    /// `If-None-Match`: 304 if the object's ETag matches.
    pub if_none_match: Option<&'a ETagCondition>,
    /// `If-Modified-Since`: 304 if the object was not modified after this time.
    pub if_modified_since: Option<&'a Timestamp>,
    /// `If-Unmodified-Since`: 412 if the object was modified after this time.
    pub if_unmodified_since: Option<&'a Timestamp>,
}

impl Preconditions<'_> {
    /// Whether any condition header is present. When none are, the front-end
    /// skips the metadata pre-resolution entirely and serves normally.
    pub fn present(&self) -> bool {
        self.if_match.is_some()
            || self.if_none_match.is_some()
            || self.if_modified_since.is_some()
            || self.if_unmodified_since.is_some()
    }
}

/// The verdict of evaluating preconditions against an object's metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Every precondition held: serve the object (200/206) or metadata.
    Proceed,
    /// A 304-group condition matched: reply `304 Not Modified`, no body.
    NotModified,
    /// A 412-group condition failed: reply `412 PreconditionFailed`.
    Failed,
}

/// Evaluates the conditional-request preconditions against `meta`, following
/// S3's RFC 7232 precedence: the 412 group (`If-Match`, else `If-Unmodified-
/// Since`) is judged first, then the 304 group (`If-None-Match`, else
/// `If-Modified-Since`). `If-Match` takes precedence over `If-Unmodified-Since`
/// and `If-None-Match` over `If-Modified-Since` (the "else" arms), exactly as
/// AWS documents.
pub fn evaluate(pre: &Preconditions, meta: &ObjectMeta) -> Outcome {
    // 412 group. If-Match wins over If-Unmodified-Since when both are present.
    if let Some(cond) = pre.if_match {
        if !etag_matches(cond, meta) {
            return Outcome::Failed;
        }
    } else if let Some(since) = pre.if_unmodified_since {
        // Modified strictly after the given instant ⇒ precondition failed.
        if let Some(modified) = object_time(meta)
            && &modified > since
        {
            return Outcome::Failed;
        }
    }

    // 304 group. If-None-Match wins over If-Modified-Since when both are present.
    if let Some(cond) = pre.if_none_match {
        if etag_matches(cond, meta) {
            return Outcome::NotModified;
        }
    } else if let Some(since) = pre.if_modified_since {
        // Not modified since the given instant (last-modified ≤ it) ⇒ 304.
        if let Some(modified) = object_time(meta)
            && &modified <= since
        {
            return Outcome::NotModified;
        }
    }

    Outcome::Proceed
}

/// The object's `Last-Modified` as a [`Timestamp`] for comparison, or `None`
/// when the origin reported none (then the date conditions cannot be judged and
/// the request proceeds).
fn object_time(meta: &ObjectMeta) -> Option<Timestamp> {
    meta.last_modified.map(Timestamp::from)
}

/// Whether an `If-Match`/`If-None-Match` condition matches the object. The
/// wildcard `*` matches any existing object (we hold its metadata, so it
/// exists). A concrete ETag matches when its opaque value equals the object's,
/// compared after stripping surrounding quotes and any weak `W/` prefix — S3
/// compares the entity-tag value, not its quoting.
fn etag_matches(cond: &ETagCondition, meta: &ObjectMeta) -> bool {
    match cond {
        ETagCondition::Any => true,
        ETagCondition::ETag(etag) => match meta.e_tag.as_deref() {
            Some(object) => normalize(object) == normalize(etag_value(etag)),
            None => false,
        },
    }
}

/// The inner value of a parsed ETag condition (strong or weak), already unquoted
/// by s3s's header parse.
fn etag_value(etag: &ETag) -> &str {
    etag.as_strong().or_else(|| etag.as_weak()).unwrap_or("")
}

/// Strips a weak `W/` prefix and surrounding double quotes so two entity-tag
/// spellings of the same value compare equal.
fn normalize(raw: &str) -> &str {
    raw.trim()
        .strip_prefix("W/")
        .unwrap_or(raw.trim())
        .trim_matches('"')
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, SystemTime};

    /// Builds object metadata with the given quoted ETag and last-modified.
    fn meta(etag: &str, modified: Option<SystemTime>) -> ObjectMeta {
        ObjectMeta {
            size: 3,
            e_tag: Some(etag.to_owned()),
            last_modified: modified,
            ..ObjectMeta::default()
        }
    }

    /// Parses an HTTP-date string the way the wire header arrives.
    fn http_date(s: &str) -> Timestamp {
        Timestamp::parse(s3s::dto::TimestampFormat::HttpDate, s).expect("valid http date")
    }

    /// Parses an ETag condition the way s3s parses the header value.
    fn cond(s: &str) -> ETagCondition {
        ETagCondition::parse_http_header(s.as_bytes()).expect("valid etag condition")
    }

    /// A fixed instant (2020-01-01T00:00:00Z) for the object's last-modified.
    fn t2020() -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(1_577_836_800)
    }

    /// No condition headers ⇒ proceed, and `present()` is false.
    #[test]
    fn no_conditions_proceed() {
        let pre = Preconditions {
            if_match: None,
            if_none_match: None,
            if_modified_since: None,
            if_unmodified_since: None,
        };
        assert!(!pre.present());
        assert_eq!(
            evaluate(&pre, &meta("\"abc\"", Some(t2020()))),
            Outcome::Proceed
        );
    }

    /// If-Match on a non-matching ETag ⇒ 412 (test_get_object_ifmatch_failed).
    #[test]
    fn if_match_mismatch_fails() {
        let c = cond("\"ABCORZ\"");
        let pre = Preconditions {
            if_match: Some(&c),
            if_none_match: None,
            if_modified_since: None,
            if_unmodified_since: None,
        };
        assert_eq!(evaluate(&pre, &meta("\"real\"", None)), Outcome::Failed);
    }

    /// If-Match on the matching ETag ⇒ proceed (test_get_object_ifmatch_good).
    #[test]
    fn if_match_hit_proceeds() {
        let c = cond("\"real\"");
        let pre = Preconditions {
            if_match: Some(&c),
            if_none_match: None,
            if_modified_since: None,
            if_unmodified_since: None,
        };
        assert_eq!(evaluate(&pre, &meta("\"real\"", None)), Outcome::Proceed);
    }

    /// If-Match `*` matches any existing object.
    #[test]
    fn if_match_wildcard_hits_existing() {
        let c = cond("*");
        let pre = Preconditions {
            if_match: Some(&c),
            if_none_match: None,
            if_modified_since: None,
            if_unmodified_since: None,
        };
        assert_eq!(
            evaluate(&pre, &meta("\"anything\"", None)),
            Outcome::Proceed
        );
    }

    /// If-None-Match on the matching ETag ⇒ 304 (test_get_object_ifnonematch_good).
    #[test]
    fn if_none_match_hit_not_modified() {
        let c = cond("\"real\"");
        let pre = Preconditions {
            if_match: None,
            if_none_match: Some(&c),
            if_modified_since: None,
            if_unmodified_since: None,
        };
        assert_eq!(
            evaluate(&pre, &meta("\"real\"", None)),
            Outcome::NotModified
        );
    }

    /// If-None-Match on a non-matching ETag ⇒ proceed
    /// (test_get_object_ifnonematch_failed).
    #[test]
    fn if_none_match_miss_proceeds() {
        let c = cond("ABCORZ");
        let pre = Preconditions {
            if_match: None,
            if_none_match: Some(&c),
            if_modified_since: None,
            if_unmodified_since: None,
        };
        assert_eq!(evaluate(&pre, &meta("\"real\"", None)), Outcome::Proceed);
    }

    /// If-Unmodified-Since before the object's mtime ⇒ 412
    /// (test_get_object_ifunmodifiedsince_good).
    #[test]
    fn if_unmodified_since_older_fails() {
        let since = http_date("Sat, 29 Oct 1994 19:43:31 GMT");
        let pre = Preconditions {
            if_match: None,
            if_none_match: None,
            if_modified_since: None,
            if_unmodified_since: Some(&since),
        };
        assert_eq!(
            evaluate(&pre, &meta("\"real\"", Some(t2020()))),
            Outcome::Failed
        );
    }

    /// If-Unmodified-Since after the object's mtime ⇒ proceed
    /// (test_get_object_ifunmodifiedsince_failed).
    #[test]
    fn if_unmodified_since_newer_proceeds() {
        let since = http_date("Sat, 29 Oct 2100 19:43:31 GMT");
        let pre = Preconditions {
            if_match: None,
            if_none_match: None,
            if_modified_since: None,
            if_unmodified_since: Some(&since),
        };
        assert_eq!(
            evaluate(&pre, &meta("\"real\"", Some(t2020()))),
            Outcome::Proceed
        );
    }

    /// If-Modified-Since after the object's mtime ⇒ 304
    /// (test_get_object_ifmodifiedsince_failed): mtime ≤ the given instant.
    #[test]
    fn if_modified_since_newer_not_modified() {
        let since = http_date("Wed, 01 Jan 2020 00:00:01 GMT");
        let pre = Preconditions {
            if_match: None,
            if_none_match: None,
            if_modified_since: Some(&since),
            if_unmodified_since: None,
        };
        assert_eq!(
            evaluate(&pre, &meta("\"real\"", Some(t2020()))),
            Outcome::NotModified
        );
    }

    /// If-Modified-Since before the object's mtime ⇒ proceed
    /// (test_get_object_ifmodifiedsince_good).
    #[test]
    fn if_modified_since_older_proceeds() {
        let since = http_date("Sat, 29 Oct 1994 19:43:31 GMT");
        let pre = Preconditions {
            if_match: None,
            if_none_match: None,
            if_modified_since: Some(&since),
            if_unmodified_since: None,
        };
        assert_eq!(
            evaluate(&pre, &meta("\"real\"", Some(t2020()))),
            Outcome::Proceed
        );
    }

    /// Precedence: If-Match is judged before If-None-Match; a failed If-Match is
    /// 412 even when If-None-Match would also match.
    #[test]
    fn if_match_precedes_if_none_match() {
        let m = cond("\"nomatch\"");
        let nm = cond("\"real\"");
        let pre = Preconditions {
            if_match: Some(&m),
            if_none_match: Some(&nm),
            if_modified_since: None,
            if_unmodified_since: None,
        };
        assert_eq!(evaluate(&pre, &meta("\"real\"", None)), Outcome::Failed);
    }

    /// Precedence: If-Match present makes If-Unmodified-Since irrelevant (S3's
    /// documented rule): a matching If-Match proceeds even if
    /// If-Unmodified-Since alone would 412.
    #[test]
    fn if_match_overrides_if_unmodified_since() {
        let m = cond("\"real\"");
        let since = http_date("Sat, 29 Oct 1994 19:43:31 GMT");
        let pre = Preconditions {
            if_match: Some(&m),
            if_none_match: None,
            if_modified_since: None,
            if_unmodified_since: Some(&since),
        };
        assert_eq!(
            evaluate(&pre, &meta("\"real\"", Some(t2020()))),
            Outcome::Proceed
        );
    }

    /// Precedence: If-None-Match present makes If-Modified-Since irrelevant: a
    /// non-matching If-None-Match proceeds even if If-Modified-Since alone would
    /// answer 304.
    #[test]
    fn if_none_match_overrides_if_modified_since() {
        let nm = cond("\"other\"");
        let since = http_date("Wed, 01 Jan 2020 00:00:01 GMT");
        let pre = Preconditions {
            if_match: None,
            if_none_match: Some(&nm),
            if_modified_since: Some(&since),
            if_unmodified_since: None,
        };
        assert_eq!(
            evaluate(&pre, &meta("\"real\"", Some(t2020()))),
            Outcome::Proceed
        );
    }

    /// A weak validator compares equal to the same value stored strong.
    #[test]
    fn weak_and_strong_etag_compare_equal() {
        let c = cond("W/\"real\"");
        assert!(etag_matches(&c, &meta("\"real\"", None)));
    }
}
