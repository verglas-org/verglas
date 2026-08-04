//! Integration tests for the S3 LIST path (issue #8): ListObjects (v1) and
//! ListObjectsV2 forwarded to the origin, paginated and never cached. Every
//! test runs against `object_store`'s in-memory backend and compares a full
//! paginated walk through the front-end against a direct listing of the same
//! store — the `aws s3 ls --recursive` parity bar. Covers >2,000 keys (forces
//! pagination), delimiter roll-up into CommonPrefixes, raw-string prefixes,
//! awkward key names (space, unicode, `+`, `%`), v1 marker and v2
//! continuation-token/start-after/max-keys walks, and IsTruncated sequencing.

use std::sync::Arc;

use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use verglas_s3::{
    BackendStore, NoopInvalidation, PassthroughList, PassthroughRead, PassthroughWrite,
};

/// The bucket the in-memory origin serves.
const BUCKET: &str = "list-bucket";

/// Serves the front-end over `store` on an ephemeral loopback port, returning
/// the base URL. Auth is disabled — these tests exercise listing only.
async fn serve(store: Arc<InMemory>) -> String {
    let app = verglas_s3::router(
        PassthroughRead::new(BackendStore::single(BUCKET, store.clone())),
        PassthroughWrite::new(BackendStore::single(BUCKET, store.clone())),
        Arc::new(PassthroughList::new(BackendStore::single(BUCKET, store))),
        Arc::new(NoopInvalidation),
        None,
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    format!("http://{addr}")
}

/// Seeds an in-memory store with empty-bodied objects at `keys`.
async fn seed(keys: &[String]) -> Arc<InMemory> {
    let store = Arc::new(InMemory::new());
    for key in keys {
        store
            .put(&Path::from(key.as_str()), PutPayload::from_static(b"x"))
            .await
            .expect("seed object");
    }
    store
}

/// Every key in `store`, ascending — the direct reference a front-end walk
/// must reproduce (`aws s3 ls --recursive` parity).
async fn direct_keys(store: &InMemory) -> Vec<String> {
    use futures::StreamExt;
    let mut keys: Vec<String> = store
        .list(None)
        .map(|m| m.expect("list").location.as_ref().to_owned())
        .collect()
        .await;
    keys.sort();
    keys
}

/// Unescapes the XML entities s3s emits in element text, exactly as a real
/// client's XML parser would — so extracted keys compare against their literal
/// stored form (e.g. `amp&amp;and.txt` -> `amp&and.txt`).
fn xml_unescape(text: &str) -> String {
    text.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&#39;", "'")
        .replace("&amp;", "&")
}

/// Extracts the text of every top-level element named `tag` from `xml`
/// (non-nested scan — fine for the flat LIST elements we assert on), with XML
/// entities unescaped.
fn all_values(xml: &str, tag: &str) -> Vec<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut out = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find(&open) {
        let after = &rest[start + open.len()..];
        let Some(end) = after.find(&close) else { break };
        out.push(xml_unescape(&after[..end]));
        rest = &after[end + close.len()..];
    }
    out
}

/// The `<Key>` values inside `<Contents>` blocks.
fn contents_keys(xml: &str) -> Vec<String> {
    all_values(xml, "Key")
}

/// The `<Prefix>` values inside `<CommonPrefixes>` blocks (not the top-level
/// prefix echo, which lives outside any `<CommonPrefixes>`).
fn common_prefixes(xml: &str) -> Vec<String> {
    let mut out = Vec::new();
    for block in all_values(xml, "CommonPrefixes") {
        out.extend(all_values(&block, "Prefix"));
    }
    out
}

/// The single-valued text of `tag`, if present.
fn value(xml: &str, tag: &str) -> Option<String> {
    all_values(xml, tag).into_iter().next()
}

/// Fetches one raw LIST XML response for `query` (already-encoded query
/// string, no leading `?`).
async fn list_raw(base: &str, query: &str) -> String {
    let url = format!("{base}/{BUCKET}?{query}");
    let response = reqwest::get(&url).await.expect("LIST request");
    assert_eq!(response.status(), 200, "LIST should be 200");
    response.text().await.expect("body")
}

/// Walks every page of a ListObjectsV2 listing, following continuation tokens,
/// and returns the concatenated (keys, common-prefixes) across all pages plus
/// the per-page `IsTruncated` sequence.
async fn walk_v2(
    base: &str,
    prefix: &str,
    delimiter: Option<&str>,
    max_keys: Option<u32>,
) -> (Vec<String>, Vec<String>, Vec<bool>) {
    let mut keys = Vec::new();
    let mut prefixes = Vec::new();
    let mut truncated_seq = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let mut query = format!("list-type=2&prefix={}", urlencoding::encode(prefix));
        if let Some(delim) = delimiter {
            query.push_str(&format!("&delimiter={}", urlencoding::encode(delim)));
        }
        if let Some(max) = max_keys {
            query.push_str(&format!("&max-keys={max}"));
        }
        if let Some(ref t) = token {
            query.push_str(&format!("&continuation-token={}", urlencoding::encode(t)));
        }
        let xml = list_raw(base, &query).await;
        keys.extend(contents_keys(&xml));
        prefixes.extend(common_prefixes(&xml));
        let truncated = value(&xml, "IsTruncated").as_deref() == Some("true");
        truncated_seq.push(truncated);
        if truncated {
            token = Some(
                value(&xml, "NextContinuationToken").expect("truncated page needs a next token"),
            );
        } else {
            break;
        }
    }
    (keys, prefixes, truncated_seq)
}

/// Walks every page of a ListObjects (v1) listing, following `marker`, and
/// returns the concatenated keys and common prefixes across all pages.
async fn walk_v1(
    base: &str,
    prefix: &str,
    delimiter: Option<&str>,
    max_keys: Option<u32>,
) -> (Vec<String>, Vec<String>) {
    let mut keys = Vec::new();
    let mut prefixes = Vec::new();
    let mut marker: Option<String> = None;
    loop {
        let mut query = format!("prefix={}", urlencoding::encode(prefix));
        if let Some(delim) = delimiter {
            query.push_str(&format!("&delimiter={}", urlencoding::encode(delim)));
        }
        if let Some(max) = max_keys {
            query.push_str(&format!("&max-keys={max}"));
        }
        if let Some(ref m) = marker {
            query.push_str(&format!("&marker={}", urlencoding::encode(m)));
        }
        let xml = list_raw(base, &query).await;
        let page_keys = contents_keys(&xml);
        let page_prefixes = common_prefixes(&xml);
        keys.extend(page_keys.clone());
        prefixes.extend(page_prefixes.clone());
        if value(&xml, "IsTruncated").as_deref() != Some("true") {
            break;
        }
        // NextMarker when present (delimited), else the last key returned.
        marker = value(&xml, "NextMarker")
            .or_else(|| page_prefixes.last().cloned().max(page_keys.last().cloned()));
        assert!(marker.is_some(), "truncated v1 page must yield a marker");
    }
    (keys, prefixes)
}

#[tokio::test]
async fn recursive_walk_matches_direct_for_over_2000_keys() {
    // >2,000 keys forces pagination at the default 1,000-key page size.
    let keys: Vec<String> = (0..2500)
        .map(|i| format!("data/part-{i:05}.parquet"))
        .collect();
    let store = seed(&keys).await;
    let expected = direct_keys(&store).await;
    let base = serve(store).await;

    let (walked, prefixes, truncated_seq) = walk_v2(&base, "", None, None).await;

    assert_eq!(
        walked, expected,
        "v2 recursive walk must equal direct listing"
    );
    assert!(prefixes.is_empty(), "no delimiter => no common prefixes");
    // 2500 keys / 1000 per page => pages truncated, truncated, final not.
    assert_eq!(truncated_seq, vec![true, true, false]);
}

#[tokio::test]
async fn v1_marker_walk_matches_direct() {
    let keys: Vec<String> = (0..2100).map(|i| format!("t/{i:05}")).collect();
    let store = seed(&keys).await;
    let expected = direct_keys(&store).await;
    let base = serve(store).await;

    let (walked, _) = walk_v1(&base, "", None, None).await;
    assert_eq!(walked, expected, "v1 marker walk must equal direct listing");
}

#[tokio::test]
async fn small_max_keys_paginates_without_dropping_or_duplicating() {
    let keys: Vec<String> = (0..97).map(|i| format!("k/{i:04}")).collect();
    let store = seed(&keys).await;
    let expected = direct_keys(&store).await;
    let base = serve(store).await;

    // Page size 10 across 97 keys => 10 pages, last short.
    let (walked, _, truncated_seq) = walk_v2(&base, "", None, Some(10)).await;
    assert_eq!(walked, expected);
    assert_eq!(truncated_seq.len(), 10);
    assert_eq!(truncated_seq.last(), Some(&false));
    assert!(
        truncated_seq[..9].iter().all(|&t| t),
        "all but last truncated"
    );
}

#[tokio::test]
async fn delimiter_rolls_up_common_prefixes() {
    let keys = ["a/1", "a/2", "b/x/1", "b/y/1", "c", "d/deep/deeper/1"].map(str::to_owned);
    let store = seed(&keys).await;
    let base = serve(store).await;

    // Top level: directories a/, b/, d/ roll up; c is a bare object.
    let xml = list_raw(&base, "list-type=2&delimiter=%2F").await;
    assert_eq!(contents_keys(&xml), vec!["c".to_owned()]);
    assert_eq!(
        common_prefixes(&xml),
        vec!["a/".to_owned(), "b/".to_owned(), "d/".to_owned()]
    );

    // Nested: under b/ the children x/ and y/ roll up.
    let xml = list_raw(&base, "list-type=2&prefix=b%2F&delimiter=%2F").await;
    assert!(contents_keys(&xml).is_empty());
    assert_eq!(
        common_prefixes(&xml),
        vec!["b/x/".to_owned(), "b/y/".to_owned()]
    );
}

#[tokio::test]
async fn delimiter_walk_matches_direct_with_pagination() {
    // Many sibling "directories", each with several objects, so roll-up spans
    // multiple small pages and the resume cursor must skip whole groups.
    let mut keys = Vec::new();
    for dir in 0..50 {
        for obj in 0..10 {
            keys.push(format!("dir{dir:03}/obj{obj}"));
        }
    }
    keys.push("toplevel".to_owned());
    let store = seed(&keys).await;
    let base = serve(store).await;

    let (contents, prefixes, _) = walk_v2(&base, "", Some("/"), Some(7)).await;
    assert_eq!(contents, vec!["toplevel".to_owned()]);
    let expected_prefixes: Vec<String> = (0..50).map(|d| format!("dir{d:03}/")).collect();
    assert_eq!(
        prefixes, expected_prefixes,
        "each directory appears exactly once"
    );
}

#[tokio::test]
async fn raw_string_prefix_matches_partial_names_not_segments() {
    let keys = [
        "part-1",
        "part-2",
        "part-a",
        "party",
        "partition/x",
        "a/b",
        "a/bc",
        "a/b/c",
    ]
    .map(str::to_owned);
    let store = seed(&keys).await;
    let base = serve(store).await;

    // "part-" is a raw string prefix: matches part-1/2/a but not "party" or
    // "partition/x".
    let (walked, _, _) = walk_v2(&base, "part-", None, None).await;
    assert_eq!(walked, vec!["part-1", "part-2", "part-a"]);

    // "a/b" must match "a/bc" (a non-segment-aligned match) and "a/b/c".
    let (walked, _, _) = walk_v2(&base, "a/b", None, None).await;
    assert_eq!(walked, vec!["a/b", "a/b/c", "a/bc"]);
}

#[tokio::test]
async fn awkward_key_names_survive_the_proxy() {
    let keys = [
        "plain.txt",
        "with space.txt",
        "unicode-café-☃.txt",
        "plus+sign.txt",
        "per%cent.txt",
        "amp&and.txt",
    ]
    .map(str::to_owned);
    let store = seed(&keys).await;
    let expected = direct_keys(&store).await;
    let base = serve(store).await;

    // Default (no encoding-type): keys come back verbatim.
    let (walked, _, _) = walk_v2(&base, "", None, None).await;
    assert_eq!(
        walked, expected,
        "special characters must round-trip verbatim"
    );
}

#[tokio::test]
async fn encoding_type_url_percent_encodes_and_decodes_back() {
    let keys = [
        "with space.txt",
        "plus+sign.txt",
        "per%cent.txt",
        "unicode-☃.txt",
    ]
    .map(str::to_owned);
    let store = seed(&keys).await;
    // The canonical (stored) key forms are the reference; decoding the
    // url-encoded wire keys must reproduce exactly these.
    let expected = direct_keys(&store).await;
    let base = serve(store).await;

    let xml = list_raw(&base, "list-type=2&encoding-type=url").await;
    // The response must announce url encoding.
    assert_eq!(value(&xml, "EncodingType").as_deref(), Some("url"));
    let raw_keys = contents_keys(&xml);
    // Encoding actually happened: a space becomes %20 (never a bare space, and
    // never '+', which would be ambiguous).
    assert!(
        raw_keys.iter().all(|k| !k.contains(' ')),
        "url encoding must not leave literal spaces: {raw_keys:?}"
    );
    assert!(
        raw_keys.iter().any(|k| k.contains("%20")),
        "the space key must be percent-encoded: {raw_keys:?}"
    );
    // Decoding every wire key reproduces the canonical keys exactly.
    let decoded: Vec<String> = raw_keys
        .iter()
        .map(|k| {
            urlencoding::decode(k)
                .expect("valid percent-encoding")
                .into_owned()
        })
        .collect();
    assert_eq!(decoded, expected);
}

#[tokio::test]
async fn start_after_skips_to_the_requested_point() {
    let keys: Vec<String> = (0..20).map(|i| format!("k{i:02}")).collect();
    let store = seed(&keys).await;
    let base = serve(store).await;

    let xml = list_raw(&base, "list-type=2&start-after=k09").await;
    let walked = contents_keys(&xml);
    let expected: Vec<String> = (10..20).map(|i| format!("k{i:02}")).collect();
    assert_eq!(walked, expected, "start-after is an exclusive lower bound");
}

#[tokio::test]
async fn empty_prefix_match_is_empty_success() {
    let store = seed(&["real/key".to_owned()]).await;
    let base = serve(store).await;

    let xml = list_raw(&base, "list-type=2&prefix=nonexistent").await;
    assert!(contents_keys(&xml).is_empty());
    assert_eq!(value(&xml, "IsTruncated").as_deref(), Some("false"));
    assert_eq!(value(&xml, "KeyCount").as_deref(), Some("0"));
}

#[tokio::test]
async fn key_count_counts_objects_and_common_prefixes() {
    let keys = ["a/1", "a/2", "b/1", "loose"].map(str::to_owned);
    let store = seed(&keys).await;
    let base = serve(store).await;

    let xml = list_raw(&base, "list-type=2&delimiter=%2F").await;
    // 2 common prefixes (a/, b/) + 1 object (loose) = KeyCount 3.
    assert_eq!(value(&xml, "KeyCount").as_deref(), Some("3"));
}

#[tokio::test]
async fn continuation_token_is_opaque_and_survives_truncation_mid_group() {
    // A page boundary that lands inside a rolled-up group must not re-emit the
    // group on the next page.
    let keys = ["g/1", "g/2", "g/3", "h/1"].map(str::to_owned);
    let store = seed(&keys).await;
    let base = serve(store).await;

    // max-keys=1 with delimiter: page 1 = g/, page 2 = h/.
    let (contents, prefixes, truncated_seq) = walk_v2(&base, "", Some("/"), Some(1)).await;
    assert!(contents.is_empty());
    assert_eq!(prefixes, vec!["g/".to_owned(), "h/".to_owned()]);
    assert_eq!(truncated_seq, vec![true, false]);
}

#[tokio::test]
async fn invalid_continuation_token_is_rejected() {
    let store = seed(&["k".to_owned()]).await;
    let base = serve(store).await;

    let url = format!("{base}/{BUCKET}?list-type=2&continuation-token=not-a-real-token%21%21");
    let response = reqwest::get(&url).await.expect("request");
    assert_eq!(response.status(), 400, "a bogus token must be rejected");
}

/// The keys from Ceph's `test_bucket_list_delimiter_prefix`: one bare object,
/// then two rolled-up directories.
fn delimiter_prefix_keys() -> Vec<String> {
    [
        "asdf",
        "boo/bar",
        "boo/baz/xyzzy",
        "cquux/bla",
        "cquux/thud",
    ]
    .map(str::to_owned)
    .to_vec()
}

#[tokio::test]
async fn v1_page_ending_on_common_prefix_returns_the_prefix_as_marker() {
    // Issue #144: a delimited page that ends on a rolled-up CommonPrefix must
    // hand back that prefix as NextMarker — not a raw key inside the group.
    let store = seed(&delimiter_prefix_keys()).await;
    let base = serve(store).await;

    // Page 1 (max-keys=2): ['asdf'] + CommonPrefix ['boo/'], NextMarker 'boo/'.
    let xml = list_raw(&base, "delimiter=%2F&max-keys=2").await;
    assert_eq!(contents_keys(&xml), vec!["asdf".to_owned()]);
    assert_eq!(common_prefixes(&xml), vec!["boo/".to_owned()]);
    assert_eq!(value(&xml, "IsTruncated").as_deref(), Some("true"));
    assert_eq!(
        value(&xml, "NextMarker").as_deref(),
        Some("boo/"),
        "cursor must be the common prefix, not a raw key inside it"
    );

    // Page 2 resumes from marker 'boo/' and must SKIP the whole boo/ group,
    // returning only cquux/ (never re-rolling boo/).
    let xml = list_raw(&base, "delimiter=%2F&max-keys=2&marker=boo%2F").await;
    assert!(contents_keys(&xml).is_empty());
    assert_eq!(common_prefixes(&xml), vec!["cquux/".to_owned()]);
    assert_eq!(value(&xml, "IsTruncated").as_deref(), Some("false"));
}

#[tokio::test]
async fn delimiter_pagination_never_duplicates_a_group_v1_and_v2() {
    // A full paginated walk at page-size 1 must yield each directory exactly
    // once across both dialects (issue #144).
    let store = seed(&delimiter_prefix_keys()).await;
    let base = serve(store).await;

    let (v1_keys, v1_prefixes) = walk_v1(&base, "", Some("/"), Some(1)).await;
    assert_eq!(v1_keys, vec!["asdf".to_owned()]);
    assert_eq!(v1_prefixes, vec!["boo/".to_owned(), "cquux/".to_owned()]);

    let (v2_keys, v2_prefixes, _) = walk_v2(&base, "", Some("/"), Some(1)).await;
    assert_eq!(v2_keys, vec!["asdf".to_owned()]);
    assert_eq!(v2_prefixes, vec!["boo/".to_owned(), "cquux/".to_owned()]);
}

#[tokio::test]
async fn max_keys_zero_is_an_empty_untruncated_page() {
    // Issue #145: max-keys=0 is a valid zero-length page of a fully-listable
    // bucket — IsTruncated is false, not true.
    let store = seed(&["bar", "baz", "foo", "quxx"].map(str::to_owned)).await;
    let base = serve(store).await;

    let xml = list_raw(&base, "list-type=2&max-keys=0").await;
    assert!(contents_keys(&xml).is_empty());
    assert_eq!(value(&xml, "IsTruncated").as_deref(), Some("false"));
    assert_eq!(value(&xml, "KeyCount").as_deref(), Some("0"));
    assert!(
        value(&xml, "NextContinuationToken").is_none(),
        "a non-truncated page must not carry a next token"
    );

    let xml = list_raw(&base, "max-keys=0").await;
    assert!(contents_keys(&xml).is_empty());
    assert_eq!(value(&xml, "IsTruncated").as_deref(), Some("false"));
}

#[tokio::test]
async fn empty_delimiter_is_omitted_from_the_response() {
    // Issue #145: an empty Delimiter="" is echoed by S3 as *no* Delimiter
    // element, and does no roll-up.
    let store = seed(&["bar", "baz", "cab", "foo"].map(str::to_owned)).await;
    let base = serve(store).await;

    for dialect in ["", "list-type=2&"] {
        let xml = list_raw(&base, &format!("{dialect}delimiter=")).await;
        assert!(
            value(&xml, "Delimiter").is_none(),
            "empty delimiter must not be echoed ({dialect})"
        );
        assert!(common_prefixes(&xml).is_empty());
        assert_eq!(contents_keys(&xml).len(), 4);
    }
}

#[tokio::test]
async fn v1_marker_is_always_present() {
    // Issue #145: v1 always includes <Marker>, empty when none was sent.
    let store = seed(&["bar", "baz", "foo", "quxx"].map(str::to_owned)).await;
    let base = serve(store).await;

    let xml = list_raw(&base, "prefix=").await;
    assert_eq!(
        value(&xml, "Marker").as_deref(),
        Some(""),
        "v1 Marker element must be present (empty string) even with no marker"
    );
}

#[tokio::test]
async fn v2_fetch_owner_controls_the_owner_element() {
    // Issue #145: fetch-owner=true attaches <Owner> to each Contents; absent it
    // is omitted.
    let store = seed(&["foo/bar", "foo/baz", "quux"].map(str::to_owned)).await;
    let base = serve(store).await;

    let xml = list_raw(&base, "list-type=2&fetch-owner=true").await;
    assert!(
        xml.contains("<Owner>"),
        "fetch-owner=true must return <Owner> in Contents: {xml}"
    );

    let xml = list_raw(&base, "list-type=2").await;
    assert!(
        !xml.contains("<Owner>"),
        "owner must be omitted by default: {xml}"
    );
}

#[tokio::test]
async fn v1_control_char_prefix_is_echoed_verbatim() {
    // Issue #145: a control-character prefix must come back verbatim in the v1
    // top-level <Prefix> even under encoding-type=url (botocore does not decode
    // that field), so it must never be percent-encoded there.
    let store = seed(&["foo/bar", "foo/baz", "quux"].map(str::to_owned)).await;
    let base = serve(store).await;

    // prefix = "\n" (0x0A), which matches nothing here.
    let xml = list_raw(&base, "prefix=%0A&encoding-type=url").await;
    assert_eq!(
        value(&xml, "Prefix").as_deref(),
        Some("\n"),
        "v1 top-level Prefix must be verbatim, not percent-encoded: {xml}"
    );
    assert!(contents_keys(&xml).is_empty());
}
