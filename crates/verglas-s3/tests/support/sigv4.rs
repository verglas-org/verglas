//! SigV4 signing helpers for auth integration tests (issue #7).

#![allow(dead_code)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::useless_vec)]

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderName, HeaderValue};

/// Static keypair tests boot the endpoint with.
pub const ACCESS_KEY: &str = "test-access-key";
/// Secret paired with [`ACCESS_KEY`].
pub const SECRET_KEY: &str = "test-secret-key-should-be-long-enough";
/// Region engines and the verifier agree on in tests.
pub const REGION: &str = "us-east-1";
/// Bucket the in-memory origin serves.
pub const BUCKET: &str = "auth-bucket";

/// SHA-256 of an empty body — the default payload hash for GET/HEAD.
const EMPTY_PAYLOAD_HASH: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// Payload hash for aws-chunked streaming SigV4 PUT bodies.
pub const STREAMING_PAYLOAD_HASH: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";

/// Signs an HTTP request with SigV4 header auth and returns headers to attach.
pub fn sign_headers(
    method: &str,
    url: &str,
    access_key: &str,
    secret_key: &str,
    region: &str,
    datetime: DateTime<Utc>,
    extra: &[(&str, &str)],
) -> HeaderMap {
    let host = host_from_url(url);
    let (path, canonical_query) = path_and_query_from_url(url);
    let amz_date = datetime.format("%Y%m%dT%H%M%SZ").to_string();
    let date_stamp = datetime.format("%Y%m%d").to_string();

    let mut header_pairs = vec![
        ("host", host),
        ("x-amz-date", amz_date.as_str()),
        ("x-amz-content-sha256", EMPTY_PAYLOAD_HASH),
    ];
    for (name, value) in extra {
        if let Some(existing) = header_pairs.iter_mut().find(|(n, _)| *n == *name) {
            existing.1 = value;
        } else {
            header_pairs.push((name, value));
        }
    }
    header_pairs.sort_by_key(|(name, _)| *name);

    let signed_headers = header_pairs
        .iter()
        .map(|(name, _)| *name)
        .collect::<Vec<_>>()
        .join(";");
    let canonical_headers = header_pairs
        .iter()
        .map(|(name, value)| format!("{name}:{value}"))
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";

    let payload_hash = header_pairs
        .iter()
        .find(|(name, _)| *name == "x-amz-content-sha256")
        .map(|(_, value)| *value)
        .unwrap_or(EMPTY_PAYLOAD_HASH);

    let canonical_request = format!(
        "{method}\n{path}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
    );
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{date_stamp}/{region}/s3/aws4_request\n{}",
        hex_sha256(canonical_request.as_bytes())
    );
    let signature =
        calculate_sigv4_signature(secret_key, &date_stamp, region, "s3", &string_to_sign);
    let credential = format!("{access_key}/{date_stamp}/{region}/s3/aws4_request");
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={credential}, SignedHeaders={signed_headers}, Signature={signature}"
    );

    let mut headers = HeaderMap::new();
    for (name, value) in header_pairs {
        headers.insert(
            HeaderName::from_bytes(name.as_bytes()).expect("header name"),
            HeaderValue::from_str(value).expect("header value"),
        );
    }
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&authorization).expect("authorization"),
    );
    headers
}

/// Builds a SigV4 presigned GET URL for `path` on `base` (no trailing slash).
pub fn presign_get(
    base: &str,
    path: &str,
    access_key: &str,
    secret_key: &str,
    region: &str,
    datetime: DateTime<Utc>,
    expires_secs: u64,
) -> String {
    let host = host_from_url(base);
    let amz_date = datetime.format("%Y%m%dT%H%M%SZ").to_string();
    let date_stamp = datetime.format("%Y%m%d").to_string();
    let credential = format!("{access_key}/{date_stamp}/{region}/s3/aws4_request");
    let signed_headers = "host";
    let canonical_uri = path;

    let expires = expires_secs.to_string();
    let mut params = [
        ("X-Amz-Algorithm", "AWS4-HMAC-SHA256"),
        ("X-Amz-Credential", credential.as_str()),
        ("X-Amz-Date", amz_date.as_str()),
        ("X-Amz-Expires", expires.as_str()),
        ("X-Amz-SignedHeaders", signed_headers),
    ]
    .to_vec();
    params.sort_by_key(|(k, _)| *k);

    let canonical_query = params
        .iter()
        .map(|(k, v)| format!("{}={}", urlencoding_encode(k), urlencoding_encode(v)))
        .collect::<Vec<_>>()
        .join("&");

    let canonical_headers = format!("host:{host}\n");
    let canonical_request = format!(
        "GET\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\nUNSIGNED-PAYLOAD"
    );

    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{date_stamp}/{region}/s3/aws4_request\n{}",
        hex_sha256(canonical_request.as_bytes())
    );

    let signature =
        calculate_sigv4_signature(secret_key, &date_stamp, region, "s3", &string_to_sign);
    format!("{base}{path}?{canonical_query}&X-Amz-Signature={signature}")
}

/// Host component extracted from a URL string (includes port when present).
fn host_from_url(url: &str) -> &str {
    url.strip_prefix("http://")
        .or_else(|| url.strip_prefix("https://"))
        .and_then(|rest| rest.split('/').next())
        .expect("url with host")
}

/// Path and SigV4 canonical query string extracted from a URL.
fn path_and_query_from_url(url: &str) -> (&str, String) {
    let full = path_from_url(url);
    match full.split_once('?') {
        Some((path, query)) => (path, canonical_query_string(query)),
        None => (full, String::new()),
    }
}

/// Builds the canonical query string for SigV4 from a raw query (no leading `?`).
fn canonical_query_string(query: &str) -> String {
    if query.is_empty() {
        return String::new();
    }
    let mut params: Vec<(String, String)> = query
        .split('&')
        .filter(|segment| !segment.is_empty())
        .map(|pair| {
            if let Some((key, value)) = pair.split_once('=') {
                (key.to_owned(), value.to_owned())
            } else {
                (pair.to_owned(), String::new())
            }
        })
        .collect();
    params.sort_by(|left, right| left.0.cmp(&right.0));
    params
        .iter()
        .map(|(key, value)| format!("{}={}", urlencoding_encode(key), urlencoding_encode(value)))
        .collect::<Vec<_>>()
        .join("&")
}

/// Path and query component of a URL.
fn path_from_url(url: &str) -> &str {
    url.strip_prefix("http://")
        .or_else(|| url.strip_prefix("https://"))
        .and_then(|rest| rest.find('/').map(|idx| &rest[idx..]))
        .unwrap_or("/")
}

/// Percent-encodes a query parameter key or value per SigV4 rules.
fn urlencoding_encode(value: &str) -> String {
    urlencoding::encode(value).into_owned()
}

/// Hex-encoded SHA-256 digest.
fn hex_sha256(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(bytes))
}

/// Derives the SigV4 signing key and returns the request signature hex string.
fn calculate_sigv4_signature(
    secret_key: &str,
    date_stamp: &str,
    region: &str,
    service: &str,
    string_to_sign: &str,
) -> String {
    let k_secret = format!("AWS4{secret_key}");
    let k_date = hmac_sign(k_secret.as_bytes(), date_stamp.as_bytes());
    let k_region = hmac_sign(&k_date, region.as_bytes());
    let k_service = hmac_sign(&k_region, service.as_bytes());
    let k_signing = hmac_sign(&k_service, b"aws4_request");
    hex::encode(hmac_sign(&k_signing, string_to_sign.as_bytes()))
}

/// HMAC-SHA256 helper returning raw 32-byte tag.
fn hmac_sign(key: &[u8], data: &[u8]) -> Vec<u8> {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("hmac key");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// Parses the `<Code>` element from an S3 error XML body.
pub fn error_code(xml: &str) -> Option<String> {
    let start = xml.find("<Code>")? + "<Code>".len();
    let end = xml[start..].find("</Code>")? + start;
    Some(xml[start..end].to_owned())
}

/// Ordered map of tag -> text for structural comparison of error XML.
pub fn error_xml_fields(xml: &str) -> HashMap<String, String> {
    let mut fields = HashMap::new();
    let mut rest = xml;
    while let Some(start) = rest.find('<') {
        rest = &rest[start + 1..];
        let Some(end) = rest.find('>') else { break };
        let tag = &rest[..end];
        if tag.starts_with('/') || tag.starts_with('?') {
            continue;
        }
        let close = format!("</{tag}>");
        if let Some(close_start) = rest.find(&close) {
            let value = &rest[end + 1..close_start];
            fields.insert(tag.to_owned(), value.to_owned());
            rest = &rest[close_start + close.len()..];
        }
    }
    fields
}

/// Builds an aws-chunked PUT body and the request headers for streaming SigV4.
///
/// `decoded` is the object bytes the server should persist; the wire body is
/// chunked with per-chunk signatures chained from the seed signature.
pub fn sign_aws_chunked_put(
    url: &str,
    access_key: &str,
    secret_key: &str,
    region: &str,
    datetime: DateTime<Utc>,
    decoded: &[u8],
    chunk_size: usize,
) -> (HeaderMap, Vec<u8>) {
    let host = host_from_url(url);
    let path = path_from_url(url);
    let amz_date = datetime.format("%Y%m%dT%H%M%SZ").to_string();
    let date_stamp = datetime.format("%Y%m%d").to_string();

    let chunk_count = decoded.len().div_ceil(chunk_size);
    let placeholders: Vec<String> = (0..chunk_count + 1).map(|_| "0".repeat(64)).collect();
    let wire_len = aws_chunked_wire_length(decoded.len(), chunk_size, &placeholders);

    let seed_signature = streaming_seed_signature(
        secret_key,
        region,
        &amz_date,
        &date_stamp,
        host,
        path,
        decoded.len(),
        wire_len,
    );

    let mut signatures = Vec::new();
    let mut prev = seed_signature.clone();
    for chunk in decoded.chunks(chunk_size) {
        let string_to_sign = chunk_string_to_sign(&amz_date, &date_stamp, region, &prev, chunk);
        let sig = calculate_sigv4_signature(secret_key, &date_stamp, region, "s3", &string_to_sign);
        prev = sig.clone();
        signatures.push(sig);
    }
    let final_string_to_sign = chunk_string_to_sign(&amz_date, &date_stamp, region, &prev, &[]);
    signatures.push(calculate_sigv4_signature(
        secret_key,
        &date_stamp,
        region,
        "s3",
        &final_string_to_sign,
    ));

    let wire_body = encode_aws_chunked_body(decoded, chunk_size, &signatures);
    assert_eq!(
        wire_body.len(),
        wire_len,
        "chunk signatures must be fixed-width hex"
    );

    let mut header_pairs = vec![
        ("content-encoding".to_owned(), "aws-chunked".to_owned()),
        ("content-length".to_owned(), wire_len.to_string()),
        ("host".to_owned(), host.to_owned()),
        (
            "x-amz-content-sha256".to_owned(),
            STREAMING_PAYLOAD_HASH.to_owned(),
        ),
        ("x-amz-date".to_owned(), amz_date.clone()),
        (
            "x-amz-decoded-content-length".to_owned(),
            decoded.len().to_string(),
        ),
    ];
    header_pairs.sort_by_key(|(name, _)| name.clone());

    let signed_headers = header_pairs
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<Vec<_>>()
        .join(";");

    let credential = format!("{access_key}/{date_stamp}/{region}/s3/aws4_request");
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={credential}, SignedHeaders={signed_headers}, Signature={seed_signature}"
    );

    let mut headers = HeaderMap::new();
    for (name, value) in &header_pairs {
        headers.insert(
            HeaderName::from_bytes(name.as_bytes()).expect("header name"),
            HeaderValue::from_str(value).expect("header value"),
        );
    }
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&authorization).expect("authorization"),
    );
    (headers, wire_body)
}

/// Computes the seed signature for an aws-chunked PUT from header fields.
fn streaming_seed_signature(
    secret_key: &str,
    region: &str,
    amz_date: &str,
    date_stamp: &str,
    host: &str,
    path: &str,
    decoded_content_length: usize,
    wire_content_length: usize,
) -> String {
    let mut header_pairs = [
        ("content-encoding", "aws-chunked".to_owned()),
        ("content-length", wire_content_length.to_string()),
        ("host", host.to_owned()),
        ("x-amz-content-sha256", STREAMING_PAYLOAD_HASH.to_owned()),
        ("x-amz-date", amz_date.to_owned()),
        (
            "x-amz-decoded-content-length",
            decoded_content_length.to_string(),
        ),
    ];
    header_pairs.sort_by_key(|(name, _)| name.to_owned());

    let signed_headers = header_pairs
        .iter()
        .map(|(name, _)| *name)
        .collect::<Vec<_>>()
        .join(";");
    let canonical_headers = header_pairs
        .iter()
        .map(|(name, value)| format!("{name}:{value}"))
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";

    let canonical_request =
        format!("PUT\n{path}\n\n{canonical_headers}\n{signed_headers}\n{STREAMING_PAYLOAD_HASH}");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{date_stamp}/{region}/s3/aws4_request\n{}",
        hex_sha256(canonical_request.as_bytes())
    );
    calculate_sigv4_signature(secret_key, date_stamp, region, "s3", &string_to_sign)
}

/// Returns the on-wire length of an aws-chunked body given chunk signatures.
fn aws_chunked_wire_length(
    decoded_len: usize,
    chunk_size: usize,
    signature_placeholders: &[String],
) -> usize {
    let chunk_count = decoded_len.div_ceil(chunk_size);
    assert_eq!(
        signature_placeholders.len(),
        chunk_count + 1,
        "need one signature per data chunk plus the final zero chunk"
    );
    let mut len = 0usize;
    let mut offset = 0usize;
    for sig in signature_placeholders.iter().take(chunk_count) {
        let data_len = chunk_size.min(decoded_len - offset);
        len += format!("{data_len:x};chunk-signature={sig}\r\n").len() + data_len + 2;
        offset += data_len;
    }
    let final_sig = signature_placeholders
        .last()
        .expect("final chunk signature");
    len += format!("0;chunk-signature={final_sig}\r\n\r\n").len();
    len
}

/// Encodes `decoded` as an aws-chunked body using precomputed chunk signatures.
fn encode_aws_chunked_body(decoded: &[u8], chunk_size: usize, signatures: &[String]) -> Vec<u8> {
    let chunk_count = decoded.len().div_ceil(chunk_size);
    assert_eq!(signatures.len(), chunk_count + 1);

    let mut body = Vec::new();
    let mut offset = 0usize;
    for sig in signatures.iter().take(chunk_count) {
        let data_len = chunk_size.min(decoded.len() - offset);
        body.extend_from_slice(format!("{data_len:x};chunk-signature={sig}\r\n").as_bytes());
        body.extend_from_slice(&decoded[offset..offset + data_len]);
        body.extend_from_slice(b"\r\n");
        offset += data_len;
    }
    let final_sig = signatures.last().expect("final chunk signature");
    body.extend_from_slice(format!("0;chunk-signature={final_sig}\r\n\r\n").as_bytes());
    body
}

/// Builds the chunk `string_to_sign` for streaming SigV4.
fn chunk_string_to_sign(
    amz_date: &str,
    date_stamp: &str,
    region: &str,
    prev_signature: &str,
    chunk_data: &[u8],
) -> String {
    let chunk_hash = if chunk_data.is_empty() {
        EMPTY_PAYLOAD_HASH.to_owned()
    } else {
        hex_sha256(chunk_data)
    };
    format!(
        "AWS4-HMAC-SHA256-PAYLOAD\n{amz_date}\n{date_stamp}/{region}/s3/aws4_request\n{prev_signature}\n{EMPTY_PAYLOAD_HASH}\n{chunk_hash}"
    )
}
