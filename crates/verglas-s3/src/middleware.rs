//! Tower/axum middleware that stamps each request with a trace id and shapes
//! HTTP responses like real S3.
//!
//! [`trace_request`] runs outermost. It mints one `request_id` per request,
//! makes it the current [`verglas_core::trace`] id for the whole request future
//! (so cache-lookup, peer-fetch, and fill logs share it), opens the request's
//! root tracing span, and — after the inner service answers — returns the same
//! id to the client. Because the id is generated here, before dispatch, the
//! value in `x-amz-request-id` is the value the logs carry: a client error can
//! be traced straight back into the serving path (#61).
//!
//! s3s renders error XML and status codes but omits `RequestId` elements and
//! the `x-amz-request-id` header that SDKs expect; [`trace_request`] adds them
//! on every response and fills standard `Message` text when s3s left it empty.
//! Error XML enrichment buffers the body, but only for 4xx/5xx responses —
//! success bodies (including large `application/xml` objects) stream through
//! untouched.
//!
//! A second, narrowly scoped transform unquotes the `<ETag>` in a
//! GetObjectAttributes response (issue #209). It is NOT a blanket success-body
//! rewriter: it fires only when the handler tagged the response with
//! [`UnquoteAttributesEtag`], so no other success body is read, buffered, or
//! inspected. See [`unquote_attributes_etag`] for why this is unavoidable.

use axum::body::Body;
use axum::extract::Request;
use axum::http::{HeaderValue, Response};
use axum::middleware::Next;
use http_body_util::BodyExt;
use tracing::Instrument;
use verglas_core::trace::RequestId;

/// AWS-shaped default messages for auth errors s3s sometimes omits.
fn default_error_message(code: &str) -> Option<&'static str> {
    match code {
        "SignatureDoesNotMatch" => Some(
            "The request signature we calculated does not match the signature you provided. Check your key and signing method.",
        ),
        "InvalidAccessKeyId" => {
            Some("The AWS Access Key Id you provided does not exist in our records.")
        }
        "AccessDenied" => Some("Access Denied"),
        "RequestTimeTooSkewed" => {
            Some("The difference between the request time and the current time is too large.")
        }
        _ => None,
    }
}

/// True when the status warrants reading and enriching a small S3 error XML body.
fn is_error_status(status: axum::http::StatusCode) -> bool {
    status.is_client_error() || status.is_server_error()
}

/// Outermost request layer (#61): mints the request id, scopes it for the whole
/// request future so the serving path logs under it, opens the root request
/// span, and stamps the id onto the response the client sees. The generated id
/// is the single source of truth — the same value logs, threads through the
/// peer RPC, and returns in `x-amz-request-id` — so a client-reported id maps
/// straight to the logs for that request.
pub async fn trace_request(request: Request, next: Next) -> Response<Body> {
    let request_id = RequestId::generate();
    // Method only on the span — never the URI path, which carries the object
    // key. Key redaction is per #61: the key appears in logs only as a hash,
    // emitted by the layers that already hold the parsed key.
    let method = request.method().clone();
    let span = tracing::info_span!(
        "s3_request",
        request_id = %request_id,
        method = %method,
    );
    // Make the id the current trace id for the whole request future (cache
    // lookup, ring routing, peer fetch, origin fill all read it) and run that
    // future inside the root span.
    let response =
        verglas_core::trace::scope(request_id.clone(), next.run(request).instrument(span)).await;
    shape_response(response, request_id.as_str()).await
}

/// Injects `x-amz-request-id` on every response. For 4xx/5xx XML error bodies,
/// also adds a `RequestId` element and standard `Message` when s3s omitted them.
async fn shape_response(response: Response<Body>, request_id: &str) -> Response<Body> {
    let (mut parts, body) = response.into_parts();

    if let Ok(value) = HeaderValue::from_str(request_id) {
        parts.headers.insert("x-amz-request-id", value);
    }

    // A `304 Not Modified` (issue #10's conditional reads) must carry no body,
    // per RFC 7232. s3s renders the outcome through its error path, which sets
    // an XML body; drop it here and the length/type headers that described it,
    // keeping the validator headers (ETag) the client needs.
    if parts.status == axum::http::StatusCode::NOT_MODIFIED {
        parts.headers.remove("content-length");
        parts.headers.remove("content-type");
        return Response::from_parts(parts, Body::empty());
    }

    // Success responses — including large application/xml objects — must stream
    // without buffering; only error bodies are collected for enrichment.
    if !is_error_status(parts.status) {
        return Response::from_parts(parts, body);
    }

    let is_xml = parts
        .headers
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.contains("xml"));

    if !is_xml {
        return Response::from_parts(parts, body);
    }

    let collected = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(error) => {
            tracing::warn!(%error, "failed to collect S3 error response body for enrichment");
            parts.headers.remove("content-length");
            return Response::from_parts(parts, Body::empty());
        }
    };

    let text = String::from_utf8_lossy(&collected);
    if !text.contains("<Error>") {
        return Response::from_parts(parts, Body::from(collected));
    }

    let enriched = enrich_error_xml(&text, request_id);
    if let Ok(len) = HeaderValue::from_str(&enriched.len().to_string()) {
        parts.headers.insert("content-length", len);
    }
    Response::from_parts(parts, Body::from(enriched))
}

/// Adds `<RequestId>` and a default `<Message>` to an S3 error XML document.
fn enrich_error_xml(xml: &str, request_id: &str) -> Vec<u8> {
    let mut out = xml.to_owned();

    if !out.contains("<Message>")
        && let Some(start) = out.find("<Code>")
        && let Some(end) = out[start..].find("</Code>")
    {
        let code_start = start + "<Code>".len();
        let code_end = start + end;
        let code = &out[code_start..code_end];
        if let Some(message) = default_error_message(code) {
            let insert_at = start + end + "</Code>".len();
            out.insert_str(insert_at, &format!("<Message>{message}</Message>"));
        }
    }

    if !out.contains("<RequestId>")
        && let Some(pos) = out.rfind("</Error>")
    {
        out.insert_str(pos, &format!("<RequestId>{request_id}</RequestId>"));
    }

    out.into_bytes()
}

/// Marker set by the GetObjectAttributes handler on its `S3Response`
/// extensions. s3s copies response extensions onto the HTTP response, so the
/// [`unquote_attributes_etag`] layer can recognise exactly this one operation's
/// body without sniffing content.
#[derive(Clone, Copy)]
pub struct UnquoteAttributesEtag;

/// Rewrites `<ETag>"value"</ETag>` to `<ETag>value</ETag>` in a
/// GetObjectAttributes body, and only there.
///
/// Why this exists: s3s serializes every XML `ETag` with surrounding quotes
/// (`SerializeContent for ETag`, a blanket impl). That is correct for List,
/// Copy, and Part bodies, but AWS reports the ETag in a GetObjectAttributes
/// body WITHOUT quotes. s3s exposes no per-response serialization hook — the
/// output is a typed `GetObjectAttributesOutput`, the serializer is internal,
/// and `S3Response` carries no raw-body override — so the quoting cannot be
/// fixed inside the handler. The proper fix is upstream in s3s
/// (https://github.com/s3s-project/s3s/issues/629); until it lands this is the
/// smallest correct local workaround.
///
/// It is safe by construction: it acts only when the handler tagged the
/// response with [`UnquoteAttributesEtag`], never on any other success body,
/// and a GetObjectAttributes body is always a small bounded XML document (an
/// attributes report, never a streamed object), so buffering it is not the
/// blanket success-body buffering issue #209 rules out.
pub async fn unquote_attributes_etag(response: Response<Body>) -> Response<Body> {
    if response
        .extensions()
        .get::<UnquoteAttributesEtag>()
        .is_none()
    {
        return response;
    }
    let (mut parts, body) = response.into_parts();
    let collected = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(error) => {
            tracing::warn!(%error, "failed to collect GetObjectAttributes body");
            parts.headers.remove("content-length");
            return Response::from_parts(parts, Body::empty());
        }
    };
    let text = String::from_utf8_lossy(&collected);
    let rewritten = unquote_etag_element(&text);
    if let Ok(len) = HeaderValue::from_str(&rewritten.len().to_string()) {
        parts.headers.insert("content-length", len);
    }
    Response::from_parts(parts, Body::from(rewritten))
}

/// Strips the quotes s3s added around the value inside the first `<ETag>`
/// element. Leaves the document untouched if there is no quoted `<ETag>`.
fn unquote_etag_element(xml: &str) -> Vec<u8> {
    let Some(open) = xml.find("<ETag>") else {
        return xml.as_bytes().to_vec();
    };
    let value_start = open + "<ETag>".len();
    let Some(rel_end) = xml[value_start..].find("</ETag>") else {
        return xml.as_bytes().to_vec();
    };
    let value_end = value_start + rel_end;
    let value = &xml[value_start..value_end];
    let unquoted = value
        .strip_prefix('"')
        .and_then(|v| v.strip_suffix('"'))
        .unwrap_or(value);
    let mut out = String::with_capacity(xml.len());
    out.push_str(&xml[..value_start]);
    out.push_str(unquoted);
    out.push_str(&xml[value_end..]);
    out.into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Proves message and request id injection for a bare Code-only error body.
    #[test]
    fn enrich_error_xml_adds_message_and_request_id() {
        let xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>SignatureDoesNotMatch</Code></Error>";
        let got = String::from_utf8(enrich_error_xml(xml, "ABCDEF0123456789")).expect("utf8");
        assert!(got.contains("<Message>The request signature we calculated does not match"));
        assert!(got.contains("<RequestId>ABCDEF0123456789</RequestId>"));
    }

    /// Existing Message and RequestId elements must not be duplicated.
    #[test]
    fn enrich_error_xml_is_idempotent() {
        let xml = concat!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
            "<Error><Code>AccessDenied</Code>",
            "<Message>Signature is required</Message>",
            "<RequestId>EXISTING</RequestId></Error>"
        );
        let got = String::from_utf8(enrich_error_xml(xml, "NEW")).expect("utf8");
        assert_eq!(got.matches("<Message>").count(), 1);
        assert!(got.contains("<RequestId>EXISTING</RequestId>"));
        assert!(!got.contains("NEW"));
    }

    /// Non-error XML must pass through unchanged.
    #[test]
    fn enrich_error_xml_skips_non_error_documents() {
        let xml = "<?xml version=\"1.0\"?><ListBucketResult></ListBucketResult>";
        let got = enrich_error_xml(xml, "IGNORED");
        assert_eq!(got, xml.as_bytes());
    }

    /// Default messages cover the auth-related codes engines see in the wild.
    #[test]
    fn default_messages_for_auth_codes() {
        assert!(default_error_message("SignatureDoesNotMatch").is_some());
        assert!(default_error_message("InvalidAccessKeyId").is_some());
        assert!(default_error_message("RequestTimeTooSkewed").is_some());
    }

    /// Only 4xx/5xx responses are candidates for body enrichment.
    #[test]
    fn is_error_status_classifies_responses() {
        assert!(is_error_status(axum::http::StatusCode::FORBIDDEN));
        assert!(is_error_status(
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        ));
        assert!(!is_error_status(axum::http::StatusCode::OK));
        assert!(!is_error_status(axum::http::StatusCode::PARTIAL_CONTENT));
    }

    /// #209: a quoted `<ETag>` in a GetObjectAttributes body is unquoted, and
    /// nothing else in the document is touched.
    #[test]
    fn unquote_etag_strips_only_the_etag_quotes() {
        let xml = concat!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
            "<GetObjectAttributesResponse><ETag>\"d41d8cd98f00b204e9800998ecf8427e\"</ETag>",
            "<ObjectSize>3</ObjectSize><StorageClass>STANDARD</StorageClass>",
            "</GetObjectAttributesResponse>"
        );
        let got = String::from_utf8(unquote_etag_element(xml)).expect("utf8");
        assert!(got.contains("<ETag>d41d8cd98f00b204e9800998ecf8427e</ETag>"));
        assert!(!got.contains("\"d41d8cd98f00b204e9800998ecf8427e\""));
        // The attribute quotes in the XML declaration and the rest of the body
        // are left intact.
        assert!(got.contains("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"));
        assert!(got.contains("<ObjectSize>3</ObjectSize>"));
    }

    /// A multipart ETag (`hash-N`) unquotes the same way.
    #[test]
    fn unquote_etag_handles_multipart_etag() {
        let xml = "<Resp><ETag>\"b2add96cc9702bbf4efb0ccdfc6b7747-3\"</ETag></Resp>";
        let got = String::from_utf8(unquote_etag_element(xml)).expect("utf8");
        assert_eq!(
            got,
            "<Resp><ETag>b2add96cc9702bbf4efb0ccdfc6b7747-3</ETag></Resp>"
        );
    }

    /// A body with no `<ETag>` passes through byte-for-byte.
    #[test]
    fn unquote_etag_ignores_bodies_without_etag() {
        let xml =
            "<GetObjectAttributesResponse><ObjectSize>3</ObjectSize></GetObjectAttributesResponse>";
        let got = unquote_etag_element(xml);
        assert_eq!(got, xml.as_bytes());
    }

    /// An already-unquoted `<ETag>` is left unchanged (idempotent).
    #[test]
    fn unquote_etag_is_idempotent() {
        let xml = "<Resp><ETag>abc123</ETag></Resp>";
        let got = String::from_utf8(unquote_etag_element(xml)).expect("utf8");
        assert_eq!(got, xml);
    }
}
