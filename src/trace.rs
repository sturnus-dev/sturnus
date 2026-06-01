//! W3C `traceparent` propagation: parse the inbound header into a request span.

#[derive(Debug, PartialEq, Eq)]
pub struct TraceContext {
    pub trace_id: String,
    pub parent_span_id: String,
}

/// Parse a W3C `traceparent` into the caller's trace and parent-span ids.
pub fn parse_traceparent(value: &str) -> Option<TraceContext> {
    let mut fields = value.trim().split('-');
    let version = fields.next()?;
    let trace_id = fields.next()?;
    let parent_id = fields.next()?;
    let _flags = fields.next()?;
    if version == "00" && fields.next().is_some() {
        return None;
    }
    if version.len() != 2 || !is_hex(version) || version == "ff" {
        return None;
    }
    if trace_id.len() != 32 || !is_hex(trace_id) || is_all_zero(trace_id) {
        return None;
    }
    if parent_id.len() != 16 || !is_hex(parent_id) || is_all_zero(parent_id) {
        return None;
    }
    Some(TraceContext {
        trace_id: trace_id.to_owned(),
        parent_span_id: parent_id.to_owned(),
    })
}

fn is_hex(s: &str) -> bool {
    s.bytes().all(|b| b.is_ascii_hexdigit())
}

fn is_all_zero(s: &str) -> bool {
    s.bytes().all(|b| b == b'0')
}

pub fn request_span(headers: &hyper::HeaderMap) -> tracing::Span {
    let request_id = uuid::Uuid::new_v4();
    match headers
        .get("traceparent")
        .and_then(|v| v.to_str().ok())
        .and_then(parse_traceparent)
    {
        Some(ctx) => tracing::info_span!(
            "request",
            %request_id,
            trace_id = %ctx.trace_id,
            parent_span_id = %ctx.parent_span_id
        ),
        None => tracing::info_span!("request", %request_id),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    #[test]
    fn parses_valid() {
        assert_eq!(
            parse_traceparent(&format!("  {VALID} ")).unwrap(),
            TraceContext {
                trace_id: "0af7651916cd43dd8448eb211c80319c".to_owned(),
                parent_span_id: "b7ad6b7169203331".to_owned(),
            }
        );
    }

    #[test]
    fn rejects_malformed() {
        for bad in [
            "",
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331", // too few fields
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01-x", // v00 extra field
            "00-0af7651916cd43dd-b7ad6b7169203331-01",              // short trace_id
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331ab-01", // long parent_id
            "00-0af7651916cd43dd8448eb211c80319g-b7ad6b7169203331-01", // non-hex
            "00-00000000000000000000000000000000-b7ad6b7169203331-01", // zero trace_id
            "00-0af7651916cd43dd8448eb211c80319c-0000000000000000-01", // zero parent_id
            "ff-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", // reserved version
        ] {
            assert!(parse_traceparent(bad).is_none(), "{bad}");
        }
    }

    #[test]
    fn request_span_smoke() {
        let mut headers = hyper::HeaderMap::new();
        headers.insert("traceparent", VALID.parse().unwrap());
        let _ = request_span(&headers);
        let _ = request_span(&hyper::HeaderMap::new());
    }
}
