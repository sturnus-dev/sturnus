use bytes::Bytes;
use futures_util::StreamExt;
use http_body_util::{Full, StreamBody};
use hyper::body::Frame;
use reqwest::Client;
use std::convert::Infallible;
use std::time::Instant;
use tracing::{debug, warn};

use crate::gcp_auth::GcpTokenProvider;
use crate::model_map::{ProviderKind, ResolvedCandidate};

pub type HyperBody = http_body_util::Either<
    Full<Bytes>,
    StreamBody<futures_util::stream::BoxStream<'static, Result<Frame<Bytes>, Infallible>>>,
>;

pub struct ProxyResult {
    pub status: hyper::StatusCode,
    pub headers: hyper::HeaderMap,
    pub body: ProxyBody,
    pub ttfc: std::time::Duration,
}

pub enum ProxyBody {
    Streaming(futures_util::stream::BoxStream<'static, Result<Frame<Bytes>, Infallible>>),
    Full(Bytes),
}

fn build_upstream_url(candidate: &ResolvedCandidate, path: &str) -> String {
    let base = candidate.base_url.trim_end_matches('/');

    // Clients send OpenAI-compatible paths like /v1/chat/completions.
    // Strip the /v1 prefix — every provider's base_url already includes
    // the correct version prefix (e.g. /v1, /v1beta1, /openai).
    let effective_path = path.strip_prefix("/v1").unwrap_or(path);

    match candidate.kind {
        ProviderKind::AzureOpenAi { ref api_version } => {
            // Azure: /openai/deployments/{model}/{path}?api-version={version}
            format!(
                "{}/deployments/{}{}?api-version={}",
                base, candidate.model, effective_path, api_version
            )
        }
        _ => format!("{}{}", base, effective_path),
    }
}

pub async fn forward_request(
    client: &Client,
    candidate: &ResolvedCandidate,
    path: &str,
    body_bytes: Bytes,
    is_streaming: bool,
    gcp_token_provider: Option<&GcpTokenProvider>,
) -> Result<ProxyResult, Box<dyn std::error::Error + Send + Sync>> {
    let rewritten_body = rewrite_model(&body_bytes, &candidate.model)?;

    let url = build_upstream_url(candidate, path);
    debug!(url = %url, model = %candidate.model, provider = %candidate.provider_name, "forwarding request");

    let mut req = client.post(&url).header("content-type", "application/json");

    match candidate.kind {
        ProviderKind::ApiKey => {
            if let Some(ref key) = candidate.api_key {
                req = req.header("authorization", format!("Bearer {key}"));
            }
        }
        ProviderKind::GcpMetadata => {
            let provider =
                gcp_token_provider.ok_or("gcp_metadata auth requires GCP token provider")?;
            let token = provider.get_token().await?;
            req = req.header("authorization", format!("Bearer {token}"));
        }
        ProviderKind::AzureOpenAi { .. } => {
            if let Some(ref key) = candidate.api_key {
                req = req.header("api-key", key.as_str());
            }
        }
        ProviderKind::Anthropic { ref version } => {
            if let Some(ref key) = candidate.api_key {
                req = req.header("x-api-key", key.as_str());
            }
            req = req.header("anthropic-version", version.as_str());
        }
    }

    let t0 = Instant::now();
    let response = req.body(rewritten_body).send().await?;

    let status = hyper::StatusCode::from_u16(response.status().as_u16())?;

    let mut headers = hyper::HeaderMap::new();
    for (k, v) in response.headers() {
        if let (Ok(name), Ok(val)) = (
            hyper::header::HeaderName::from_bytes(k.as_str().as_bytes()),
            hyper::header::HeaderValue::from_bytes(v.as_bytes()),
        ) {
            headers.insert(name, val);
        }
    }

    if is_streaming {
        let mut stream = response.bytes_stream();

        let first_chunk = stream.next().await;
        let ttfc = t0.elapsed();

        let first_chunk = match first_chunk {
            Some(Ok(chunk)) => chunk,
            Some(Err(e)) => {
                warn!("error reading first chunk: {e}");
                return Err(e.into());
            }
            None => Bytes::new(),
        };

        let first =
            futures_util::stream::once(
                async move { Ok::<_, Infallible>(Frame::data(first_chunk)) },
            );
        let rest = build_relay_stream(stream);
        let combined = first.chain(rest);

        Ok(ProxyResult {
            status,
            headers,
            body: ProxyBody::Streaming(Box::pin(combined)),
            ttfc,
        })
    } else {
        let full_body = response.bytes().await?;
        let ttfc = t0.elapsed();

        Ok(ProxyResult {
            status,
            headers,
            body: ProxyBody::Full(full_body),
            ttfc,
        })
    }
}

/// Extract the value of a top-level JSON string field by scanning raw bytes.
/// Returns the byte range of the value (excluding quotes) and the value itself.
/// Only scans top-level keys (brace depth == 1) to avoid matching nested fields.
pub fn extract_json_field<'a>(
    body: &'a [u8],
    field: &str,
) -> Option<(std::ops::Range<usize>, &'a str)> {
    let needle = format!("\"{}\"", field);
    let needle_bytes = needle.as_bytes();
    let mut depth: u32 = 0;
    let mut i = 0;

    while i < body.len() {
        match body[i] {
            b'{' => depth += 1,
            b'}' => depth = depth.saturating_sub(1),
            b'"' if depth == 1 && body[i..].starts_with(needle_bytes) => {
                // Found the key at top level — skip past `"field"` and any `: `
                let after_key = i + needle_bytes.len();
                let mut j = after_key;
                while j < body.len() && (body[j] == b':' || body[j] == b' ') {
                    j += 1;
                }
                if j < body.len() && body[j] == b'"' {
                    // String value — find closing quote
                    let val_start = j + 1;
                    let mut k = val_start;
                    while k < body.len() && body[k] != b'"' {
                        if body[k] == b'\\' {
                            k += 1; // skip escaped char
                        }
                        k += 1;
                    }
                    let val_bytes = &body[val_start..k];
                    if let Ok(val) = std::str::from_utf8(val_bytes) {
                        return Some((val_start..k, val));
                    }
                }
                // Non-string value (e.g., `"stream": true`) — skip
                i = after_key;
                continue;
            }
            b'"' => {
                // Skip over other string values to avoid false matches
                i += 1;
                while i < body.len() && body[i] != b'"' {
                    if body[i] == b'\\' {
                        i += 1;
                    }
                    i += 1;
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Check if a top-level JSON boolean field is `true` by scanning raw bytes.
pub fn extract_json_bool(body: &[u8], field: &str) -> Option<bool> {
    let needle = format!("\"{}\"", field);
    let needle_bytes = needle.as_bytes();
    let mut depth: u32 = 0;
    let mut i = 0;

    while i < body.len() {
        match body[i] {
            b'{' => depth += 1,
            b'}' => depth = depth.saturating_sub(1),
            b'"' if depth == 1 && body[i..].starts_with(needle_bytes) => {
                let after_key = i + needle_bytes.len();
                let mut j = after_key;
                while j < body.len() && (body[j] == b':' || body[j] == b' ') {
                    j += 1;
                }
                if body[j..].starts_with(b"true") {
                    return Some(true);
                } else if body[j..].starts_with(b"false") {
                    return Some(false);
                }
                return None;
            }
            b'"' => {
                i += 1;
                while i < body.len() && body[i] != b'"' {
                    if body[i] == b'\\' {
                        i += 1;
                    }
                    i += 1;
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Splice the real model name into the JSON body without full parsing.
pub fn rewrite_model(
    body: &[u8],
    real_model: &str,
) -> Result<Bytes, Box<dyn std::error::Error + Send + Sync>> {
    let (range, _) =
        extract_json_field(body, "model").ok_or("missing 'model' field in request body")?;

    let mut result = Vec::with_capacity(body.len() + real_model.len());
    result.extend_from_slice(&body[..range.start]);
    result.extend_from_slice(real_model.as_bytes());
    result.extend_from_slice(&body[range.end..]);
    Ok(Bytes::from(result))
}

/// On error, emits an SSE error event and terminates the stream (instead of
/// silently truncating).
fn build_relay_stream<S, E>(
    stream: S,
) -> impl futures_util::Stream<Item = Result<Frame<Bytes>, Infallible>>
where
    S: futures_util::Stream<Item = Result<Bytes, E>> + Send + 'static,
    E: std::fmt::Display,
{
    stream.scan(false, |errored, result| {
        if *errored {
            return futures_util::future::ready(None);
        }
        match result {
            Ok(chunk) => futures_util::future::ready(Some(Ok(Frame::data(chunk)))),
            Err(e) => {
                warn!("stream chunk error: {e}");
                *errored = true;
                let error_event = format!(
                    "data: {}\n\n",
                    serde_json::json!({
                        "error": {
                            "message": "upstream stream error",
                            "type": "proxy_error",
                        }
                    })
                );
                futures_util::future::ready(Some(Ok(Frame::data(Bytes::from(error_event)))))
            }
        }
    })
}

pub fn into_hyper_body(proxy_body: ProxyBody) -> HyperBody {
    match proxy_body {
        ProxyBody::Full(bytes) => http_body_util::Either::Left(Full::new(bytes)),
        ProxyBody::Streaming(stream) => http_body_util::Either::Right(StreamBody::new(stream)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::stream;
    use futures_util::StreamExt;

    #[tokio::test]
    async fn stream_error_emits_sse_error_event() {
        // Simulate a stream: good chunk, then error, then another good chunk
        let source: Vec<Result<Bytes, String>> = vec![
            Ok(Bytes::from("data: {\"chunk\":1}\n\n")),
            Err("connection reset".to_string()),
            Ok(Bytes::from("data: {\"chunk\":2}\n\n")),
        ];

        let mock_stream = stream::iter(source);
        let relay = build_relay_stream(mock_stream);
        let chunks: Vec<_> = relay.collect().await;

        // Should have: good chunk, error event — stream terminates after error
        assert!(
            chunks.len() >= 2,
            "expected at least 2 frames, got {}",
            chunks.len()
        );

        // First frame is the good data
        let first = chunks[0].as_ref().unwrap().data_ref().unwrap();
        assert_eq!(first.as_ref(), b"data: {\"chunk\":1}\n\n");

        // Second frame should be an SSE error event
        let second = chunks[1].as_ref().unwrap().data_ref().unwrap();
        let second_str = std::str::from_utf8(second.as_ref()).unwrap();
        assert!(
            second_str.contains("error"),
            "expected error event, got: {second_str}"
        );
    }

    #[test]
    fn rewrite_model_replaces_alias() {
        let body = br#"{"model":"fast","messages":[{"role":"user","content":"hi"}]}"#;
        let result = rewrite_model(body, "gpt-4o-mini").unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(parsed["model"], "gpt-4o-mini");
        assert_eq!(parsed["messages"][0]["content"], "hi");
    }

    #[test]
    fn rewrite_model_preserves_other_fields() {
        let body = br#"{"model":"smart","stream":true,"temperature":0.7}"#;
        let result = rewrite_model(body, "gpt-4.1").unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(parsed["model"], "gpt-4.1");
        assert_eq!(parsed["stream"], true);
        assert_eq!(parsed["temperature"], 0.7);
    }

    #[test]
    fn extract_model_from_large_body() {
        // Simulate a body with a large base64 image after the model field
        let prefix = br#"{"model":"fast","messages":[{"role":"user","content":"#;
        let image_data = "A".repeat(1_000_000); // 1MB of data
        let suffix = br#""}]}"#;
        let mut body = Vec::with_capacity(prefix.len() + image_data.len() + suffix.len());
        body.extend_from_slice(prefix);
        body.extend_from_slice(image_data.as_bytes());
        body.extend_from_slice(suffix);

        let (_, val) = extract_json_field(&body, "model").unwrap();
        assert_eq!(val, "fast");
    }

    #[test]
    fn extract_ignores_nested_model_field() {
        let body = br#"{"model":"fast","messages":[{"model":"nested"}]}"#;
        let (_, val) = extract_json_field(body, "model").unwrap();
        assert_eq!(val, "fast");
    }

    #[test]
    fn extract_bool_true() {
        let body = br#"{"model":"test","stream":true}"#;
        assert_eq!(extract_json_bool(body, "stream"), Some(true));
    }

    #[test]
    fn extract_bool_false() {
        let body = br#"{"model":"test","stream":false}"#;
        assert_eq!(extract_json_bool(body, "stream"), Some(false));
    }

    #[test]
    fn extract_bool_missing() {
        let body = br#"{"model":"test"}"#;
        assert_eq!(extract_json_bool(body, "stream"), None);
    }

    fn make_candidate(kind: ProviderKind, base_url: &str) -> ResolvedCandidate {
        ResolvedCandidate {
            provider_name: "test".into(),
            model: "gpt-4o".into(),
            base_url: base_url.into(),
            api_key: Some("test-key".into()),
            kind,
            stats_index: 0,
        }
    }

    #[test]
    fn url_default_deduplicates_v1() {
        let c = make_candidate(ProviderKind::ApiKey, "https://api.openai.com/v1");
        let url = build_upstream_url(&c, "/v1/chat/completions");
        assert_eq!(url, "https://api.openai.com/v1/chat/completions");
    }

    #[test]
    fn url_vertex_strips_v1() {
        let c = make_candidate(
            ProviderKind::GcpMetadata,
            "https://us-central1-aiplatform.googleapis.com/v1beta1/projects/p/locations/l/endpoints/openapi",
        );
        let url = build_upstream_url(&c, "/v1/chat/completions");
        assert_eq!(
            url,
            "https://us-central1-aiplatform.googleapis.com/v1beta1/projects/p/locations/l/endpoints/openapi/chat/completions"
        );
    }

    #[test]
    fn url_azure_includes_deployment_and_version() {
        let c = make_candidate(
            ProviderKind::AzureOpenAi {
                api_version: "2024-10-21".into(),
            },
            "https://my-resource.openai.azure.com/openai",
        );
        let url = build_upstream_url(&c, "/v1/chat/completions");
        assert_eq!(
            url,
            "https://my-resource.openai.azure.com/openai/deployments/gpt-4o/chat/completions?api-version=2024-10-21"
        );
    }
}
