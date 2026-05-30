use bytes::Bytes;
use futures_util::StreamExt;
use http_body_util::{Full, StreamBody};
use hyper::body::Frame;
use reqwest::Client;
use serde_json::{Map, Value};
use std::collections::BTreeMap;
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
    mut body: Map<String, Value>,
    is_streaming: bool,
    gcp_token_provider: Option<&GcpTokenProvider>,
) -> Result<ProxyResult, Box<dyn std::error::Error + Send + Sync>> {
    prepare_body(
        &mut body,
        &candidate.model,
        candidate.attribution_labels.as_deref(),
    );
    let body_bytes = Bytes::from(serde_json::to_vec(&body)?);

    let url = build_upstream_url(candidate, path);
    debug!(url = %url, model = %candidate.model, provider = %candidate.provider_name, "forwarding request");

    let mut req = client.post(&url).header("content-type", "application/json");

    match candidate.kind {
        ProviderKind::ApiKey => {
            if let Some(ref key) = candidate.api_key {
                req = req.header("authorization", format!("Bearer {key}"));
            }
        }
        ProviderKind::GcpAdc => {
            let provider = gcp_token_provider.ok_or("gcp auth requires GCP token provider")?;
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
    let response = req.body(body_bytes).send().await?;

    let status = hyper::StatusCode::from_u16(response.status().as_u16())?;

    let headers = sanitize_response_headers(response.headers());

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

/// Replace the model alias with the resolved upstream model name, and
/// merge sidecar attribution labels into `labels` (sidecar wins on key
/// collision; disjoint client keys preserved).
pub fn prepare_body(
    body: &mut Map<String, Value>,
    real_model: &str,
    attribution_labels: Option<&BTreeMap<String, String>>,
) {
    body.insert("model".to_string(), Value::String(real_model.to_string()));

    if let Some(labels) = attribution_labels {
        let target = body
            .entry("labels")
            .or_insert_with(|| Value::Object(Map::new()));
        if !target.is_object() {
            *target = Value::Object(Map::new());
        }
        if let Value::Object(map) = target {
            for (k, v) in labels {
                map.insert(k.clone(), Value::String(v.clone()));
            }
        }
    }
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

// Hop-by-hop (RFC 9110 §7.6.1) plus origin-scoped headers; everything else relays through.
const STRIP_HEADERS: &[reqwest::header::HeaderName] = &[
    reqwest::header::CONNECTION,
    reqwest::header::TRANSFER_ENCODING,
    reqwest::header::TE,
    reqwest::header::TRAILER,
    reqwest::header::UPGRADE,
    reqwest::header::PROXY_AUTHENTICATE,
    reqwest::header::PROXY_AUTHORIZATION,
    reqwest::header::SET_COOKIE,
    reqwest::header::ALT_SVC,
];

fn sanitize_response_headers(upstream: &reqwest::header::HeaderMap) -> hyper::HeaderMap {
    let mut headers = hyper::HeaderMap::new();
    for (k, v) in upstream {
        if STRIP_HEADERS.contains(k) || k.as_str().eq_ignore_ascii_case("keep-alive") {
            continue;
        }
        if let (Ok(name), Ok(val)) = (
            hyper::header::HeaderName::from_bytes(k.as_str().as_bytes()),
            hyper::header::HeaderValue::from_bytes(v.as_bytes()),
        ) {
            headers.append(name, val);
        }
    }
    headers
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

    fn map_from(json: &str) -> Map<String, Value> {
        match serde_json::from_str(json).unwrap() {
            Value::Object(m) => m,
            _ => panic!("test JSON is not an object"),
        }
    }

    fn labels_of(items: &[(&str, &str)]) -> BTreeMap<String, String> {
        items
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn prepare_body_replaces_model_alias() {
        let mut body = map_from(r#"{"model":"fast","temperature":0.7}"#);
        prepare_body(&mut body, "gpt-4o-mini", None);
        assert_eq!(body["model"], Value::String("gpt-4o-mini".into()));
        assert_eq!(body["temperature"], Value::from(0.7));
    }

    #[test]
    fn prepare_body_without_attribution_leaves_labels_untouched() {
        let mut body = map_from(r#"{"model":"x","labels":{"client":"v"}}"#);
        prepare_body(&mut body, "real-model", None);
        assert_eq!(body["labels"]["client"], "v");
    }

    #[test]
    fn prepare_body_injects_labels_into_empty_body() {
        let mut body = map_from(r#"{"model":"x"}"#);
        let labels = labels_of(&[("service", "foo")]);
        prepare_body(&mut body, "real-model", Some(&labels));
        assert_eq!(body["labels"]["service"], "foo");
    }

    #[test]
    fn prepare_body_merges_with_existing_labels_sidecar_wins() {
        let mut body =
            map_from(r#"{"model":"x","labels":{"tenant":"abc","service":"client-set"}}"#);
        let labels = labels_of(&[("service", "sidecar-set"), ("owner", "team-a")]);
        prepare_body(&mut body, "real-model", Some(&labels));
        assert_eq!(body["labels"]["tenant"], "abc");
        assert_eq!(body["labels"]["service"], "sidecar-set");
        assert_eq!(body["labels"]["owner"], "team-a");
    }

    #[test]
    fn prepare_body_overwrites_when_existing_labels_wrong_shape() {
        let mut body = map_from(r#"{"model":"x","labels":"not-an-object"}"#);
        let labels = labels_of(&[("service", "foo")]);
        prepare_body(&mut body, "real-model", Some(&labels));
        assert_eq!(body["labels"]["service"], "foo");
        assert!(body["labels"].is_object());
    }

    fn make_candidate(kind: ProviderKind, base_url: &str) -> ResolvedCandidate {
        ResolvedCandidate {
            provider_name: "test".into(),
            model: "gpt-4o".into(),
            base_url: base_url.into(),
            api_key: Some("test-key".into()),
            kind,
            stats_index: 0,
            provider_header: hyper::header::HeaderValue::from_static("test"),
            affinity_header: hyper::header::HeaderValue::from_static("test/gpt-4o"),
            attribution_labels: None,
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
            ProviderKind::GcpAdc,
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

    #[test]
    fn sanitize_headers_strips_unsafe_and_preserves_rest() {
        let mut upstream = reqwest::header::HeaderMap::new();
        upstream.insert("content-type", "application/json".parse().unwrap());
        upstream.insert(
            reqwest::header::TRANSFER_ENCODING,
            "chunked".parse().unwrap(),
        );
        upstream.insert(reqwest::header::CONNECTION, "keep-alive".parse().unwrap());
        upstream.insert(reqwest::header::ALT_SVC, "h3=\":443\"".parse().unwrap());
        upstream.append(reqwest::header::SET_COOKIE, "__cf_bm=abc".parse().unwrap());
        upstream.append(reqwest::header::SET_COOKIE, "__cfduid=def".parse().unwrap());
        upstream.append(reqwest::header::VARY, "accept".parse().unwrap());
        upstream.append(reqwest::header::VARY, "origin".parse().unwrap());

        let sanitized = sanitize_response_headers(&upstream);

        for stripped in [
            reqwest::header::SET_COOKIE,
            reqwest::header::TRANSFER_ENCODING,
            reqwest::header::CONNECTION,
            reqwest::header::ALT_SVC,
        ] {
            assert!(
                !sanitized.contains_key(&stripped),
                "{stripped} should be stripped"
            );
        }
        assert_eq!(sanitized.get("content-type").unwrap(), "application/json");
        assert_eq!(sanitized.get_all(reqwest::header::VARY).iter().count(), 2);
    }
}
