use bytes::Bytes;
use futures_util::StreamExt;
use http_body_util::{Full, StreamBody};
use hyper::body::{Body, Frame, SizeHint};
use reqwest::Client;
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;
use tokio::sync::OwnedSemaphorePermit;
use tracing::{debug, warn};

use crate::body::RawBody;
use crate::gcp_auth::GcpTokenProvider;
use crate::model_map::{ProviderKind, ResolvedCandidate};

pub type HyperBody = http_body_util::Either<
    Full<Bytes>,
    StreamBody<futures_util::stream::BoxStream<'static, Result<Frame<Bytes>, Infallible>>>,
>;

/// Upstream latency tagged with what was measured: TTFC for streaming,
/// full response time for non-streaming.
#[derive(Debug, Clone, Copy)]
pub enum UpstreamLatency {
    Ttfc(std::time::Duration),
    Total(std::time::Duration),
}

pub struct ProxyResult {
    pub status: hyper::StatusCode,
    pub headers: hyper::HeaderMap,
    pub body: ProxyBody,
    pub latency: UpstreamLatency,
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

/// The serialized outbound body as one buffer, carrying the buffer-budget
/// permit it occupies. Implemented as a consume-once `Body` stream (not handed
/// to reqwest as a reusable buffer) so reqwest keeps no second copy for replay —
/// the bytes, and the permit, are released as soon as the upload completes.
pub struct OutboundBody {
    data: Option<Bytes>,
    _permit: Option<OwnedSemaphorePermit>,
}

impl Body for OutboundBody {
    type Data = Bytes;
    type Error = Infallible;

    // Yield the whole buffer as a single frame; the stream then ends.
    fn poll_frame(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        Poll::Ready(self.get_mut().data.take().map(|b| Ok(Frame::data(b))))
    }

    fn is_end_stream(&self) -> bool {
        self.data.is_none()
    }

    // allow hyper to set Content-Length for HTTP/1.1
    fn size_hint(&self) -> SizeHint {
        let remaining = self.data.as_ref().map_or(0, Bytes::len);
        SizeHint::with_exact(u64::try_from(remaining).unwrap_or(u64::MAX))
    }
}

pub async fn forward_request(
    client: &Client,
    candidate: &ResolvedCandidate,
    path: &str,
    body: OutboundBody,
    is_streaming: bool,
    max_response_bytes: usize,
    gcp_token_provider: Option<&GcpTokenProvider>,
) -> Result<ProxyResult, Box<dyn std::error::Error + Send + Sync>> {
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
    // reqwest drops the streamed body once the upload completes, releasing
    // the buffer-budget permit it carries — no separate bookkeeping.
    let response = req.body(reqwest::Body::wrap(body)).send().await?;

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
            latency: UpstreamLatency::Ttfc(ttfc),
        })
    } else {
        let full_body = read_capped(response, max_response_bytes).await?;
        let total = t0.elapsed();

        Ok(ProxyResult {
            status,
            headers,
            body: ProxyBody::Full(full_body),
            latency: UpstreamLatency::Total(total),
        })
    }
}

/// Buffer a non-streaming response, bounded by `max_body_bytes`.
/// Responses over the cap surface as a 502 response.
/// Note: this is not part of the aggregate buffer budget so is the one potential source of an OOMKill.
/// Streaming responses relay unbuffered and never reach here.
async fn read_capped(
    response: reqwest::Response,
    cap: usize,
) -> Result<Bytes, Box<dyn std::error::Error + Send + Sync>> {
    let over_cap = || -> Box<dyn std::error::Error + Send + Sync> {
        format!("upstream response exceeds the {cap} byte cap").into()
    };
    if response
        .content_length()
        .is_some_and(|len| usize::try_from(len).map_or(true, |len| len > cap))
    {
        return Err(over_cap());
    }
    let mut stream = response.bytes_stream();
    let mut buf = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if buf.len() + chunk.len() > cap {
            return Err(over_cap());
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(Bytes::from(buf))
}

/// Replace the model alias with the resolved upstream model name, and
/// merge sidecar attribution labels into `labels` (sidecar wins on key
/// collision; disjoint client keys preserved).
pub fn prepare_body(
    body: &mut RawBody<'_>,
    real_model: &str,
    attribution_labels: Option<&BTreeMap<String, String>>,
) -> serde_json::Result<()> {
    body.set("model", serde_json::value::to_raw_value(real_model)?);

    if let Some(labels) = attribution_labels {
        // Wrong-shape client `labels` (non-object) are overwritten.
        let mut merged: Map<String, Value> = body
            .get("labels")
            .and_then(|raw| serde_json::from_str(raw.get()).ok())
            .unwrap_or_default();
        for (k, v) in labels {
            merged.insert(k.clone(), Value::String(v.clone()));
        }
        body.set("labels", serde_json::value::to_raw_value(&merged)?);
    }
    Ok(())
}

/// Rewrite the body for the chosen candidate and serialize it for sending.
/// Consumes the parsed body, which borrows the raw request buffer — the
/// caller can drop that buffer as soon as this returns, leaving only the
/// outbound copy, which is freed once the upload completes.
pub fn prepare_outbound(
    mut body: RawBody<'_>,
    real_model: &str,
    attribution_labels: Option<&BTreeMap<String, String>>,
    permit: Option<OwnedSemaphorePermit>,
) -> serde_json::Result<OutboundBody> {
    prepare_body(&mut body, real_model, attribution_labels)?;
    Ok(OutboundBody {
        data: Some(Bytes::from(serde_json::to_vec(&body)?)),
        _permit: permit,
    })
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
mod tests;
