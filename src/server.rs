use bytes::Bytes;
use http_body_util::{BodyExt, Limited};
use hyper::body::Body as _;
use hyper::header::{HeaderValue, CONTENT_TYPE};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use hyper_util::server::graceful::GracefulShutdown;
use std::collections::HashSet;
use std::convert::Infallible;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tracing::{debug, error, info, warn, Instrument};

use crate::gcp_auth::GcpTokenProvider;
use crate::metrics::Metrics;
use crate::model_map::ModelMap;
use crate::proxy::{self, UpstreamLatency};
use crate::router::{self, RoundRobinState};
use crate::tracker::{LatencyMode, Tracker};

pub struct AppState {
    pub model_map: ModelMap,
    pub tracker: Tracker,
    pub rr_state: RoundRobinState,
    pub client: reqwest::Client,
    pub exploit_k: f64,
    pub gcp_token_provider: Option<GcpTokenProvider>,
    pub budget: BufferBudget,
    pub metrics: Metrics,
    pub shutting_down: AtomicBool,
}

/// Caps how much memory all in-flight requests may spend buffering bodies,
/// and the size of any single body. The one place that decides admission:
/// too big for the cap → 413, momentarily over budget → 429.
pub struct BufferBudget {
    semaphore: Arc<tokio::sync::Semaphore>,
    permits: usize,
    pub max_body_bytes: usize,
}

impl BufferBudget {
    pub fn new(budget_bytes: usize, max_body_bytes: usize) -> Self {
        let permits = (budget_bytes / 1024).clamp(1, tokio::sync::Semaphore::MAX_PERMITS);
        Self {
            semaphore: Arc::new(tokio::sync::Semaphore::new(permits)),
            permits,
            max_body_bytes: max_body_bytes.min(permits * 1024),
        }
    }

    /// Reserve permits for `bytes` of buffer (1 KB granularity, at least
    /// one), or None if the budget is exhausted.
    pub fn reserve(&self, bytes: usize) -> Option<tokio::sync::OwnedSemaphorePermit> {
        let want = u32::try_from(bytes.div_ceil(1024).max(1)).unwrap_or(u32::MAX);
        self.semaphore.clone().try_acquire_many_owned(want).ok()
    }

    /// The budget's total size in KB-permits.
    pub fn permits(&self) -> usize {
        self.permits
    }

    pub fn available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }
}

type BoxBody = http_body_util::Either<
    http_body_util::Full<Bytes>,
    http_body_util::StreamBody<
        futures_util::stream::BoxStream<'static, Result<hyper::body::Frame<Bytes>, Infallible>>,
    >,
>;

fn json_error(status: hyper::StatusCode, message: &str) -> Response<BoxBody> {
    let body = serde_json::json!({
        "error": {
            "message": message,
            "type": "invalid_request_error",
        }
    });
    // serializing a static JSON value can't fail.
    #[allow(clippy::expect_used)]
    let bytes = Bytes::from(serde_json::to_vec(&body).expect("serialize static JSON value"));
    json_response(status, bytes)
}

pub async fn handle_request(
    mut req: Request<hyper::body::Incoming>,
    state: Arc<AppState>,
) -> Result<Response<BoxBody>, Infallible> {
    let path = req.uri().path().to_string();
    let method = req.method().clone();

    debug!(%method, %path, "incoming request");

    // GET endpoints
    if method == hyper::Method::GET {
        if path == "/health" || path == "/healthz" {
            if state.shutting_down.load(Ordering::Relaxed) {
                let body = Bytes::from(r#"{"status":"shutting_down"}"#);
                return Ok(json_response(hyper::StatusCode::SERVICE_UNAVAILABLE, body));
            }
            let body = Bytes::from(r#"{"status":"ok"}"#);
            return Ok(json_response(hyper::StatusCode::OK, body));
        }
        if path == "/status" {
            return Ok(build_status_response(&state));
        }
        if path == "/metrics" {
            return match state.metrics.encode() {
                Ok(buf) => {
                    let body = Bytes::from(buf);
                    let mut resp = Response::new(http_body_util::Either::Left(
                        http_body_util::Full::new(body),
                    ));
                    resp.headers_mut().insert(
                        CONTENT_TYPE,
                        HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
                    );
                    Ok(resp)
                }
                Err(e) => {
                    error!(error = %e, "failed to encode metrics");
                    Ok(json_error(
                        hyper::StatusCode::INTERNAL_SERVER_ERROR,
                        "failed to encode metrics",
                    ))
                }
            };
        }
        return Ok(json_error(hyper::StatusCode::NOT_FOUND, "not found"));
    }

    if method != hyper::Method::POST {
        return Ok(json_error(
            hyper::StatusCode::METHOD_NOT_ALLOWED,
            "method not allowed",
        ));
    }

    let affinity_header = req.headers_mut().remove("x-session-affinity");

    let max_body = state.budget.max_body_bytes;

    // We have to reserve the request size before buffering it
    // We 413 if too large or 429 if we don't have memory for it just now.
    let Some(declared_len) = req
        .body()
        .size_hint()
        .exact()
        .and_then(|n| usize::try_from(n).ok())
    else {
        return Ok(json_error(
            hyper::StatusCode::LENGTH_REQUIRED,
            "Content-Length is required",
        ));
    };
    if declared_len > max_body {
        return Ok(json_error(
            hyper::StatusCode::PAYLOAD_TOO_LARGE,
            &format!("request body too large (max {} MB)", max_body / 1024 / 1024),
        ));
    }
    let Some(permit) = state.budget.reserve(declared_len) else {
        state.metrics.buffer_rejections_total.inc();
        let mut resp = json_error(
            hyper::StatusCode::TOO_MANY_REQUESTS,
            "buffer budget exhausted, retry shortly",
        );
        resp.headers_mut().insert(
            hyper::header::RETRY_AFTER,
            hyper::header::HeaderValue::from_static("1"),
        );
        return Ok(resp);
    };

    let body_bytes = match Limited::new(req, max_body).collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) if e.is::<http_body_util::LengthLimitError>() => {
            return Ok(json_error(
                hyper::StatusCode::PAYLOAD_TOO_LARGE,
                &format!("request body too large (max {} MB)", max_body / 1024 / 1024),
            ));
        }
        Err(e) => {
            warn!("failed to read request body: {e}");
            return Ok(json_error(
                hyper::StatusCode::BAD_REQUEST,
                "failed to read request body",
            ));
        }
    };

    let body: crate::body::RawBody = match serde_json::from_slice(&body_bytes) {
        Ok(b) => b,
        Err(e) if e.classify() == serde_json::error::Category::Data => {
            return Ok(json_error(
                hyper::StatusCode::BAD_REQUEST,
                "request body must be a JSON object",
            ));
        }
        Err(e) => {
            return Ok(json_error(
                hyper::StatusCode::BAD_REQUEST,
                &format!("request body is not valid JSON: {e}"),
            ));
        }
    };

    let Some(alias) = body.get_as::<String>("model") else {
        return Ok(json_error(
            hyper::StatusCode::BAD_REQUEST,
            "missing 'model' field in request body",
        ));
    };

    let Some(candidates) = state.model_map.get(&alias) else {
        let available = state.model_map.alias_names();
        return Ok(json_error(
            hyper::StatusCode::BAD_REQUEST,
            &format!(
                "unknown model alias '{}', available: {:?}",
                alias, available
            ),
        ));
    };

    let affinity: Option<(&str, &str)> = affinity_header
        .as_ref()
        .and_then(|value| value.to_str().ok())
        .and_then(|string| string.split_once('/'));

    // A pin is honored until the pinned candidate's error-rate EWMA breaches
    // `error_threshold`; routing weights never consult the threshold.
    let pinned = affinity.and_then(|(provider, model)| {
        let found = candidates
            .iter()
            .find(|c| c.provider_name == provider && c.model == model)?;
        if state.tracker.is_degraded(found.stats_index) {
            debug!(provider = %provider, model = %model, "affinity candidate above error threshold, re-routing");
            None
        } else {
            Some(found)
        }
    });

    // Needed before routing: mode selects which EWMA the router compares.
    let is_streaming = body.get_as::<bool>("stream").unwrap_or(false);
    let latency_mode = if is_streaming {
        LatencyMode::Streaming
    } else {
        LatencyMode::NonStreaming
    };

    let candidate = if let Some(c) = pinned {
        c
    } else {
        match router::select_candidate(
            &alias,
            candidates,
            &state.tracker,
            &state.rr_state,
            state.exploit_k,
            latency_mode,
        ) {
            Some(c) => c,
            None => {
                return Ok(json_error(
                    hyper::StatusCode::BAD_GATEWAY,
                    &format!("no healthy candidate for model alias '{alias}'"),
                ));
            }
        }
    };

    let stats_index = candidate.stats_index;

    info!(
        alias = %alias,
        provider = %candidate.provider_name,
        model = %candidate.model,
        streaming = is_streaming,
        "routing request"
    );

    let outbound = match proxy::prepare_outbound(
        body,
        &candidate.model,
        candidate.attribution_labels.as_deref(),
        Some(permit),
    ) {
        Ok(bytes) => bytes,
        Err(e) => {
            error!(error = %e, "failed to serialize outbound body");
            return Ok(json_error(
                hyper::StatusCode::INTERNAL_SERVER_ERROR,
                "failed to serialize outbound body",
            ));
        }
    };
    // Only the outbound copy is held while waiting on the upstream.
    drop(body_bytes);

    let result = proxy::forward_request(
        &state.client,
        candidate,
        &path,
        outbound,
        is_streaming,
        state.budget.max_body_bytes,
        state.gcp_token_provider.as_ref(),
    )
    .await;

    let provider: &str = &candidate.provider_name;
    let model: &str = &candidate.model;

    match result {
        Ok(proxy_result) => {
            let status = proxy_result.status;

            // Variant carries what was measured, so mode, metric and duration agree.
            let (observed_mode, latency, latency_metric) = match proxy_result.latency {
                UpstreamLatency::Ttfc(d) => {
                    (LatencyMode::Streaming, d, &state.metrics.ttfc_seconds)
                }
                UpstreamLatency::Total(d) => {
                    (LatencyMode::NonStreaming, d, &state.metrics.latency_seconds)
                }
            };

            if status.is_success() {
                state
                    .tracker
                    .record_success(stats_index, observed_mode, latency);
                debug!(
                    provider = %provider,
                    model = %model,
                    status = %status,
                    mode = observed_mode.as_str(),
                    latency_ms = latency.as_millis(),
                    "response received"
                );
            } else {
                warn!(
                    provider = %provider,
                    model = %model,
                    status = %status,
                    mode = observed_mode.as_str(),
                    latency_ms = latency.as_millis(),
                    "upstream returned error status"
                );
                state.tracker.record_error(stats_index);
            }

            let status_str = status.as_u16().to_string();
            state
                .metrics
                .requests_total
                .with_label_values(&[alias.as_str(), provider, model, status_str.as_str()])
                .inc();
            latency_metric
                .with_label_values(&[alias.as_str(), provider, model])
                .observe(latency.as_secs_f64());

            let body = proxy::into_hyper_body(proxy_result.body);
            let mut resp = Response::new(body);
            *resp.status_mut() = proxy_result.status;
            let headers = resp.headers_mut();
            headers.extend(proxy_result.headers);
            headers.insert("x-sturnus-provider", candidate.provider_header.clone());
            headers.insert("x-session-affinity", candidate.affinity_header.clone());
            Ok(resp)
        }
        Err(e) => {
            warn!(
                provider = %provider,
                model = %model,
                error = %e,
                "upstream request failed"
            );

            state.tracker.record_error(stats_index);
            state
                .metrics
                .errors_total
                .with_label_values(&[alias.as_str(), provider, model])
                .inc();

            Ok(json_error(
                hyper::StatusCode::BAD_GATEWAY,
                &format!("upstream error: {e}"),
            ))
        }
    }
}

fn json_response(status: hyper::StatusCode, body: Bytes) -> Response<BoxBody> {
    let mut resp = Response::new(http_body_util::Either::Left(http_body_util::Full::new(
        body,
    )));
    *resp.status_mut() = status;
    resp.headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    resp
}

pub async fn run_server(
    listener: TcpListener,
    state: Arc<AppState>,
    shutdown: impl Future<Output = ()>,
    shutdown_timeout: Duration,
) {
    let graceful = GracefulShutdown::new();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, remote) = match result {
                    Ok(conn) => conn,
                    Err(e) => {
                        error!(error = %e, "accept error");
                        continue;
                    }
                };
                let io = TokioIo::new(stream);
                let state = state.clone();

                let conn = http1::Builder::new()
                    .serve_connection(io, service_fn(move |req| {
                        let state = state.clone();
                        let span = crate::trace::request_span(req.headers());
                        handle_request(req, state).instrument(span)
                    }));

                let conn = graceful.watch(conn);

                tokio::spawn(async move {
                    if let Err(e) = conn.await {
                        if !e.to_string().contains("connection closed") {
                            error!(remote = %remote, error = %e, "connection error");
                        }
                    }
                });
            }
            _ = &mut shutdown => {
                info!("shutting down, draining connections");
                state.shutting_down.store(true, Ordering::Relaxed);
                break;
            }
        }
    }

    drop(listener);

    let drain = graceful.shutdown();
    tokio::select! {
        _ = drain => {
            info!("all connections drained");
        }
        _ = tokio::time::sleep(shutdown_timeout) => {
            warn!(timeout_secs = shutdown_timeout.as_secs(), "drain timeout reached, dropping remaining connections");
        }
    }
}

fn build_status_response(state: &AppState) -> Response<BoxBody> {
    let mut candidates = Vec::new();
    let mut seen = HashSet::new();

    for (_alias, c) in state.model_map.iter() {
        if !seen.insert(c.stats_index) {
            continue;
        }
        let stats = state.tracker.stats(c.stats_index);
        let streaming_ewma = stats.ewma_ms(LatencyMode::Streaming);
        let nonstreaming_ewma = stats.ewma_ms(LatencyMode::NonStreaming);
        let any_warm = streaming_ewma != u64::MAX || nonstreaming_ewma != u64::MAX;
        let status = if !any_warm {
            "cold"
        } else if state.tracker.is_degraded(c.stats_index) {
            "degraded"
        } else {
            "warm"
        };

        candidates.push(serde_json::json!({
            "provider": c.provider_name,
            "model": c.model,
            "status": status,
            "streaming_ewma_ms": (streaming_ewma != u64::MAX).then_some(streaming_ewma),
            "nonstreaming_ewma_ms": (nonstreaming_ewma != u64::MAX).then_some(nonstreaming_ewma),
            "error_rate": 1.0 - stats.success_rate(),
        }));
    }

    let body = serde_json::json!({
        "candidates": candidates,
    });
    // serializing the status object can't fail.
    #[allow(clippy::expect_used)]
    let bytes = Bytes::from(serde_json::to_vec_pretty(&body).expect("serialize status JSON"));
    json_response(hyper::StatusCode::OK, bytes)
}
