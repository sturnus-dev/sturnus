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
    fn reserve(&self, bytes: usize) -> Option<tokio::sync::OwnedSemaphorePermit> {
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
            headers.insert("x-llmrouter-provider", candidate.provider_header.clone());
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::Metrics;
    use crate::model_map::ModelMap;
    use crate::router::RoundRobinState;
    use crate::tracker::Tracker;
    use tokio::sync::oneshot;

    fn test_state_with_upstream(upstream_port: u16) -> Arc<AppState> {
        test_state_with_budget(upstream_port, 128 * 1024)
    }

    fn test_state_with_budget(upstream_port: u16, budget_kb: usize) -> Arc<AppState> {
        crate::init_crypto();
        let config: crate::config::Config = toml::from_str(&format!(
            r#"
[provider.test]
base_url = "http://127.0.0.1:{upstream_port}"
api_key = "fake"

[model]
fast = [{{ provider = "test", model = "test-model" }}]
"#,
        ))
        .unwrap();

        let mut tracker = Tracker::new(0.3, 0.5);
        let model_map = ModelMap::from_config(&config, &mut tracker).unwrap();
        let mut rr_state = RoundRobinState::new();
        for alias in config.model.keys() {
            rr_state.register_alias(alias.clone());
        }

        Arc::new(AppState {
            model_map,
            tracker,
            rr_state,
            client: reqwest::Client::new(),
            exploit_k: 3.0,
            gcp_token_provider: None,
            budget: BufferBudget::new(budget_kb * 1024, 32 * 1024 * 1024),
            metrics: Metrics::new(),
            shutting_down: AtomicBool::new(false),
        })
    }

    fn test_state() -> Arc<AppState> {
        test_state_with_upstream(9999)
    }

    #[tokio::test]
    async fn budget_exhaustion_sheds_with_429_and_retry_after() {
        let state = test_state();
        // Drain the whole budget so the next request finds no permits.
        let _held = state.budget.reserve(state.budget.permits() * 1024).unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (_tx, rx) = oneshot::channel::<()>();
        tokio::spawn(run_server(
            listener,
            state.clone(),
            async {
                let _ = rx.await;
            },
            Duration::from_secs(1),
        ));

        let resp = reqwest::Client::new()
            .post(format!("http://{addr}/v1/chat/completions"))
            .json(&serde_json::json!({"model": "fast", "messages": []}))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 429);
        assert_eq!(resp.headers().get("retry-after").unwrap(), "1");
        assert_eq!(state.metrics.buffer_rejections_total.get(), 1);
    }

    #[tokio::test]
    async fn missing_content_length_gets_411() {
        let state = test_state();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (_tx, rx) = oneshot::channel::<()>();
        tokio::spawn(run_server(
            listener,
            state.clone(),
            async {
                let _ = rx.await;
            },
            Duration::from_secs(1),
        ));

        // A streamed body has no Content-Length: reqwest sends it chunked.
        let chunks: Vec<Result<bytes::Bytes, std::io::Error>> =
            vec![Ok(bytes::Bytes::from_static(br#"{"model":"fast"}"#))];
        let resp = reqwest::Client::new()
            .post(format!("http://{addr}/v1/chat/completions"))
            .header("content-type", "application/json")
            .body(reqwest::Body::wrap_stream(futures_util::stream::iter(
                chunks,
            )))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 411);
        // No budget held or rejection counted: this is a client error.
        assert_eq!(state.budget.available_permits(), state.budget.permits());
        assert_eq!(state.metrics.buffer_rejections_total.get(), 0);
    }

    #[tokio::test]
    async fn request_larger_than_whole_budget_gets_413() {
        // 4 KB total budget; an 8 KB request can never fit, so it must get
        // a non-retryable 413 instead of a 429.
        let state = test_state_with_budget(9999, 4);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (_tx, rx) = oneshot::channel::<()>();
        tokio::spawn(run_server(
            listener,
            state.clone(),
            async {
                let _ = rx.await;
            },
            Duration::from_secs(1),
        ));

        let body = format!(r#"{{"model":"fast","content":"{}"}}"#, "x".repeat(8 * 1024));
        let resp = reqwest::Client::new()
            .post(format!("http://{addr}/v1/chat/completions"))
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 413);
        assert_eq!(state.metrics.buffer_rejections_total.get(), 0);
    }

    #[tokio::test]
    async fn health_endpoint_returns_ok() {
        let state = test_state();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (_tx, rx) = oneshot::channel::<()>();

        tokio::spawn(run_server(
            listener,
            state,
            async {
                let _ = rx.await;
            },
            Duration::from_secs(1),
        ));

        let resp = reqwest::get(format!("http://{addr}/health")).await.unwrap();
        assert_eq!(resp.status(), 200);
    }

    #[tokio::test]
    async fn metrics_endpoint_returns_prometheus_text() {
        let state = test_state();

        state
            .metrics
            .requests_total
            .with_label_values(&["fast", "test", "test-model", "200"])
            .inc();
        state
            .metrics
            .ttfc_seconds
            .with_label_values(&["fast", "test", "test-model"])
            .observe(0.1);
        state
            .metrics
            .latency_seconds
            .with_label_values(&["fast", "test", "test-model"])
            .observe(0.2);
        state
            .metrics
            .errors_total
            .with_label_values(&["fast", "test", "test-model"])
            .inc();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (_tx, rx) = oneshot::channel::<()>();

        tokio::spawn(run_server(
            listener,
            state,
            async {
                let _ = rx.await;
            },
            Duration::from_secs(1),
        ));

        let resp = reqwest::get(format!("http://{addr}/metrics"))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let content_type = resp
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(
            content_type.starts_with("text/plain"),
            "expected text/plain content-type, got {content_type}"
        );
        let body = resp.text().await.unwrap();
        assert!(body.contains("llmrouter_requests_total"));
        assert!(body.contains("llmrouter_ttfc_seconds"));
        assert!(body.contains("llmrouter_latency_seconds"));
        assert!(body.contains("llmrouter_errors_total"));
    }

    #[tokio::test]
    async fn shutdown_signal_stops_accept_loop() {
        let state = test_state();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = oneshot::channel::<()>();

        let server_handle = tokio::spawn(run_server(
            listener,
            state,
            async {
                let _ = rx.await;
            },
            Duration::from_secs(1),
        ));

        // Confirm server is up
        let resp = reqwest::get(format!("http://{addr}/health")).await.unwrap();
        assert_eq!(resp.status(), 200);

        // Send shutdown signal
        tx.send(()).unwrap();

        // Server task should complete
        tokio::time::timeout(std::time::Duration::from_secs(2), server_handle)
            .await
            .expect("server did not shut down within 2s")
            .expect("server task panicked");

        // New connections should be refused
        let result = reqwest::get(format!("http://{addr}/health")).await;
        assert!(
            result.is_err(),
            "expected connection refused after shutdown"
        );
    }

    #[tokio::test]
    async fn health_returns_503_during_shutdown() {
        let state = test_state();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (_tx, rx) = oneshot::channel::<()>();

        // Mark as shutting down before starting so we can test the endpoint
        state.shutting_down.store(true, Ordering::Relaxed);

        tokio::spawn(run_server(
            listener,
            state,
            async {
                let _ = rx.await;
            },
            Duration::from_secs(1),
        ));

        let resp = reqwest::get(format!("http://{addr}/health")).await.unwrap();
        assert_eq!(resp.status(), 503);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(body["status"], "shutting_down");
    }

    /// Mock upstream that accepts one request, signals readiness, waits for a
    /// release signal, then responds.
    async fn slow_upstream() -> (
        u16,
        oneshot::Sender<()>,
        oneshot::Receiver<()>,
        tokio::task::JoinHandle<()>,
    ) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = oneshot::channel::<()>();
        let (ready_tx, ready_rx) = oneshot::channel::<()>();
        let handle = tokio::spawn(async move {
            let (mut conn, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let _ = conn.read(&mut buf).await.unwrap();
            let _ = ready_tx.send(());
            let _ = rx.await;
            conn.write_all(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 15\r\n\r\n{\"result\":\"ok\"}"
            ).await.unwrap();
        });
        (port, tx, ready_rx, handle)
    }

    /// Mock upstream that accepts one request and never responds.
    async fn hanging_upstream() -> (u16, tokio::task::JoinHandle<()>) {
        use tokio::io::AsyncReadExt;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let handle = tokio::spawn(async move {
            let (mut conn, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let _ = conn.read(&mut buf).await.unwrap();
            tokio::time::sleep(Duration::from_secs(60)).await;
            drop(conn);
        });
        (port, handle)
    }

    fn proxy_request(
        addr: std::net::SocketAddr,
    ) -> tokio::task::JoinHandle<Result<reqwest::Response, reqwest::Error>> {
        crate::init_crypto();
        tokio::spawn(async move {
            reqwest::Client::new()
                .post(format!("http://{addr}/v1/chat/completions"))
                .json(&serde_json::json!({"model": "fast", "messages": []}))
                .send()
                .await
        })
    }

    #[tokio::test]
    async fn graceful_drain_waits_for_inflight_request() {
        let (mock_port, upstream_tx, upstream_ready, mock_handle) = slow_upstream().await;
        let state = test_state_with_upstream(mock_port);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        let mut server_handle = tokio::spawn(run_server(
            listener,
            state,
            async {
                let _ = shutdown_rx.await;
            },
            Duration::from_secs(5),
        ));

        let client_handle = proxy_request(addr);
        // Request is in-flight once the upstream has received it.
        upstream_ready.await.unwrap();

        shutdown_tx.send(()).unwrap();

        // Server should still be draining
        let poll = tokio::time::timeout(Duration::from_millis(500), &mut server_handle).await;
        assert!(
            poll.is_err(),
            "server exited while request was still in flight"
        );

        // Release the upstream; request completes, server exits
        upstream_tx.send(()).unwrap();
        let resp = client_handle.await.unwrap().unwrap();
        assert_eq!(resp.status(), 200);

        tokio::time::timeout(Duration::from_secs(2), server_handle)
            .await
            .expect("server did not shut down after drain")
            .expect("server task panicked");

        mock_handle.abort();
    }

    #[tokio::test]
    async fn graceful_drain_timeout_force_exits() {
        let (mock_port, mock_handle) = hanging_upstream().await;
        let state = test_state_with_upstream(mock_port);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        let server_handle = tokio::spawn(run_server(
            listener,
            state,
            async {
                let _ = shutdown_rx.await;
            },
            Duration::from_millis(300),
        ));

        let client_handle = proxy_request(addr);
        tokio::time::sleep(Duration::from_millis(100)).await;

        shutdown_tx.send(()).unwrap();

        // Server should exit after drain timeout despite hanging upstream
        tokio::time::timeout(Duration::from_secs(3), server_handle)
            .await
            .expect("server did not exit after drain timeout")
            .expect("server task panicked");

        mock_handle.abort();
        client_handle.abort();
    }

    /// Mock upstream that accepts N requests and responds immediately.
    async fn instant_upstream(n: usize) -> (u16, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let handle = tokio::spawn(async move {
            for _ in 0..n {
                let (mut conn, _) = listener.accept().await.unwrap();
                let mut buf = vec![0u8; 4096];
                let _ = conn.read(&mut buf).await.unwrap();
                conn.write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 15\r\n\r\n{\"result\":\"ok\"}",
                )
                .await
                .unwrap();
            }
        });
        (port, handle)
    }

    async fn failing_upstream(status_line: &'static str) -> (u16, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let handle = tokio::spawn(async move {
            let (mut conn, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let _ = conn.read(&mut buf).await.unwrap();
            let body = b"{\"error\":\"nope\"}";
            let resp = format!(
                "HTTP/1.1 {status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                body.len()
            );
            conn.write_all(resp.as_bytes()).await.unwrap();
            conn.write_all(body).await.unwrap();
        });
        (port, handle)
    }

    fn start_server(
        state: Arc<AppState>,
    ) -> (
        std::net::SocketAddr,
        oneshot::Sender<()>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let listener = TcpListener::from_std(listener).unwrap();
        let (tx, rx) = oneshot::channel::<()>();
        let handle = tokio::spawn(run_server(
            listener,
            state,
            async {
                let _ = rx.await;
            },
            Duration::from_secs(1),
        ));
        (addr, tx, handle)
    }

    #[tokio::test]
    async fn response_includes_session_affinity_header() {
        let (mock_port, mock_handle) = instant_upstream(1).await;
        let state = test_state_with_upstream(mock_port);
        let (addr, _tx, _server) = start_server(state);

        let resp = reqwest::Client::new()
            .post(format!("http://{addr}/v1/chat/completions"))
            .json(&serde_json::json!({"model": "fast", "messages": []}))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);
        let affinity = resp
            .headers()
            .get("x-session-affinity")
            .unwrap()
            .to_str()
            .unwrap();
        assert_eq!(affinity, "test/test-model");

        mock_handle.abort();
    }

    #[tokio::test]
    async fn affinity_header_pins_to_candidate() {
        crate::init_crypto();
        // Two distinct upstreams for the same model alias
        let (port_a, handle_a) = instant_upstream(1).await;
        let (port_b, handle_b) = instant_upstream(0).await;

        let config: crate::config::Config = toml::from_str(&format!(
            r#"
[provider.alpha]
base_url = "http://127.0.0.1:{port_a}"
api_key = "fake"

[provider.beta]
base_url = "http://127.0.0.1:{port_b}"
api_key = "fake"

[model]
fast = [
  {{ provider = "alpha", model = "test-model" }},
  {{ provider = "beta",  model = "test-model" }},
]
"#,
        ))
        .unwrap();

        let mut tracker = Tracker::new(0.3, 0.5);
        let model_map = ModelMap::from_config(&config, &mut tracker).unwrap();
        let mut rr_state = RoundRobinState::new();
        for alias in config.model.keys() {
            rr_state.register_alias(alias.clone());
        }

        // Make beta look fast (10ms) and alpha slow (500ms) so the router
        // naturally prefers beta, then pin to alpha and verify the pin wins.
        // The request is non-streaming, so seed that EWMA.
        let fast_candidates = model_map.get("fast").unwrap();
        let alpha_idx = fast_candidates
            .iter()
            .find(|c| c.provider_name == "alpha")
            .unwrap()
            .stats_index;
        let beta_idx = fast_candidates
            .iter()
            .find(|c| c.provider_name == "beta")
            .unwrap()
            .stats_index;
        tracker.record_success(
            alpha_idx,
            LatencyMode::NonStreaming,
            Duration::from_millis(500),
        );
        tracker.record_success(
            beta_idx,
            LatencyMode::NonStreaming,
            Duration::from_millis(10),
        );

        let state = Arc::new(AppState {
            model_map,
            tracker,
            rr_state,
            client: reqwest::Client::new(),
            exploit_k: 3.0,
            gcp_token_provider: None,
            budget: BufferBudget::new(128 * 1024 * 1024, 32 * 1024 * 1024),
            metrics: Metrics::new(),
            shutting_down: AtomicBool::new(false),
        });
        let (addr, _tx, _server) = start_server(state);
        let client = reqwest::Client::new();

        // Pin affinity to the slow provider (alpha)
        let resp = client
            .post(format!("http://{addr}/v1/chat/completions"))
            .header("x-session-affinity", "alpha/test-model")
            .json(&serde_json::json!({"model": "fast", "messages": []}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let provider = resp
            .headers()
            .get("x-llmrouter-provider")
            .unwrap()
            .to_str()
            .unwrap();
        assert_eq!(
            provider, "alpha",
            "affinity should pin to alpha even though beta is faster"
        );

        handle_a.abort();
        handle_b.abort();
    }

    #[tokio::test]
    async fn affinity_pin_broken_when_error_ewma_breaches_threshold() {
        crate::init_crypto();
        let (port_a, handle_a) = instant_upstream(0).await;
        let (port_b, handle_b) = instant_upstream(1).await;

        // beta listed first: the first Weyl tick lands on the first weight
        // unit, which would be alpha's probe slice otherwise.
        let config: crate::config::Config = toml::from_str(&format!(
            r#"
[provider.alpha]
base_url = "http://127.0.0.1:{port_a}"
api_key = "fake"

[provider.beta]
base_url = "http://127.0.0.1:{port_b}"
api_key = "fake"

[model]
fast = [
  {{ provider = "beta",  model = "test-model" }},
  {{ provider = "alpha", model = "test-model" }},
]
"#,
        ))
        .unwrap();

        let mut tracker = Tracker::new(0.3, 0.5);
        let model_map = ModelMap::from_config(&config, &mut tracker).unwrap();
        let mut rr_state = RoundRobinState::new();
        for alias in config.model.keys() {
            rr_state.register_alias(alias.clone());
        }

        let fast_candidates = model_map.get("fast").unwrap();
        let alpha_idx = fast_candidates
            .iter()
            .find(|c| c.provider_name == "alpha")
            .unwrap()
            .stats_index;
        let beta_idx = fast_candidates
            .iter()
            .find(|c| c.provider_name == "beta")
            .unwrap()
            .stats_index;

        // alpha warm but failing (error EWMA > 0.5); beta healthy.
        tracker.record_success(
            alpha_idx,
            LatencyMode::NonStreaming,
            Duration::from_millis(500),
        );
        for _ in 0..5 {
            tracker.record_error(alpha_idx);
        }
        tracker.record_success(
            beta_idx,
            LatencyMode::NonStreaming,
            Duration::from_millis(10),
        );

        let state = Arc::new(AppState {
            model_map,
            tracker,
            rr_state,
            client: reqwest::Client::new(),
            exploit_k: 3.0,
            gcp_token_provider: None,
            budget: BufferBudget::new(128 * 1024 * 1024, 32 * 1024 * 1024),
            metrics: Metrics::new(),
            shutting_down: AtomicBool::new(false),
        });
        let (addr, _tx, _server) = start_server(state);

        // Pin to the failing provider: must break and re-route.
        let resp = reqwest::Client::new()
            .post(format!("http://{addr}/v1/chat/completions"))
            .header("x-session-affinity", "alpha/test-model")
            .json(&serde_json::json!({"model": "fast", "messages": []}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let provider = resp
            .headers()
            .get("x-llmrouter-provider")
            .unwrap()
            .to_str()
            .unwrap();
        assert_eq!(
            provider, "beta",
            "pin to a candidate above the error threshold should be broken"
        );
        let affinity = resp
            .headers()
            .get("x-session-affinity")
            .unwrap()
            .to_str()
            .unwrap();
        assert_eq!(affinity, "beta/test-model");

        handle_a.abort();
        handle_b.abort();
    }

    #[tokio::test]
    async fn invalid_affinity_header_ignored() {
        let (mock_port, mock_handle) = instant_upstream(1).await;
        let state = test_state_with_upstream(mock_port);
        let (addr, _tx, _server) = start_server(state);

        let resp = reqwest::Client::new()
            .post(format!("http://{addr}/v1/chat/completions"))
            .header("x-session-affinity", "garbage")
            .json(&serde_json::json!({"model": "fast", "messages": []}))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);
        let affinity = resp
            .headers()
            .get("x-session-affinity")
            .unwrap()
            .to_str()
            .unwrap();
        assert_eq!(affinity, "test/test-model");

        mock_handle.abort();
    }

    #[tokio::test]
    async fn upstream_error_status_relayed_and_metered() {
        let (mock_port, mock_handle) = failing_upstream("503 Service Unavailable").await;
        let state = test_state_with_upstream(mock_port);
        let (addr, _tx, _server) = start_server(state.clone());

        let resp = reqwest::Client::new()
            .post(format!("http://{addr}/v1/chat/completions"))
            .json(&serde_json::json!({"model": "fast", "messages": []}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 503);

        let metrics = String::from_utf8(state.metrics.encode().unwrap()).unwrap();
        assert!(
            metrics.contains("status_code=\"503\""),
            "expected requests_total to carry the upstream status: {metrics}"
        );

        mock_handle.abort();
    }

    #[tokio::test]
    async fn ewma_not_updated_on_error_response() {
        let (mock_port, mock_handle) = failing_upstream("401 Unauthorized").await;
        let state = test_state_with_upstream(mock_port);

        let stats_index = state
            .model_map
            .get("fast")
            .unwrap()
            .first()
            .unwrap()
            .stats_index;

        assert!(
            state
                .tracker
                .stats(stats_index)
                .is_cold(LatencyMode::NonStreaming),
            "precondition: EWMA should start uninitialized"
        );

        let (addr, _tx, _server) = start_server(state.clone());
        let resp = reqwest::Client::new()
            .post(format!("http://{addr}/v1/chat/completions"))
            .json(&serde_json::json!({"model": "fast", "messages": []}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 401);

        assert!(
            state
                .tracker
                .stats(stats_index)
                .is_cold(LatencyMode::NonStreaming),
            "fast 4xx responses must not pollute EWMA latency stats"
        );

        mock_handle.abort();
    }

    /// Mock upstream that captures the request's header block, drains the
    /// declared body, and returns 200.
    async fn capturing_upstream() -> (u16, oneshot::Receiver<String>, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = oneshot::channel::<String>();
        let handle = tokio::spawn(async move {
            let (mut conn, _) = listener.accept().await.unwrap();
            let mut buf = Vec::new();
            let mut tmp = [0u8; 8192];
            let head_end = loop {
                let n = conn.read(&mut tmp).await.unwrap();
                if n == 0 {
                    return;
                }
                buf.extend_from_slice(&tmp[..n]);
                if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                    break pos + 4;
                }
            };
            let head = String::from_utf8_lossy(&buf[..head_end - 4]).into_owned();
            let content_len: usize = head
                .to_ascii_lowercase()
                .lines()
                .find_map(|l| {
                    l.strip_prefix("content-length:")
                        .map(|v| v.trim().to_string())
                })
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            // Drain the body so reqwest's upload completes cleanly.
            let mut body_read = buf.len() - head_end;
            while body_read < content_len {
                let n = conn.read(&mut tmp).await.unwrap();
                if n == 0 {
                    break;
                }
                body_read += n;
            }
            let _ = tx.send(head);
            conn.write_all(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 15\r\n\r\n{\"result\":\"ok\"}"
            ).await.unwrap();
        });
        (port, rx, handle)
    }

    #[tokio::test]
    async fn outbound_request_carries_content_length() {
        let (mock_port, head_rx, mock_handle) = capturing_upstream().await;
        let state = test_state_with_upstream(mock_port);
        let (addr, _tx, _server) = start_server(state);

        // Large enough that the pre-fix code streamed it chunked (dropping
        // Content-Length); the sized body must now declare an exact length.
        let big = "x".repeat(512 * 1024);
        let resp = reqwest::Client::new()
            .post(format!("http://{addr}/v1/chat/completions"))
            .json(&serde_json::json!({"model": "fast", "content": big}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);

        let head = tokio::time::timeout(Duration::from_secs(5), head_rx)
            .await
            .unwrap()
            .unwrap()
            .to_ascii_lowercase();
        assert!(
            head.contains("content-length:"),
            "outbound request must carry Content-Length:\n{head}"
        );
        assert!(
            !head.contains("transfer-encoding: chunked"),
            "outbound request must not be chunked:\n{head}"
        );

        mock_handle.abort();
    }

    #[tokio::test]
    async fn permits_restored_after_forward() {
        let (mock_port, mock_handle) = instant_upstream(1).await;
        let state = test_state_with_upstream(mock_port);
        let total = state.budget.permits();
        let (addr, _tx, _server) = start_server(state.clone());

        let resp = reqwest::Client::new()
            .post(format!("http://{addr}/v1/chat/completions"))
            .json(&serde_json::json!({"model": "fast", "messages": []}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);

        // The permit lives in the outbound body and is released when the
        // upload completes, so the budget returns whole — no leak.
        let mut waited = 0;
        while state.budget.available_permits() != total && waited < 50 {
            tokio::time::sleep(Duration::from_millis(20)).await;
            waited += 1;
        }
        assert_eq!(state.budget.available_permits(), total);

        mock_handle.abort();
    }

    /// Mock upstream that declares a body far larger than it sends.
    async fn oversized_response_upstream() -> (u16, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let handle = tokio::spawn(async move {
            let (mut conn, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let _ = conn.read(&mut buf).await.unwrap();
            let _ = conn
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 1048576\r\n\r\n",
                )
                .await;
            let _ = conn.write_all(&vec![b'x'; 1024]).await;
        });
        (port, handle)
    }

    #[tokio::test]
    async fn oversized_nonstreaming_response_rejected() {
        // 4 KB budget → 4 KB response cap; the 1 MB response can't be buffered.
        let (mock_port, mock_handle) = oversized_response_upstream().await;
        let state = test_state_with_budget(mock_port, 4);
        let stats_index = state
            .model_map
            .get("fast")
            .unwrap()
            .first()
            .unwrap()
            .stats_index;
        let (addr, _tx, _server) = start_server(state.clone());

        let resp = reqwest::Client::new()
            .post(format!("http://{addr}/v1/chat/completions"))
            .json(&serde_json::json!({"model": "fast", "messages": []}))
            .send()
            .await
            .unwrap();

        // The over-cap response surfaces as an upstream error, not a giant buffer.
        assert_eq!(resp.status(), 502);
        assert!(
            state.tracker.stats(stats_index).success_rate() < 1.0,
            "an oversized response must feed the error signal"
        );

        mock_handle.abort();
    }
}
