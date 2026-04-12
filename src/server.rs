use bytes::Bytes;
use http_body_util::{BodyExt, Limited};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use hyper_util::server::graceful::GracefulShutdown;
use std::convert::Infallible;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tracing::{debug, error, info, warn};

use crate::gcp_auth::GcpTokenProvider;
use crate::metrics::Metrics;
use crate::model_map::ModelMap;
use crate::proxy;
use crate::router::{self, candidate_key, RoundRobinState};
use crate::tracker::{CandidateKey, Tracker};

pub struct AppState {
    pub model_map: ModelMap,
    pub tracker: Tracker,
    pub rr_state: RoundRobinState,
    pub client: reqwest::Client,
    pub explore_ratio: f64,
    pub gcp_token_provider: Option<Arc<GcpTokenProvider>>,
    pub max_body_bytes: usize,
    pub metrics: Metrics,
    pub shutting_down: AtomicBool,
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
    let bytes = Bytes::from(serde_json::to_vec(&body).expect("serialize static JSON value"));
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(http_body_util::Either::Left(http_body_util::Full::new(
            bytes,
        )))
        .expect("build error response")
}

pub async fn handle_request(
    req: Request<hyper::body::Incoming>,
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
                return Ok(json_response(503, body));
            }
            let body = Bytes::from(r#"{"status":"ok"}"#);
            return Ok(json_response(200, body));
        }
        if path == "/status" {
            return Ok(build_status_response(&state));
        }
        if path == "/metrics" {
            return match state.metrics.encode() {
                Ok(buf) => {
                    let body = Bytes::from(buf);
                    let resp = Response::builder()
                        .status(200)
                        .header("content-type", "text/plain; version=0.0.4; charset=utf-8")
                        .body(http_body_util::Either::Left(http_body_util::Full::new(
                            body,
                        )))
                        .expect("build metrics response");
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

    let affinity: Option<CandidateKey> = req
        .headers()
        .get("x-session-affinity")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            let (provider, model) = v.split_once('/')?;
            Some((Arc::from(provider), Arc::from(model)))
        });

    let max_body = state.max_body_bytes;
    let body_bytes = match Limited::new(req, max_body).collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            error!("failed to read request body: {e}");
            return Ok(json_error(
                hyper::StatusCode::PAYLOAD_TOO_LARGE,
                &format!("request body too large (max {} MB)", max_body / 1024 / 1024),
            ));
        }
    };

    let alias = match proxy::extract_json_field(&body_bytes, "model") {
        Some((_, val)) => val.to_string(),
        None => {
            return Ok(json_error(
                hyper::StatusCode::BAD_REQUEST,
                "missing 'model' field in request body",
            ));
        }
    };

    let candidates = match state.model_map.get(&alias) {
        Some(c) => c,
        None => {
            let available = state.model_map.alias_names();
            return Ok(json_error(
                hyper::StatusCode::BAD_REQUEST,
                &format!(
                    "unknown model alias '{}', available: {:?}",
                    alias, available
                ),
            ));
        }
    };

    let pinned = affinity.as_ref().and_then(|(provider, model)| {
        let key = (provider.clone(), model.clone());
        let found = candidates.iter().find(|c| candidate_key(c) == key)?;
        if state.tracker.is_degraded(&key) {
            debug!(provider = %provider, model = %model, "affinity provider degraded, re-routing");
            None
        } else {
            Some(found)
        }
    });

    let candidate = if let Some(c) = pinned {
        c
    } else {
        match router::select_candidate(
            &alias,
            candidates,
            &state.tracker,
            &state.rr_state,
            state.explore_ratio,
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

    let key = candidate_key(candidate);

    let is_streaming = proxy::extract_json_bool(&body_bytes, "stream").unwrap_or(false);

    info!(
        alias = %alias,
        provider = %candidate.provider_name,
        model = %candidate.model,
        streaming = is_streaming,
        "routing request"
    );

    let result = proxy::forward_request(
        &state.client,
        candidate,
        &path,
        body_bytes,
        is_streaming,
        state.gcp_token_provider.as_ref(),
    )
    .await;

    let provider: &str = &candidate.provider_name;
    let model: &str = &candidate.model;

    match result {
        Ok(proxy_result) => {
            state.tracker.record_ttfc(&key, proxy_result.ttfc);

            if proxy_result.status.is_success() {
                state.tracker.record_success(&key);
            } else {
                state.tracker.record_error(&key);
            }

            let status_str = proxy_result.status.as_u16().to_string();
            state
                .metrics
                .requests_total
                .with_label_values(&[alias.as_str(), provider, model, status_str.as_str()])
                .inc();
            state
                .metrics
                .ttfc_seconds
                .with_label_values(&[alias.as_str(), provider, model])
                .observe(proxy_result.ttfc.as_secs_f64());

            debug!(
                provider = %provider,
                model = %model,
                status = %proxy_result.status,
                ttfc_ms = proxy_result.ttfc.as_millis(),
                "response received"
            );

            let mut builder = Response::builder().status(proxy_result.status);
            for (k, v) in &proxy_result.headers {
                builder = builder.header(k, v);
            }
            builder = builder.header("x-llmrouter-provider", provider);
            builder = builder.header("x-session-affinity", format!("{provider}/{model}"));

            let body = proxy::into_hyper_body(proxy_result.body);
            Ok(builder.body(body).expect("build proxy response"))
        }
        Err(e) => {
            warn!(
                provider = %provider,
                model = %model,
                error = %e,
                "upstream request failed"
            );

            state.tracker.record_error(&key);
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

fn json_response(status: u16, body: Bytes) -> Response<BoxBody> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(http_body_util::Either::Left(http_body_util::Full::new(
            body,
        )))
        .expect("build JSON response")
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
                        async move { handle_request(req, state).await }
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

    for (key, stats) in &state.tracker.stats {
        let ewma = stats.ewma_ms.load(Ordering::Relaxed);
        let degraded = state.tracker.is_degraded(key);
        let status = if ewma == u64::MAX {
            "cold"
        } else if degraded {
            "degraded"
        } else {
            "warm"
        };

        candidates.push(serde_json::json!({
            "provider": &*key.0,
            "model": &*key.1,
            "status": status,
            "ewma_ms": if ewma == u64::MAX { None } else { Some(ewma) },
            "error_rate": stats.error_rate(),
        }));
    }

    let body = serde_json::json!({
        "candidates": candidates,
    });
    let bytes = Bytes::from(serde_json::to_vec_pretty(&body).expect("serialize status JSON"));
    json_response(200, bytes)
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

        let model_map = ModelMap::from_config(&config);
        let mut tracker = Tracker::new(0.3, 30, 0.5, 10_000);
        let mut rr_state = RoundRobinState::new();
        for (alias, candidates) in &config.model {
            rr_state.register_alias(alias.clone());
            for c in candidates {
                tracker.register((Arc::from(c.provider.as_str()), Arc::from(c.model.as_str())));
            }
        }

        Arc::new(AppState {
            model_map,
            tracker,
            rr_state,
            client: reqwest::Client::new(),
            explore_ratio: 0.2,
            gcp_token_provider: None,
            max_body_bytes: 100 * 1024 * 1024,
            metrics: Metrics::new(),
            shutting_down: AtomicBool::new(false),
        })
    }

    fn test_state() -> Arc<AppState> {
        test_state_with_upstream(9999)
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

    /// Mock upstream that accepts one request, waits for a signal, then responds.
    async fn slow_upstream() -> (u16, oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = oneshot::channel::<()>();
        let handle = tokio::spawn(async move {
            let (mut conn, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let _ = conn.read(&mut buf).await.unwrap();
            let _ = rx.await;
            conn.write_all(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 15\r\n\r\n{\"result\":\"ok\"}"
            ).await.unwrap();
        });
        (port, tx, handle)
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
        let (mock_port, upstream_tx, mock_handle) = slow_upstream().await;
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
        tokio::time::sleep(Duration::from_millis(100)).await;

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

        let model_map = ModelMap::from_config(&config);
        let mut tracker = Tracker::new(0.3, 30, 0.5, 10_000);
        let mut rr_state = RoundRobinState::new();
        for (alias, candidates) in &config.model {
            rr_state.register_alias(alias.clone());
            for c in candidates {
                tracker.register((Arc::from(c.provider.as_str()), Arc::from(c.model.as_str())));
            }
        }

        // Make beta look fast (10ms) and alpha slow (500ms) so the router
        // naturally prefers beta. We then pin affinity to alpha and verify
        // the router honours the pin instead of picking beta.
        let alpha_key: CandidateKey = (Arc::from("alpha"), Arc::from("test-model"));
        let beta_key: CandidateKey = (Arc::from("beta"), Arc::from("test-model"));
        tracker.record_ttfc(&alpha_key, Duration::from_millis(500));
        tracker.record_success(&alpha_key);
        tracker.record_ttfc(&beta_key, Duration::from_millis(10));
        tracker.record_success(&beta_key);

        let state = Arc::new(AppState {
            model_map,
            tracker,
            rr_state,
            client: reqwest::Client::new(),
            explore_ratio: 0.0,
            gcp_token_provider: None,
            max_body_bytes: 100 * 1024 * 1024,
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
}
