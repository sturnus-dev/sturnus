//! Integration tests for the HTTP proxy server (relocated from src/server.rs).
//! Shared raw-socket mock upstreams live in `tests/common`.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::oneshot;

use sturnus::metrics::Metrics;
use sturnus::model_map::ModelMap;
use sturnus::router::RoundRobinState;
use sturnus::server::{run_server, AppState, BufferBudget};
use sturnus::tracker::{LatencyMode, Tracker};

mod common;

fn test_state_with_upstream(upstream_port: u16) -> Arc<AppState> {
    test_state_with_budget(upstream_port, 128 * 1024)
}

fn test_state_with_budget(upstream_port: u16, budget_kb: usize) -> Arc<AppState> {
    sturnus::init_crypto();
    let config: sturnus::config::Config = toml::from_str(&format!(
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
    assert!(body.contains("sturnus_requests_total"));
    assert!(body.contains("sturnus_ttfc_seconds"));
    assert!(body.contains("sturnus_latency_seconds"));
    assert!(body.contains("sturnus_errors_total"));
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
    let (listener, port) = common::bind().await;
    let (tx, rx) = oneshot::channel::<()>();
    let (ready_tx, ready_rx) = oneshot::channel::<()>();
    let handle = tokio::spawn(async move {
        let (mut conn, _) = listener.accept().await.unwrap();
        common::read_once(&mut conn).await;
        let _ = ready_tx.send(());
        let _ = rx.await;
        common::write_ok(&mut conn, r#"{"result":"ok"}"#).await;
    });
    (port, tx, ready_rx, handle)
}

/// Mock upstream that accepts one request and never responds.
async fn hanging_upstream() -> (u16, tokio::task::JoinHandle<()>) {
    let (listener, port) = common::bind().await;
    let handle = tokio::spawn(async move {
        let (mut conn, _) = listener.accept().await.unwrap();
        common::read_once(&mut conn).await;
        tokio::time::sleep(Duration::from_secs(60)).await;
        drop(conn);
    });
    (port, handle)
}

fn proxy_request(
    addr: std::net::SocketAddr,
) -> tokio::task::JoinHandle<Result<reqwest::Response, reqwest::Error>> {
    sturnus::init_crypto();
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
    let (listener, port) = common::bind().await;
    let handle = tokio::spawn(async move {
        for _ in 0..n {
            let (mut conn, _) = listener.accept().await.unwrap();
            common::read_once(&mut conn).await;
            common::write_ok(&mut conn, r#"{"result":"ok"}"#).await;
        }
    });
    (port, handle)
}

async fn failing_upstream(status_line: &'static str) -> (u16, tokio::task::JoinHandle<()>) {
    let (listener, port) = common::bind().await;
    let handle = tokio::spawn(async move {
        let (mut conn, _) = listener.accept().await.unwrap();
        common::read_once(&mut conn).await;
        common::write_status(&mut conn, status_line, r#"{"error":"nope"}"#).await;
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
    sturnus::init_crypto();
    // Two distinct upstreams for the same model alias
    let (port_a, handle_a) = instant_upstream(1).await;
    let (port_b, handle_b) = instant_upstream(0).await;

    let config: sturnus::config::Config = toml::from_str(&format!(
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
        .get("x-sturnus-provider")
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
    sturnus::init_crypto();
    let (port_a, handle_a) = instant_upstream(0).await;
    let (port_b, handle_b) = instant_upstream(1).await;

    // beta listed first: the first Weyl tick lands on the first weight
    // unit, which would be alpha's probe slice otherwise.
    let config: sturnus::config::Config = toml::from_str(&format!(
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
        .get("x-sturnus-provider")
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
    let (listener, port) = common::bind().await;
    let (tx, rx) = oneshot::channel::<String>();
    let handle = tokio::spawn(async move {
        let (mut conn, _) = listener.accept().await.unwrap();
        let Ok(head) = common::read_request(&mut conn).await else {
            return;
        };
        let _ = tx.send(head);
        common::write_ok(&mut conn, r#"{"result":"ok"}"#).await;
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
    use tokio::io::AsyncWriteExt;
    let (listener, port) = common::bind().await;
    let handle = tokio::spawn(async move {
        let (mut conn, _) = listener.accept().await.unwrap();
        common::read_once(&mut conn).await;
        // Declare 1 MB but send only 1 KB.
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
