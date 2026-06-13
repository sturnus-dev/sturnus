//! Regression test for per-request memory held while waiting on a slow
//! upstream. Several concurrent multi-MB requests are proxied to a mock
//! provider that swallows the upload and then stalls before responding;
//! during that stall the sidecar should hold (almost) none of the request
//! bytes — the raw buffer is dropped after parsing and the outbound chunks
//! drain as the upload proceeds.
//!
//! Clients speak raw TCP and the mock upstream discards what it reads, so
//! neither side of the harness holds the payloads itself: live heap during
//! the stall is attributable to the router.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::print_stderr)]

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use llmrouter::metrics::Metrics;
use llmrouter::model_map::ModelMap;
use llmrouter::router::RoundRobinState;
use llmrouter::server::{run_server, AppState, BufferBudget};
use llmrouter::tracker::Tracker;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[global_allocator]
static ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Application bytes currently allocated (jemalloc's `stats.allocated`).
fn live_bytes() -> isize {
    tikv_jemalloc_ctl::epoch::advance().unwrap();
    isize::try_from(tikv_jemalloc_ctl::stats::allocated::read().unwrap()).unwrap()
}

const CONCURRENT_REQUESTS: usize = 4;
const PAYLOAD_BYTES: usize = 2 * 1024 * 1024;
/// How long the mock provider stalls after consuming the upload.
const UPSTREAM_HOLD: Duration = Duration::from_millis(1500);

/// Mock provider: discard the request (Content-Length or chunked), signal
/// that the upload finished, stall, then answer with a tiny JSON body.
async fn run_mock_upstream(listener: TcpListener, uploaded_tx: tokio::sync::mpsc::Sender<()>) {
    loop {
        let Ok((mut socket, _)) = listener.accept().await else {
            return;
        };
        let uploaded_tx = uploaded_tx.clone();
        tokio::spawn(async move {
            // 64 KB scratch reused for the whole request: nothing accumulates.
            let mut buf = vec![0u8; 64 * 1024];
            let mut header = Vec::new();
            let mut header_done = false;
            let mut body_remaining: Option<usize> = None;
            // Rolling tail to spot the chunked terminator across reads.
            let mut tail = [0u8; 8];
            loop {
                let Ok(n) = socket.read(&mut buf).await else {
                    return;
                };
                if n == 0 {
                    return;
                }
                let mut chunk = &buf[..n];
                if !header_done {
                    header.extend_from_slice(chunk);
                    let Some(end) = header.windows(4).position(|w| w == b"\r\n\r\n") else {
                        continue;
                    };
                    header_done = true;
                    let head = String::from_utf8_lossy(&header[..end]).to_lowercase();
                    body_remaining = head
                        .lines()
                        .find_map(|l| l.strip_prefix("content-length:"))
                        .and_then(|v| v.trim().parse::<usize>().ok());
                    let body_start = end + 4 - (header.len() - n);
                    chunk = &buf[body_start..n];
                    header.clear();
                    header.shrink_to_fit();
                }
                match body_remaining {
                    Some(ref mut remaining) => {
                        *remaining = remaining.saturating_sub(chunk.len());
                        if *remaining > 0 {
                            continue;
                        }
                    }
                    None => {
                        // Chunked: done at the 0-length terminator.
                        let mut window = tail.to_vec();
                        window.extend_from_slice(chunk);
                        let len = chunk.len().min(tail.len());
                        let mut new_tail = [0u8; 8];
                        new_tail[8 - len..].copy_from_slice(&chunk[chunk.len() - len..]);
                        if len < 8 {
                            new_tail[..8 - len].copy_from_slice(&tail[len..]);
                        }
                        tail = new_tail;
                        if !window.windows(5).any(|w| w == b"0\r\n\r\n") {
                            continue;
                        }
                    }
                }
                break;
            }

            let _ = uploaded_tx.send(()).await;
            tokio::time::sleep(UPSTREAM_HOLD).await;
            let body = r#"{"id":"mock","choices":[]}"#;
            let resp = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = socket.write_all(resp.as_bytes()).await;
        });
    }
}

/// Send one large chat completion over raw TCP, writing the payload from a
/// shared 64 KB pattern so the client never holds it in full.
async fn send_large_request(addr: std::net::SocketAddr) {
    let prefix = br#"{"model":"fast","stream":false,"content":""#;
    let suffix = br#""}"#;
    let total = prefix.len() + PAYLOAD_BYTES + suffix.len();

    let mut socket = TcpStream::connect(addr).await.unwrap();
    let head = format!(
        "POST /v1/chat/completions HTTP/1.1\r\nhost: llmrouter\r\ncontent-type: application/json\r\ncontent-length: {total}\r\nconnection: close\r\n\r\n"
    );
    socket.write_all(head.as_bytes()).await.unwrap();
    socket.write_all(prefix).await.unwrap();
    let pattern = vec![b'A'; 64 * 1024];
    let mut written = 0;
    while written < PAYLOAD_BYTES {
        let n = pattern.len().min(PAYLOAD_BYTES - written);
        socket.write_all(&pattern[..n]).await.unwrap();
        written += n;
    }
    socket.write_all(suffix).await.unwrap();

    // Drain the response until the upstream-side close propagates.
    let mut sink = vec![0u8; 16 * 1024];
    while socket.read(&mut sink).await.unwrap_or(0) > 0 {}
}

fn router_state(upstream_port: u16) -> Arc<AppState> {
    llmrouter::init_crypto();
    let config: llmrouter::config::Config = toml::from_str(&format!(
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
        budget: BufferBudget::new(128 * 1024 * 1024, 32 * 1024 * 1024),
        metrics: Metrics::new(),
        shutting_down: AtomicBool::new(false),
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn request_memory_drains_while_waiting_on_upstream() {
    let upstream_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_port = upstream_listener.local_addr().unwrap().port();
    let (uploaded_tx, mut uploaded_rx) = tokio::sync::mpsc::channel(CONCURRENT_REQUESTS);
    tokio::spawn(run_mock_upstream(upstream_listener, uploaded_tx));

    let router_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let router_addr = router_listener.local_addr().unwrap();
    let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(run_server(
        router_listener,
        router_state(upstream_port),
        async {
            let _ = shutdown_rx.await;
        },
        Duration::from_secs(5),
    ));
    tokio::time::sleep(Duration::from_millis(50)).await;

    let baseline = live_bytes();

    let clients: Vec<_> = (0..CONCURRENT_REQUESTS)
        .map(|_| tokio::spawn(send_large_request(router_addr)))
        .collect();

    // Wait until the mock provider has swallowed every upload...
    for _ in 0..CONCURRENT_REQUESTS {
        tokio::time::timeout(Duration::from_secs(10), uploaded_rx.recv())
            .await
            .expect("upstream never received the uploads")
            .expect("upstream channel closed");
    }
    // ...give the freed chunks a beat to actually drop...
    tokio::time::sleep(Duration::from_millis(400)).await;

    // ...and sample while all requests are stalled on the upstream. The
    // budget for everything the router may still hold is well under one
    // copy of the in-flight payloads.
    let held = live_bytes() - baseline;
    let limit = (CONCURRENT_REQUESTS * PAYLOAD_BYTES) as isize;
    eprintln!(
        "router holds {} KB during the upstream stall ({} x {} KB in flight)",
        held / 1024,
        CONCURRENT_REQUESTS,
        PAYLOAD_BYTES / 1024
    );
    assert!(
        held < limit,
        "router holds {} KB while waiting on upstream; expected < {} KB \
         (≈1x the in-flight payloads). Old behavior held ~3x.",
        held / 1024,
        limit / 1024,
    );

    for client in clients {
        client.await.unwrap();
    }
}
