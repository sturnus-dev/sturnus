# llmrouter

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![GitHub Release](https://img.shields.io/github/v/release/dannyboland/llmrouter)](https://github.com/dannyboland/llmrouter/releases)
[![Docker Image](https://img.shields.io/badge/docker-ghcr.io%2Fdannyboland%2Fllmrouter-blue?logo=docker)](https://ghcr.io/dannyboland/llmrouter)

**Automatic latency-based routing across LLM providers. A single static binary, zero infrastructure.**

LLM providers have variable latency and availability that can break production features. llmrouter is a lightweight sidecar that sits beside your app, exposes an OpenAI-compatible API, and automatically shifts traffic to whichever provider is fastest and available right now.


## Quick start

**Docker** — best for production deployments and Kubernetes sidecars:

```bash
docker run -v ./config.toml:/config.toml \
  -p 4000:4000 \
  ghcr.io/dannyboland/llmrouter:latest
```

**Pre-built binary** — best for CI, scripting, or running without Docker:

```bash
# Linux (x86_64) — also available for aarch64-unknown-linux-musl, x86_64-apple-darwin, aarch64-apple-darwin
curl -fsSL https://github.com/dannyboland/llmrouter/releases/latest/download/llmrouter-x86_64-unknown-linux-musl -o llmrouter
chmod +x llmrouter
./llmrouter --config config.toml
```

Then point any OpenAI-compatible SDK at llmrouter — the only change is the base URL:

```diff
- client = OpenAI(base_url="https://api.openai.com/v1", api_key="sk-...")
+ client = OpenAI(base_url="http://127.0.0.1:4000/v1", api_key="unused")
```

```python
from openai import OpenAI
client = OpenAI(base_url="http://127.0.0.1:4000/v1", api_key="unused")
response = client.chat.completions.create(
    model="fast",  # resolved by llmrouter to lowest-latency candidate
    messages=[{"role": "user", "content": "Hello"}],
)
```

## Features

- **Latency- and error-aware routing** — proportional weighting by *effective latency* per (provider, model): the latency EWMA divided by the success-rate EWMA, i.e. the expected time per successful response. The best candidate gets the bulk of traffic while slower or erroring ones keep a small, shrinking share. That share doubles as a probe, so a provider that recovers wins traffic back automatically — no winner-take-all oscillation, no thresholds to trip or age out.
- **Session affinity** — a stateless `x-session-affinity` header pins follow-up requests to the same provider across pods, with automatic fallback when the pinned candidate's error rate breaches `error_threshold`.
- **SSE streaming passthrough** — relays `text/event-stream` chunks as they arrive, with no buffering.
- **Memory-bounded** — request buffers are capped per request and in aggregate; the aggregate budget is auto-sized from the container's cgroup memory limit, and bursts beyond it shed load with `429` + `Retry-After` instead of OOMing the pod. The request body is forwarded with its `Content-Length` and freed as soon as the upload completes.
- **HTTP/2 to providers** — negotiated via ALPN with automatic HTTP/1.1 fallback, so concurrent requests multiplex over one connection instead of churning the pool.
- **Vertex AI support** — GKE Workload Identity auth via the metadata server, with automatic token refresh.
- **Zero infrastructure** — a single static binary; no Redis, database, or control plane.

## Contents

- [Configuration](#configuration)
- [Endpoints](#endpoints)
- [Observability](#observability)
- [Session affinity](#session-affinity)
- [How routing works](#how-routing-works)
- [Why llmrouter](#why-llmrouter)
- [Docker](#docker)
- [Performance](#performance)
- [Building](#building)

## Configuration

```toml
listen = "127.0.0.1:4000"

# Providers: where to send requests
[provider.openai]
base_url = "https://api.openai.com/v1"
api_key = "${OPENAI_API_KEY}"

[provider.groq]
base_url = "https://api.groq.com/openai/v1"
api_key = "${GROQ_API_KEY}"

# Vertex AI via GKE Workload Identity (no API key needed)
[provider.vertex]
vertex_ai = { project_id = "my-gcp-project", location = "us-central1" }

# Azure OpenAI
[provider.azure]
api_key = "${AZURE_OPENAI_KEY}"
azure_openai = { resource_name = "my-resource", api_version = "2024-10-21" }

# Google AI Studio
[provider.gemini]
api_key = "${GEMINI_API_KEY}"
google_ai = { api_version = "v1beta" }  # api_version defaults to v1beta

# Anthropic
[provider.anthropic]
api_key = "${ANTHROPIC_API_KEY}"
anthropic = { version = "2023-06-01" }  # version defaults to 2023-06-01

# Model map: aliases the client uses → provider+model candidates
[model]
fast = [
  { provider = "groq", model = "llama-3.3-70b-versatile" },
  { provider = "openai", model = "gpt-4o-mini" },
]

smart = [
  { provider = "openai", model = "gpt-4.1" },
  { provider = "vertex", model = "google/gemini-2.5-flash" },
]

[routing]
ewma_alpha = 0.3          # smoothing for the latency and success-rate EWMAs (higher = more reactive)
error_threshold = 0.5      # error-rate EWMA above which a session-affinity pin is broken (routing weights are unaffected)
```

Environment variables in `${VAR}` syntax are interpolated at config load time. Where they're available in an `.env` file (`KEY=VALUE` per line), pass it with `--env-file`:

```bash
llmrouter --env-file /secrets/.env
```

<details>
<summary><b>Vertex billing attribution</b></summary>

For Vertex providers, llmrouter can inject sidecar-controlled `labels` into outbound requests so the resulting spend shows up tagged in GCP Billing Export. The labels live in a top-level `[attribution]` block (typically deployment identity sourced from env vars) and are merged into each request body for any Vertex provider that opts in:

```toml
[attribution]
service = "${SERVICE_NAME}"
owner = "${OWNER}"
env = "${ENV}"

[provider.vertex]
vertex_ai = { project_id = "my-project", location = "us-central1", attribution = true }
```

Sidecar keys take precedence over any client-supplied `labels` keys with the same name; disjoint client keys are preserved. The feature is currently scoped to Vertex only. Keys and values must conform to Vertex naming rules (`[a-z][a-z0-9_-]{0,62}`).

</details>

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/v1/chat/completions` | Proxied to upstream (model alias resolved) |
| POST | `/v1/embeddings` | Proxied to upstream (model alias resolved) |
| GET | `/health` | Returns `{"status":"ok"}` |
| GET | `/status` | Returns current streaming/non-streaming EWMAs, error rate, and status per candidate |
| GET | `/metrics` | Prometheus metrics (see below) |

## Observability

### Metrics

Prometheus metrics on `/metrics`, all labelled by `alias`, `provider`, `model`:

| Metric | Type | Meaning |
|--------|------|---------|
| `llmrouter_requests_total` | counter | Completed responses, additionally labelled by `status_code` (includes upstream 4xx/5xx) |
| `llmrouter_ttfc_seconds` | histogram | Streaming time-to-first-chunk (streaming requests only) |
| `llmrouter_latency_seconds` | histogram | Non-streaming full response time (non-streaming requests only) |
| `llmrouter_errors_total` | counter | Transport failures that never produced a response (timeout, connect, DNS) |
| `llmrouter_buffer_rejections_total` | counter | Requests shed with `429` because the aggregate buffer budget was full (no per-alias labels) |

Connection failures are zero-initialised at startup so a missing series is never mistaken for "no errors".

### Logging

Structured logging via `tracing`: human-readable and coloured on a terminal (respecting `NO_COLOR`), newline-delimited JSON when piped or redirected. Override with `--log-format <auto|pretty|json>` or `LLMROUTER_LOG_FORMAT`; filter with `RUST_LOG` (default `llmrouter=info`).

Every request is wrapped in a span carrying a `request_id` that correlates all log lines for that request; when the client sends a W3C `traceparent`, the `trace_id` and `parent_span_id` are attached too. Notable events at the default `info` level and above:

- **Upstream error status** (`warn`) — an upstream answered 4xx/5xx; relayed to the client verbatim and logged with the status.
- **Upstream request failed** (`warn`) — the request never reached the upstream (transport error); returned to the client as `502`.
- **Stream chunk error** (`warn`) — a streamed response broke partway through.

## Session affinity

Every response includes an `x-session-affinity` header (e.g. `openai/gpt-4o-mini`). Pass it back on subsequent requests to pin to the same provider — useful for multi-turn conversations where context is provider-specific:

```python
response = client.chat.completions.create(
    model="smart",
    messages=[{"role": "user", "content": "Hello"}],
)
affinity = response.headers["x-session-affinity"]  # e.g. "openai/gpt-4o-mini"

response = client.chat.completions.create(
    model="smart",
    messages=[{"role": "user", "content": "Follow-up"}],
    extra_headers={"x-session-affinity": affinity},
)
```

Fully stateless — works across pods with no shared state. The pin is honored until the pinned candidate's error-rate EWMA breaches `error_threshold` (at the default smoothing, roughly two consecutive errors), at which point the header is ignored and a new provider is selected — check the updated `x-session-affinity` in the response. Unknown or malformed headers fall back to normal routing.

## How routing works

1. Client sends `POST /v1/chat/completions` with `"model": "fast"`
2. Sidecar looks up the `fast` alias and computes each candidate's **effective latency**: its latency EWMA divided by its success-rate EWMA. A candidate erroring with probability `p` needs ~`1/(1-p)` attempts per success, so errors inflate effective latency the way slowness does — one signal, no separate health state
3. Each candidate is weighted by `(best_effective / its_effective)^k`, so the best gets the bulk of traffic and worse ones a shrinking-but-nonzero share. A deterministic low-discrepancy sequence (golden-ratio Weyl sequence) turns those weights into picks — no RNG, and the long-run split matches the weights without same-candidate bursts
4. Because worse candidates always keep a small share, their EWMAs stay fresh — a provider that recovers (faster responses *or* errors stopping) wins traffic back automatically, with no winner-take-all herd and no threshold or time window to wait out; a cold candidate (no latency data yet) probes at a quarter of the best candidate's rate, scaled by its success rate, until its first samples land
5. The `model` field is rewritten to the real model name, auth headers are set, and the request is forwarded
6. TTFC is measured at first chunk arrival and fed back into the EWMA; the response status (any non-2xx counts as an error, including upstream 4xx) feeds the success-rate EWMA

The best provider is exploited heavily while worse ones keep enough traffic to stay measured. A candidate's probe share shrinks with how bad it looks but is floored at 1%, so re-detecting a recovered provider costs at most ~100 requests — and during an outage at most ~1% of an alias's traffic is spent on the failing candidate.

## Why llmrouter

Most LLM gateways are either a hosted SaaS you route all your traffic (and keys) through, or a large application with a significant surface area. llmrouter is deliberately the opposite — **a single static binary, not a platform**: a Rust codebase you can read in an afternoon, with a small auditable surface area, MIT-licensed and running entirely inside your infrastructure. It speaks the OpenAI API, so any OpenAI-compatible SDK works by changing one base URL. To rewrite the `model` field, each request body is buffered (capped at 32 MB by default) and validated as JSON — but only `model` is touched: every other field is forwarded byte-for-byte, preserving key order, number precision, and formatting. Responses, including SSE streams, are relayed untouched.

If you need a full LLMOps platform — spend tracking, prompt management, a UI, dozens of integrations — llmrouter is intentionally not that.

<details>
<summary><b>Design choices &amp; deliberate omissions</b></summary>

llmrouter has a bounded scope by design and has some deliberate omissions:

- **No request-level failover or retries.** llmrouter is a transparent proxy: it surfaces upstream errors to the client verbatim rather than silently retrying within a black box. Error responses still feed the routing signal, so a flaky provider is quickly deprioritized for subsequent traffic — but the individual failed request is returned as-is. Client SDKs (OpenAI, Anthropic, LangChain, etc.) already ship mature, configurable retry and backoff; configure it there and let llmrouter steer those retries toward the healthiest provider.
- **Latency-based, not cost or quality-based.** Routing optimizes time-to-first-chunk *within an alias*, and every model routed under that alias should be largely interchangeable. llmrouter never trades quality or cost for speed — it just picks the fastest among options you've already deemed equivalent.

</details>

## Docker

When running in Docker or as a Kubernetes sidecar, set `listen = "0.0.0.0:4000"` in your config — the default `127.0.0.1` only accepts connections from within the container itself.

Memory needs no tuning: the aggregate request-buffer budget defaults to half the container's memory limit (read from cgroups at startup, logged with its source), so a small sidecar sheds excess load with `429`s rather than getting OOM-killed. Override with `routing.max_buffered_bytes` if you want a different ceiling.

The image is published as a multi-arch (amd64/arm64) scratch container to `ghcr.io/dannyboland/llmrouter`. Tags follow semver: `:latest`, `:4.0`, `:4.0.0`.

To inject secrets via a mounted `.env` file:

```bash
docker run -v ./config.toml:/config.toml \
  -v ./secrets.env:/secrets/.env:ro \
  -p 4000:4000 \
  ghcr.io/dannyboland/llmrouter:latest --env-file /secrets/.env
```

<details>
<summary><b>Vertex credentials outside GKE</b></summary>

On GKE, workload identity is picked up automatically. Elsewhere, supply credentials one of two ways.

A service account key, pointed to by `GOOGLE_APPLICATION_CREDENTIALS` (recommended for production):

```bash
docker run -v ./config.toml:/config.toml \
  -v ./sa-key.json:/sa-key.json:ro \
  -e GOOGLE_APPLICATION_CREDENTIALS=/sa-key.json \
  -p 4000:4000 \
  ghcr.io/dannyboland/llmrouter:latest
```

Or gcloud ADC for local dev, mounted to `$HOME/.config/gcloud/` (the image sets `HOME=/root`):

```bash
docker run -v ./config.toml:/config.toml \
  -v ~/.config/gcloud/application_default_credentials.json:/root/.config/gcloud/application_default_credentials.json:ro \
  -p 4000:4000 \
  ghcr.io/dannyboland/llmrouter:latest
```

</details>

## Performance

llmrouter's own overhead is negligible next to the hundreds of milliseconds, or even many seconds, of provider latency it routes around. It's a thin Rust proxy in the hot path, not a platform. If you want to measure it yourself, the [Ferro Labs AI gateway benchmark](https://github.com/ferro-labs/ai-gateway-performance-benchmarks) is a reasonable methodology.

## Building

```bash
# Development
cargo build

# Release (static binary with LTO)
cargo build --release

# Run tests
cargo test
```

## License

MIT
