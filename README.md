# sturnus

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![GitHub Release](https://img.shields.io/github/v/release/sturnus-dev/sturnus)](https://github.com/sturnus-dev/sturnus/releases)
[![Docker Image](https://img.shields.io/badge/docker-ghcr.io%2Fsturnus--dev%2Fsturnus-blue?logo=docker)](https://ghcr.io/sturnus-dev/sturnus)

**Automatic latency-based routing across LLM providers. A single static binary, zero infrastructure.**

LLM providers have variable latency and availability that can break production features. sturnus is a lightweight sidecar that sits beside your app, exposes an OpenAI-compatible API, and automatically shifts traffic to whichever provider is fastest and available right now.

## Quick start

sturnus needs a `config.toml` — copy `config.example.toml` and add your providers.

**Docker** — best for production deployments and Kubernetes sidecars:

```bash
docker run -v ./config.toml:/config.toml \
  -p 4000:4000 \
  ghcr.io/sturnus-dev/sturnus:latest
```

**`cargo install`** — best for local testing if you have a Rust toolchain:

```bash
cargo install sturnus
sturnus --config config.toml
```

Prebuilt static binaries for Linux and macOS (x86_64 and aarch64) are attached to every [release](https://github.com/sturnus-dev/sturnus/releases/latest).

Then point any OpenAI-compatible SDK at sturnus — the only change is the base URL:

```diff
- client = OpenAI(base_url="https://api.openai.com/v1", api_key="sk-...")
+ client = OpenAI(base_url="http://127.0.0.1:4000/v1", api_key="unused")
```

```python
from openai import OpenAI
client = OpenAI(base_url="http://127.0.0.1:4000/v1", api_key="unused")
response = client.chat.completions.create(
    model="fast",  # resolved by sturnus to the fastest available candidate
    messages=[{"role": "user", "content": "Hello"}],
)
```

## Features

- **Latency- and error-aware routing** — the fastest healthy provider gets the bulk of traffic, while slower or erroring ones keep a small, shrinking share. That share doubles as a probe, so a recovered provider wins its traffic back automatically, with no thresholds to trip.
- **Session affinity** — a stateless `x-session-affinity` header pins follow-up requests to the same provider across pods.
- **Transparent passthrough** — only the `model` field is rewritten: the request body is otherwise forwarded byte-for-byte, preserving key order, number precision, and formatting. Responses, including SSE `text/event-stream` chunks, are relayed untouched as they arrive.
- **Memory-bounded** — request buffers are capped per request and in aggregate; bursts beyond the memory budget shed load with `429` + `Retry-After` instead of OOMing the pod.
- **Vertex AI support** — GKE Workload Identity auth via the metadata server, with automatic token refresh.
- **Zero infrastructure** — a single static binary; no Redis, database, or control plane.

## Why sturnus

Most LLM gateways are either a hosted SaaS you route all your traffic (and keys) through, or a large application with a significant surface area. sturnus is the opposite — **a single static binary** with a small auditable surface area, MIT-licensed and running entirely inside your infrastructure. It speaks the OpenAI API, so any OpenAI-compatible SDK works by changing one base URL. The core capability of sturnus is automatic latency-based routing across providers — something that most gateways put behind an enterprise tier. Each sidecar routes independently from what it observes locally, so there is no shared state to run.

If you need a full LLMOps platform (spend tracking, prompt management, a UI, dozens of integrations), sturnus is not that.

<details>
<summary><b>Design choices &amp; deliberate omissions</b></summary>

sturnus has a bounded scope by design and has some deliberate omissions:

- **No request-level failover or retries.** sturnus is a transparent proxy: it surfaces upstream errors to the client verbatim rather than silently retrying within a black box. Error responses still feed the routing signal, so a flaky provider is quickly deprioritized for subsequent traffic — but the individual failed request is returned as-is. Client SDKs (OpenAI, Anthropic, LangChain, etc.) already ship mature, configurable retry and backoff; configure it there and let sturnus steer those retries toward the healthiest provider.
- **Latency-based, not cost or quality-based.** Routing optimizes time-to-first-chunk *within an alias*, and every model routed under that alias should be largely interchangeable. sturnus never trades quality or cost for speed — it just picks the fastest among options you've already deemed equivalent.

</details>

## Contents

- [Configuration](#configuration)
- [Endpoints](#endpoints)
- [Observability](#observability)
- [Session affinity](#session-affinity)
- [How routing works](#how-routing-works)
- [Docker](#docker)
- [Building](#building)

## Configuration

```toml
# use 127.0.0.1:4000 if running locally rather than in a container
listen = "0.0.0.0:4000"

# Providers: where to send requests
[provider.openai]
base_url = "https://api.openai.com/v1"
api_key = "${OPENAI_API_KEY}"

# Vertex AI via GKE Workload Identity (no API key needed)
[provider.vertex]
vertex_ai = { project_id = "my-gcp-project", location = "us-central1" }

# Model map: aliases the client uses → provider+model candidates
[model]
fast = [
  { provider = "openai", model = "gpt-4o-mini" },
  { provider = "vertex", model = "google/gemini-2.5-flash" },
]

[routing]
ewma_alpha = 0.3          # smoothing for the latency and success-rate EWMAs (higher = more reactive)
error_threshold = 0.5      # error-rate EWMA above which a session-affinity pin is broken (routing weights are unaffected)
```

See [`config.example.toml`](config.example.toml) for all providers (Groq, Azure, Google AI Studio, Anthropic, local OpenAI-compatible) and options.

Environment variables in `${VAR}` syntax are interpolated at config load time. Where they're available in an `.env` file (`KEY=VALUE` per line), pass it with `--env-file`:

```bash
sturnus --env-file /secrets/.env
```

<details>
<summary><b>Vertex billing attribution</b></summary>

For Vertex providers, sturnus can inject sidecar-controlled `labels` into outbound requests so the resulting spend shows up tagged in GCP Billing Export. The labels live in a top-level `[attribution]` block (typically deployment identity sourced from env vars) and are merged into each request body for any Vertex provider that opts in:

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
| `sturnus_requests_total` | counter | Completed responses, additionally labelled by `status_code` (includes upstream 4xx/5xx) |
| `sturnus_ttfc_seconds` | histogram | Streaming time-to-first-chunk (streaming requests only) |
| `sturnus_latency_seconds` | histogram | Non-streaming full response time (non-streaming requests only) |
| `sturnus_errors_total` | counter | Transport failures that never produced a response (timeout, connect, DNS) |
| `sturnus_buffer_rejections_total` | counter | Requests shed with `429` because the aggregate buffer budget was full (no per-alias labels) |

Connection failures are zero-initialised at startup so a missing series is never mistaken for "no errors".

### Logging

Structured logging via `tracing`: coloured text on a terminal (respecting `NO_COLOR`), newline-delimited JSON when piped or redirected. Set the format with `--log-format <auto|pretty|json>` (or `STURNUS_LOG_FORMAT`) and the level with `RUST_LOG` (default `sturnus=info`).

Each request gets a span with a `request_id`; a client-supplied W3C `traceparent` propagates as `trace_id` and `parent_span_id` for cross-service correlation.

## Session affinity

Every response includes an `x-session-affinity` header (e.g. `openai/gpt-4o-mini`). Pass it back on subsequent requests to pin to the same provider — useful for multi-turn conversations where context is provider-specific:

```python
response = client.chat.completions.create(
    model="fast",
    messages=[{"role": "user", "content": "Hello"}],
)
affinity = response.headers["x-session-affinity"]  # e.g. "openai/gpt-4o-mini"

response = client.chat.completions.create(
    model="fast",
    messages=[{"role": "user", "content": "Follow-up"}],
    extra_headers={"x-session-affinity": affinity},
)
```

Fully stateless — works across pods with no shared state. The pin is honored until the pinned candidate's error-rate EWMA breaches `error_threshold` (at the default smoothing, roughly two consecutive errors), at which point the header is ignored and a new provider is selected — check the updated `x-session-affinity` in the response. Unknown or malformed headers fall back to normal routing.

## How routing works

1. Client sends `POST /v1/chat/completions` with `"model": "fast"`.
2. Sidecar looks up the `fast` alias and computes each candidate's **effective latency**: its latency EWMA divided by its success-rate EWMA. A candidate erroring with probability `p` needs ~`1/(1-p)` attempts per success, so errors inflate effective latency the same way slowness does.
3. Each candidate is weighted by `(best_effective / its_effective)^k`, so the best gets the bulk of traffic and worse ones a shrinking-but-nonzero share. A deterministic low-discrepancy sequence (golden-ratio Weyl sequence) turns those weights into picks.
4. Because worse candidates always keep a small share, their EWMAs stay fresh — a provider that recovers (faster responses *or* errors stopping) wins traffic back automatically; a cold candidate (no latency data yet) probes at a quarter of the best candidate's rate, scaled by its success rate, until its first samples land.
5. The `model` field is rewritten to the real model name, auth headers are set, and the request is forwarded.
6. TTFC is measured at first chunk arrival and fed back into the EWMA; the response status (any non-2xx counts as an error, including upstream 4xx) feeds the success-rate EWMA.

The best provider is exploited heavily while worse ones keep enough traffic to stay measured. A candidate's probe share shrinks with how bad it looks but is floored at 1%, so re-detecting a recovered provider costs at most ~100 requests — and during an outage at most ~1% of an alias's traffic is spent on the failing candidate.

## Docker

When running in Docker or as a Kubernetes sidecar, `listen` must be `0.0.0.0:4000` (the value in `config.example.toml`) — `127.0.0.1` only accepts connections from within the container itself.

On Kubernetes, run sturnus as a [native sidecar](https://kubernetes.io/docs/concepts/workloads/pods/sidecar-containers/) — an init container with `restartPolicy: Always` (stable since v1.29). It then starts before the app container and is terminated after it, so the proxy is ready for the app's first request and stays up while the app drains.

Memory needs no tuning: the aggregate request-buffer budget defaults to half the container's memory limit (read from cgroups at startup, logged with its source), so a small sidecar sheds excess load with `429`s rather than getting OOM-killed. Override with `routing.max_buffered_bytes` if you want a different ceiling.

The image is published as a multi-arch (amd64/arm64) scratch container to `ghcr.io/sturnus-dev/sturnus`. Tags follow semver: `:latest`, `:5.0`, `:5.0.0`.

To inject secrets via a mounted `.env` file:

```bash
docker run -v ./config.toml:/config.toml \
  -v ./secrets.env:/secrets/.env:ro \
  -p 4000:4000 \
  ghcr.io/sturnus-dev/sturnus:latest --env-file /secrets/.env
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
  ghcr.io/sturnus-dev/sturnus:latest
```

Or gcloud ADC for local dev, mounted to `$HOME/.config/gcloud/` (the image sets `HOME=/root`):

```bash
docker run -v ./config.toml:/config.toml \
  -v ~/.config/gcloud/application_default_credentials.json:/root/.config/gcloud/application_default_credentials.json:ro \
  -p 4000:4000 \
  ghcr.io/sturnus-dev/sturnus:latest
```

</details>

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
