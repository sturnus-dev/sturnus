# llmrouter

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![GitHub Release](https://img.shields.io/github/v/release/dannyboland/llmrouter)](https://github.com/dannyboland/llmrouter/releases)
[![Docker Image](https://img.shields.io/badge/docker-ghcr.io%2Fdannyboland%2Fllmrouter-blue?logo=docker)](https://ghcr.io/dannyboland/llmrouter)

**Automatic latency-based routing across LLM providers. ~1 ms overhead. Zero infrastructure.**

LLM providers have variable latency and availability that can break production features. llmrouter is a lightweight sidecar that sits beside your app, exposes an OpenAI-compatible API, and automatically shifts traffic to whichever provider is fastest right now.

## Features

- **Latency-based routing** — tracks EWMA of time-to-first-chunk per (provider, model) and routes to the fastest candidate
- **Explore/exploit** — configurable fraction of traffic round-robins across all healthy candidates, discovering cold providers and re-sampling warm ones to detect latency changes
- **Error rate tracking** — time-based sliding window deprioritizes failing candidates; degraded candidates are excluded from traffic until errors age out
- **Session affinity** — stateless `x-session-affinity` header pins subsequent requests to the same provider, works across pods with no shared state, with automatic fallback on degradation
- **SSE streaming passthrough** — relays `text/event-stream` chunks as they arrive with no buffering
- **Vertex AI support** — GKE Workload Identity auth via metadata server with automatic token refresh
- **Zero infrastructure** — single static binary, no Redis/database/control plane

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
ewma_alpha = 0.3          # EWMA smoothing factor (higher = more reactive)
explore_ratio = 0.2        # fraction of traffic that round-robins across all healthy candidates
error_threshold = 0.5      # error rate above which a candidate is excluded
error_decay_secs = 300     # time window for error rate calculation; old errors age out naturally
```

Environment variables in `${VAR}` syntax are interpolated at config load time.

### Vertex billing attribution

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

### Loading secrets

Where environment variables are available in an .env file, this can be passed with `--env-file`:

```bash
# Single .env file (KEY=VALUE per line)
llmrouter --env-file /secrets/.env
```

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/v1/chat/completions` | Proxied to upstream (model alias resolved) |
| POST | `/v1/embeddings` | Proxied to upstream (model alias resolved) |
| GET | `/health` | Returns `{"status":"ok"}` |
| GET | `/status` | Returns current EWMA, error rate, and status per candidate |
| GET | `/metrics` | Prometheus metrics (request counts, TTFC histogram, errors) |

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

Fully stateless — works across pods with no shared state. If the pinned provider degrades, the header is ignored and a new provider is selected (check the updated `x-session-affinity` in the response).

## How routing works

1. Client sends `POST /v1/chat/completions` with `"model": "fast"`
2. Sidecar looks up the `fast` alias and partitions candidates into **warm** (have latency data, healthy), **cold** (no data yet), and **degraded** (high error rate)
3. ~20% of requests (configurable via `explore_ratio`) round-robin across all healthy candidates (warm + cold) to discover new providers and detect latency changes
4. Remaining requests go to the warm candidate with the lowest EWMA time-to-first-chunk
5. Degraded candidates are excluded entirely; they recover when errors age out of the time window
6. The `model` field is rewritten to the real model name, auth headers are set, and the request is forwarded
7. TTFC is measured at first chunk arrival and fed back into the EWMA

## Docker

When running in Docker or as a Kubernetes sidecar, set `listen = "0.0.0.0:4000"` in your config — the default `127.0.0.1` only accepts connections from within the container itself.

The image is published as a multi-arch (amd64/arm64) scratch container to `ghcr.io/dannyboland/llmrouter`. Tags follow semver: `:latest`, `:3.0`, `:3.0.0`.

To inject secrets via a mounted `.env` file:

```bash
# .env file
docker run -v ./config.toml:/config.toml \
  -v ./secrets.env:/secrets/.env:ro \
  -p 4000:4000 \
  ghcr.io/dannyboland/llmrouter:latest --env-file /secrets/.env
```

## Performance

**~1.3 ms of end-to-end overhead** measured with the [Ferro Labs AI gateway benchmark](https://github.com/ferro-labs/ai-gateway-performance-benchmarks) methodology.

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
