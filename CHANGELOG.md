# Changelog

All notable changes to this project are documented here.

The project was renamed from **llmrouter** to **sturnus** in 5.0.0. The previous
name collided with other projects, and as the project matured it made sense to
rename it.

Releases up to and including 4.3.0 were published under the `llmrouter` name;
their history lives in the git log and tags.

## [5.0.0] - 2026-06-17

Project renamed from `llmrouter` to `sturnus`. This is a breaking release: the
binary, crate, container image, Prometheus metric names, response header, and
environment variables all change name. Functionality is otherwise unchanged from
4.3.0.

### Changed (breaking)

- **Prometheus metrics renamed** `llmrouter_* → sturnus_*`: `sturnus_requests_total`,
  `sturnus_ttfc_seconds`, `sturnus_latency_seconds`, `sturnus_errors_total`,
  `sturnus_buffer_rejections_total`. Update dashboards, alerts, and recording rules.
- **Response header renamed** `x-llmrouter-provider → x-sturnus-provider`. Update any
  client keying off the old header.
- **Environment variables renamed** `LLMROUTER_LOG_FORMAT → STURNUS_LOG_FORMAT`; the
  default `RUST_LOG` filter is now `sturnus=info`.
- **Binary, crate, and container image renamed** to `sturnus`; the image is published
  to `ghcr.io/sturnus-dev/sturnus` and releases to `github.com/sturnus-dev/sturnus`.

### Migration

- Rename the metric prefix in any Grafana/Prometheus dashboards and alert rules
  (`llmrouter_` → `sturnus_`).
- Replace reads of `x-llmrouter-provider` with `x-sturnus-provider`.
- Update the binary/image name and any `LLMROUTER_LOG_FORMAT` env var.
- Config file format is unchanged; existing `config.toml` files work as-is.
