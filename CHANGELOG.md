# Changelog

All notable changes to this project are documented here.

The project was renamed from **llmrouter** to **sturnus** in 5.0.0. The previous
name collided with other projects, and as the project matured it made sense to
rename it.

Releases up to and including 4.3.0 were published under the `llmrouter` name;
their history lives in the git log and tags.

## [5.2.0] - 2026-07-14

### Added

- **Optional separate metrics listener** via a new `metrics_listen` config key.
  When set (e.g. `metrics_listen = "0.0.0.0:4040"`), the observability endpoints
  (`/metrics`, `/health`, `/healthz`, `/status`) are served on a second socket
  that never exposes the proxy, so `listen` can bind loopback for the
  credential-bearing proxy while Prometheus still scrapes metrics off a routable
  address. On shutdown this listener stays up until the proxy has finished
  draining, so the final scrape window isn't lost.

## [5.1.0] - 2026-07-14

### Added

- **healthcheck subcommand** added to support exec probe healthchecks.

## [5.0.0] - 2026-06-17

Project renamed from `llmrouter` to `sturnus`. This is a breaking release: the
binary, crate, container image, Prometheus metric names, response header, and
environment variables all change name, and the routing config fields deprecated
since 4.1/4.2 are removed.

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

### Removed

- **Deprecated `routing` fields dropped**: `explore_ratio` (no-op since 4.1.0),
  `error_decay_secs` and `max_error_window_entries` (no-ops since 4.2.0). They were
  already ignored; a config that still sets them now loads with the keys ignored
  rather than logging a deprecation warning.

### Migration

- Rename the metric prefix in any Grafana/Prometheus dashboards and alert rules
  (`llmrouter_` → `sturnus_`).
- Replace reads of `x-llmrouter-provider` with `x-sturnus-provider`.
- Update the binary/image name and any `LLMROUTER_LOG_FORMAT` env var.
- Existing `config.toml` files still load. The removed `routing` fields
  (`explore_ratio`, `error_decay_secs`, `max_error_window_entries`) are now silently
  ignored; delete them to keep configs tidy.
