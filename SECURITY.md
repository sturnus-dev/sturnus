# Security Policy

sturnus sits in the path of every LLM request and handles your provider API keys, so security reports are taken seriously.

## Reporting a vulnerability

Please **do not** open a public GitHub issue for security vulnerabilities.

Instead, report privately via one of:

- GitHub's [private vulnerability reporting](https://github.com/sturnus-dev/sturnus/security/advisories/new) (preferred)
- Email: **danny@boland.dev**

Please include enough detail to reproduce — affected version, configuration (with secrets redacted), and the impact you observed. You'll get an acknowledgement as soon as possible, and be kept informed as the issue is investigated and fixed.

## Supported versions

Fixes are released against the latest published version. Please confirm an issue reproduces on the most recent release before reporting.

## Scope notes

- API keys and credentials are read from config (`${VAR}` interpolation) or an `--env-file` and are never written to logs. Reports of secret leakage into logs, metrics, traces, or proxied responses are in scope.
- Upstream `Set-Cookie` and hop-by-hop headers are stripped from proxied responses. Header-handling or request-smuggling issues are in scope.
- sturnus forwards to the upstream base URLs you configure; it is not a general-purpose open proxy. Issues that require an attacker to already control the config are generally out of scope.
