# Contributing

Thanks for your interest in llmrouter. Contributions are welcome — bug reports, fixes, tests, docs, and new providers especially.

## Development

```bash
cargo build            # debug build
cargo test             # run the test suite
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

CI runs `fmt --check`, `clippy -D warnings`, `cargo test`, and a release build on every pull request, so please run all four locally before opening one. The codebase forbids `unsafe`, denies leftover `todo!`/`dbg!`/`unimplemented!`, and warns on `unwrap`/`expect` outside tests — keep new code in that spirit.

## Pull requests

- Keep PRs focused; one logical change per PR.
- Add or update tests for behaviour changes.
- Update the README if you change user-facing behaviour or configuration.
- Don't bump the version in `Cargo.toml`; releases are cut separately.

## Scope

llmrouter is intentionally a lean, single-binary routing layer, not an LLMOps platform. That focus is the point, so some things are deliberately out of scope:

**Welcome:**
- Bug fixes and reliability improvements
- New providers that fit the existing OpenAI-compatible proxy model
- Test coverage, documentation, and performance work
- Routing-signal improvements (latency/error tracking)

**Probably out of scope** (open an issue to discuss before building):
- Request-level retries or failover — by design, llmrouter is a transparent proxy and leaves retries to client SDKs (see "Design choices" in the README)
- Cost or quality-based routing — routing optimizes latency within an alias you've declared interchangeable
- Spend tracking, prompt management, a web UI, or a control plane
- Anything that requires a database, Redis, or shared state

If you're unsure whether a change fits, please open an issue first.

## Reporting security issues

Please do not open a public issue for security vulnerabilities. See [SECURITY.md](SECURITY.md).
