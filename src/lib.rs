#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

pub mod body;
pub mod config;
pub mod gcp_auth;
pub mod metrics;
pub mod model_map;
pub mod proxy;
pub mod router;
pub mod server;
pub mod trace;
pub mod tracker;

/// Install ring as the process-wide rustls crypto provider. Idempotent.
pub fn init_crypto() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}
