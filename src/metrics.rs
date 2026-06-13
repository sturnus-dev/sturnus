// Metric setup builds static descriptors; an expect failure here is a build-time bug.
#![allow(clippy::expect_used)]

use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, Opts, Registry, TextEncoder,
};

pub struct Metrics {
    registry: Registry,
    pub requests_total: IntCounterVec,
    /// Streaming time-to-first-chunk.
    pub ttfc_seconds: HistogramVec,
    /// Non-streaming full response time.
    pub latency_seconds: HistogramVec,
    pub errors_total: IntCounterVec,
    /// Requests shed with 429 because the aggregate buffer budget was full.
    pub buffer_rejections_total: IntCounter,
}

impl std::fmt::Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Metrics").finish_non_exhaustive()
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();

        let requests_total = IntCounterVec::new(
            Opts::new("llmrouter_requests_total", "Total upstream requests"),
            &["alias", "provider", "model", "status_code"],
        )
        .expect("valid metric descriptor");

        // Distinct metrics, not one with a mode label: they measure different things.
        let latency_buckets = vec![0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0];

        let ttfc_seconds = HistogramVec::new(
            HistogramOpts::new(
                "llmrouter_ttfc_seconds",
                "Streaming time-to-first-chunk from upstream",
            )
            .buckets(latency_buckets.clone()),
            &["alias", "provider", "model"],
        )
        .expect("valid metric descriptor");

        let latency_seconds = HistogramVec::new(
            HistogramOpts::new(
                "llmrouter_latency_seconds",
                "Non-streaming full response time from upstream",
            )
            .buckets(latency_buckets),
            &["alias", "provider", "model"],
        )
        .expect("valid metric descriptor");

        let errors_total = IntCounterVec::new(
            Opts::new(
                "llmrouter_errors_total",
                "Connection failures before reaching upstream",
            ),
            &["alias", "provider", "model"],
        )
        .expect("valid metric descriptor");

        let buffer_rejections_total = IntCounter::new(
            "llmrouter_buffer_rejections_total",
            "Requests rejected because the aggregate buffer budget was full",
        )
        .expect("valid metric descriptor");

        registry
            .register(Box::new(requests_total.clone()))
            .expect("unique metric name");
        registry
            .register(Box::new(ttfc_seconds.clone()))
            .expect("unique metric name");
        registry
            .register(Box::new(latency_seconds.clone()))
            .expect("unique metric name");
        registry
            .register(Box::new(errors_total.clone()))
            .expect("unique metric name");
        registry
            .register(Box::new(buffer_rejections_total.clone()))
            .expect("unique metric name");

        Self {
            registry,
            requests_total,
            ttfc_seconds,
            latency_seconds,
            errors_total,
            buffer_rejections_total,
        }
    }

    /// Zero-init error counters so a missing series isn't mistaken for "no errors".
    pub fn init_zero(&self, aliases: &[(&str, &str, &str)]) {
        for &(alias, provider, model) in aliases {
            self.errors_total
                .with_label_values(&[alias, provider, model]);
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, prometheus::Error> {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buf = Vec::new();
        encoder.encode(&metric_families, &mut buf)?;
        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_returns_valid_prometheus_text() {
        let m = Metrics::new();
        m.requests_total
            .with_label_values(&["fast", "openai", "gpt-4o-mini", "200"])
            .inc();
        m.ttfc_seconds
            .with_label_values(&["fast", "openai", "gpt-4o-mini"])
            .observe(0.42);
        m.latency_seconds
            .with_label_values(&["fast", "openai", "gpt-4o-mini"])
            .observe(1.5);
        m.errors_total
            .with_label_values(&["fast", "groq", "llama-3"])
            .inc();

        let output = String::from_utf8(m.encode().unwrap()).unwrap();
        assert!(output.contains("llmrouter_requests_total"));
        assert!(output.contains("llmrouter_ttfc_seconds"));
        assert!(output.contains("llmrouter_latency_seconds"));
        assert!(output.contains("llmrouter_errors_total"));
        assert!(output.contains("alias=\"fast\""));
        assert!(output.contains("provider=\"openai\""));
        assert!(output.contains("status_code=\"200\""));
    }
}
