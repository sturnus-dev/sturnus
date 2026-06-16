use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Streaming TTFC and non-streaming response time are tracked separately;
/// blending them in one EWMA would distort routing for both.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LatencyMode {
    Streaming,
    NonStreaming,
}

impl LatencyMode {
    pub fn as_str(self) -> &'static str {
        match self {
            LatencyMode::Streaming => "streaming",
            LatencyMode::NonStreaming => "nonstreaming",
        }
    }
}

#[derive(Debug)]
pub struct CandidateStats {
    /// EWMA of streaming TTFC in milliseconds. u64::MAX = cold (no data).
    streaming_ewma_ms: AtomicU64,
    /// EWMA of non-streaming response time in milliseconds. u64::MAX = cold.
    nonstreaming_ewma_ms: AtomicU64,
    /// EWMA of request outcomes (success = 1.0, error = 0.0) as f64 bits.
    /// Starts optimistic at 1.0; recovers via probe traffic, not a clock.
    success_rate: AtomicU64,
}

impl Default for CandidateStats {
    fn default() -> Self {
        Self::new()
    }
}

impl CandidateStats {
    pub fn new() -> Self {
        Self {
            streaming_ewma_ms: AtomicU64::new(u64::MAX),
            nonstreaming_ewma_ms: AtomicU64::new(u64::MAX),
            success_rate: AtomicU64::new(1.0_f64.to_bits()),
        }
    }

    fn ewma_field(&self, mode: LatencyMode) -> &AtomicU64 {
        match mode {
            LatencyMode::Streaming => &self.streaming_ewma_ms,
            LatencyMode::NonStreaming => &self.nonstreaming_ewma_ms,
        }
    }

    pub fn ewma_ms(&self, mode: LatencyMode) -> u64 {
        self.ewma_field(mode).load(Ordering::Relaxed)
    }

    pub fn is_cold(&self, mode: LatencyMode) -> bool {
        self.ewma_ms(mode) == u64::MAX
    }

    pub fn success_rate(&self) -> f64 {
        f64::from_bits(self.success_rate.load(Ordering::Relaxed))
    }

    pub fn record_outcome(&self, success: bool, alpha: f64) {
        let sample = if success { 1.0 } else { 0.0 };
        let _ = self
            .success_rate
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some((alpha * sample + (1.0 - alpha) * f64::from_bits(current)).to_bits())
            });
    }

    pub fn update_ewma(&self, mode: LatencyMode, observed_ms: u64, alpha: f64) {
        let _ = self
            .ewma_field(mode)
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
                // EWMA of positive millis; rounds to a bounded non-negative u64.
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                Some(if old == u64::MAX {
                    observed_ms
                } else {
                    (alpha * observed_ms as f64 + (1.0 - alpha) * old as f64).round() as u64
                })
            });
    }
}

/// Per-candidate stats stored in a flat `Vec`, addressed by a stable index
/// minted at `register` time. Indices are safe because `register` only
/// appends and the tracker is frozen behind `&Tracker` once moved into
/// `AppState`. Cross-alias sharing is preserved by `ModelMap` deduping
/// (provider, model) pairs to the same index. One `alpha` smooths both the
/// latency and success-rate EWMAs, so the signals react on the same timescale.
#[derive(Debug)]
pub struct Tracker {
    stats: Vec<CandidateStats>,
    alpha: f64,
    error_threshold: f64,
}

impl Tracker {
    pub fn new(alpha: f64, error_threshold: f64) -> Self {
        Self {
            stats: Vec::new(),
            alpha,
            error_threshold,
        }
    }

    /// Allocates a new stats slot and returns its index. The index is stable
    /// for the lifetime of the tracker.
    pub fn register(&mut self) -> usize {
        let idx = self.stats.len();
        self.stats.push(CandidateStats::new());
        idx
    }

    pub fn stats(&self, index: usize) -> &CandidateStats {
        &self.stats[index]
    }

    pub fn record_success(&self, index: usize, mode: LatencyMode, observed: Duration) {
        // request latency in millis always fits u64.
        #[allow(clippy::cast_possible_truncation)]
        let ms = observed.as_millis() as u64;
        self.stats[index].update_ewma(mode, ms, self.alpha);
        self.stats[index].record_outcome(true, self.alpha);
    }

    pub fn record_error(&self, index: usize) {
        self.stats[index].record_outcome(false, self.alpha);
    }

    /// Error-rate EWMA above `error_threshold`. Not consulted by routing —
    /// it only breaks session-affinity pins and labels `/status`.
    pub fn is_degraded(&self, index: usize) -> bool {
        1.0 - self.stats[index].success_rate() > self.error_threshold
    }
}

#[cfg(test)]
mod tests;
